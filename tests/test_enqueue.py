"""Unit tests for scheduler enqueue helpers."""

from __future__ import annotations

from typing import Any

from bifrost_market_data.scheduler.enqueue import (
    insert_job,
    insert_jobs_bulk,
    payload_hash,
    trim_option_snapshots,
    trim_old_jobs,
)


class _EnqueueCursor:
    def __init__(self, parent: _EnqueueConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "unnest(" in q:
            rows = []
            for kind, ph in zip(params[0], params[2]):
                if (kind, ph) in self.parent.seen_keys:
                    continue
                self.parent.seen_keys.add((kind, ph))
                self.parent.next_id += 1
                rows.append((self.parent.next_id, kind, ph))
            self.parent._fetchall = rows
        elif "returning id" in q:
            # Simulate dedup: same (kind, hash) already inserted → None
            kind = params[0] if params else None
            ph = params[2] if params and len(params) > 2 else None
            key = (kind, ph)
            if key in self.parent.seen_keys:
                self.parent._fetchone = None
            else:
                self.parent.seen_keys.add(key)
                self.parent.next_id += 1
                self.parent._fetchone = (self.parent.next_id,)
        elif "delete from" in q:
            self.rowcount = self.parent.delete_rowcount
        elif ") capped" in q:
            # The cheap "are we over the backstop" count.
            self.parent._fetchone = (self.parent.finished_rows,)
        elif "order by finished_at desc" in q:
            # The row-cap cutoff: the finished_at of the keep_max-th newest job.
            self.parent._fetchone = self.parent.cutoff
        else:
            self.parent._fetchone = None

    def fetchone(self) -> Any:
        return self.parent._fetchone

    def fetchall(self) -> list[Any]:
        return list(getattr(self.parent, "_fetchall", []))

    def __enter__(self) -> _EnqueueCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _EnqueueConn:
    def __init__(
        self,
        *,
        delete_rowcount: int = 0,
        cutoff: Any = ("2026-09-02",),
        finished_rows: int = 0,
    ) -> None:
        self.cutoff = cutoff
        # How many finished rows the cheap cap check should report.
        self.finished_rows = finished_rows
        self.statements: list[tuple[str, Any]] = []
        self.committed = 0
        self.rolled_back = 0
        self.seen_keys: set[tuple[Any, Any]] = set()
        self.next_id = 0
        self._fetchone: Any = None
        self.delete_rowcount = delete_rowcount

    def cursor(self) -> _EnqueueCursor:
        return _EnqueueCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1


def test_payload_hash_deterministic() -> None:
    a = payload_hash({"symbol": "AAPL", "from": "2024-01-01"})
    b = payload_hash({"from": "2024-01-01", "symbol": "AAPL"})
    assert a == b
    assert len(a) == 16
    assert a != payload_hash({"symbol": "MSFT", "from": "2024-01-01"})


def test_payload_hash_empty() -> None:
    assert payload_hash({}) == payload_hash(None)
    assert len(payload_hash({})) == 16


def test_insert_job_returns_id() -> None:
    conn = _EnqueueConn()
    job_id = insert_job(conn, kind="stock_daily", payload={"symbol": "AAPL"}, priority=5)
    assert job_id == 1
    assert conn.committed == 1
    sql = conn.statements[0][0]
    assert "INSERT INTO ops_jobs.job_ingest" in sql
    assert "ON CONFLICT (kind, payload_hash)" in sql
    assert "DO NOTHING" in sql
    assert "RETURNING id" in sql


def test_insert_job_dedup_returns_none() -> None:
    conn = _EnqueueConn()
    payload = {"symbol": "AAPL", "from": "2024-06-20", "to": "2024-06-20"}
    first = insert_job(conn, kind="stock_daily", payload=payload)
    second = insert_job(conn, kind="stock_daily", payload=payload)
    assert first == 1
    assert second is None


def test_trim_old_jobs() -> None:
    """One age pass and one cap pass, each stopping on a short batch."""
    conn = _EnqueueConn(delete_rowcount=3, finished_rows=5001)
    n = trim_old_jobs(conn, keep_hours=48, keep_max=5000, batch_size=20)
    assert n == 6  # a short batch ends each pass: 3 by age + 3 by the row cap
    deletes = [st for st in conn.statements if "DELETE FROM ops_jobs.job_ingest" in st[0]]
    assert len(deletes) == 2
    # Every batch commits, so a run that runs out of budget keeps its progress.
    assert conn.committed >= 3


def test_trim_keeps_deleting_until_a_batch_comes_up_short() -> None:
    """A backlog must not need more than one statement's worth of budget.

    The single-statement form had to delete 1.23M rows at once, which the API's
    60-second budget cancelled, so trim stopped completing at all and 1.26M
    finished rows stayed on the table.
    """
    conn = _EnqueueConn(delete_rowcount=20, finished_rows=5001)
    calls = {"n": 0}
    original = conn.cursor

    def counting_cursor() -> Any:
        cur = original()
        inner = cur.execute

        def execute(query: str, params: Any = None) -> None:
            if "DELETE FROM ops_jobs.job_ingest" in query:
                calls["n"] += 1
                # The fourth delete comes up short and ends the pass.
                conn.delete_rowcount = 20 if calls["n"] < 4 else 7
            inner(query, params)

        cur.execute = execute  # type: ignore[method-assign]
        return cur

    conn.cursor = counting_cursor  # type: ignore[method-assign]
    n = trim_old_jobs(conn, keep_hours=48, keep_max=5000, batch_size=20)
    # Three full batches, then a short one ends the age pass; the cap pass then
    # runs its own. The point is that one statement is not the limit.
    assert calls["n"] >= 4, "trim gave up after one statement"
    assert n == 20 + 20 + 20 + 7 + 7


def test_retention_is_a_window_not_a_row_count() -> None:
    """How far back the queue can be questioned must not depend on how busy it was.

    40,000 finished rows was seven days at a normal day's volume and fifteen
    minutes at 2,700 jobs a minute, so the doctor's "failed in 24h" was reading
    a quarter of an hour.
    """
    conn = _EnqueueConn(delete_rowcount=0)
    trim_old_jobs(conn, keep_hours=48, keep_max=5_000_000, batch_size=20)
    age = [st for st in conn.statements if "finished_at < now()" in st[0]]
    assert age, "no age pass ran"
    assert "make_interval(secs =>" in age[0][0]
    assert age[0][1][0] == 48 * 3600.0


def test_trim_carries_its_own_statement_budget() -> None:
    """The caller's connection is not the budget.

    The scheduler CLI opens with the `bifrost` role's 2s default, under which a
    20,000-row delete competing with the workers' writes is cancelled every
    time — which is exactly how the first attempt at this fix failed on DEV.
    """
    conn = _EnqueueConn(delete_rowcount=1)
    trim_old_jobs(conn, keep_hours=48, keep_max=100)
    budgets = [st[0] for st in conn.statements if "SET LOCAL statement_timeout" in st[0]]
    assert len(budgets) >= 2, "the cutoff and every delete batch must set their own budget"


def test_trim_row_cap_orders_the_way_the_index_does() -> None:
    """`finished_at DESC NULLS LAST, id DESC` matched no index and seq-scanned."""
    conn = _EnqueueConn(delete_rowcount=0, finished_rows=40_001)
    trim_old_jobs(conn, keep_hours=48, keep_max=40000)
    cutoff = [st[0] for st in conn.statements if "OFFSET" in st[0]]
    assert cutoff, "no cutoff query ran"
    assert "NULLS LAST" not in cutoff[0]
    assert "id DESC" not in cutoff[0]


def test_insert_jobs_bulk_one_statement_one_commit() -> None:
    conn = _EnqueueConn()
    ids = insert_jobs_bulk(
        conn,
        [
            ("stock_daily", {"symbol": "AAPL"}, 5, 3),
            ("stock_daily", {"symbol": "MSFT"}, 5, 3),
            ("stock_daily", {"symbol": "AAPL"}, 5, 3),  # in-batch duplicate
        ],
    )
    assert ids == [1, 2, None]
    inserts = [st for st in conn.statements if "insert into" in st[0].lower()]
    assert len(inserts) == 1
    assert conn.committed == 1
    # A second batch dedups against what is already pending.
    again = insert_jobs_bulk(conn, [("stock_daily", {"symbol": "MSFT"}, 5, 3), ("calendar", {}, 1, 3)])
    assert again == [None, 3]
    assert insert_jobs_bulk(conn, []) == []


# ── Intraday snapshot retention ──


def test_intraday_snapshot_trim_batches_on_the_primary_key_not_ctid() -> None:
    """option_snapshot is partitioned, so a ctid names a row only within one partition.

    462,252 ctid values are shared by more than one row in this table; batching
    on them would delete rows nobody selected. job_ingest is an ordinary table,
    which is why its trim can use ctid.
    """
    conn = _EnqueueConn(delete_rowcount=1)
    trim_option_snapshots(conn, keep_days=30, intraday_only=True, batch_size=5000)
    deletes = [st[0] for st in conn.statements if "DELETE FROM raw_market.option_snapshot" in st[0]]
    assert deletes, "no delete ran"
    assert "ctid" not in deletes[0]
    assert "(t.option_ticker, t.snapshot_ts) IN (" in deletes[0]


def test_intraday_snapshot_trim_is_bounded_and_carries_its_budget() -> None:
    """One unbounded statement took 22s over eighteen partitions and deleted nothing."""
    conn = _EnqueueConn(delete_rowcount=1)
    n = trim_option_snapshots(conn, keep_days=30, intraday_only=True, batch_size=5000)
    assert n == 1  # a short batch ends the pass
    assert any("LIMIT %s" in st[0] for st in conn.statements)
    assert any("SET LOCAL statement_timeout" in st[0] for st in conn.statements)
    assert conn.committed >= 1


def test_intraday_snapshot_trim_keeps_the_eod_anchor() -> None:
    """16:00 New York is the observation; everything else is the intraday shape."""
    conn = _EnqueueConn(delete_rowcount=0)
    trim_option_snapshots(conn, keep_days=30, intraday_only=True)
    sql = [st[0] for st in conn.statements if "option_snapshot" in st[0]][0]
    assert "America/New_York" in sql
    assert "<> time '16:00'" in sql


def test_snapshot_retention_does_not_depend_on_dropping_a_partition() -> None:
    """All eighteen partitions are owned by postgres; the plugin's role has DML only.

    Dropping the month is cheaper, so it is still attempted — but retention has
    to happen without it, or it never happens at all.
    """
    conn = _EnqueueConn(delete_rowcount=1)
    trim_option_snapshots(conn, keep_days=90)
    sql = [st[0] for st in conn.statements if "option_snapshot" in st[0]][0]
    assert "DROP TABLE" not in sql
    assert "16:00" not in sql  # the long window takes the EOD rows too


def test_the_row_cap_costs_nothing_when_it_is_not_engaged() -> None:
    """Asking for the 5,000,000th newest row walks the whole index.

    After a mass delete its dead entries make that a heap fetch per row —
    measured at over 30 seconds against 144k live rows, which cancelled the
    whole trim. The backstop must not charge for itself when it is not needed.
    """
    conn = _EnqueueConn(delete_rowcount=0, cutoff=("2026-09-02",))
    trim_old_jobs(conn, keep_hours=48, keep_max=5_000_000)
    counted = [st for st in conn.statements if "LIMIT %s\n            ) capped" in st[0]]
    assert counted, "the cheap count did not run"
    offsets = [st for st in conn.statements if "OFFSET" in st[0]]
    assert offsets == [], "the cutoff ran even though the cap was not engaged"
