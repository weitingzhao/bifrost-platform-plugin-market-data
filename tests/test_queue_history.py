"""Queue history: the series that survives the trim."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bifrost_market_data import queue_history as qh

AT = datetime(2026, 9, 9, 15, 0, tzinfo=timezone.utc)


class _Cur:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent
        self._rows: list[Any] = []
        self.rowcount = 0

    def execute(self, sql: str, params: Any = None) -> None:
        q = " ".join(sql.lower().split())
        self.parent.statements.append((q, params))
        if "filter (where status = 'pending')" in q:
            self._rows = self.parent.depth
        elif "created_delta" in q and "select kind" in q:
            self._rows = self.parent.deltas
        elif "from ops_jobs.queue_sample" in q:
            self._rows = self.parent.series
        else:
            self._rows = []
        self.rowcount = self.parent.rowcount

    def executemany(self, sql: str, rows: Any) -> None:
        self.parent.statements.append((" ".join(sql.lower().split()), None))
        self.parent.written.extend(list(rows))

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *a: object) -> None:
        return None


class _Conn:
    def __init__(self, **kw: Any) -> None:
        self.depth = kw.get("depth", [])
        self.deltas = kw.get("deltas", [])
        self.series = kw.get("series", [])
        self.rowcount = kw.get("rowcount", 0)
        self.statements: list[tuple[str, Any]] = []
        self.written: list[Any] = []
        self.commits = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None


def test_a_sample_carries_depth_and_what_moved() -> None:
    conn = _Conn(
        depth=[("option_daily", 2396642, 4, 82247.0)],
        deltas=[("option_daily", 120, 3300, 2, 0.263, 0.939)],
    )
    assert qh.take_sample(conn, now=AT) == 1
    (row,) = conn.written
    ts, kind, pending, running, created, done, failed, oldest, p50, p95 = row
    assert (ts, kind) == (AT, "option_daily")
    assert (pending, running) == (2396642, 4)
    assert (created, done, failed) == (120, 3300, 2)
    assert (oldest, p50, p95) == (82247.0, 0.263, 0.939)


def test_a_kind_that_only_finished_work_still_gets_a_row() -> None:
    """Nothing pending is a fact about the queue, not a reason to record nothing."""
    conn = _Conn(depth=[], deltas=[("calendar", 1, 1, 0, 0.4, 0.4)])
    assert qh.take_sample(conn, now=AT) == 1
    (row,) = conn.written
    assert row[1] == "calendar"
    assert (row[2], row[3]) == (0, 0)  # depth is zero, not unknown
    assert row[5] == 1


def test_a_failed_read_writes_no_sample() -> None:
    class _Boom(_Conn):
        def cursor(self) -> _Cur:
            raise RuntimeError("statement timeout")

    conn = _Boom()
    assert qh.take_sample(conn, now=AT) == 0
    assert conn.written == []


def test_the_backfill_leaves_depth_unset() -> None:
    """A past queue depth cannot be recovered from jobs that already finished."""
    conn = _Conn(rowcount=812)
    assert qh.backfill_from_jobs(conn) == 812
    sql = conn.statements[-1][0]
    assert "insert into ops_jobs.queue_sample" in sql
    cols = sql.split("insert into ops_jobs.queue_sample (")[1].split(")")[0]
    assert "pending" not in cols and "running" not in cols
    assert "on conflict (sample_ts, kind) do nothing" in sql


def test_the_series_sums_across_kinds_unless_one_is_named() -> None:
    conn = _Conn(series=[(AT, 2396642, 4, 120, 3300, 2, 82247.0, 0.9)])
    whole = qh.read_series(conn, hours=6)
    assert whole[0]["kind"] is None
    assert whole[0]["pending"] == 2396642
    assert "group by" in conn.statements[-1][0]

    conn2 = _Conn(series=[(AT, "option_daily", 2396642, 4, 120, 3300, 2, 82247.0, 0.26, 0.9)])
    one = qh.read_series(conn2, hours=6, kind="option_daily")
    assert one[0]["kind"] == "option_daily"
    assert one[0]["p50_sec"] == 0.26
    assert "group by" not in conn2.statements[-1][0]
