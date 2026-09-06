"""Unit tests for scheduler enqueue helpers."""

from __future__ import annotations

from typing import Any

from bifrost_market_data.scheduler.enqueue import insert_job, insert_jobs_bulk, payload_hash, trim_old_jobs


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
    def __init__(self, *, delete_rowcount: int = 0) -> None:
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
    conn = _EnqueueConn(delete_rowcount=3)
    n = trim_old_jobs(conn, keep_days=7, keep_max=5000)
    assert n == 6  # two DELETE statements × rowcount 3
    assert conn.committed == 1
    assert any("DELETE FROM ops_jobs.job_ingest" in s[0] for s in conn.statements)


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
