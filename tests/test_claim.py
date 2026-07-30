"""Unit tests for claim_job / mark_done / mark_failed."""

from __future__ import annotations

import json
from typing import Any

import pytest

from bifrost_market_data.worker.claim import (
    JobRow,
    claim_job,
    job_row_from_record,
    mark_done,
    mark_failed,
)


class _FakeCursor:
    def __init__(self, fetch_results: list[Any]) -> None:
        self.fetch_results = list(fetch_results)
        self.statements: list[tuple[str, Any]] = []

    def execute(self, query: str, params: Any = None) -> None:
        self.statements.append((query, params))

    def fetchone(self) -> Any:
        if not self.fetch_results:
            return None
        return self.fetch_results.pop(0)

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, fetch_results: list[Any] | None = None) -> None:
        self.cur = _FakeCursor(fetch_results or [])
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def _sample_row(
    *,
    job_id: int = 1,
    kind: str = "stock_daily",
    status: str = "running",
    attempts: int = 1,
    max_attempts: int = 3,
) -> tuple[Any, ...]:
    return (
        job_id,
        kind,
        {"symbol": "AAPL"},
        "hash1",
        0,
        status,
        None,
        attempts,
        max_attempts,
        None,
        None,
        None,
        None,
    )


def test_job_row_from_record() -> None:
    row = job_row_from_record(_sample_row())
    assert isinstance(row, JobRow)
    assert row.id == 1
    assert row.kind == "stock_daily"
    assert row.payload["symbol"] == "AAPL"
    assert row.attempts == 1


def test_claim_job_returns_none_when_empty() -> None:
    conn = _FakeConn(fetch_results=[None])
    assert claim_job(conn, ["stock_daily"]) is None
    assert conn.committed
    assert "FOR UPDATE SKIP LOCKED" in conn.cur.statements[0][0]
    assert conn.cur.statements[0][1] == (["stock_daily"],)


def test_claim_job_selects_and_updates() -> None:
    pending = _sample_row(status="pending", attempts=0)
    running = _sample_row(status="running", attempts=1)
    conn = _FakeConn(fetch_results=[pending, running])
    job = claim_job(conn, ["stock_daily", "ticker_sync"])
    assert job is not None
    assert job.status == "running"
    assert job.attempts == 1
    assert conn.committed
    assert len(conn.cur.statements) == 2
    assert "UPDATE" in conn.cur.statements[1][0]
    assert conn.cur.statements[0][1] == (["stock_daily", "ticker_sync"],)


def test_claim_job_empty_kinds() -> None:
    conn = _FakeConn()
    assert claim_job(conn, []) is None
    assert not conn.cur.statements


def test_mark_done() -> None:
    conn = _FakeConn()
    mark_done(conn, 7, {"rows": 3})
    assert conn.committed
    sql, params = conn.cur.statements[0]
    assert "status = 'done'" in sql
    assert params[1] == 7
    assert json.loads(params[0])["rows"] == 3


def test_mark_failed_requeues_when_retries_remain() -> None:
    conn = _FakeConn()
    status = mark_failed(conn, 9, "boom", attempts=1, max_attempts=3)
    assert status == "pending"
    sql, params = conn.cur.statements[0]
    assert params[0] == "pending"
    assert "finished_at = NULL" in sql
    assert json.loads(params[1])["error"] == "boom"


def test_mark_failed_permanent_when_attempts_exhausted() -> None:
    conn = _FakeConn()
    status = mark_failed(conn, 9, "boom", attempts=3, max_attempts=3)
    assert status == "failed"
    sql, params = conn.cur.statements[0]
    assert params[0] == "failed"
    assert "finished_at = now()" in sql


def test_claim_rolls_back_on_error() -> None:
    conn = _FakeConn(fetch_results=[_sample_row()])

    def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("db down")

    conn.cur.execute = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        claim_job(conn, ["stock_daily"])
    assert conn.rolled_back
