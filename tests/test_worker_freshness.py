"""Integration: process_one_job updates ingest_freshness after mark_done."""

from __future__ import annotations

from typing import Any

import pytest

from bifrost_market_data.worker.claim import JobRow
from bifrost_market_data.worker.health import HealthState
from bifrost_market_data.worker.loop import process_one_job


def _job(*, kind: str = "stock_daily_grouped") -> JobRow:
    return JobRow(
        id=42,
        kind=kind,
        payload={"from": "2024-06-20", "to": "2024-06-20"},
        payload_hash=None,
        priority=0,
        status="running",
        result=None,
        attempts=1,
        max_attempts=3,
        created_at=None,
        updated_at=None,
        started_at=None,
        finished_at=None,
    )


class _FreshCur:
    def __init__(self, parent: "_FreshConn") -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        if self.parent.fail_freshness and "ingest_freshness" in query.lower():
            raise RuntimeError("freshness boom")

    def __enter__(self) -> _FreshCur:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FreshConn:
    def __init__(self, *, fail_freshness: bool = False) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.commits = 0
        self.fail_freshness = fail_freshness

    def cursor(self) -> _FreshCur:
        return _FreshCur(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None


@pytest.mark.asyncio
async def test_process_one_job_calls_freshness(monkeypatch: pytest.MonkeyPatch) -> None:
    done: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_done",
        lambda conn, job_id, result=None: done.append((job_id, result)),
    )
    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_failed",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not fail")),
    )

    async def ok_handler(job: JobRow) -> dict[str, Any]:
        return {"rows_written": 5, "date": "2024-06-20"}

    conn = _FreshConn()
    health = HealthState(pool="stocks")
    await process_one_job(
        conn,
        _job(),
        handlers={"stock_daily_grouped": ok_handler},
        health=health,
    )

    assert done == [(42, {"rows_written": 5, "date": "2024-06-20"})]
    assert health.jobs_done == 1
    assert health.jobs_failed == 0
    freshness_stmts = [s for s in conn.statements if "ingest_freshness" in s[0].lower()]
    assert len(freshness_stmts) == 1
    assert freshness_stmts[0][1] == ("stock_daily", 5, "ok")  # aliased dimension


@pytest.mark.asyncio
async def test_process_one_job_freshness_failure_does_not_fail_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    done: list[int] = []
    failed: list[Any] = []
    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_done",
        lambda conn, job_id, result=None: done.append(job_id),
    )
    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_failed",
        lambda *a, **k: failed.append((a, k)),
    )

    async def ok_handler(_job: JobRow) -> dict[str, Any]:
        return {"rows_written": 3}

    conn = _FreshConn(fail_freshness=True)
    health = HealthState(pool="stocks")
    await process_one_job(
        conn,
        _job(kind="stock_daily"),
        handlers={"stock_daily": ok_handler},
        health=health,
    )

    assert done == [42]
    assert failed == []
    assert health.jobs_done == 1
    assert health.jobs_failed == 0
