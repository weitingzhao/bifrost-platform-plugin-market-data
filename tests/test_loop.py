"""Unit tests for worker loop dispatch and shutdown."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from bifrost_market_data.worker.claim import JobRow
from bifrost_market_data.worker.loop import (
    POOL_KINDS,
    kinds_for_pool,
    process_one_job,
    run_loop,
)


def _job(
    *,
    job_id: int = 1,
    kind: str = "stock_daily",
    attempts: int = 1,
    max_attempts: int = 3,
) -> JobRow:
    return JobRow(
        id=job_id,
        kind=kind,
        payload={"symbol": "AAPL"},
        payload_hash=None,
        priority=0,
        status="running",
        result=None,
        attempts=attempts,
        max_attempts=max_attempts,
        created_at=None,
        updated_at=None,
        started_at=None,
        finished_at=None,
    )


def test_kinds_for_pool() -> None:
    assert "stock_daily" in kinds_for_pool("stocks")
    assert "ticker_related" in kinds_for_pool("stocks")
    assert "option_snapshot" in kinds_for_pool("options")
    with pytest.raises(ValueError):
        kinds_for_pool("unknown")
    assert set(POOL_KINDS["stocks"]).isdisjoint(POOL_KINDS["options"])


@pytest.mark.asyncio
async def test_process_one_job_marks_done(monkeypatch: pytest.MonkeyPatch) -> None:
    done: list[tuple[Any, ...]] = []
    failed: list[tuple[Any, ...]] = []

    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_done",
        lambda conn, job_id, result=None: done.append((job_id, result)),
    )
    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_failed",
        lambda *a, **k: failed.append((a, k)),
    )

    async def ok_handler(job: JobRow) -> dict[str, Any]:
        return {"ok": True, "id": job.id}

    await process_one_job(MagicMock(), _job(), handlers={"stock_daily": ok_handler})
    assert done == [(1, {"ok": True, "id": 1})]
    assert failed == []


@pytest.mark.asyncio
async def test_process_one_job_marks_failed_on_handler_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed: list[dict[str, Any]] = []

    def _fail(conn: Any, job_id: int, error_msg: str, *, attempts: int, max_attempts: int) -> str:
        failed.append(
            {
                "job_id": job_id,
                "error": error_msg,
                "attempts": attempts,
                "max_attempts": max_attempts,
            }
        )
        return "pending"

    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_failed", _fail)
    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_done", lambda *a, **k: None)

    async def bad_handler(_job: JobRow) -> dict[str, Any]:
        raise RuntimeError("handler boom")

    await process_one_job(MagicMock(), _job(), handlers={"stock_daily": bad_handler})
    assert len(failed) == 1
    assert failed[0]["error"] == "handler boom"
    assert failed[0]["attempts"] == 1


@pytest.mark.asyncio
async def test_process_one_job_unknown_kind(monkeypatch: pytest.MonkeyPatch) -> None:
    failed: list[str] = []

    def _fail(conn: Any, job_id: int, error_msg: str, *, attempts: int, max_attempts: int) -> str:
        failed.append(error_msg)
        return "failed"

    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_failed", _fail)
    await process_one_job(MagicMock(), _job(kind="unknown_kind"), handlers={})
    assert failed and "no handler" in failed[0]


@pytest.mark.asyncio
async def test_run_loop_claim_dispatch_and_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    jobs = [_job(job_id=11), None]
    claims: list[Any] = []
    done_ids: list[int] = []

    def claim_fn(conn: Any, kinds: list[str]) -> JobRow | None:
        claims.append(list(kinds))
        return jobs.pop(0) if jobs else None

    class _Conn:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_done",
        lambda conn, job_id, result=None: done_ids.append(job_id),
    )
    monkeypatch.setattr(
        "bifrost_market_data.worker.loop.mark_failed",
        lambda *a, **k: None,
    )

    async def ok_handler(job: JobRow) -> dict[str, Any]:
        return {"status": "ok", "job_id": job.id}

    stop = asyncio.Event()

    async def _stop_soon() -> None:
        await asyncio.sleep(0.15)
        stop.set()

    task = asyncio.create_task(_stop_soon())
    await run_loop(
        pool="stocks",
        cfg={"worker": {"poll_interval_sec": 0.05, "concurrency": 1}},
        shutdown_event=stop,
        handlers={"stock_daily": ok_handler},
        health_port=0,  # ephemeral
        connect=_Conn,
        claim_fn=claim_fn,
        reclaim_fn=None,
        poll_interval_sec=0.05,
        concurrency=1,
    )
    await task
    assert claims
    assert "stock_daily" in claims[0]
    assert 11 in done_ids


@pytest.mark.asyncio
async def test_run_loop_sleeps_when_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    claim_count = {"n": 0}

    def claim_fn(conn: Any, kinds: list[str]) -> JobRow | None:
        claim_count["n"] += 1
        return None

    class _Conn:
        def close(self) -> None:
            return None

    stop = asyncio.Event()

    async def _stop_soon() -> None:
        await asyncio.sleep(0.12)
        stop.set()

    stopper = asyncio.create_task(_stop_soon())
    await run_loop(
        pool="options",
        cfg={},
        shutdown_event=stop,
        handlers={},
        health_port=0,
        connect=_Conn,
        claim_fn=claim_fn,
        reclaim_fn=None,
        poll_interval_sec=0.05,
        concurrency=1,
    )
    await stopper
    assert claim_count["n"] >= 2


@pytest.mark.asyncio
async def test_process_one_job_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    failed: list[str] = []

    def _fail(conn: Any, job_id: int, error_msg: str, *, attempts: int, max_attempts: int) -> str:
        failed.append(error_msg)
        return "pending"

    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_failed", _fail)
    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_done", lambda *a, **k: None)

    async def hang(_job: JobRow) -> dict[str, Any]:
        await asyncio.sleep(1)
        return {"ok": True}

    await process_one_job(
        MagicMock(),
        _job(),
        handlers={"stock_daily": hang},
        timeout_sec=0.05,
    )
    assert failed and "job_timeout" in failed[0]
