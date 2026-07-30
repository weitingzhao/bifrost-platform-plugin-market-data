"""Health HTTP endpoint tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from bifrost_market_data.worker.health import HealthState, start_health_server


@pytest.mark.asyncio
async def test_health_endpoint_returns_snapshot() -> None:
    state = HealthState(pool="stocks")
    state.record_claim(datetime(2026, 7, 29, 20, 0, 0, tzinfo=timezone.utc))
    state.record_done()
    state.record_done()
    state.record_failed()

    server = await start_health_server(state, host="127.0.0.1", port=0)
    try:
        sock = server.sockets[0]
        port = sock.getsockname()[1]
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{port}/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["pool"] == "stocks"
        assert data["jobs_done"] == 2
        assert data["jobs_failed"] == 1
        assert data["last_claim_at"] is not None
        assert "2026-07-29" in data["last_claim_at"]
        assert data["uptime_sec"] >= 0
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_health_404_for_other_paths() -> None:
    state = HealthState(pool="options")
    server = await start_health_server(state, host="127.0.0.1", port=0)
    try:
        port = server.sockets[0].getsockname()[1]
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"http://127.0.0.1:{port}/ready")
        assert resp.status_code == 404
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_health_state_snapshot_defaults() -> None:
    state = HealthState(pool="stocks")
    snap = state.snapshot()
    assert snap["last_claim_at"] is None
    assert snap["jobs_done"] == 0
    await asyncio.sleep(0)  # keep async marker consistent
