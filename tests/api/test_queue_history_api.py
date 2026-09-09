"""The queue-history route and its one-per-process sampler."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api import queue_history as mod
from bifrost_market_data.api.app import create_app


def test_queue_history_route_serves_the_recorded_series(monkeypatch: Any) -> None:
    points = [{"sample_ts": "2026-09-09T15:00:00+00:00", "pending": 2396642, "done_delta": 3300}]
    monkeypatch.setattr(mod, "require_db", lambda: _Conn())
    monkeypatch.setattr(mod, "read_series", lambda conn, **kw: points)
    body = TestClient(create_app()).get("/market/ingest/queue-history?hours=6").json()
    assert body["ok"] is True
    assert body["data"]["points"] == points
    assert body["data"]["interval_sec"] == mod.SAMPLE_INTERVAL_SEC


def test_the_sampler_starts_once_per_process(monkeypatch: Any) -> None:
    started: list[str] = []

    class _Thread:
        def __init__(self, target: Any, name: str = "", daemon: bool = False) -> None:
            started.append(name)

        def start(self) -> None:
            return None

    monkeypatch.setattr(mod.threading, "Thread", _Thread)
    monkeypatch.setattr(mod, "_sampler_started", False)
    assert mod.start_sampler() is True
    # A second call is a no-op: eleven workers and one API must not each sample.
    assert mod.start_sampler() is False
    assert started == ["queue-sampler"]


class _Conn:
    def close(self) -> None:
        return None
