"""GET /market/ingest/queue-summary rollup."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import ingest as ingest_mod


class _Conn:
    def __init__(self, rows: list[tuple[str, str, int]]) -> None:
        self._rows = rows

    def cursor(self):
        return self

    def execute(self, *a, **k):
        return None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None


def test_queue_summary_groups_pending_running(monkeypatch) -> None:
    conn = _Conn(
        [
            ("stock_daily_grouped", "pending", 12),
            ("stock_daily_grouped", "running", 1),
            ("option_daily", "pending", 40),
        ]
    )
    monkeypatch.setattr(ingest_mod, "require_db", lambda: conn)
    client = TestClient(create_app())
    resp = client.get("/market/ingest/queue-summary")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["pending"] == 52
    assert body["running"] == 1
    assert body["active"] == 53
    by_kind = {k["kind"]: k for k in body["kinds"]}
    assert by_kind["stock_daily_grouped"]["pending"] == 12
    assert by_kind["stock_daily_grouped"]["running"] == 1
    assert by_kind["option_daily"]["pending"] == 40
