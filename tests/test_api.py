"""API skeleton tests (offline — no DB required)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import health as health_mod


def test_health_returns_200(monkeypatch) -> None:
    monkeypatch.setattr(health_mod, "_probe_db", lambda: "ok")
    client = TestClient(create_app())
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["service"] == "market-data-api"
    assert data["db"] == "ok"


def test_health_degraded_when_db_unreachable(monkeypatch) -> None:
    monkeypatch.setattr(health_mod, "_probe_db", lambda: "unreachable")
    client = TestClient(create_app())
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["db"] == "unreachable"


def test_analytics_max_pain_returns_501() -> None:
    client = TestClient(create_app())
    resp = client.get("/market/analytics/max-pain")
    assert resp.status_code == 501
    assert resp.json()["detail"] == "Not implemented"


def test_analytics_placeholders_return_501() -> None:
    client = TestClient(create_app())
    for path in (
        "/market/analytics/atm-iv",
        "/market/analytics/pcr",
        "/market/analytics/iv-percentile",
    ):
        resp = client.get(path)
        assert resp.status_code == 501, path
