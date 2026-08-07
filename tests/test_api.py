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


def test_analytics_max_pain_reads_table(monkeypatch) -> None:
    """D9=A: GET /market/analytics/max-pain reads persisted rows (not permanent 501)."""
    from bifrost_market_data.api import analytics as analytics_mod

    sample = [
        {
            "symbol": "AAPL",
            "trade_date": "2024-06-20",
            "expiry": "2025-06-20",
            "max_pain_strike": 100.0,
            "total_oi": 32,
            "total_pain_at_strike": 12345.0,
            "computed_at": "2024-06-20T22:45:00+00:00",
        }
    ]
    monkeypatch.setattr(
        analytics_mod,
        "query_max_pain",
        lambda *_a, **_k: sample,
    )
    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/analytics/max-pain", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["rows"][0]["max_pain_strike"] == 100.0
    assert data["symbol"] == "AAPL"


def test_analytics_max_pain_404_when_empty(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    monkeypatch.setattr(analytics_mod, "query_max_pain", lambda *_a, **_k: [])
    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/analytics/max-pain", params={"symbol": "ZZZZ"})
    assert resp.status_code == 404


class _DummyConn:
    def close(self) -> None:
        return None


def test_analytics_atm_iv_reads_table(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    sample = [
        {
            "symbol": "AAPL",
            "trade_date": "2024-06-20",
            "expiry": "2025-06-20",
            "atm_strike": 100.0,
            "atm_iv": 0.26,
            "underlying_price": 100.0,
            "iv_source": "snapshot",
            "computed_at": "2024-06-20T23:00:00+00:00",
        }
    ]
    monkeypatch.setattr(analytics_mod, "query_atm_iv", lambda *_a, **_k: sample)
    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/analytics/atm-iv", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["atm_iv"] == 0.26


def test_analytics_pcr_reads_table(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    sample = [
        {
            "symbol": "AAPL",
            "trade_date": "2024-06-20",
            "pcr_oi": 2.0,
            "pcr_volume": 1.5,
            "total_put_oi": 200,
            "total_call_oi": 100,
            "total_put_volume": 80,
            "total_call_volume": 40,
            "computed_at": "2024-06-20T23:00:00+00:00",
        }
    ]
    monkeypatch.setattr(analytics_mod, "query_pcr", lambda *_a, **_k: sample)
    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/analytics/pcr", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["pcr_oi"] == 2.0


def test_analytics_iv_percentile_reads_table(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    sample = [
        {
            "symbol": "AAPL",
            "trade_date": "2024-06-20",
            "iv_current": 0.30,
            "iv_percentile_1y": 60.0,
            "iv_rank_1y": 50.0,
            "lookback_days": 5,
            "computed_at": "2024-06-20T23:15:00+00:00",
        }
    ]
    monkeypatch.setattr(analytics_mod, "query_iv_percentile", lambda *_a, **_k: sample)
    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/analytics/iv-percentile", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["iv_percentile_1y"] == 60.0


def test_analytics_atm_iv_404_when_empty(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    monkeypatch.setattr(analytics_mod, "query_atm_iv", lambda *_a, **_k: [])
    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/analytics/atm-iv", params={"symbol": "ZZZZ"})
    assert resp.status_code == 404
