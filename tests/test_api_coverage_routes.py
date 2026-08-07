"""Offline tests for Wave 5-B DB-read coverage / reference / corp / status routes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app


class _DummyConn:
    def close(self) -> None:
        return None


def test_coverage_db_summary(monkeypatch) -> None:
    from bifrost_market_data.api import coverage as mod

    sample = {
        "ok": True,
        "source": "db",
        "counts": {"tickers": 10, "stock_daily": 1000},
        "freshness": [],
        "generated_at": "2026-08-06T12:00:00Z",
    }
    monkeypatch.setattr(mod, "query_db_summary", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/coverage/db-summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["counts"]["tickers"] == 10


def test_coverage_watchlist(monkeypatch) -> None:
    from bifrost_market_data.api import coverage as mod

    sample = {
        "ok": True,
        "source": "public.watchlist",
        "symbols_count": 2,
        "symbols": [{"symbol": "AAPL"}, {"symbol": "NVDA"}],
    }
    monkeypatch.setattr(mod, "query_watchlist_coverage", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/coverage/watchlist")
    assert resp.status_code == 200
    assert resp.json()["symbols_count"] == 2


def test_corporate_actions(monkeypatch) -> None:
    from bifrost_market_data.api import corp_actions as mod

    rows = [
        {
            "id": 1,
            "symbol": "AAPL",
            "action_type": "dividend",
            "ex_date": "2026-05-10",
            "amount": 0.25,
        }
    ]
    monkeypatch.setattr(mod, "query_corporate_actions", lambda *_a, **_k: rows)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/corporate-actions", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["count"] == 1
    assert data["rows"][0]["action_type"] == "dividend"


def test_market_status(monkeypatch) -> None:
    from bifrost_market_data.api import status_ext as mod

    sample = {
        "ok": True,
        "service": "market-data-api",
        "db": "ok",
        "polygon_configured": True,
        "freshness_summary": [{"dimension": "stock-eod", "status": "ok"}],
    }
    monkeypatch.setattr(mod, "query_status_summary", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "connect_db", lambda **_k: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["db"] == "ok"
    assert data["polygon_configured"] is True


def test_reference_ticker_search(monkeypatch) -> None:
    from bifrost_market_data.api import reference_db as mod

    results = [{"symbol": "AAPL", "name": "Apple Inc.", "active": True}]
    monkeypatch.setattr(mod, "query_ticker_search", lambda *_a, **_k: results)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/reference/tickers/search", params={"q": "AAP", "limit": 5})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["results"][0]["symbol"] == "AAPL"


def test_reference_overview_coverage(monkeypatch) -> None:
    from bifrost_market_data.api import reference_db as mod

    sample = {"ok": True, "total": 100, "filled": 80, "missing": 20, "source": "market.ticker"}
    monkeypatch.setattr(mod, "query_overview_coverage", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/reference/tickers/overview-coverage")
    assert resp.status_code == 200
    assert resp.json()["missing"] == 20


def test_daily_checklist(monkeypatch) -> None:
    from bifrost_market_data.api import corp_actions as mod

    sample = {
        "ok": True,
        "trade_date": "2026-08-06",
        "symbols": {"AAPL": {"symbol": "AAPL", "stock_daily_rows": 1}},
        "freshness": {},
    }
    monkeypatch.setattr(mod, "query_daily_checklist", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/daily-checklist", params={"symbols": "AAPL", "trade_date": "2026-08-06"})
    assert resp.status_code == 200
    assert resp.json()["symbols"]["AAPL"]["stock_daily_rows"] == 1
