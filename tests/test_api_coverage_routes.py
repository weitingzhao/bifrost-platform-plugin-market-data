"""Offline tests for Wave 5-B DB-read coverage / reference / corp / status routes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app


class _DummyConn:
    def close(self) -> None:
        return None


def test_coverage_quality_score(monkeypatch) -> None:
    from bifrost_market_data.api import coverage as mod

    sample = {
        "ok": True,
        "summary": "PASS",
        "checks": [
            {"check": "stock_daily_coverage", "ok": True, "detail": "symbols=4500"},
            {"check": "option_snapshot_coverage", "ok": True, "detail": "missing=0"},
            {"check": "option_oi_coverage", "ok": True, "detail": "gaps=0"},
            {"check": "freshness", "ok": True, "detail": "ok"},
        ],
    }
    monkeypatch.setattr(mod, "run_all_checks", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/coverage/quality-score")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["summary"] == "PASS"
    assert len(data["checks"]) == 4
    assert data["checks"][0]["check"] == "stock_daily_coverage"


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


def test_coverage_inventory(monkeypatch) -> None:
    from bifrost_market_data.api import coverage as mod

    sample = {
        "ok": True,
        "scope": "watchlist",
        "watchlist_symbols": ["AAPL", "NVDA"],
        "stock_daily": {
            "symbols": 2,
            "total_rows": 1000,
            "min_date": "2021-01-04",
            "max_date": "2026-08-07",
        },
        "stock_min": None,
        "option": {
            "underlyings": 2,
            "total_contracts": 500,
            "total_expiries": 20,
            "snapshot_symbols": 2,
            "snapshot_latest": "2026-08-07",
            "oi_symbols": 2,
            "oi_latest": "2026-08-07",
        },
        "analytics": {
            "max_pain": {"symbols": 2, "days": 10, "latest": "2026-08-07"},
            "atm_iv": {"symbols": 2, "days": 10, "latest": "2026-08-07"},
            "pcr": {"symbols": 2, "days": 3, "latest": "2026-08-07"},
            "iv_percentile": {"symbols": 2, "days": 3, "latest": "2026-08-07"},
        },
        "generated_at": "2026-08-07T20:30:00Z",
    }
    monkeypatch.setattr(mod, "query_inventory", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    resp = client.get("/market/coverage/inventory")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["scope"] == "watchlist"
    assert data["stock_min"] is None
    assert data["stock_daily"]["symbols"] == 2
    assert data["option"]["underlyings"] == 2
    assert data["analytics"]["max_pain"]["symbols"] == 2
    assert len(data["watchlist_symbols"]) == 2


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
