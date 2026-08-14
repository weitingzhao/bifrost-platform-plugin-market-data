"""Unit tests for stocks_db (DB-read daily bars) and reference_db ticker endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import stocks_db as stocks_db_mod
from bifrost_market_data.api import reference_db as reference_db_mod


class _DummyConn:
    def close(self) -> None:
        return None


def _patch_db(monkeypatch):
    """Patch require_db in both stocks_db and reference_db modules."""
    monkeypatch.setattr(stocks_db_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(reference_db_mod, "require_db", lambda: _DummyConn())


# ---------------------------------------------------------------------------
# stocks/db/bars/daily
# ---------------------------------------------------------------------------


class TestStocksDbBarsDaily:
    def test_returns_grouped_bars(self, monkeypatch) -> None:
        sample = {
            "AAPL": [
                {"symbol": "AAPL", "bar_time": "2026-01-02", "open": 150.0, "high": 155.0, "low": 149.0, "close": 154.0, "volume": 1000000, "source": "massive"},
                {"symbol": "AAPL", "bar_time": "2026-01-03", "open": 154.0, "high": 156.0, "low": 153.0, "close": 155.5, "volume": 900000, "source": "massive"},
            ],
            "NVDA": [
                {"symbol": "NVDA", "bar_time": "2026-01-02", "open": 800.0, "high": 810.0, "low": 795.0, "close": 805.0, "volume": 2000000, "source": "massive"},
            ],
        }
        monkeypatch.setattr(stocks_db_mod, "query_daily_bars", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/daily", params={"symbols": "AAPL,NVDA", "days": "100"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 3
        assert "AAPL" in data["data"]
        assert "NVDA" in data["data"]
        assert len(data["data"]["AAPL"]) == 2

    def test_empty_symbols_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/db/bars/daily", params={"symbols": ""})
        assert resp.status_code == 400

    def test_missing_symbols_returns_422(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/db/bars/daily")
        assert resp.status_code == 422

    def test_days_validation(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/db/bars/daily", params={"symbols": "AAPL", "days": "0"})
        assert resp.status_code == 422
        resp = client.get("/market/stocks/db/bars/daily", params={"symbols": "AAPL", "days": "5000"})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# stocks/db/bars/daily/close
# ---------------------------------------------------------------------------


class TestStocksDbBarsDailyClose:
    def test_returns_close_series(self, monkeypatch) -> None:
        sample = {
            "AAPL": [
                {"symbol": "AAPL", "bar_time": "2026-01-02", "close": 154.0},
                {"symbol": "AAPL", "bar_time": "2026-01-03", "close": 155.5},
            ],
        }
        monkeypatch.setattr(stocks_db_mod, "query_daily_close", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/daily/close", params={"symbols": "AAPL"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["data"]["AAPL"][0]["close"] == 154.0

    def test_default_days_is_420(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(conn, *, symbols, days):
            captured["days"] = days
            return {}

        monkeypatch.setattr(stocks_db_mod, "query_daily_close", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        client.get("/market/stocks/db/bars/daily/close", params={"symbols": "AAPL"})
        assert captured["days"] == 420


# ---------------------------------------------------------------------------
# stocks/db/bars/daily/spy-close
# ---------------------------------------------------------------------------


class TestStocksDbSpyClose:
    def test_returns_float_list(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "query_spy_close", lambda *_a, **_k: [100.0, 101.5, 99.8])
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/daily/spy-close", params={"days": "100"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["values"] == [100.0, 101.5, 99.8]
        assert data["count"] == 3

    def test_empty_when_no_data(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "query_spy_close", lambda *_a, **_k: [])
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/daily/spy-close")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


# ---------------------------------------------------------------------------
# reference/ticker (single)
# ---------------------------------------------------------------------------


class TestReferenceTicker:
    def test_returns_full_ticker(self, monkeypatch) -> None:
        sample = {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "market": "stocks",
            "locale": "us",
            "primary_exchange": "XNAS",
            "instrument_type": "CS",
            "active": True,
            "currency": "USD",
            "cik": "0000320193",
            "composite_figi": "BBG000B9XRY4",
            "sic_code": "3571",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "market_cap": 3000000000000.0,
            "list_date": "1980-12-12",
            "homepage_url": "https://apple.com",
            "total_employees": 164000,
            "description": "Apple designs...",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        monkeypatch.setattr(reference_db_mod, "query_ticker_single", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/reference/ticker", params={"symbol": "AAPL"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["ticker"]["symbol"] == "AAPL"
        assert data["ticker"]["market_cap"] == 3000000000000.0

    def test_404_when_not_found(self, monkeypatch) -> None:
        monkeypatch.setattr(reference_db_mod, "query_ticker_single", lambda *_a, **_k: None)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/reference/ticker", params={"symbol": "ZZZZ"})
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# reference/tickers/batch
# ---------------------------------------------------------------------------


class TestReferenceTickersBatch:
    def test_returns_batch(self, monkeypatch) -> None:
        sample = [
            {"symbol": "AAPL", "name": "Apple Inc.", "market": "stocks"},
            {"symbol": "NVDA", "name": "NVIDIA Corp", "market": "stocks"},
        ]
        monkeypatch.setattr(reference_db_mod, "query_ticker_batch", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/reference/tickers/batch", params={"symbols": "AAPL,NVDA"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["tickers"][0]["symbol"] == "AAPL"

    def test_empty_symbols_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/reference/tickers/batch", params={"symbols": ""})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# query_daily_bars unit (no HTTP, test SQL logic with mock cursor)
# ---------------------------------------------------------------------------


class TestQueryDailyBarsUnit:
    def test_groups_by_symbol(self, monkeypatch) -> None:
        mock_rows = [
            ("AAPL", date(2026, 1, 2), 150.0, 155.0, 149.0, 154.0, 1000000),
            ("AAPL", date(2026, 1, 3), 154.0, 156.0, 153.0, 155.5, 900000),
            ("NVDA", date(2026, 1, 2), 800.0, 810.0, 795.0, 805.0, 2000000),
        ]

        class MockCursor:
            def execute(self, *a, **kw): pass
            def fetchall(self): return mock_rows
            def __enter__(self): return self
            def __exit__(self, *a): pass

        class MockConn:
            def cursor(self): return MockCursor()

        monkeypatch.setattr(stocks_db_mod, "table_exists", lambda *_a, **_k: True)
        result = stocks_db_mod.query_daily_bars(MockConn(), symbols=["AAPL", "NVDA"], days=400)
        assert "AAPL" in result
        assert "NVDA" in result
        assert len(result["AAPL"]) == 2
        assert len(result["NVDA"]) == 1
        assert result["AAPL"][0]["bar_time"] == "2026-01-02"
        assert result["AAPL"][0]["source"] == "massive"

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "table_exists", lambda *_a, **_k: False)
        result = stocks_db_mod.query_daily_bars(None, symbols=["AAPL"], days=400)
        assert result == {}


class TestQuerySpyCloseUnit:
    def test_returns_floats(self, monkeypatch) -> None:
        mock_rows = [(100.0,), (101.5,), (99.8,)]

        class MockCursor:
            def execute(self, *a, **kw): pass
            def fetchall(self): return mock_rows
            def __enter__(self): return self
            def __exit__(self, *a): pass

        class MockConn:
            def cursor(self): return MockCursor()

        monkeypatch.setattr(stocks_db_mod, "table_exists", lambda *_a, **_k: True)
        result = stocks_db_mod.query_spy_close(MockConn(), days=420)
        assert result == [100.0, 101.5, 99.8]
