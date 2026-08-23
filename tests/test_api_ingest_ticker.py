"""Tests for ticker reference ingest endpoints (W0-P2)."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import ingest_ticker as mod


class _DummyConn:
    """Minimal psycopg-like connection stub."""

    def __init__(self) -> None:
        self._cursors: list[_DummyCursor] = []

    def cursor(self) -> "_DummyCursor":
        c = _DummyCursor()
        self._cursors.append(c)
        return c

    def commit(self) -> None:
        pass

    def close(self) -> None:
        pass


class _DummyCursor:
    rowcount: int = 1

    def __init__(self) -> None:
        self._fetch_result: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        if "SELECT 1 FROM raw_market.ticker" in sql:
            self._fetch_result = None
        else:
            self._fetch_result = None

    def executemany(self, sql: str, params_list: Any) -> None:
        pass

    def fetchone(self) -> Any:
        return self._fetch_result

    def __enter__(self) -> "_DummyCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def _patch(monkeypatch: Any) -> _DummyConn:
    conn = _DummyConn()
    monkeypatch.setattr(mod, "require_db", lambda: conn)
    return conn


# ---------------------------------------------------------------------------
# POST /market/reference/ticker/upsert
# ---------------------------------------------------------------------------


class TestUpsertSingle:
    def test_insert_new_ticker(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())

        resp = client.post(
            "/market/reference/ticker/upsert",
            json={
                "symbol": "NVDA",
                "name": "NVIDIA Corporation",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": True,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["symbol"] == "NVDA"
        assert data["action"] == "inserted"

    def test_update_existing_ticker(self, monkeypatch) -> None:
        _patch(monkeypatch)

        def execute_with_exists(self, sql, params=None):
            if "SELECT 1 FROM raw_market.ticker" in sql:
                self._fetch_result = (1,)
            else:
                self._fetch_result = None

        monkeypatch.setattr(_DummyCursor, "execute", execute_with_exists)
        client = TestClient(create_app())

        resp = client.post(
            "/market/reference/ticker/upsert",
            json={"symbol": "AAPL", "name": "Apple Inc."},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["action"] == "updated"

    def test_empty_symbol_rejected(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post("/market/reference/ticker/upsert", json={"symbol": ""})
        assert resp.status_code == 422

    def test_missing_symbol_rejected(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post("/market/reference/ticker/upsert", json={"name": "No sym"})
        assert resp.status_code == 422

    def test_symbol_normalized_to_uppercase(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert",
            json={"symbol": " nvda "},
        )
        assert resp.status_code == 200
        assert resp.json()["symbol"] == "NVDA"

    def test_type_maps_to_instrument_type(self, monkeypatch) -> None:
        _patch(monkeypatch)
        captured: list[tuple[Any, ...]] = []
        orig_execute = _DummyCursor.execute

        def capture_execute(self, sql, params=None):
            if params and "INSERT INTO raw_market.ticker" in sql:
                captured.append(params)
            orig_execute(self, sql, params)

        monkeypatch.setattr(_DummyCursor, "execute", capture_execute)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert",
            json={"symbol": "SPY", "type": "ETF"},
        )
        assert resp.status_code == 200
        assert len(captured) == 1
        # instrument_type is at index 5 in _MARKET_TICKER_COLS
        assert captured[0][5] == "ETF"

    def test_currency_name_fallback(self, monkeypatch) -> None:
        _patch(monkeypatch)
        captured: list[tuple[Any, ...]] = []
        orig_execute = _DummyCursor.execute

        def capture_execute(self, sql, params=None):
            if params and "INSERT INTO raw_market.ticker" in sql:
                captured.append(params)
            orig_execute(self, sql, params)

        monkeypatch.setattr(_DummyCursor, "execute", capture_execute)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert",
            json={"symbol": "SPY", "currency_name": "USD"},
        )
        assert resp.status_code == 200
        assert len(captured) == 1
        # currency is at index 7
        assert captured[0][7] == "USD"


# ---------------------------------------------------------------------------
# POST /market/reference/ticker/upsert-batch
# ---------------------------------------------------------------------------


class TestUpsertBatch:
    def test_batch_upsert(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())

        resp = client.post(
            "/market/reference/ticker/upsert-batch",
            json={
                "tickers": [
                    {"symbol": "NVDA", "name": "NVIDIA", "market": "stocks"},
                    {"symbol": "SPY", "name": "SPDR S&P 500", "type": "ETF"},
                    {"symbol": "AAPL", "name": "Apple Inc."},
                ],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["written"] == 3

    def test_empty_tickers_rejected(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert-batch",
            json={"tickers": []},
        )
        assert resp.status_code == 422

    def test_single_ticker_in_batch(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert-batch",
            json={"tickers": [{"symbol": "TSLA"}]},
        )
        assert resp.status_code == 200
        assert resp.json()["written"] == 1


# ---------------------------------------------------------------------------
# POST /market/reference/ticker/upsert-overview
# ---------------------------------------------------------------------------


class TestUpsertOverview:
    def test_overview_upsert_success(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())

        resp = client.post(
            "/market/reference/ticker/upsert-overview",
            json={
                "symbol": "NVDA",
                "sector": "Technology",
                "industry": "Semiconductors",
                "description": "NVIDIA designs GPUs...",
                "market_cap": 3200000000000,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["symbol"] == "NVDA"

    def test_overview_ticker_not_found(self, monkeypatch) -> None:
        _patch(monkeypatch)
        monkeypatch.setattr(_DummyCursor, "rowcount", 0)
        client = TestClient(create_app())

        resp = client.post(
            "/market/reference/ticker/upsert-overview",
            json={"symbol": "ZZZZZZ", "sector": "Tech"},
        )
        assert resp.status_code == 404

    def test_overview_empty_symbol_rejected(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert-overview",
            json={"symbol": "", "sector": "X"},
        )
        assert resp.status_code == 422

    def test_overview_exchange_fallback(self, monkeypatch) -> None:
        """When primary_exchange is absent, exchange is used."""
        _patch(monkeypatch)
        captured: list[tuple[Any, ...]] = []
        orig_execute = _DummyCursor.execute

        def capture_execute(self, sql, params=None):
            if params and "UPDATE market.ticker" in sql:
                captured.append(params)
            orig_execute(self, sql, params)

        monkeypatch.setattr(_DummyCursor, "execute", capture_execute)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert-overview",
            json={"symbol": "NVDA", "exchange": "XNAS"},
        )
        assert resp.status_code == 200
        assert len(captured) == 1
        # exchange (primary_exchange param) is at index 2
        assert captured[0][2] == "XNAS"

    def test_overview_symbol_normalized(self, monkeypatch) -> None:
        _patch(monkeypatch)
        client = TestClient(create_app())
        resp = client.post(
            "/market/reference/ticker/upsert-overview",
            json={"symbol": " nvda ", "sector": "Tech"},
        )
        assert resp.status_code == 200
        assert resp.json()["symbol"] == "NVDA"


# ---------------------------------------------------------------------------
# helpers unit tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_normalize_symbol(self) -> None:
        assert mod._normalize_symbol("  nvda  ") == "NVDA"
        assert mod._normalize_symbol("AAPL") == "AAPL"

    def test_parse_list_date_none(self) -> None:
        assert mod._parse_list_date(None) is None

    def test_parse_list_date_string(self) -> None:
        from datetime import date
        assert mod._parse_list_date("2024-01-15") == date(2024, 1, 15)

    def test_parse_list_date_invalid(self) -> None:
        assert mod._parse_list_date("not-a-date") is None

    def test_row_values_length(self) -> None:
        body = mod.TickerUpsertBody(symbol="TEST")
        vals = mod._row_values(body)
        assert len(vals) == len(mod._MARKET_TICKER_COLS)
        assert vals[0] == "TEST"
