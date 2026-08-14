"""Unit tests for POST /market/stocks/bars/ingest and DELETE /market/stocks/bars."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import ingest_bars as ingest_bars_mod


class _FakeCursor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, Any]] = []
        self.rowcount: int = 0

    def execute(self, sql: str, params: Any = None) -> None:
        self.queries.append((sql, params))

    def executemany(self, sql: str, params_seq: Any) -> None:
        self.queries.append((sql, list(params_seq)))

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *a: object) -> None:
        pass


class _FakeConn:
    def __init__(self) -> None:
        self._cursor = _FakeCursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


@pytest.fixture()
def fake_conn(monkeypatch) -> _FakeConn:
    conn = _FakeConn()
    monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)
    return conn


@pytest.fixture()
def client() -> TestClient:
    return TestClient(create_app())


# ---------------------------------------------------------------------------
# POST /market/stocks/bars/ingest
# ---------------------------------------------------------------------------


class TestIngestBars:
    def test_upsert_daily(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        payload = {
            "rows": [
                {"symbol": "NVDA", "period": "1 D", "bar_time": "2026-08-14", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000000},
                {"symbol": "AAPL", "period": "1 D", "bar_time": "2026-08-13", "open": 200, "high": 210, "low": 195, "close": 205, "volume": 500000},
            ]
        }
        resp = client.post("/market/stocks/bars/ingest", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["written"] == 2
        assert conn.committed
        assert conn.closed

        sql, params = conn._cursor.queries[0]
        assert "market.stock_daily" in sql
        assert "ON CONFLICT (symbol, bar_date) DO UPDATE" in sql
        assert len(params) == 2

    def test_upsert_minute(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        payload = {
            "rows": [
                {"symbol": "NVDA", "period": "1 min", "bar_time": "2026-08-14T09:30:00", "open": 100, "high": 101, "low": 99.5, "close": 100.5, "volume": 50000},
                {"symbol": "NVDA", "period": "5 mins", "bar_time": "2026-08-14T09:30:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 150000},
                {"symbol": "NVDA", "period": "1 hour", "bar_time": "2026-08-14T09:00:00", "open": 100, "high": 105, "low": 98, "close": 104, "volume": 800000},
            ]
        }
        resp = client.post("/market/stocks/bars/ingest", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["written"] == 3

        sql, params = conn._cursor.queries[0]
        assert "market.stock_minute" in sql
        assert "ON CONFLICT (symbol, period, bar_time) DO UPDATE" in sql
        assert len(params) == 3

    def test_mixed_daily_and_minute(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        payload = {
            "rows": [
                {"symbol": "NVDA", "period": "1 D", "bar_time": "2026-08-14", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000000},
                {"symbol": "NVDA", "period": "1 min", "bar_time": "2026-08-14T09:30:00", "open": 100, "high": 101, "low": 99.5, "close": 100.5, "volume": 50000},
            ]
        }
        resp = client.post("/market/stocks/bars/ingest", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["written"] == 2
        assert len(conn._cursor.queries) == 2

    def test_invalid_period_returns_400(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        payload = {
            "rows": [
                {"symbol": "NVDA", "period": "2 min", "bar_time": "2026-08-14", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000000},
            ]
        }
        resp = client.post("/market/stocks/bars/ingest", json=payload)
        assert resp.status_code == 400
        assert "invalid period" in resp.json()["detail"]

    def test_empty_rows_returns_422(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.post("/market/stocks/bars/ingest", json={"rows": []})
        assert resp.status_code == 422

    def test_empty_symbol_returns_400(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        payload = {
            "rows": [
                {"symbol": "", "period": "1 D", "bar_time": "2026-08-14", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000000},
            ]
        }
        resp = client.post("/market/stocks/bars/ingest", json=payload)
        assert resp.status_code == 400
        assert "empty symbol" in resp.json()["detail"]

    def test_symbol_normalized_to_upper(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        payload = {
            "rows": [
                {"symbol": "nvda", "period": "1 D", "bar_time": "2026-08-14", "open": 100, "high": 105, "low": 99, "close": 103, "volume": 1000000},
            ]
        }
        resp = client.post("/market/stocks/bars/ingest", json=payload)
        assert resp.status_code == 200
        _, params = conn._cursor.queries[0]
        assert params[0][0] == "NVDA"


# ---------------------------------------------------------------------------
# DELETE /market/stocks/bars
# ---------------------------------------------------------------------------


class TestDeleteBars:
    def test_delete_daily(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        conn._cursor.rowcount = 5
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete("/market/stocks/bars", params={"symbol": "NVDA", "delete_daily": "true"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["deleted_daily"] == 5
        assert data["deleted_minute"] == 0
        assert conn.committed
        assert conn.closed

    def test_delete_minute_periods(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        conn._cursor.rowcount = 10
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete(
            "/market/stocks/bars",
            params={"symbol": "NVDA", "periods": "1 min,5 mins"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["deleted_minute"] == 10
        assert data["deleted_daily"] == 0

    def test_delete_both(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        conn._cursor.rowcount = 3
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete(
            "/market/stocks/bars",
            params={"symbol": "AAPL", "delete_daily": "true", "periods": "1 hour"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["deleted_daily"] == 3
        assert data["deleted_minute"] == 3

    def test_nothing_to_delete_returns_400(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete("/market/stocks/bars", params={"symbol": "NVDA"})
        assert resp.status_code == 400
        assert "nothing to delete" in resp.json()["detail"]

    def test_empty_symbol_returns_400(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete(
            "/market/stocks/bars",
            params={"symbol": "", "delete_daily": "true"},
        )
        assert resp.status_code == 400
        assert "empty symbol" in resp.json()["detail"]

    def test_missing_symbol_returns_422(self, monkeypatch, client) -> None:
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete("/market/stocks/bars", params={"delete_daily": "true"})
        assert resp.status_code == 422

    def test_invalid_period_ignored(self, monkeypatch, client) -> None:
        """Invalid periods in comma list are silently skipped; if none remain, 400."""
        conn = _FakeConn()
        monkeypatch.setattr(ingest_bars_mod, "require_db", lambda: conn)

        resp = client.delete(
            "/market/stocks/bars",
            params={"symbol": "NVDA", "periods": "2 min,bad"},
        )
        assert resp.status_code == 400
