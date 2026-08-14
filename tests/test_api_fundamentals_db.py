"""Unit tests for fundamentals_db (DB-read financials) endpoints."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import fundamentals_db as fdb_mod


class _DummyConn:
    def close(self) -> None:
        return None


def _patch_db(monkeypatch):
    monkeypatch.setattr(fdb_mod, "require_db", lambda: _DummyConn())


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/db/short-interest
# ---------------------------------------------------------------------------


class TestShortInterestEndpoint:
    def test_returns_grouped_data(self, monkeypatch) -> None:
        sample = {
            "NVDA": [
                {
                    "symbol": "NVDA",
                    "settlement_date": "2026-08-01",
                    "short_interest": 12345678,
                    "avg_daily_volume": 98765,
                    "days_to_cover": 1.5,
                },
                {
                    "symbol": "NVDA",
                    "settlement_date": "2026-07-15",
                    "short_interest": 11000000,
                    "avg_daily_volume": 95000,
                    "days_to_cover": 1.4,
                },
            ],
            "AAPL": [
                {
                    "symbol": "AAPL",
                    "settlement_date": "2026-08-01",
                    "short_interest": 9000000,
                    "avg_daily_volume": 70000,
                    "days_to_cover": 0.9,
                },
            ],
        }
        monkeypatch.setattr(fdb_mod, "query_short_interest", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/db/short-interest",
            params={"symbols": "NVDA,AAPL", "settlements": "6"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 3
        assert len(data["data"]["NVDA"]) == 2
        assert len(data["data"]["AAPL"]) == 1
        assert data["data"]["NVDA"][0]["settlement_date"] == "2026-08-01"

    def test_empty_symbols_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/db/short-interest", params={"symbols": ""})
        assert resp.status_code == 400

    def test_missing_symbols_returns_422(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/db/short-interest")
        assert resp.status_code == 422

    def test_default_settlements_is_6(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(conn, *, symbols, settlements):
            captured["settlements"] = settlements
            return {}

        monkeypatch.setattr(fdb_mod, "query_short_interest", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        client.get("/market/stocks/fundamentals/db/short-interest", params={"symbols": "NVDA"})
        assert captured["settlements"] == 6


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/db/short-volume
# ---------------------------------------------------------------------------


class TestShortVolumeEndpoint:
    def test_returns_grouped_data(self, monkeypatch) -> None:
        sample = {
            "NVDA": [
                {
                    "symbol": "NVDA",
                    "trade_date": "2026-08-13",
                    "short_volume": 5000000,
                    "short_volume_ratio": 0.45,
                    "total_volume": 11000000,
                },
            ],
        }
        monkeypatch.setattr(fdb_mod, "query_short_volume", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/db/short-volume",
            params={"symbols": "NVDA", "trade_days": "30"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1
        assert data["data"]["NVDA"][0]["short_volume"] == 5000000
        assert data["data"]["NVDA"][0]["short_volume_ratio"] == 0.45

    def test_empty_symbols_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/db/short-volume", params={"symbols": ""})
        assert resp.status_code == 400

    def test_default_trade_days_is_60(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(conn, *, symbols, trade_days):
            captured["trade_days"] = trade_days
            return {}

        monkeypatch.setattr(fdb_mod, "query_short_volume", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        client.get("/market/stocks/fundamentals/db/short-volume", params={"symbols": "NVDA"})
        assert captured["trade_days"] == 60


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/db/financials
# ---------------------------------------------------------------------------


class TestFinancialsEndpoint:
    def test_returns_rows(self, monkeypatch) -> None:
        sample = [
            {
                "symbol": "NVDA",
                "report_type": "income_statement",
                "period_date": "2026-06-30",
                "timeframe": "quarterly",
                "data": {"revenue": 50000000000},
                "fetched_at": "2026-07-15T10:00:00+00:00",
            },
        ]
        monkeypatch.setattr(fdb_mod, "query_financials", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/db/financials",
            params={"symbol": "NVDA", "report_type": "income_statement", "timeframe": "quarterly"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1
        assert data["rows"][0]["report_type"] == "income_statement"
        assert data["rows"][0]["data"]["revenue"] == 50000000000

    def test_no_filters(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(conn, *, symbol, report_type, timeframe, limit):
            captured.update(symbol=symbol, report_type=report_type, timeframe=timeframe, limit=limit)
            return []

        monkeypatch.setattr(fdb_mod, "query_financials", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/db/financials", params={"symbol": "AAPL"})
        assert resp.status_code == 200
        assert captured["symbol"] == "AAPL"
        assert captured["report_type"] is None
        assert captured["timeframe"] is None
        assert captured["limit"] == 20

    def test_invalid_report_type_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/db/financials",
            params={"symbol": "NVDA", "report_type": "invalid_type"},
        )
        assert resp.status_code == 400

    def test_invalid_timeframe_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/db/financials",
            params={"symbol": "NVDA", "timeframe": "monthly"},
        )
        assert resp.status_code == 400

    def test_empty_symbol_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/db/financials", params={"symbol": ""})
        assert resp.status_code == 400

    def test_missing_symbol_returns_422(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/db/financials")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# query_short_interest unit (SQL logic with mock cursor)
# ---------------------------------------------------------------------------


class TestQueryShortInterestUnit:
    def test_groups_by_symbol(self, monkeypatch) -> None:
        mock_rows = [
            ("NVDA", date(2026, 8, 1), 12345678, 98765, 1.5),
            ("NVDA", date(2026, 7, 15), 11000000, 95000, 1.4),
            ("AAPL", date(2026, 8, 1), 9000000, 70000, 0.9),
        ]

        class MockCursor:
            def execute(self, *a, **kw):
                pass

            def fetchall(self):
                return mock_rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()

        monkeypatch.setattr(fdb_mod, "table_exists", lambda *_a, **_k: True)
        result = fdb_mod.query_short_interest(MockConn(), symbols=["NVDA", "AAPL"], settlements=6)
        assert "NVDA" in result
        assert "AAPL" in result
        assert len(result["NVDA"]) == 2
        assert len(result["AAPL"]) == 1
        assert result["NVDA"][0]["settlement_date"] == "2026-08-01"
        assert result["NVDA"][0]["short_interest"] == 12345678
        assert result["NVDA"][0]["days_to_cover"] == 1.5

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(fdb_mod, "table_exists", lambda *_a, **_k: False)
        result = fdb_mod.query_short_interest(None, symbols=["NVDA"], settlements=6)
        assert result == {}


# ---------------------------------------------------------------------------
# query_short_volume unit
# ---------------------------------------------------------------------------


class TestQueryShortVolumeUnit:
    def test_groups_by_symbol(self, monkeypatch) -> None:
        mock_rows = [
            ("NVDA", date(2026, 8, 13), 5000000, 0.45, 11000000),
            ("AAPL", date(2026, 8, 13), 3000000, 0.32, 9500000),
        ]

        class MockCursor:
            def execute(self, *a, **kw):
                pass

            def fetchall(self):
                return mock_rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()

        monkeypatch.setattr(fdb_mod, "table_exists", lambda *_a, **_k: True)
        result = fdb_mod.query_short_volume(MockConn(), symbols=["NVDA", "AAPL"], trade_days=60)
        assert "NVDA" in result
        assert "AAPL" in result
        assert result["NVDA"][0]["short_volume"] == 5000000
        assert result["NVDA"][0]["short_volume_ratio"] == 0.45
        assert result["AAPL"][0]["trade_date"] == "2026-08-13"

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(fdb_mod, "table_exists", lambda *_a, **_k: False)
        result = fdb_mod.query_short_volume(None, symbols=["NVDA"], trade_days=60)
        assert result == {}


# ---------------------------------------------------------------------------
# query_financials unit
# ---------------------------------------------------------------------------


class TestQueryFinancialsUnit:
    def test_returns_rows_with_all_fields(self, monkeypatch) -> None:
        mock_rows = [
            (
                "NVDA",
                "income_statement",
                date(2026, 6, 30),
                "quarterly",
                {"revenue": 50000000000},
                datetime(2026, 7, 15, 10, 0, 0, tzinfo=timezone.utc),
            ),
        ]

        class MockCursor:
            def execute(self, *a, **kw):
                pass

            def fetchall(self):
                return mock_rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()

        monkeypatch.setattr(fdb_mod, "table_exists", lambda *_a, **_k: True)
        result = fdb_mod.query_financials(
            MockConn(), symbol="NVDA", report_type="income_statement", timeframe="quarterly", limit=20
        )
        assert len(result) == 1
        assert result[0]["symbol"] == "NVDA"
        assert result[0]["report_type"] == "income_statement"
        assert result[0]["period_date"] == "2026-06-30"
        assert result[0]["timeframe"] == "quarterly"
        assert result[0]["data"]["revenue"] == 50000000000
        assert result[0]["fetched_at"] == "2026-07-15T10:00:00+00:00"

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(fdb_mod, "table_exists", lambda *_a, **_k: False)
        result = fdb_mod.query_financials(
            None, symbol="NVDA", report_type=None, timeframe=None, limit=20
        )
        assert result == []
