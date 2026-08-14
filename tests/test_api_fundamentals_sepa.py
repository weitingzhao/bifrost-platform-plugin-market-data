"""Unit tests for fundamentals_sepa (SEPA financial aggregate) endpoints."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import fundamentals_sepa as sepa_mod


class _DummyConn:
    def close(self) -> None:
        return None


def _patch_db(monkeypatch):
    monkeypatch.setattr(sepa_mod, "require_db", lambda: _DummyConn())


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/financials
# ---------------------------------------------------------------------------


class TestSepaFinancialsBatch:
    def test_returns_grouped_data(self, monkeypatch) -> None:
        sample = {
            "NVDA": [
                {
                    "symbol": "NVDA",
                    "report_type": "income_statement",
                    "period_date": "2026-06-30",
                    "period_type": "quarterly",
                    "fiscal_year": 2026,
                    "fiscal_quarter": 2,
                    "data": {"revenues": 50000000000},
                    "fetched_at": "2026-07-15T10:00:00+00:00",
                },
            ],
            "AAPL": [
                {
                    "symbol": "AAPL",
                    "report_type": "income_statement",
                    "period_date": "2026-06-30",
                    "period_type": "quarterly",
                    "fiscal_year": 2026,
                    "fiscal_quarter": 3,
                    "data": {"revenues": 80000000000},
                    "fetched_at": "2026-07-20T12:00:00+00:00",
                },
            ],
        }
        monkeypatch.setattr(sepa_mod, "query_financials_batch", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/financials",
            params={"symbols": "NVDA,AAPL", "report_type": "income_statement", "period_type": "quarterly"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert len(data["data"]["NVDA"]) == 1
        assert data["data"]["NVDA"][0]["data"]["revenues"] == 50000000000

    def test_empty_symbols_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/financials",
            params={"symbols": "", "report_type": "income_statement"},
        )
        assert resp.status_code == 400

    def test_invalid_report_type_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/financials",
            params={"symbols": "NVDA", "report_type": "invalid_type"},
        )
        assert resp.status_code == 400

    def test_invalid_period_type_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/financials",
            params={"symbols": "NVDA", "report_type": "income_statement", "period_type": "monthly"},
        )
        assert resp.status_code == 400

    def test_missing_report_type_returns_422(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/financials",
            params={"symbols": "NVDA"},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/income-rows
# ---------------------------------------------------------------------------


class TestSepaIncomeRows:
    def test_returns_quarterly_and_annual(self, monkeypatch) -> None:
        sample = {
            "quarterly": [
                {"timeframe": "quarterly", "fiscal_year": 2025, "fiscal_quarter": 1,
                 "period_end": "2025-03-31", "data": {"revenues": 10000}},
                {"timeframe": "quarterly", "fiscal_year": 2025, "fiscal_quarter": 2,
                 "period_end": "2025-06-30", "data": {"revenues": 12000}},
            ],
            "annual": [
                {"timeframe": "annual", "fiscal_year": 2024, "fiscal_quarter": None,
                 "period_end": "2024-12-31", "data": {"revenues": 40000}},
            ],
        }
        monkeypatch.setattr(sepa_mod, "query_income_rows_for_sepa", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/income-rows",
            params={"symbol": "NVDA"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["symbol"] == "NVDA"
        assert data["quarterly_count"] == 2
        assert data["annual_count"] == 1
        assert len(data["quarterly"]) == 2
        assert len(data["annual"]) == 1

    def test_empty_symbol_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/income-rows",
            params={"symbol": ""},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/income-ext
# ---------------------------------------------------------------------------


class TestSepaIncomeExtBatch:
    def test_returns_batched_data(self, monkeypatch) -> None:
        sample = {
            "NVDA": [
                {"symbol": "NVDA", "fiscal_year": 2025, "fiscal_quarter": 1,
                 "period_end": "2025-03-31", "data": {"revenues": 10000}},
            ],
        }
        monkeypatch.setattr(sepa_mod, "query_financials_ext_batch", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/income-ext",
            params={"symbols": "NVDA"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1

    def test_empty_symbols_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/income-ext",
            params={"symbols": ""},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/balance-sheet-ext
# ---------------------------------------------------------------------------


class TestSepaBalanceSheetExtBatch:
    def test_returns_data_with_max_quarters(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(*_a, **kw):
            captured.update(kw)
            return {"AAPL": [{"symbol": "AAPL", "period_end": "2025-06-30", "data": {}}]}

        monkeypatch.setattr(sepa_mod, "query_financials_ext_batch", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/balance-sheet-ext",
            params={"symbols": "AAPL", "max_quarters": "8"},
        )
        assert resp.status_code == 200
        assert captured["report_type"] == "balance_sheet"
        assert captured["max_rows_per_symbol"] == 8


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/cash-flow-ext
# ---------------------------------------------------------------------------


class TestSepaCashFlowExtBatch:
    def test_returns_data_with_default_max_quarters(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(*_a, **kw):
            captured.update(kw)
            return {}

        monkeypatch.setattr(sepa_mod, "query_financials_ext_batch", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/cash-flow-ext",
            params={"symbols": "MSFT"},
        )
        assert resp.status_code == 200
        assert captured["report_type"] == "cash_flow_statement"
        assert captured["max_rows_per_symbol"] == 6


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/ratios-latest
# ---------------------------------------------------------------------------


class TestSepaRatiosLatest:
    def test_returns_latest_per_symbol(self, monkeypatch) -> None:
        sample = {
            "NVDA": {"symbol": "NVDA", "date": "2026-08-01", "data": {"price_to_earnings": 55.2}},
            "AAPL": {"symbol": "AAPL", "date": "2026-08-01", "data": {"price_to_earnings": 28.3}},
        }
        monkeypatch.setattr(sepa_mod, "query_ratios_latest_batch", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/ratios-latest",
            params={"symbols": "NVDA,AAPL"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["data"]["NVDA"]["data"]["price_to_earnings"] == 55.2


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/short-interest-latest
# ---------------------------------------------------------------------------


class TestSepaShortInterestLatest:
    def test_returns_grouped_data(self, monkeypatch) -> None:
        sample = {
            "NVDA": [
                {"symbol": "NVDA", "settlement_date": "2026-08-01", "data": {"short_interest": 12345678}},
            ],
        }
        monkeypatch.setattr(sepa_mod, "query_short_interest_latest_batch", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/short-interest-latest",
            params={"symbols": "NVDA"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1

    def test_custom_max_rows(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(*_a, **kw):
            captured.update(kw)
            return {}

        monkeypatch.setattr(sepa_mod, "query_short_interest_latest_batch", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        client.get(
            "/market/stocks/fundamentals/sepa/short-interest-latest",
            params={"symbols": "NVDA", "max_rows": "5"},
        )
        assert captured["max_rows"] == 5


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/short-volume-recent
# ---------------------------------------------------------------------------


class TestSepaShortVolumeRecent:
    def test_returns_grouped_data(self, monkeypatch) -> None:
        sample = {
            "NVDA": [
                {"symbol": "NVDA", "trade_date": "2026-08-13", "data": {"short_volume": 5000000}},
            ],
        }
        monkeypatch.setattr(sepa_mod, "query_short_volume_recent_batch", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/short-volume-recent",
            params={"symbols": "NVDA"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1

    def test_default_max_days(self, monkeypatch) -> None:
        captured: dict[str, Any] = {}

        def mock_query(*_a, **kw):
            captured.update(kw)
            return {}

        monkeypatch.setattr(sepa_mod, "query_short_volume_recent_batch", mock_query)
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        client.get(
            "/market/stocks/fundamentals/sepa/short-volume-recent",
            params={"symbols": "NVDA"},
        )
        assert captured["max_days"] == 10


# ---------------------------------------------------------------------------
# GET /market/stocks/fundamentals/sepa/gaps
# ---------------------------------------------------------------------------


class TestSepaGaps:
    def test_returns_gap_symbols(self, monkeypatch) -> None:
        sample = {"count": 3, "symbols": ["AAA", "BBB", "CCC"]}
        monkeypatch.setattr(sepa_mod, "query_gaps", lambda *_a, **_k: sample)
        _patch_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/fundamentals/sepa/gaps",
            params={"report_type": "income_statement"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 3
        assert data["symbols"] == ["AAA", "BBB", "CCC"]

    def test_invalid_report_type_returns_400(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get(
            "/market/stocks/fundamentals/sepa/gaps",
            params={"report_type": "invalid_type"},
        )
        assert resp.status_code == 400

    def test_missing_report_type_returns_422(self, monkeypatch) -> None:
        _patch_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/fundamentals/sepa/gaps")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Query helper unit tests
# ---------------------------------------------------------------------------


class TestQueryFinancialsBatchUnit:
    def test_groups_by_symbol_with_tuple_rows(self, monkeypatch) -> None:
        mock_rows = [
            ("AAPL", "income_statement", date(2025, 12, 31), "annual",
             2025, None, {"revenues": 400000}, datetime(2026, 1, 15, tzinfo=timezone.utc)),
            ("NVDA", "income_statement", date(2025, 12, 31), "annual",
             2025, None, {"revenues": 600000}, datetime(2026, 1, 15, tzinfo=timezone.utc)),
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

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_financials_batch(
            MockConn(), symbols=["AAPL", "NVDA"], report_type="income_statement",
        )
        assert "AAPL" in result
        assert "NVDA" in result
        assert result["AAPL"][0]["data"]["revenues"] == 400000
        assert result["NVDA"][0]["period_date"] == "2025-12-31"

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: False)
        assert sepa_mod.query_financials_batch(None, symbols=["X"], report_type="ratios") == {}


class TestQueryIncomeRowsUnit:
    def test_returns_quarterly_and_annual(self, monkeypatch) -> None:
        q_rows = [
            {"timeframe": "quarterly", "fiscal_year": 2025, "fiscal_quarter": 1,
             "period_end": date(2025, 3, 31), "data": {"revenues": 10000}},
        ]
        a_rows = [
            {"timeframe": "annual", "fiscal_year": 2024, "fiscal_quarter": None,
             "period_end": date(2024, 12, 31), "data": {"revenues": 40000}},
        ]
        call_count = 0

        class MockCursor:
            def execute(self, *a, **kw):
                pass

            def fetchall(self):
                nonlocal call_count
                call_count += 1
                return q_rows if call_count == 1 else a_rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

        class MockConn:
            def cursor(self):
                return MockCursor()

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_income_rows_for_sepa(MockConn(), symbol="NVDA")
        assert len(result["quarterly"]) == 1
        assert len(result["annual"]) == 1
        assert result["quarterly"][0]["period_end"] == "2025-03-31"

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: False)
        result = sepa_mod.query_income_rows_for_sepa(None, symbol="NVDA")
        assert result == {"quarterly": [], "annual": []}


class TestQueryRatiosLatestUnit:
    def test_returns_latest_per_symbol(self, monkeypatch) -> None:
        mock_rows = [
            {"symbol": "NVDA", "date": date(2026, 8, 1), "data": {"price_to_earnings": 55.2}},
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

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_ratios_latest_batch(MockConn(), symbols=["NVDA"])
        assert "NVDA" in result
        assert result["NVDA"]["date"] == "2026-08-01"
        assert result["NVDA"]["data"]["price_to_earnings"] == 55.2


class TestQueryExtBatchUnit:
    def test_with_max_rows_per_symbol(self, monkeypatch) -> None:
        mock_rows = [
            {"symbol": "NVDA", "fiscal_year": 2025, "fiscal_quarter": 4,
             "period_end": date(2025, 12, 31), "data": {"total_assets": 99999}, "rn": 1},
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

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_financials_ext_batch(
            MockConn(), symbols=["NVDA"], report_type="balance_sheet", max_rows_per_symbol=6,
        )
        assert "NVDA" in result
        assert result["NVDA"][0]["period_end"] == "2025-12-31"
        assert "rn" not in result["NVDA"][0]

    def test_without_max_rows_per_symbol(self, monkeypatch) -> None:
        mock_rows = [
            {"symbol": "NVDA", "fiscal_year": 2025, "fiscal_quarter": 1,
             "period_end": date(2025, 3, 31), "data": {"revenues": 10000}},
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

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_financials_ext_batch(
            MockConn(), symbols=["NVDA"], report_type="income_statement",
        )
        assert "NVDA" in result
        assert len(result["NVDA"]) == 1


class TestQueryShortInterestLatestUnit:
    def test_groups_by_symbol(self, monkeypatch) -> None:
        mock_rows = [
            {"symbol": "NVDA", "settlement_date": date(2026, 7, 15), "data": {"short_interest": 11000000}, "rn": 2},
            {"symbol": "NVDA", "settlement_date": date(2026, 8, 1), "data": {"short_interest": 12345678}, "rn": 1},
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

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_short_interest_latest_batch(MockConn(), symbols=["NVDA"], max_rows=2)
        assert "NVDA" in result
        assert len(result["NVDA"]) == 2

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: False)
        assert sepa_mod.query_short_interest_latest_batch(None, symbols=["X"]) == {}


class TestQueryShortVolumeRecentUnit:
    def test_groups_by_symbol_tuple_rows(self, monkeypatch) -> None:
        mock_rows = [
            ("NVDA", date(2026, 8, 12), {"short_volume": 4000000}, 2),
            ("NVDA", date(2026, 8, 13), {"short_volume": 5000000}, 1),
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

        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_short_volume_recent_batch(MockConn(), symbols=["NVDA"], max_days=10)
        assert "NVDA" in result
        assert len(result["NVDA"]) == 2
        assert result["NVDA"][0]["trade_date"] == "2026-08-12"

    def test_returns_empty_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: False)
        assert sepa_mod.query_short_volume_recent_batch(None, symbols=["X"]) == {}


class TestQueryGapsUnit:
    def test_returns_symbols_when_table_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: False)
        result = sepa_mod.query_gaps(None, report_type="income_statement")
        assert result["count"] == 0
        assert result["symbols"] == []

    def test_unsupported_report_type(self, monkeypatch) -> None:
        monkeypatch.setattr(sepa_mod, "table_exists", lambda *_a, **_k: True)
        monkeypatch.setattr(sepa_mod, "_view_or_table_exists", lambda *_a, **_k: True)
        result = sepa_mod.query_gaps(None, report_type="comprehensive_income")
        assert "error" in result
