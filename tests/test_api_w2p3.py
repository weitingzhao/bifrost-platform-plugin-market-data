"""Unit tests for W2-P3 endpoints: PCR, option_daily, coverage extensions, bars extensions."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import pcr as pcr_mod
from bifrost_market_data.api import option_daily as option_daily_mod
from bifrost_market_data.api import coverage as coverage_mod
from bifrost_market_data.api import stocks_db as stocks_db_mod


class _DummyConn:
    def close(self) -> None:
        return None


def _patch_require_db(monkeypatch):
    """Patch require_db in all relevant modules."""
    monkeypatch.setattr(pcr_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(option_daily_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(coverage_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(stocks_db_mod, "require_db", lambda: _DummyConn())


# ---------------------------------------------------------------------------
# PCR
# ---------------------------------------------------------------------------


class TestPcrEndpoint:
    def test_returns_oi_trend(self, monkeypatch) -> None:
        sample = {
            "ok": True,
            "symbol": "NVDA",
            "type": "oi",
            "lookback_days": 365,
            "count": 2,
            "latest_ratio": 0.8,
            "trend": [
                {"trade_date": "2026-01-01", "put_value": 800, "call_value": 1000, "ratio": 0.8},
                {"trade_date": "2026-01-02", "put_value": 900, "call_value": 1100, "ratio": 0.818},
            ],
        }
        monkeypatch.setattr(pcr_mod, "query_pcr_aggregate", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/options/analytics/pcr", params={"symbol": "NVDA", "type": "oi"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["symbol"] == "NVDA"
        assert data["type"] == "oi"
        assert len(data["trend"]) == 2

    def test_returns_volume_trend(self, monkeypatch) -> None:
        sample = {
            "ok": True,
            "symbol": "NVDA",
            "type": "volume",
            "lookback_days": 60,
            "count": 1,
            "latest_ratio": 0.5,
            "trend": [{"trade_date": "2026-01-01", "put_value": 500, "call_value": 1000, "ratio": 0.5}],
        }
        monkeypatch.setattr(pcr_mod, "query_pcr_aggregate", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/options/analytics/pcr", params={"symbol": "NVDA", "type": "volume"})
        assert resp.status_code == 200
        assert resp.json()["type"] == "volume"

    def test_invalid_type_returns_error(self, monkeypatch) -> None:
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/options/analytics/pcr", params={"symbol": "NVDA", "type": "invalid"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is False


# ---------------------------------------------------------------------------
# Option Daily
# ---------------------------------------------------------------------------


class TestOptionDailyEndpoint:
    def test_returns_option_daily_rows(self, monkeypatch) -> None:
        sample = {
            "ok": True,
            "symbol": "NVDA",
            "rows": [
                {
                    "option_ticker": "O:NVDA260918C00120000",
                    "underlying": "NVDA",
                    "expiry": "2026-09-18",
                    "strike": 120.0,
                    "option_right": "C",
                    "bar_date": "2026-08-01",
                    "open": 5.0,
                    "high": 6.0,
                    "low": 4.5,
                    "close": 5.5,
                    "volume": 1000,
                }
            ],
            "count": 1,
        }
        monkeypatch.setattr(option_daily_mod, "query_option_daily", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/options/daily", params={"symbol": "NVDA", "days": "30"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1
        assert data["rows"][0]["option_ticker"] == "O:NVDA260918C00120000"

    def test_available_dates(self, monkeypatch) -> None:
        sample = {"ok": True, "symbol": "NVDA", "dates": ["2026-08-14", "2026-08-13"]}
        monkeypatch.setattr(option_daily_mod, "query_option_daily_available_dates", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/options/daily/available-dates", params={"symbol": "NVDA"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["dates"]) == 2


# ---------------------------------------------------------------------------
# Coverage extensions (sepa-stats, distributions)
# ---------------------------------------------------------------------------


class TestCoverageSepaStats:
    def test_returns_table_stats(self, monkeypatch) -> None:
        sample = {
            "ok": True,
            "tables": [
                {"table": "market.stock_daily", "row_count": 500000, "latest": "2026-08-14"},
                {"table": "market.option_contract", "row_count": 12000, "latest": "2026-08-14T12:00:00+00:00"},
            ],
        }
        monkeypatch.setattr(coverage_mod, "query_sepa_stats", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/coverage/sepa-stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["tables"]) == 2
        assert data["tables"][0]["row_count"] == 500000


class TestCoverageDistributions:
    def test_returns_distributions(self, monkeypatch) -> None:
        sample = {
            "ok": True,
            "table": "market.stock_daily",
            "distributions": [
                {"symbol": "AAPL", "row_count": 5000},
                {"symbol": "NVDA", "row_count": 4500},
            ],
            "count": 2,
        }
        monkeypatch.setattr(coverage_mod, "query_distributions", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/coverage/distributions", params={"table": "stock_daily"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["distributions"][0]["symbol"] == "AAPL"

    def test_invalid_table_returns_error(self, monkeypatch) -> None:
        sample = {"ok": False, "error": "Invalid table; choose from: ['stock_daily', ...]"}
        monkeypatch.setattr(coverage_mod, "query_distributions", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/coverage/distributions", params={"table": "nonexistent"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is False


# ---------------------------------------------------------------------------
# Stocks DB bars extensions
# ---------------------------------------------------------------------------


class TestStocksDbBars:
    def test_returns_bars(self, monkeypatch) -> None:
        sample = [
            {"symbol": "NVDA", "period": "1 D", "time": 1723593600.0, "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 50000},
        ]
        monkeypatch.setattr(stocks_db_mod, "query_bars", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars", params={"symbol": "NVDA", "period": "1 D", "limit": "200"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1
        assert data["rows"][0]["close"] == 103.0

    def test_missing_symbol_returns_400(self, monkeypatch) -> None:
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())
        resp = client.get("/market/stocks/db/bars", params={"symbol": ""})
        assert resp.status_code == 400


class TestStocksDbBarsLatest:
    def test_returns_latest_ts(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "query_bars_latest", lambda *_a, **_k: 1723593600.0)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/latest", params={"symbol": "NVDA"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["latest_ts"] == 1723593600.0

    def test_returns_null_when_no_data(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "query_bars_latest", lambda *_a, **_k: None)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/latest", params={"symbol": "ZZZZ"})
        assert resp.status_code == 200
        assert resp.json()["latest_ts"] is None


class TestStocksDbBarsRange:
    def test_returns_timestamps(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "query_bar_times_in_range", lambda *_a, **_k: [1.0, 2.0, 3.0])
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get(
            "/market/stocks/db/bars/range",
            params={"symbol": "NVDA", "period": "1 D", "start_ts": "1.0", "end_ts": "10.0"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 3


class TestStocksDbBarsBenchmark:
    def test_returns_benchmark(self, monkeypatch) -> None:
        sample = {
            "SPY": {"bar_time": 1723593600.0, "close": 450.0, "prev_close": 448.0},
            "QQQ": {"bar_time": 1723593600.0, "close": 380.0, "prev_close": 378.5},
        }
        monkeypatch.setattr(stocks_db_mod, "query_bars_benchmark", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/benchmark", params={"symbols": "SPY,QQQ"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert "SPY" in data["data"]
        assert data["data"]["SPY"]["close"] == 450.0


class TestStocksDbBarsFallbackPrice:
    def test_returns_fallback(self, monkeypatch) -> None:
        sample = {"ok": True, "symbol": "NVDA", "found": True, "close": 120.5, "bar_time": 1723593600.0, "prev_close": 119.0}
        monkeypatch.setattr(stocks_db_mod, "query_fallback_price", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/fallback-price", params={"symbol": "NVDA"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["found"] is True
        assert data["close"] == 120.5

    def test_not_found(self, monkeypatch) -> None:
        sample = {"ok": True, "symbol": "ZZZZ", "found": False}
        monkeypatch.setattr(stocks_db_mod, "query_fallback_price", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/fallback-price", params={"symbol": "ZZZZ"})
        assert resp.status_code == 200
        assert resp.json()["found"] is False


class TestStocksDbBarsStats:
    def test_returns_stats(self, monkeypatch) -> None:
        sample = {"stock_day": 1500, "stock_min": {"1 min": 100, "5 mins": 200, "1 hour": 50}}
        monkeypatch.setattr(stocks_db_mod, "query_bars_stats", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/stats", params={"symbol": "NVDA"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["stock_day"] == 1500
        assert data["stock_min"]["5 mins"] == 200


class TestStocksDbBarsCoverage:
    def test_returns_coverage(self, monkeypatch) -> None:
        sample = [
            {
                "symbol": "NVDA",
                "stock_day": {"count": 1200, "min_day": "2021-01-04", "max_day": "2026-08-14", "min_ts": 1.0, "max_ts": 2.0},
                "stock_min": {
                    "1 min": {"count": 0, "min_ts": None, "max_ts": None},
                    "5 mins": {"count": 500, "min_ts": 1.0, "max_ts": 2.0},
                    "1 hour": {"count": 0, "min_ts": None, "max_ts": None},
                },
            }
        ]
        monkeypatch.setattr(stocks_db_mod, "query_bars_coverage", lambda *_a, **_k: sample)
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/coverage", params={"symbols": "NVDA"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1
        assert data["symbols"][0]["stock_day"]["count"] == 1200


class TestStocksDbCaretSymbols:
    def test_returns_caret_symbols(self, monkeypatch) -> None:
        monkeypatch.setattr(stocks_db_mod, "query_caret_symbols", lambda *_a, **_k: ["^GSPC", "^VIX"])
        _patch_require_db(monkeypatch)
        client = TestClient(create_app())

        resp = client.get("/market/stocks/db/bars/caret-symbols")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["symbols"] == ["^GSPC", "^VIX"]
        assert data["count"] == 2
