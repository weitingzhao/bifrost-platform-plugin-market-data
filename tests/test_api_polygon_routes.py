"""Tests for Polygon pass-through API routes (mocked client)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api.deps import get_polygon_client
from bifrost_market_data.polygon.client import PolygonClient


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock(spec=PolygonClient)
    return client


@pytest.fixture
def api_client(mock_client: AsyncMock) -> TestClient:
    app = create_app()

    async def _override() -> PolygonClient:
        return mock_client  # type: ignore[return-value]

    app.dependency_overrides[get_polygon_client] = _override
    return TestClient(app)


def test_stock_bars_range(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.get_json = AsyncMock(return_value={"status": "OK", "results": [{"c": 1}]})
    resp = api_client.get(
        "/market/stocks/bars/range",
        params={
            "ticker": "AAPL",
            "from": "2024-01-01",
            "to": "2024-01-31",
            "timespan": "day",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "OK"
    mock_client.get_json.assert_awaited_once()


def test_reference_tickers(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_reference_tickers_query = AsyncMock(
        return_value={"status": "OK", "results": [{"ticker": "AAPL"}]}
    )
    resp = api_client.get("/market/tickers", params={"limit": 5})
    assert resp.status_code == 200
    assert resp.json()["results"][0]["ticker"] == "AAPL"
    mock_client.fetch_reference_tickers_query.assert_awaited_once()


def test_market_ops_status(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_market_status_now = AsyncMock(
        return_value={"market": "open", "serverTime": "2024-06-20T14:30:00-04:00"}
    )
    resp = api_client.get("/market/market-ops/status")
    assert resp.status_code == 200
    assert resp.json()["market"] == "open"
    mock_client.fetch_market_status_now.assert_awaited_once()


def test_fundamentals_income_statements(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_financial_statement = AsyncMock(
        return_value={"status": "OK", "results": [{"revenue": 100}]}
    )
    resp = api_client.get(
        "/market/stocks/fundamentals/income-statements",
        params={"ticker": "AAPL"},
    )
    assert resp.status_code == 200
    assert resp.json()["results"][0]["revenue"] == 100
    mock_client.fetch_financial_statement.assert_awaited_once_with(
        "income-statements",
        ticker="AAPL",
        timeframe=None,
        fiscal_year=None,
        fiscal_quarter=None,
        period_end=None,
        filing_date=None,
        limit=10,
        sort=None,
    )


def test_technical_indicator_sma(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_indicator = AsyncMock(
        return_value={"status": "OK", "results": {"values": [{"value": 150.0}]}}
    )
    resp = api_client.get("/market/technical-indicators/sma/AAPL", params={"window": 20})
    assert resp.status_code == 200
    assert resp.json()["results"]["values"][0]["value"] == 150.0
    mock_client.fetch_indicator.assert_awaited_once()
    args, kwargs = mock_client.fetch_indicator.await_args
    assert args[0] == "sma"
    assert args[1] == "AAPL"
    assert kwargs["window"] == 20


def test_trades_quotes_last_trade(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_last_trade = AsyncMock(
        return_value={"status": "OK", "results": {"p": 1.25}}
    )
    resp = api_client.get("/market/trades-quotes/last-trade/O:AAPL240621C00150000")
    assert resp.status_code == 200
    assert resp.json()["results"]["p"] == 1.25
    mock_client.fetch_last_trade.assert_awaited_once_with("O:AAPL240621C00150000")


def test_technical_indicator_rejects_unknown(api_client: TestClient) -> None:
    resp = api_client.get("/market/technical-indicators/xyz/AAPL")
    assert resp.status_code == 400
    assert "Unknown indicator" in resp.json()["detail"]


def test_stock_open_close(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_open_close = AsyncMock(return_value={"status": "OK", "open": 100.0})
    resp = api_client.get("/market/stocks/bars/open-close/AAPL/2024-06-20")
    assert resp.status_code == 200
    assert resp.json()["open"] == 100.0


def test_market_ops_conditions(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_market_conditions = AsyncMock(
        return_value={"status": "OK", "results": [{"id": 1}]}
    )
    resp = api_client.get("/market/market-ops/conditions")
    assert resp.status_code == 200
    assert resp.json()["results"][0]["id"] == 1


def test_fundamentals_ratios(api_client: TestClient, mock_client: AsyncMock) -> None:
    mock_client.fetch_ratios = AsyncMock(return_value={"status": "OK", "results": []})
    resp = api_client.get(
        "/market/stocks/fundamentals/ratios",
        params={"ticker": "MSFT"},
    )
    assert resp.status_code == 200
    mock_client.fetch_ratios.assert_awaited_once()


def test_polygon_error_maps_to_http(api_client: TestClient, mock_client: AsyncMock) -> None:
    from bifrost_market_data.polygon.errors import PolygonAPIError

    mock_client.fetch_market_status_now = AsyncMock(
        side_effect=PolygonAPIError("not found", status_code=404)
    )
    resp = api_client.get("/market/market-ops/status")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "not found"


def test_missing_api_key_returns_503(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi import HTTPException

    from bifrost_market_data.api import deps as deps_mod

    def _raise() -> str:
        raise HTTPException(status_code=503, detail="Polygon API key not configured")

    monkeypatch.setattr(deps_mod, "resolve_polygon_api_key", _raise)
    app = create_app()
    client = TestClient(app)
    resp = client.get("/market/market-ops/status")
    assert resp.status_code == 503
