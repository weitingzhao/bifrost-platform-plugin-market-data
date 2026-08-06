"""PolygonClient unit tests (httpx mock transport)."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from bifrost_market_data.polygon.client import PolygonClient, parse_retry_after, redact_url
from bifrost_market_data.polygon.errors import PolygonAPIError
from bifrost_market_data.polygon.rate_limit import TokenBucket


class _ScriptedHandler(httpx.AsyncBaseTransport):
    """Return scripted responses in order based on path contains."""

    def __init__(self, scripts: list[tuple[str, int, Any, dict[str, str] | None]]) -> None:
        self.scripts = list(scripts)
        self.calls: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.calls.append(url)
        if not self.scripts:
            return httpx.Response(500, json={"error": "no script left"}, request=request)
        match, status, body, headers = self.scripts.pop(0)
        if match and match not in url:
            # Put it back and try to find a matching script
            self.scripts.insert(0, (match, status, body, headers))
            for i, item in enumerate(self.scripts):
                if item[0] in url or not item[0]:
                    match, status, body, headers = self.scripts.pop(i)
                    break
            else:
                return httpx.Response(404, json={"error": f"no match for {url}"}, request=request)
        content = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode()
        return httpx.Response(
            status,
            content=content,
            headers={"Content-Type": "application/json", **(headers or {})},
            request=request,
        )


def _client(scripts: list[tuple[str, int, Any, dict[str, str] | None]], **kwargs: Any) -> PolygonClient:
    transport = _ScriptedHandler(scripts)
    limiter = kwargs.pop("limiter", TokenBucket(rate=1000.0, capacity=1000))
    return PolygonClient(
        "test-key",
        tier="developer",
        limiter=limiter,
        transport=transport,
        max_retries=kwargs.pop("max_retries", 5),
        **kwargs,
    )


def test_redact_url() -> None:
    redacted = redact_url("https://api.polygon.io/x?apiKey=secret&limit=1")
    assert "secret" not in redacted
    assert "apiKey=" in redacted
    assert "limit=1" in redacted


@pytest.mark.asyncio
async def test_fetch_stock_aggs_paginates() -> None:
    scripts = [
        (
            "/v2/aggs/",
            200,
            {
                "status": "OK",
                "results": [{"t": 1, "c": 10}],
                "next_url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/a/b?cursor=2",
            },
            None,
        ),
        (
            "cursor=2",
            200,
            {"status": "OK", "results": [{"t": 2, "c": 11}]},
            None,
        ),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_stock_aggs("AAPL", from_value="2024-01-01", to_value="2024-01-31")
    assert len(data["results"]) == 2
    assert data["pages"] == 2
    assert data["next_url"] is None
    assert data["truncated"] is False


@pytest.mark.asyncio
async def test_fetch_options_contracts() -> None:
    scripts = [
        (
            "/v3/reference/options/contracts",
            200,
            {"status": "OK", "results": [{"ticker": "O:AAPL250620C00150000"}]},
            None,
        ),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_options_contracts(underlying_ticker="AAPL")
    assert data["results"][0]["ticker"].startswith("O:AAPL")


@pytest.mark.asyncio
async def test_fetch_options_snapshot() -> None:
    scripts = [
        (
            "/v3/snapshot/options/SPY",
            200,
            {"status": "OK", "results": [{"details": {"ticker": "O:SPY..."}}]},
            None,
        ),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_options_snapshot("SPY")
    assert len(data["results"]) == 1


@pytest.mark.asyncio
async def test_retries_on_503_then_succeeds() -> None:
    scripts = [
        ("/v2/aggs/", 503, {"status": "ERROR", "error": "busy"}, None),
        ("/v2/aggs/", 200, {"status": "OK", "results": [{"c": 1}]}, None),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_stock_aggs("IBM", from_value="2024-01-01", to_value="2024-01-02")
    assert data["results"][0]["c"] == 1


@pytest.mark.asyncio
async def test_logic_error_raises() -> None:
    scripts = [
        ("/v3/reference/tickers/ZZZ", 200, {"status": "ERROR", "error": "not found"}, None),
    ]
    async with _client(scripts) as client:
        with pytest.raises(PolygonAPIError) as ei:
            await client.fetch_ticker_details("ZZZ")
    assert "not found" in str(ei.value)


@pytest.mark.asyncio
async def test_429_retries_with_retry_after() -> None:
    scripts = [
        ("/stocks/v1/splits", 429, {"error": "slow down"}, {"Retry-After": "0.01"}),
        ("/stocks/v1/splits", 200, {"status": "OK", "results": [{"ticker": "AAPL"}]}, None),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_splits("AAPL")
    assert data["results"][0]["ticker"] == "AAPL"


@pytest.mark.asyncio
async def test_market_status_upcoming_list_payload() -> None:
    scripts = [
        ("/v1/marketstatus/upcoming", 200, [{"date": "2026-01-01", "status": "closed"}], None),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_market_status_upcoming()
    assert data["results"][0]["date"] == "2026-01-01"


@pytest.mark.asyncio
async def test_missing_api_key_rejected() -> None:
    with pytest.raises(ValueError):
        PolygonClient("")


def test_parse_retry_after_seconds_and_http_date() -> None:
    assert parse_retry_after("1.5") == pytest.approx(1.5)
    assert parse_retry_after(None) is None
    assert parse_retry_after("not-a-date") is None
    from datetime import datetime, timezone

    now = datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)
    # 12:00:05 GMT
    delta = parse_retry_after("Wed, 29 Jul 2026 12:00:05 GMT", now=now)
    assert delta == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_option_aggs_omits_adjusted_query_param() -> None:
    scripts = [
        (
            "/v2/aggs/",
            200,
            {"status": "OK", "results": [{"c": 1.2}]},
            None,
        ),
    ]
    client = _client(scripts)
    transport = client._http._transport
    assert isinstance(transport, _ScriptedHandler)
    async with client:
        await client.fetch_stock_aggs(
            "O:AAPL250620C00150000",
            from_value="2024-01-01",
            to_value="2024-01-02",
        )
    assert transport.calls
    assert "adjusted=" not in transport.calls[0]


@pytest.mark.asyncio
async def test_pagination_truncated_flag(caplog: pytest.LogCaptureFixture) -> None:
    scripts = [
        (
            "/v2/aggs/",
            200,
            {
                "status": "OK",
                "results": [{"t": 1}],
                "next_url": "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/a/b?cursor=more",
            },
            None,
        ),
    ]
    async with _client(scripts) as client:
        with caplog.at_level("WARNING"):
            data = await client.fetch_stock_aggs(
                "AAPL",
                from_value="2024-01-01",
                to_value="2024-01-02",
                max_pages=1,
            )
    assert data["truncated"] is True
    assert data["pages"] == 1
    assert any("truncated" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_fetch_stock_snapshot_all_paginates_tickers() -> None:
    scripts = [
        (
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            200,
            {
                "status": "OK",
                "tickers": [{"ticker": "AAPL"}],
                "next_url": "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?cursor=2",
            },
            None,
        ),
        (
            "cursor=2",
            200,
            {"status": "OK", "tickers": [{"ticker": "MSFT"}]},
            None,
        ),
    ]
    async with _client(scripts) as client:
        data = await client.fetch_stock_snapshot_all()
    assert len(data["tickers"]) == 2
    assert data["pages"] == 2
    assert data["truncated"] is False


@pytest.mark.asyncio
async def test_fetch_stock_snapshot_single_and_movers() -> None:
    scripts = [
        (
            "/v2/snapshot/locale/us/markets/stocks/tickers/AAPL",
            200,
            {"status": "OK", "ticker": {"ticker": "AAPL", "todaysChangePerc": 1.2}},
            None,
        ),
        (
            "/v2/snapshot/locale/us/markets/stocks/gainers",
            200,
            {"status": "OK", "tickers": [{"ticker": "XYZ", "todaysChangePerc": 9.0}]},
            None,
        ),
    ]
    async with _client(scripts) as client:
        single = await client.fetch_stock_snapshot_single("aapl")
        movers = await client.fetch_stock_gainers_losers("gainers")
    assert single["ticker"]["ticker"] == "AAPL"
    assert movers["tickers"][0]["ticker"] == "XYZ"
