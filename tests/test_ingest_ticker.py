"""Tests for ticker_sync ingest handler."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.ticker_sync import handle_ticker_sync
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_ticker_sync_universe() -> None:
    client = mock_client(
        fetch_reference_tickers={
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "usd",
                    "cik": "320193",
                    "composite_figi": "BBG000B9XRY4",
                }
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "universe"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["mode"] == "universe"
    assert "market.ticker" in conn.upsert_sqls()[0]


@pytest.mark.asyncio
async def test_universe_does_not_overwrite_detail_fields() -> None:
    client = mock_client(
        fetch_reference_tickers={
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    await handle_ticker_sync(make_job("ticker_sync", {"mode": "universe"}), client, conn)
    sql = conn.upsert_sqls()[0]
    # Universe ON CONFLICT must not clobber detail-only overview columns
    for col in (
        "sector",
        "industry",
        "market_cap",
        "description",
        "homepage_url",
        "total_employees",
        "sic_code",
        "list_date",
    ):
        assert f"{col} = EXCLUDED.{col}" not in sql
    for col in ("name", "market", "locale", "primary_exchange", "instrument_type", "active"):
        assert f"{col} = EXCLUDED.{col}" in sql
    # list row should leave sector/industry as None (not empty string)
    row = conn.statements[0][1][0]
    assert row[11] is None  # sector
    assert row[12] is None  # industry


@pytest.mark.asyncio
async def test_ticker_sync_detail() -> None:
    client = mock_client(
        fetch_ticker_details={
            "results": {
                "ticker": "AAPL",
                "name": "Apple Inc",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": True,
                "market_cap": 3e12,
                "sic_code": "3571",
                "description": "Consumer electronics",
                "list_date": "1980-12-12",
                "homepage_url": "https://www.apple.com",
                "total_employees": 160000,
            }
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "detail", "symbol": "aapl"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["symbol"] == "AAPL"
    row = conn.statements[0][1][0]
    assert row[0] == "AAPL"
    assert row[13] == 3e12  # market_cap
