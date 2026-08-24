"""Tests for ticker_type ingest handler."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.ticker_type import handle_ticker_type
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_ticker_type_writes_dictionary() -> None:
    client = mock_client(
        fetch_ticker_types={
            "results": [
                {
                    "code": "CS",
                    "description": "Common Stock",
                    "asset_class": "stocks",
                    "locale": "us",
                },
                {
                    "code": "ETF",
                    "description": "Exchange Traded Fund",
                    "asset_class": "stocks",
                    "locale": "us",
                },
                {
                    "code": "CS",
                    "description": "Common Stock",
                    "asset_class": "stocks",
                    "locale": "us",
                },  # duplicate ignored
            ]
        }
    )
    conn = FakeConn()
    result = await handle_ticker_type(make_job("ticker_type", {}), client, conn)
    assert result["rows_written"] == 2
    assert conn.committed == 1
    assert any("TRUNCATE raw_market.ticker_type" in s[0] for s in conn.statements)
    assert "raw_market.ticker_type" in conn.upsert_sqls()[0]
    rows = [s[1] for s in conn.statements if "INSERT INTO" in s[0]][0]
    assert rows[0][0] == "CS"
    assert rows[0][2] == "stocks"
    assert rows[1][0] == "ETF"


@pytest.mark.asyncio
async def test_ticker_type_empty_results_truncates() -> None:
    client = mock_client(fetch_ticker_types={"results": []})
    conn = FakeConn()
    result = await handle_ticker_type(make_job("ticker_type", {}), client, conn)
    assert result["rows_written"] == 0
    assert any("TRUNCATE raw_market.ticker_type" in s[0] for s in conn.statements)
    assert conn.committed == 1
