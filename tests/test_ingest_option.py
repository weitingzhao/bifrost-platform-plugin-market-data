"""Tests for option_daily and option_minute ingest handlers."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.option_daily import handle_option_daily
from bifrost_market_data.ingest.option_minute import handle_option_minute
from ingest_testutil import FakeConn, make_job, mock_client

OT = "O:AAPL250620C00150000"


@pytest.mark.asyncio
async def test_option_daily_upsert() -> None:
    client = mock_client(
        fetch_stock_aggs={
            "results": [
                {"t": 1_704_153_600_000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 5, "vw": 1.1, "n": 1},
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_option_daily(
        make_job("option_daily", {"option_ticker": OT, "from": "2024-01-01", "to": "2024-01-31"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["underlying"] == "AAPL"
    sql = conn.upsert_sqls()[0]
    assert "market.option_daily" in sql
    row = conn.statements[0][1][0]
    assert row[0] == OT
    assert row[1] == "AAPL"
    assert row[4] == "C"
    assert float(row[3]) == 150.0


@pytest.mark.asyncio
async def test_option_minute_upsert() -> None:
    client = mock_client(
        fetch_stock_aggs={
            "results": [{"t": 1_704_153_600_000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_option_minute(
        make_job(
            "option_minute",
            {"option_ticker": OT, "from": "2024-01-02", "to": "2024-01-02", "multiplier": 1},
        ),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["period"] == "1 minute"
    assert "market.option_minute" in conn.upsert_sqls()[0]


@pytest.mark.asyncio
async def test_option_daily_invalid_ticker() -> None:
    with pytest.raises(ValueError, match="option_ticker"):
        await handle_option_daily(
            make_job("option_daily", {"option_ticker": "BAD", "from": "a", "to": "b"}),
            mock_client(),
            FakeConn(),
        )
