"""Tests for option_daily, option_minute, and option_trades ingest handlers."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.option_daily import handle_option_daily
from bifrost_market_data.ingest.option_minute import handle_option_minute
from bifrost_market_data.ingest.option_trades import handle_option_trades
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
async def test_option_trades_upsert() -> None:
    # 2024-01-02 15:00 ET ≈ 2024-01-02 20:00 UTC
    sip_ns = 1_704_217_200_000_000_000
    client = mock_client(
        fetch_option_trades={
            "results": [
                {
                    "sip_timestamp": sip_ns,
                    "sequence_number": 42,
                    "price": 1.25,
                    "size": 10,
                    "exchange": 46,
                    "conditions": [209],
                    "participant_timestamp": sip_ns,
                }
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_option_trades(
        make_job(
            "option_trades",
            {"option_ticker": OT, "from": "2024-01-02", "to": "2024-01-02"},
        ),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["underlying"] == "AAPL"
    assert "market.option_trades" in conn.upsert_sqls()[0]
    row = conn.statements[0][1][0]
    assert row[0] == OT
    assert row[7] == 42
    assert float(row[8]) == 1.25
    assert row[9] == 10


@pytest.mark.asyncio
async def test_option_daily_invalid_ticker() -> None:
    with pytest.raises(ValueError, match="option_ticker"):
        await handle_option_daily(
            make_job("option_daily", {"option_ticker": "BAD", "from": "a", "to": "b"}),
            mock_client(),
            FakeConn(),
        )
