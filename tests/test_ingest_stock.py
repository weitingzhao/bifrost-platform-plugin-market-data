"""Tests for stock_daily and stock_minute ingest handlers."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.stock_daily import handle_stock_daily
from bifrost_market_data.ingest.stock_minute import handle_stock_minute
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_stock_daily_upsert() -> None:
    # 2024-01-02 UTC
    client = mock_client(
        fetch_stock_aggs={
            "results": [
                {"t": 1_704_153_600_000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100, "vw": 1.2, "n": 10},
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_stock_daily(
        make_job("stock_daily", {"symbol": "aapl", "from": "2024-01-01", "to": "2024-01-31"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["symbol"] == "AAPL"
    client.fetch_stock_aggs.assert_awaited_once()
    sql = conn.upsert_sqls()[0]
    assert "market.stock_daily" in sql
    assert "ON CONFLICT (symbol, bar_date)" in sql
    rows = conn.statements[0][1]
    assert rows[0][0] == "AAPL"
    assert str(rows[0][1]) == "2024-01-02"


@pytest.mark.asyncio
async def test_stock_daily_empty_results() -> None:
    client = mock_client(fetch_stock_aggs={"results": [], "pages": 1})
    conn = FakeConn()
    result = await handle_stock_daily(
        make_job("stock_daily", {"symbol": "AAPL", "from": "2024-01-01", "to": "2024-01-02"}),
        client,
        conn,
    )
    assert result["rows_written"] == 0
    assert conn.upsert_sqls() == []


@pytest.mark.asyncio
async def test_stock_daily_requires_symbol() -> None:
    with pytest.raises(ValueError, match="symbol"):
        await handle_stock_daily(make_job("stock_daily", {"from": "a", "to": "b"}), mock_client(), FakeConn())


@pytest.mark.asyncio
async def test_stock_minute_upsert() -> None:
    client = mock_client(
        fetch_stock_aggs={
            "results": [
                {"t": 1_704_153_600_000, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10, "vw": 1.1, "n": 2},
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_stock_minute(
        make_job(
            "stock_minute",
            {
                "symbol": "AAPL",
                "from": "2024-01-02",
                "to": "2024-01-02",
                "multiplier": 5,
                "timespan": "minute",
            },
        ),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["period"] == "5 minute"
    sql = conn.upsert_sqls()[0]
    assert "market.stock_minute" in sql
    assert "ON CONFLICT (symbol, period, bar_time)" in sql
