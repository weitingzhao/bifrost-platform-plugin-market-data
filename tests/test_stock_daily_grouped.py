"""Tests for stock_daily_grouped ingest handler."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.stock_daily_grouped import handle_stock_daily_grouped
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_stock_daily_grouped_upsert() -> None:
    # 2024-01-02 UTC
    client = mock_client(
        fetch_grouped_daily={
            "results": [
                {
                    "T": "AAPL",
                    "t": 1_704_153_600_000,
                    "o": 1,
                    "h": 2,
                    "l": 0.5,
                    "c": 1.5,
                    "v": 100,
                    "vw": 1.2,
                    "n": 10,
                },
                {
                    "T": "MSFT",
                    "t": 1_704_153_600_000,
                    "o": 10,
                    "h": 11,
                    "l": 9,
                    "c": 10.5,
                    "v": 200,
                    "vw": 10.1,
                    "n": 20,
                },
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_stock_daily_grouped(
        make_job(
            "stock_daily_grouped",
            {"from": "2024-01-02", "to": "2024-01-02", "market": "stocks"},
        ),
        client,
        conn,
    )
    assert result["rows_written"] == 2
    assert result["date"] == "2024-01-02"
    client.fetch_grouped_daily.assert_awaited_once_with(
        "2024-01-02", locale="us", market="stocks"
    )
    sql = conn.upsert_sqls()[0]
    assert "market.stock_daily" in sql
    assert "ON CONFLICT (symbol, bar_date)" in sql
    rows = conn.statements[0][1]
    symbols = {r[0] for r in rows}
    assert symbols == {"AAPL", "MSFT"}


@pytest.mark.asyncio
async def test_stock_daily_grouped_requires_from() -> None:
    with pytest.raises(ValueError, match="from"):
        await handle_stock_daily_grouped(
            make_job("stock_daily_grouped", {"market": "stocks"}),
            mock_client(),
            FakeConn(),
        )


@pytest.mark.asyncio
async def test_stock_daily_grouped_empty() -> None:
    client = mock_client(fetch_grouped_daily={"results": [], "pages": 1})
    conn = FakeConn()
    result = await handle_stock_daily_grouped(
        make_job("stock_daily_grouped", {"from": "2024-01-02"}),
        client,
        conn,
    )
    assert result["rows_written"] == 0
    assert conn.upsert_sqls() == []
