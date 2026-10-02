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
    assert [c.kwargs.get("adjusted", True) for c in client.fetch_grouped_daily.await_args_list] == [True, False]
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


def _bars(**closes: float) -> dict:
    return {
        "results": [
            {"T": sym, "t": 1_761_710_400_000, "o": c, "h": c, "l": c, "c": c, "v": 1, "vw": c, "n": 1}
            for sym, c in closes.items()
        ],
        "pages": 1,
    }


@pytest.mark.asyncio
async def test_grouped_stores_the_close_the_session_printed() -> None:
    """HON 2025-10-29: 200.65 adjusted for the 2025-10-30 spin-off, 212.89 as traded."""
    from unittest.mock import AsyncMock

    client = mock_client()

    async def grouped(date_str, *, locale="us", market="stocks", adjusted=True):
        return _bars(HON=200.6503) if adjusted else _bars(HON=212.89)

    client.fetch_grouped_daily = AsyncMock(side_effect=grouped)
    conn = FakeConn()
    await handle_stock_daily_grouped(make_job("stock_daily_grouped", {"from": "2025-10-29"}), client, conn)
    sql = conn.upsert_sqls()[0]
    assert "close_unadjusted = EXCLUDED.close_unadjusted" in sql
    row = conn.statements[0][1][0]
    assert row[5] == 200.6503 and row[-1] == 212.89


@pytest.mark.asyncio
async def test_the_unadjusted_backfill_touches_only_its_column() -> None:
    from bifrost_market_data.ingest.stock_daily_grouped import handle_stock_daily_unadjusted

    client = mock_client(fetch_grouped_daily=_bars(HON=212.89, AAPL=270.0))
    conn = FakeConn()
    result = await handle_stock_daily_unadjusted(
        make_job("stock_daily_unadjusted", {"from": "2025-10-29"}), client, conn
    )
    client.fetch_grouped_daily.assert_awaited_once_with(
        "2025-10-29", locale="us", market="stocks", adjusted=False
    )
    sql, params = conn.statements[0]
    flat = " ".join(sql.split())
    assert flat.startswith("UPDATE raw_market.stock_daily AS s SET close_unadjusted = v.close_unadjusted")
    assert "INSERT" not in sql and "fetched_at" not in sql and " close =" not in flat
    assert params == (["AAPL", "HON"], [270.0, 212.89], "2025-10-29")
    assert result["vendor_rows"] == 2


@pytest.mark.asyncio
async def test_the_unadjusted_backfill_needs_a_date() -> None:
    from bifrost_market_data.ingest.stock_daily_grouped import handle_stock_daily_unadjusted

    with pytest.raises(ValueError, match="from"):
        await handle_stock_daily_unadjusted(make_job("stock_daily_unadjusted", {}), mock_client(), FakeConn())
