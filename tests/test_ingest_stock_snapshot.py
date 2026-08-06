"""Tests for stock_snapshot and stock_movers ingest handlers."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.stock_movers import handle_stock_movers
from bifrost_market_data.ingest.stock_snapshot import handle_stock_snapshot
from ingest_testutil import FakeConn, make_job, mock_client


def _sample_ticker(symbol: str = "AAPL", change_pct: float = 1.5) -> dict:
    return {
        "ticker": symbol,
        "todaysChange": 2.25,
        "todaysChangePerc": change_pct,
        "day": {"o": 100.0, "h": 105.0, "l": 99.0, "c": 102.25, "v": 1_000_000, "vw": 101.5},
        "prevDay": {"c": 100.0},
    }


@pytest.mark.asyncio
async def test_stock_snapshot_all_upsert() -> None:
    client = mock_client(
        fetch_stock_snapshot_all={
            "status": "OK",
            "tickers": [_sample_ticker("AAPL"), _sample_ticker("MSFT", 0.5)],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_stock_snapshot(
        make_job("stock_snapshot", {"mode": "all", "session_date": "2024-06-20"}),
        client,
        conn,
    )
    assert result["rows_written"] == 2
    assert result["mode"] == "all"
    assert result["session_date"] == "2024-06-20"
    assert "market.stock_snapshot" in conn.upsert_sqls()[0]
    rows = conn.statements[0][1]
    assert rows[0][0] == "AAPL"
    assert rows[0][1].isoformat() == "2024-06-20"
    assert rows[0][2] == 100.0  # open
    assert rows[0][8] == 100.0  # prev_close
    assert rows[0][10] == 1.5  # change_pct
    client.fetch_stock_snapshot_all.assert_awaited_once()
    client.fetch_stock_snapshot_single.assert_not_called()


@pytest.mark.asyncio
async def test_stock_snapshot_single() -> None:
    client = mock_client(
        fetch_stock_snapshot_single={
            "status": "OK",
            "ticker": _sample_ticker("AAPL"),
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_stock_snapshot(
        make_job("stock_snapshot", {"symbol": "aapl", "session_date": "2024-06-21"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["mode"] == "single"
    client.fetch_stock_snapshot_single.assert_awaited_once_with("AAPL")
    client.fetch_stock_snapshot_all.assert_not_called()


@pytest.mark.asyncio
async def test_stock_snapshot_empty() -> None:
    client = mock_client(fetch_stock_snapshot_all={"status": "OK", "tickers": [], "pages": 1})
    conn = FakeConn()
    result = await handle_stock_snapshot(make_job("stock_snapshot", {}), client, conn)
    assert result["rows_written"] == 0
    assert conn.upsert_sqls() == []


@pytest.mark.asyncio
async def test_stock_movers_both_directions() -> None:
    client = mock_client(
        fetch_stock_gainers_losers={
            "status": "OK",
            "tickers": [_sample_ticker("XYZ", 8.0)],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_stock_movers(
        make_job("stock_movers", {"direction": "both", "session_date": "2024-06-20"}),
        client,
        conn,
    )
    assert result["rows_written"] == 2  # gainers + losers each return one ticker
    assert result["directions"] == ["gainers", "losers"]
    assert "market.stock_movers" in conn.upsert_sqls()[0]
    assert client.fetch_stock_gainers_losers.await_count == 2


@pytest.mark.asyncio
async def test_stock_movers_gainers_only() -> None:
    client = mock_client(
        fetch_stock_gainers_losers={
            "status": "OK",
            "tickers": [_sample_ticker("GAIN", 12.0)],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_stock_movers(
        make_job(
            "stock_movers",
            {"direction": "gainers", "session_date": "2024-06-20"},
        ),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["directions"] == ["gainers"]
    row = conn.statements[0][1][0]
    assert row[0] == "gainers"
    assert row[1] == "GAIN"
    assert row[3] == 12.0
    assert row[4] == 102.25  # day close as price


@pytest.mark.asyncio
async def test_stock_movers_invalid_direction() -> None:
    with pytest.raises(ValueError, match="direction"):
        await handle_stock_movers(
            make_job("stock_movers", {"direction": "flat"}),
            mock_client(),
            FakeConn(),
        )
