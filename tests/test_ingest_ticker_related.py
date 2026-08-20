"""Tests for ticker_related ingest handler."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.ticker_related import handle_ticker_related
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_ticker_related_writes_peers() -> None:
    client = mock_client(
        fetch_related_companies={
            "results": [
                {"ticker": "MSFT"},
                {"ticker": "GOOGL"},
                {"ticker": "MSFT"},  # duplicate ignored
            ]
        }
    )
    conn = FakeConn()
    result = await handle_ticker_related(
        make_job("ticker_related", {"symbol": "aapl"}),
        client,
        conn,
    )
    assert result["symbol"] == "AAPL"
    assert result["rows_written"] == 2
    assert result["peers"] == 2
    assert conn.committed == 1
    delete_sqls = [s[0] for s in conn.statements if "DELETE FROM market.ticker_related" in s[0]]
    assert len(delete_sqls) == 1
    assert "market.ticker_related" in conn.upsert_sqls()[0]
    rows = conn.statements[1][1]
    assert rows[0][0] == "AAPL"
    assert rows[0][1] == "MSFT"
    assert rows[0][2] == 0
    assert rows[1][1] == "GOOGL"
    assert rows[1][2] == 1


@pytest.mark.asyncio
async def test_ticker_related_requires_symbol() -> None:
    client = mock_client(fetch_related_companies={"results": []})
    conn = FakeConn()
    with pytest.raises(ValueError, match="requires symbol"):
        await handle_ticker_related(make_job("ticker_related", {}), client, conn)


@pytest.mark.asyncio
async def test_ticker_related_empty_results_clears() -> None:
    client = mock_client(fetch_related_companies={"results": []})
    conn = FakeConn()
    result = await handle_ticker_related(
        make_job("ticker_related", {"symbol": "AAPL"}),
        client,
        conn,
    )
    assert result["rows_written"] == 0
    assert result["peers"] == 0
    assert any("DELETE FROM market.ticker_related" in s[0] for s in conn.statements)
    assert conn.committed == 1
