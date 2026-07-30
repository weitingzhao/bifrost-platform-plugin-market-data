"""Tests for splits / dividends corporate_action handlers."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.corporate_action import handle_dividends, handle_splits
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_splits_upsert() -> None:
    client = mock_client(
        fetch_splits={
            "results": [
                {
                    "execution_date": "2020-08-31",
                    "split_from": 1,
                    "split_to": 4,
                    "adjustment_type": "split",
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_splits(make_job("splits", {"symbol": "AAPL"}), client, conn)
    assert result["rows_written"] == 1
    assert result["action_type"] == "split"
    row = conn.statements[0][1][0]
    assert row[0] == "AAPL"
    assert row[1] == "split"
    assert row[5] == 1.0
    assert row[6] == 4.0


@pytest.mark.asyncio
async def test_dividends_upsert() -> None:
    client = mock_client(
        fetch_dividends={
            "results": [
                {
                    "ex_dividend_date": "2024-05-10",
                    "record_date": "2024-05-13",
                    "pay_date": "2024-05-16",
                    "cash_amount": 0.25,
                    "currency": "USD",
                    "dividend_type": "CD",
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_dividends(make_job("dividends", {"symbol": "AAPL"}), client, conn)
    assert result["rows_written"] == 1
    assert result["action_type"] == "dividend"
    row = conn.statements[0][1][0]
    assert row[1] == "dividend"
    assert row[7] == 0.25
    assert row[8] == "USD"


@pytest.mark.asyncio
async def test_splits_idempotent_sql() -> None:
    client = mock_client(fetch_splits={"results": [], "pages": 1})
    conn = FakeConn()
    r1 = await handle_splits(make_job("splits", {"symbol": "MSFT"}), client, conn)
    r2 = await handle_splits(make_job("splits", {"symbol": "MSFT"}), client, conn)
    assert r1["rows_written"] == 0
    assert r2["rows_written"] == 0
