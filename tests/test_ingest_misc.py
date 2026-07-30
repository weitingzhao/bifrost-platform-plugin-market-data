"""Tests for option_expiration, option_oi, and calendar handlers."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.calendar import handle_calendar
from bifrost_market_data.ingest.option_expiration import handle_option_expiration
from bifrost_market_data.ingest.option_oi import handle_option_open_interest
from bifrost_market_data.ingest import build_handler_registry, raw_handler_kinds
from bifrost_market_data.worker.loop import POOL_KINDS
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_option_expiration() -> None:
    client = mock_client(
        fetch_options_contracts={
            "results": [
                {"ticker": "O:AAPL250620C00150000", "underlying_ticker": "AAPL", "expiration_date": "2025-06-20"},
                {"ticker": "O:AAPL250620P00150000", "underlying_ticker": "AAPL", "expiration_date": "2025-06-20"},
                {"ticker": "O:AAPL250718C00150000", "underlying_ticker": "AAPL", "expiration_date": "2025-07-18"},
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_option_expiration(
        make_job("option_expiration", {"underlying": "AAPL"}),
        client,
        conn,
    )
    assert result["rows_written"] == 2
    assert "market.option_expiration" in conn.upsert_sqls()[0]


@pytest.mark.asyncio
async def test_option_open_interest() -> None:
    client = mock_client(
        fetch_options_snapshot={
            "results": [
                {
                    "details": {
                        "ticker": "O:AAPL250620C00150000",
                        "expiration_date": "2025-06-20",
                        "strike_price": 150,
                        "contract_type": "call",
                    },
                    "open_interest": 999,
                    "underlying_asset": {"ticker": "AAPL"},
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_option_open_interest(
        make_job("option_open_interest", {"underlying": "AAPL", "trade_date": "2024-06-20"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["trade_date"] == "2024-06-20"
    row = conn.statements[0][1][0]
    assert row[6] == 999
    assert "market.option_open_interest" in conn.upsert_sqls()[0]


@pytest.mark.asyncio
async def test_calendar() -> None:
    client = mock_client(
        fetch_market_status_upcoming={
            "results": [
                {"date": "2024-07-04", "status": "closed", "name": "Independence Day", "exchange": "NYSE"},
                {"date": "2024-07-03", "status": "early-close", "name": "Early Close", "exchange": "NYSE"},
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_calendar(make_job("calendar", {}), client, conn)
    assert result["rows_written"] == 2
    assert "data_ops.us_trading_calendar" in conn.upsert_sqls()[0]
    rows = {str(r[0]): r for r in conn.statements[0][1]}
    assert rows["2024-07-04"][1] is False
    assert rows["2024-07-03"][1] is True  # early-close still trading day flag True


def test_registry_covers_all_pool_kinds() -> None:
    kinds = set(raw_handler_kinds())
    for pool_kinds in POOL_KINDS.values():
        for k in pool_kinds:
            assert k in kinds, f"missing handler for {k}"
    client = mock_client()
    registry = build_handler_registry(client, connect=FakeConn)
    assert set(registry.keys()) == kinds
