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
                {
                    "date": "2024-07-04",
                    "status": "closed",
                    "name": "Independence Day",
                    "exchange": "NYSE",
                    "open": "2024-07-04T13:30:00.000Z",
                    "close": "2024-07-04T20:00:00.000Z",
                },
                {
                    "date": "2024-07-03",
                    "status": "early-close",
                    "name": "Early Close",
                    "exchange": "NYSE",
                    "open": "2024-07-03T13:30:00.000Z",
                    "close": "2024-07-03T18:00:00.000Z",
                },
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_calendar(make_job("calendar", {}), client, conn)
    assert result["rows_written"] == 2
    assert result["holiday_rows_written"] == 2
    sqls = conn.upsert_sqls()
    assert any("market.us_market_holiday" in s for s in sqls)
    assert not any("us_trading_calendar" in s for s in sqls)
    hol_stmt = next(s for s in conn.statements if "us_market_holiday" in s[0])
    hol_rows = {(r[0], str(r[1])): r for r in hol_stmt[1]}
    assert hol_rows[("NYSE", "2024-07-04")][3] == "closed"
    assert hol_rows[("NYSE", "2024-07-03")][3] == "early-close"


def test_registry_covers_all_pool_kinds() -> None:
    kinds = set(raw_handler_kinds())
    for pool_kinds in POOL_KINDS.values():
        for k in pool_kinds:
            assert k in kinds, f"missing handler for {k}"
    client = mock_client()
    registry = build_handler_registry(client, connect=FakeConn)
    assert set(registry.keys()) == kinds
