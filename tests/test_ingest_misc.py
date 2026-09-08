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


@pytest.mark.asyncio
async def test_full_market_fundamentals_and_corporate_handlers() -> None:
    from bifrost_market_data.ingest.corporate_action import handle_dividends_market, handle_splits_market
    from bifrost_market_data.ingest.financials_market import (
        handle_ratios_market,
        handle_short_interest_market,
        handle_short_volume_market,
    )
    from datetime import date as _date

    client = mock_client(
        fetch_ratios_market={"results": [{"ticker": "AAPL", "date": "2026-09-04", "return_on_equity": 1.5}, {"date": "2026-09-04"}], "pages": 6},
        fetch_short_volume_market={"results": [{"ticker": "AAPL", "date": "2026-09-04", "short_volume_ratio": 34.9}], "pages": 12},
        fetch_short_interest_market={"results": [{"ticker": "AAPL", "settlement_date": "2026-08-31", "short_interest": 1}], "pages": 3},
        fetch_dividends_market={"results": [{"ticker": "AAPL", "ex_dividend_date": "2026-09-10", "cash_amount": 0.25, "currency": "USD"}], "pages": 1},
        fetch_splits_market={"results": [{"ticker": "NVDA", "execution_date": "2026-09-08", "split_from": 1, "split_to": 4}], "pages": 1},
    )
    r = await handle_ratios_market(make_job("ratios_market", {"date": "2026-09-04"}), client, FakeConn())
    assert r["rows_written"] == 1 and r["pages"] == 6  # the row without a ticker is dropped
    r = await handle_short_volume_market(make_job("short_volume_market", {"date": "2026-09-04"}), client, FakeConn())
    assert r["rows_written"] == 1
    conn = FakeConn()
    r = await handle_short_interest_market(make_job("short_interest_market", {"settlement_date_gte": "2026-08-15"}), client, conn)
    assert r["rows_written"] == 1
    row = next(p for _, p in conn.statements if isinstance(p, list))[0]
    assert row[0] == "AAPL" and row[1] == _date(2026, 8, 31) and row[2] == "biweekly"
    conn = FakeConn()
    r = await handle_dividends_market(make_job("dividends_market", {"from": "2026-09-01", "to": "2026-10-30"}), client, conn)
    assert r["rows_written"] == 1
    client.fetch_dividends_market.assert_awaited_once_with("2026-09-01", "2026-10-30")
    r = await handle_splits_market(make_job("splits_market", {"from": "2026-09-01", "to": "2026-10-30"}), client, FakeConn())
    assert r["rows_written"] == 1


@pytest.mark.asyncio
async def test_whole_market_pull_fails_when_it_hits_the_page_cap() -> None:
    """A partial market is a failure the doctor can act on, not a quiet success."""
    from bifrost_market_data.ingest.financials_market import handle_short_interest_market

    client = mock_client(
        fetch_short_interest_market={
            "results": [{"ticker": "AAPL", "settlement_date": "2026-08-14", "short_interest": 1}],
            "pages": 30,
            "truncated": True,
        }
    )
    with pytest.raises(RuntimeError, match="page cap"):
        await handle_short_interest_market(
            make_job("short_interest_market", {"settlement_date_gte": "2026-07-01"}), client, FakeConn()
        )
