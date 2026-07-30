"""Tests for option_snapshot ingest handler."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from bifrost_market_data.ingest._upsert import daily_snapshot_anchor
from bifrost_market_data.ingest.option_snapshot import _snapshot_ts, handle_option_snapshot
from ingest_testutil import FakeConn, make_job, mock_client

_NY = ZoneInfo("America/New_York")


@pytest.mark.asyncio
async def test_option_snapshot_upsert() -> None:
    client = mock_client(
        fetch_options_snapshot={
            "results": [
                {
                    "details": {
                        "ticker": "O:AAPL250620C00150000",
                        "expiration_date": "2025-06-20",
                        "strike_price": 150,
                        "contract_type": "call",
                        "exercise_style": "american",
                        "shares_per_contract": 100,
                    },
                    "greeks": {"delta": 0.5, "gamma": 0.01, "theta": -0.02, "vega": 0.1},
                    "implied_volatility": 0.25,
                    "open_interest": 1234,
                    "day": {
                        "open": 1,
                        "high": 2,
                        "low": 0.5,
                        "close": 1.5,
                        "previous_close": 1.4,
                        "change_percent": 7.1,
                        "volume": 10,
                        "vwap": 1.2,
                        "last_updated": 1_704_153_600_000_000_000,
                    },
                    "underlying_asset": {"ticker": "AAPL"},
                }
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_option_snapshot(
        make_job("option_snapshot", {"underlying": "AAPL"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["contracts_written"] == 1
    sqls = "\n".join(conn.upsert_sqls())
    assert "market.option_contract" in sqls
    assert "market.option_snapshot" in sqls
    # snapshot row values
    snap_stmt = next(s for s in conn.statements if "option_snapshot" in s[0])
    row = snap_stmt[1][0]
    assert row[0] == "O:AAPL250620C00150000"
    assert row[3] == 0.25  # iv
    assert row[4] == 0.5  # delta
    assert row[8] == 1234  # oi
    assert conn.committed == 1  # single transaction for multi-table write


def test_snapshot_fallback_ts_is_stable_ny_session() -> None:
    """Missing trade/day timestamps use NY 16:00 daily anchor (not wall-clock now())."""
    ts1 = _snapshot_ts({"details": {"ticker": "O:AAPL250620C00150000"}})
    ts2 = _snapshot_ts({})
    expected = daily_snapshot_anchor()
    assert ts1 == expected
    assert ts2 == expected
    assert ts1.tzinfo is not None
    assert ts1.astimezone(_NY).hour == 16
    assert ts1.astimezone(_NY).minute == 0


def test_daily_snapshot_anchor_uses_ny_calendar_date() -> None:
    # 2024-06-20 02:00 UTC == 2024-06-19 22:00 NY → NY date is June 19
    utc_early = datetime(2024, 6, 20, 2, 0, tzinfo=timezone.utc)
    anchor = daily_snapshot_anchor(utc_early)
    assert anchor.astimezone(_NY).date().isoformat() == "2024-06-19"
    assert anchor.astimezone(_NY).hour == 16
