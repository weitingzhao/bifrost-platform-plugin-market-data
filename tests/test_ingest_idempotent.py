"""Idempotency tests: re-running the same ingest yields the same conflict keys."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.option_snapshot import handle_option_snapshot
from bifrost_market_data.ingest.stock_daily import handle_stock_daily
from ingest_testutil import FakeConn, make_job, mock_client


def _stock_payload() -> dict:
    return {
        "results": [
            {
                "t": 1_704_153_600_000,
                "o": 1,
                "h": 2,
                "l": 0.5,
                "c": 1.5,
                "v": 100,
                "vw": 1.2,
                "n": 10,
            },
        ],
        "pages": 1,
        "truncated": False,
    }


def _snapshot_payload_no_ts() -> dict:
    """No last_trade / day.last_updated → forces daily anchor path."""
    return {
        "results": [
            {
                "details": {
                    "ticker": "O:AAPL250620C00150000",
                    "expiration_date": "2025-06-20",
                    "strike_price": 150,
                    "contract_type": "call",
                },
                "greeks": {"delta": 0.4},
                "implied_volatility": 0.2,
                "open_interest": 10,
                "day": {"close": 1.1, "volume": 1},
                "underlying_asset": {"ticker": "AAPL"},
            }
        ],
        "pages": 1,
    }


@pytest.mark.asyncio
async def test_stock_daily_rerun_same_conflict_keys() -> None:
    client = mock_client(fetch_stock_aggs=_stock_payload())
    job = make_job("stock_daily", {"symbol": "AAPL", "from": "2024-01-01", "to": "2024-01-31"})
    conn1, conn2 = FakeConn(), FakeConn()
    r1 = await handle_stock_daily(job, client, conn1)
    r2 = await handle_stock_daily(job, client, conn2)
    assert r1["rows_written"] == r2["rows_written"] == 1
    keys1 = [(row[0], str(row[1])) for row in conn1.statements[0][1]]
    keys2 = [(row[0], str(row[1])) for row in conn2.statements[0][1]]
    assert keys1 == keys2 == [("AAPL", "2024-01-02")]
    assert "ON CONFLICT (symbol, bar_date)" in conn1.upsert_sqls()[0]


@pytest.mark.asyncio
async def test_option_snapshot_rerun_same_snapshot_ts() -> None:
    client = mock_client(fetch_options_snapshot=_snapshot_payload_no_ts())
    job = make_job("option_snapshot", {"underlying": "AAPL"})
    conn1, conn2 = FakeConn(), FakeConn()
    r1 = await handle_option_snapshot(job, client, conn1)
    r2 = await handle_option_snapshot(job, client, conn2)
    assert r1["rows_written"] == r2["rows_written"] == 1
    snap1 = next(s for s in conn1.statements if "option_snapshot" in s[0])
    snap2 = next(s for s in conn2.statements if "option_snapshot" in s[0])
    # PK = (option_ticker, snapshot_ts)
    assert snap1[1][0][0] == snap2[1][0][0] == "O:AAPL250620C00150000"
    assert snap1[1][0][2] == snap2[1][0][2]
