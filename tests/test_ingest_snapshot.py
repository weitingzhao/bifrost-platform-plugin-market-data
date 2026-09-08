"""Tests for option_snapshot ingest handler."""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from bifrost_market_data.ingest._upsert import daily_snapshot_anchor, session_anchor
from bifrost_market_data.ingest import option_snapshot as mod
from bifrost_market_data.ingest.option_snapshot import _last_trade_ts, handle_option_snapshot
from ingest_testutil import FakeConn, make_job, mock_client

_NY = ZoneInfo("America/New_York")


@pytest.fixture
def chain_on(monkeypatch: pytest.MonkeyPatch):
    """Pin the session the vendor chain is pretending to reflect."""

    def _set(session: date) -> None:
        monkeypatch.setattr(mod, "chain_session", lambda conn, now=None: session)

    return _set


@pytest.mark.asyncio
async def test_option_snapshot_upsert(chain_on) -> None:
    chain_on(date(2024, 6, 20))
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
        make_job("option_snapshot", {"underlying": "AAPL", "trade_date": "2024-06-20"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["contracts_written"] == 1
    # The session's open interest comes out of the same download.
    assert result["oi_rows_written"] == 1
    assert result["trade_date"] == "2024-06-20"
    assert result["freshness_extra"] == {"option_open_interest": 1}
    sqls = "\n".join(conn.upsert_sqls())
    assert "market.option_contract" in sqls
    assert "market.option_snapshot" in sqls
    assert "market.option_open_interest" in sqls
    oi_stmt = next(s for s in conn.statements if "option_open_interest" in s[0])
    oi_row = oi_stmt[1][0]
    assert oi_row[0] == "O:AAPL250620C00150000"
    assert oi_row[5] == date(2024, 6, 20)
    assert oi_row[6] == 1234
    # snapshot row values
    snap_stmt = next(s for s in conn.statements if "option_snapshot" in s[0])
    row = snap_stmt[1][0]
    assert row[0] == "O:AAPL250620C00150000"
    # Every row of the session's chain carries the session anchor, not the
    # contract's own last trade time — that moved to last_trade_ts.
    assert row[2] == session_anchor(date(2024, 6, 20))
    assert result["observed_at"] == session_anchor(date(2024, 6, 20)).isoformat()
    assert row[3] == datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)  # day.last_updated (ns)
    assert row[4] == 0.25  # iv
    assert row[5] == 0.5  # delta
    assert row[9] == 1234  # oi
    assert conn.committed == 1  # single transaction for multi-table write


@pytest.mark.asyncio
async def test_option_snapshot_oi_defaults_to_session_anchor(chain_on) -> None:
    chain_on(daily_snapshot_anchor().date())
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
                    "open_interest": 7,
                    "day": {"close": 1.1},
                },
                {
                    # No OI on this contract → snapshot row only, no OI row.
                    "details": {
                        "ticker": "O:AAPL250620P00150000",
                        "expiration_date": "2025-06-20",
                        "strike_price": 150,
                        "contract_type": "put",
                    },
                    "day": {"close": 0.9},
                },
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_option_snapshot(make_job("option_snapshot", {"underlying": "AAPL"}), client, conn)
    assert result["rows_written"] == 2
    assert result["oi_rows_written"] == 1
    assert result["trade_date"] == daily_snapshot_anchor().date().isoformat()
    assert conn.committed == 1


def test_last_trade_ts_is_none_when_the_contract_never_traded() -> None:
    """A quiet contract has no last trade time — it must not key the row."""
    assert _last_trade_ts({"details": {"ticker": "O:AAPL250620C00150000"}}) is None
    assert _last_trade_ts({}) is None


def test_last_trade_ts_prefers_the_sip_timestamp() -> None:
    item = {
        "last_trade": {"sip_timestamp": 1_704_153_600_000_000_000},
        "day": {"last_updated": 1_704_240_000_000_000_000},
    }
    assert _last_trade_ts(item) == datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)
    assert _last_trade_ts({"day": {"last_updated": 1_704_240_000_000_000_000}}) == datetime(
        2024, 1, 3, 0, 0, tzinfo=timezone.utc
    )


@pytest.mark.asyncio
async def test_catch_up_run_writes_the_session_it_heals(chain_on) -> None:
    chain_on(date(2026, 9, 4))
    """A run today for an older session keys rows to that session, not to today."""
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
                    "last_trade": {"sip_timestamp": 1_704_153_600_000_000_000},
                    "open_interest": 5,
                    "day": {"close": 1.0},
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_option_snapshot(
        make_job("option_snapshot", {"underlying": "AAPL", "trade_date": "2026-09-04"}),
        client,
        conn,
    )
    snap_row = next(s for s in conn.statements if "option_snapshot" in s[0])[1][0]
    oi_row = next(s for s in conn.statements if "option_open_interest" in s[0])[1][0]
    assert snap_row[2] == session_anchor(date(2026, 9, 4))
    assert oi_row[5] == date(2026, 9, 4)
    assert result["observed_at"] == session_anchor(date(2026, 9, 4)).isoformat()


@pytest.mark.asyncio
async def test_intraday_run_keys_rows_to_the_observation_instant() -> None:
    """Several observations a session must not collapse onto one anchor."""
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
                    "day": {"close": 1.0},
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    observed = "2026-09-04T14:30:00+00:00"
    result = await handle_option_snapshot(
        make_job(
            "option_snapshot",
            {"underlying": "AAPL", "trade_date": "2026-09-04", "intraday": True, "observed_at": observed},
        ),
        client,
        conn,
    )
    snap_row = next(s for s in conn.statements if "option_snapshot" in s[0])[1][0]
    assert snap_row[2] == datetime(2026, 9, 4, 14, 30, tzinfo=timezone.utc)
    assert snap_row[2] != session_anchor(date(2026, 9, 4))
    assert result["observed_at"] == observed


def test_daily_snapshot_anchor_uses_ny_calendar_date() -> None:
    # 2024-06-20 02:00 UTC == 2024-06-19 22:00 NY → NY date is June 19
    utc_early = datetime(2024, 6, 20, 2, 0, tzinfo=timezone.utc)
    anchor = daily_snapshot_anchor(utc_early)
    assert anchor.astimezone(_NY).date().isoformat() == "2024-06-19"
    assert anchor.astimezone(_NY).hour == 16


@pytest.mark.asyncio
async def test_snapshot_refuses_a_session_the_chain_no_longer_shows(chain_on) -> None:
    """Once Monday opens, Friday's chain is gone — do not label Monday's as Friday."""
    chain_on(date(2026, 9, 8))
    client = mock_client(fetch_options_snapshot={"results": [], "pages": 1})
    conn = FakeConn()
    result = await handle_option_snapshot(
        make_job("option_snapshot", {"underlying": "AAPL", "trade_date": "2026-09-04"}),
        client,
        conn,
    )
    assert result["skipped"] is True
    assert result["reason"] == "stale_session"
    assert "2026-09-08" in result["detail"]
    assert result["rows_written"] == 0
    assert conn.statements == []
    client.fetch_options_snapshot.assert_not_awaited()
