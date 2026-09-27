"""Tests for ticker_sync ingest handler."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.ticker_sync import handle_ticker_sync
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_ticker_sync_universe() -> None:
    client = mock_client(
        fetch_reference_tickers={
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                    "currency_name": "usd",
                    "cik": "320193",
                    "composite_figi": "BBG000B9XRY4",
                }
            ],
            "pages": 1,
            "truncated": False,
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "universe"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["mode"] == "universe"
    assert "market.ticker" in conn.upsert_sqls()[0]
    # A complete listing deactivates names the vendor no longer lists.
    deact = [st for st in conn.statements if "set active = false" in st[0].lower()]
    assert len(deact) == 1
    assert deact[0][1] == ("stocks", "CS", "CS", ["AAPL"])


@pytest.mark.asyncio
async def test_ticker_sync_universe_truncated_does_not_deactivate() -> None:
    client = mock_client(
        fetch_reference_tickers={
            "results": [{"ticker": "AAPL", "market": "stocks", "type": "CS", "active": True}],
            "pages": 100,
            "truncated": True,
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(make_job("ticker_sync", {"mode": "universe"}), client, conn)
    assert result["deactivated"] == 0
    assert not any("set active = false" in st[0].lower() for st in conn.statements)


@pytest.mark.asyncio
async def test_universe_does_not_overwrite_detail_fields() -> None:
    client = mock_client(
        fetch_reference_tickers={
            "results": [
                {
                    "ticker": "AAPL",
                    "name": "Apple",
                    "market": "stocks",
                    "locale": "us",
                    "primary_exchange": "XNAS",
                    "type": "CS",
                    "active": True,
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    await handle_ticker_sync(make_job("ticker_sync", {"mode": "universe"}), client, conn)
    sql = conn.upsert_sqls()[0]
    # Universe ON CONFLICT must not clobber detail-only overview columns
    for col in (
        "sector",
        "industry",
        "market_cap",
        "description",
        "homepage_url",
        "total_employees",
        "sic_code",
        "list_date",
    ):
        assert f"{col} = EXCLUDED.{col}" not in sql
    for col in ("name", "market", "locale", "primary_exchange", "instrument_type", "active"):
        assert f"{col} = EXCLUDED.{col}" in sql
    # list row should leave sector/industry as None (not empty string)
    row = conn.statements[0][1][0]
    assert row[11] is None  # sector
    assert row[12] is None  # industry


@pytest.mark.asyncio
async def test_ticker_sync_detail() -> None:
    client = mock_client(
        fetch_ticker_details={
            "results": {
                "ticker": "AAPL",
                "name": "Apple Inc",
                "market": "stocks",
                "locale": "us",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": True,
                "market_cap": 3e12,
                "sic_code": "3571",
                "description": "Consumer electronics",
                "list_date": "1980-12-12",
                "homepage_url": "https://www.apple.com",
                "total_employees": 160000,
            }
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "detail", "symbol": "aapl"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1
    assert result["symbol"] == "AAPL"
    row = conn.statements[0][1][0]
    assert row[0] == "AAPL"
    assert row[13] == 3e12  # market_cap


# ── delisted mode ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delisted_mode_stores_the_retired_listing() -> None:
    """The universe walk asks for active names, so a symbol already gone when it
    first ran has no row — and without one, no rename through it can be proven,
    because the proof is the old symbol's CIK and FIGI.

    SATS is the case: delisted 2026-06-24, and its 32,263 pre-rename option_daily
    rows stayed split from ECHO's because nothing could show the pair was a
    rename. `/v3/reference/tickers/{symbol}` 404s on a retired listing, so this is
    the list form with an exact ticker and active=false.
    """
    client = mock_client(
        fetch_reference_tickers={
            "results": [
                {
                    "ticker": "SATS",
                    "name": "EchoStar Corporation",
                    "type": "CS",
                    "active": False,
                    "cik": "0001415404",
                    "composite_figi": "BBG000TGLV00",
                    "delisted_utc": "2026-06-24",
                }
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "delisted", "symbol": "SATS"}),
        client,
        conn,
    )
    assert result == {
        "rows_written": 1,
        "mode": "delisted",
        "symbol": "SATS",
        "delisted": True,
    }


@pytest.mark.asyncio
async def test_an_answer_of_nothing_writes_nothing_and_says_so() -> None:
    """Five of the seven symbols in that state are not retired listings at all —
    SPY, QQQ and IWM are ETFs the CS-only walk never writes, SPX is an index and
    SW1 an adjusted root. Asking about them has to be free, because the caller's
    selection is deliberately loose: the vendor judges what is retired.
    """
    client = mock_client(fetch_reference_tickers={"results": [], "pages": 1})
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "delisted", "symbol": "SW1"}),
        client,
        conn,
    )
    assert result["rows_written"] == 0
    assert result["delisted"] is False, "absence is an answer: do not look for a rename"


@pytest.mark.asyncio
async def test_delisted_mode_ignores_a_row_for_another_symbol() -> None:
    """An unfiltered type means the answer can carry more than was asked for."""
    client = mock_client(
        fetch_reference_tickers={
            "results": [
                {"ticker": "SATSW", "name": "warrant", "active": False},
                {"ticker": "SATS", "name": "EchoStar", "active": False, "cik": "0001415404"},
            ],
            "pages": 1,
        }
    )
    conn = FakeConn()
    result = await handle_ticker_sync(
        make_job("ticker_sync", {"mode": "delisted", "symbol": "SATS"}),
        client,
        conn,
    )
    assert result["rows_written"] == 1, "only the exact ticker"


@pytest.mark.asyncio
async def test_delisted_mode_needs_a_symbol() -> None:
    client = mock_client(fetch_reference_tickers={"results": []})
    with pytest.raises(ValueError):
        await handle_ticker_sync(
            make_job("ticker_sync", {"mode": "delisted"}), client, FakeConn()
        )


def test_the_nightly_walk_cannot_erase_a_retirement_date() -> None:
    """This is the one that would have bitten quietly.

    The universe list asks for active listings and those never report a
    delisted_utc, so putting the column in the on-conflict update set would null
    out what the delisted lookup stored — every night, invisibly, and the repair
    that depends on it would just stop finding pairs again.
    """
    from bifrost_market_data.ingest.ticker_sync import _COLS, _UNIVERSE_UPDATE_COLS

    assert "delisted_utc" in _COLS, "the delisted lookup writes it"
    assert "delisted_utc" not in _UNIVERSE_UPDATE_COLS, "and the walk must not touch it"


def test_the_client_can_ask_about_a_retired_symbol() -> None:
    """The param builder always accepted `ticker`; nothing passed it through."""
    import inspect

    from bifrost_market_data.polygon.client import PolygonClient

    sig = inspect.signature(PolygonClient.fetch_reference_tickers)
    assert "ticker" in sig.parameters
