"""Tests for extended financials handlers (ratios / short_interest / short_volume)."""

from __future__ import annotations

import pytest

from bifrost_market_data.ingest.financials_ext import (
    handle_ratios,
    handle_short_interest,
    handle_short_volume,
)
from ingest_testutil import FakeConn, make_job, mock_client


@pytest.mark.asyncio
async def test_ratios_upsert_period_end_date() -> None:
    client = mock_client(
        fetch_ratios={
            "results": [
                {
                    "period_end": "2024-03-31",
                    "pe_ratio": 22.5,
                    "roe": 0.18,
                },
                {
                    "period_end": "2024-06-30",
                    "pe_ratio": 24.1,
                    "roe": 0.20,
                },
            ],
        }
    )
    conn = FakeConn()
    result = await handle_ratios(make_job("ratios", {"symbol": "AAPL"}), client, conn)

    assert result["rows_written"] == 2
    assert result["symbol"] == "AAPL"
    sql = conn.statements[0][0]
    assert "market.stock_financials" in sql
    assert "::jsonb" in sql
    types = {r[1] for r in conn.statements[0][1]}
    assert types == {"ratios"}
    dates = {str(r[2]) for r in conn.statements[0][1]}
    assert dates == {"2024-03-31", "2024-06-30"}


@pytest.mark.asyncio
async def test_ratios_skips_rows_without_period_date() -> None:
    client = mock_client(
        fetch_ratios={
            "results": [
                {"pe_ratio": 10.0},  # no date fields
                {"end_date": "2024-01-01", "pe_ratio": 12.0},
            ]
        }
    )
    conn = FakeConn()
    result = await handle_ratios(make_job("ratios", {"symbol": "MSFT"}), client, conn)
    assert result["rows_written"] == 1


@pytest.mark.asyncio
async def test_ratios_missing_symbol_raises() -> None:
    client = mock_client(fetch_ratios={"results": []})
    with pytest.raises(ValueError, match="ratios payload requires symbol"):
        await handle_ratios(make_job("ratios", {}), client, FakeConn())


@pytest.mark.asyncio
async def test_short_interest_upsert() -> None:
    client = mock_client(
        fetch_short_interest={
            "results": [
                {
                    "settlement_date": "2024-08-15",
                    "short_interest": 12_345_000,
                    "days_to_cover": 3.4,
                }
            ]
        }
    )
    conn = FakeConn()
    result = await handle_short_interest(
        make_job("short_interest", {"symbol": "NVDA"}), client, conn
    )
    assert result["rows_written"] == 1
    row = conn.statements[0][1][0]
    assert row[0] == "NVDA"
    assert row[1] == "short_interest"
    assert row[3] == "biweekly"


@pytest.mark.asyncio
async def test_short_volume_upsert() -> None:
    client = mock_client(
        fetch_short_volume={
            "results": [
                {"date": "2024-08-19", "short_volume": 3_100_000, "total_volume": 8_400_000},
                {"date": "2024-08-20", "short_volume": 2_950_000, "total_volume": 7_900_000},
            ]
        }
    )
    conn = FakeConn()
    result = await handle_short_volume(
        make_job("short_volume", {"symbol": "TSLA"}), client, conn
    )
    assert result["rows_written"] == 2
    rows = conn.statements[0][1]
    assert {r[3] for r in rows} == {"daily"}
    assert {str(r[2]) for r in rows} == {"2024-08-19", "2024-08-20"}


@pytest.mark.asyncio
async def test_short_volume_empty_results_no_upsert() -> None:
    client = mock_client(fetch_short_volume={"results": []})
    conn = FakeConn()
    result = await handle_short_volume(
        make_job("short_volume", {"symbol": "TSLA"}), client, conn
    )
    assert result["rows_written"] == 0
    assert conn.statements == []


@pytest.mark.asyncio
async def test_registry_registers_new_handlers() -> None:
    from bifrost_market_data.ingest import raw_handler_kinds

    kinds = set(raw_handler_kinds())
    assert {"ratios", "short_interest", "short_volume"}.issubset(kinds)
