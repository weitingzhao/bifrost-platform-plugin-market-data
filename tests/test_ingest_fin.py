"""Tests for the financials ingest handler (the vendor's v1 statements)."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from bifrost_market_data.ingest.financials import handle_financials
from bifrost_market_data.polygon import endpoints as ep
from ingest_testutil import FakeConn, make_job, mock_client

_CIK = "0000000101"


def _record(**over: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "cik": _CIK,
        "tickers": ["ZZFN"],
        "timeframe": "quarterly",
        "fiscal_year": 2026,
        "fiscal_quarter": 2,
        "period_end": "2026-06-30",
        "filing_date": "2026-08-05",
    }
    rec.update(over)
    return rec


def _client(
    income: list[dict[str, Any]] | None = None,
    balance: list[dict[str, Any]] | None = None,
    cash: list[dict[str, Any]] | None = None,
    **page: Any,
) -> Any:
    answers = {
        "income-statements": income or [],
        "balance-sheets": balance or [],
        "cash-flow-statements": cash or [],
    }
    client = mock_client()

    async def fetch(kind: str, ticker: str, *, timeframe: str | None = None) -> dict[str, Any]:
        return {"results": answers[kind], "pages": 1, "truncated": False, **page}

    client.fetch_financial_statements = AsyncMock(side_effect=fetch)
    return client


def _upserted(conn: FakeConn) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for sql, params in conn.statements:
        if "INSERT INTO raw_market." in sql and isinstance(params, list):
            rows.extend(params)
    return rows


def _deletes(conn: FakeConn) -> list[tuple[str, Any]]:
    return [(sql, params) for sql, params in conn.statements if "DELETE FROM raw_market." in sql]


@pytest.mark.asyncio
async def test_each_statement_lands_in_its_table_as_the_v1_record() -> None:
    client = _client(
        income=[_record(revenue=1000.0, basic_earnings_per_share=1.25)],
        balance=[_record(total_assets=5000.0)],
        cash=[_record(net_cash_from_operating_activities=300.0)],
    )
    conn = FakeConn()
    result = await handle_financials(make_job("financials", {"symbol": "zzfn"}), client, conn)

    assert result["rows_written"] == 3
    assert result["void"] is False
    assert result["cik"] == _CIK
    tables = [sql.split("INSERT INTO raw_market.")[1].split()[0] for sql in conn.upsert_sqls()]
    assert sorted(tables) == ["balance_sheet", "cash_flow", "income_statement"]
    income = next(r for r in _upserted(conn) if "revenue" in json.loads(r[5]))
    assert (income[0], str(income[1]), income[2]) == ("ZZFN", "2026-06-30", "quarterly")
    data = json.loads(income[5])
    # Stored as it came: flat numbers, no {value, unit} wrapper.
    assert data["revenue"] == 1000.0
    assert data["basic_earnings_per_share"] == 1.25
    assert str(income[6]) == "2026-08-05"
    # Every statement asked the vendor by tickers, the filter it honours.
    kinds = sorted(c.args[0] for c in client.fetch_financial_statements.await_args_list)
    assert kinds == ["balance-sheets", "cash-flow-statements", "income-statements"]
    assert all(c.args[1] == "ZZFN" for c in client.fetch_financial_statements.await_args_list)
    # A real answer clears any earlier void note for the symbol.
    assert any("delete from ops_jobs.symbol_source_void" in st[0].lower() for st in conn.statements)


@pytest.mark.asyncio
async def test_trailing_twelve_months_is_stored_as_ttm() -> None:
    client = _client(income=[_record(timeframe="trailing_twelve_months", revenue=4000.0)])
    conn = FakeConn()
    await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)
    (row,) = _upserted(conn)
    assert row[2] == "ttm"


@pytest.mark.asyncio
async def test_a_whole_history_replaces_the_symbols_periods() -> None:
    """Legacy periods v1 does not report are deleted in the same transaction."""
    client = _client(
        income=[
            _record(period_end="2026-03-31", fiscal_quarter=1, revenue=900.0),
            _record(timeframe="annual", period_end="2025-12-31", fiscal_quarter=4, revenue=3600.0),
        ]
    )
    conn = FakeConn()
    await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)

    (delete,) = _deletes(conn)
    assert "raw_market.income_statement" in delete[0]
    symbol, covered, dates, types = delete[1]
    assert symbol == "ZZFN"
    assert covered == ["annual", "quarterly"]
    assert [str(d) for d in dates] == ["2025-12-31", "2026-03-31"]
    assert types == ["annual", "quarterly"]
    # Delete and upsert commit together: one commit per statement written.
    delete_at = conn.statements.index(delete)
    assert "INSERT INTO raw_market.income_statement" in conn.statements[delete_at + 1][0]


@pytest.mark.asyncio
async def test_an_empty_answer_is_a_void_and_keeps_the_history() -> None:
    conn = FakeConn()
    result = await handle_financials(make_job("financials", {"symbol": "ZZFN"}), _client(), conn)
    assert result["rows_written"] == 0
    assert result["void"] is True
    assert _deletes(conn) == []
    void = next(st for st in conn.statements if "symbol_source_void" in st[0])
    assert "ON CONFLICT (symbol, data_type)" in void[0]
    assert void[1][:2] == ("ZZFN", "financials")


@pytest.mark.asyncio
async def test_a_statement_with_no_rows_keeps_its_table() -> None:
    """Income answered, balance sheet did not: only the income table is replaced."""
    client = _client(income=[_record(revenue=1000.0)])
    conn = FakeConn()
    await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)
    deletes = _deletes(conn)
    assert len(deletes) == 1
    assert "raw_market.income_statement" in deletes[0][0]


@pytest.mark.asyncio
async def test_a_period_type_with_no_rows_keeps_its_legacy_rows() -> None:
    """Annual rows and no quarters (a BDC): the quarterly series is left alone."""
    client = _client(income=[_record(timeframe="annual", period_end="2025-12-31", revenue=3600.0)])
    conn = FakeConn()
    await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)
    ((sql, params),) = _deletes(conn)
    assert "t.period_type = ANY(%s::text[])" in sql
    assert params[1] == ["annual"]


@pytest.mark.asyncio
async def test_only_the_company_holding_the_ticker_now_is_kept() -> None:
    """A ticker reused by another company must not splice two businesses."""
    old = "0000000202"
    client = _client(
        income=[
            _record(cik=old, period_end="2021-06-30", fiscal_year=2021, revenue=5.0),
            _record(cik=old, period_end="2025-09-30", fiscal_year=2025, revenue=7.0),
            _record(period_end="2025-09-30", fiscal_year=2025, revenue=800.0),
            _record(period_end="2026-06-30", revenue=820.0),
        ]
    )
    conn = FakeConn()
    result = await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)
    rows = _upserted(conn)
    assert sorted(json.loads(r[5])["revenue"] for r in rows) == [800.0, 820.0]
    assert result["other_company_rows"] == 2
    assert result["cik"] == _CIK


@pytest.mark.asyncio
async def test_a_period_filed_twice_keeps_the_later_filing() -> None:
    """A fiscal-year relabel reports one period under two labels."""
    client = _client(
        income=[
            _record(fiscal_year=2026, fiscal_quarter=2, filing_date="2026-08-05", revenue=100.0),
            _record(fiscal_year=2027, fiscal_quarter=1, filing_date="2027-08-04", revenue=100.0),
        ]
    )
    conn = FakeConn()
    await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)
    (row,) = _upserted(conn)
    assert (row[3], row[4], str(row[6])) == (2027, 1, "2027-08-04")


@pytest.mark.asyncio
async def test_one_timeframe_is_part_of_the_history_and_prunes_nothing() -> None:
    client = _client(income=[_record(timeframe="trailing_twelve_months", revenue=4000.0)])
    conn = FakeConn()
    await handle_financials(make_job("financials", {"symbol": "ZZFN", "timeframe": "ttm"}), client, conn)
    assert _deletes(conn) == []
    assert len(_upserted(conn)) == 1
    call = client.fetch_financial_statements.await_args_list[0]
    assert call.kwargs["timeframe"] == "trailing_twelve_months"


@pytest.mark.asyncio
async def test_a_truncated_answer_prunes_nothing() -> None:
    client = _client(income=[_record(revenue=1000.0)], truncated=True)
    conn = FakeConn()
    result = await handle_financials(make_job("financials", {"symbol": "ZZFN"}), client, conn)
    assert result["truncated"] is True
    assert _deletes(conn) == []
    assert len(_upserted(conn)) == 1


def test_statements_filter_on_tickers() -> None:
    """``ticker`` is ignored by the vendor and answers with other companies."""
    params = ep.financial_statement_params(ticker="zzfn", timeframe="annual", limit=5000)
    assert params["tickers"] == "ZZFN"
    assert "ticker" not in params
    assert params["limit"] == 1000
