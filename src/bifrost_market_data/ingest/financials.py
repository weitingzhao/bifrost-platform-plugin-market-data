"""kind=financials → raw_market split entity tables, from the vendor's v1 statements.

One job is one symbol's whole history: the income statement, balance sheet and
cash-flow statement for every period the vendor has (back to about 2009).
``data`` holds the v1 record as it came: flat, standardized numbers such as
``revenue``, ``total_assets`` and ``net_cash_from_operating_activities``.

This replaced /vX/reference/financials, which the vendor retires on
2026-10-09. The two cannot be spliced: v1 restates EPS for later splits (KLAC
8.73 then, 0.87 now) and classifies banks differently, so a series the v1
source answers for (one statement, one period type) is rebuilt from v1 alone
and its legacy periods are deleted. A series it has nothing for keeps its
legacy rows, which readers accept alongside.
"""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_market_data.ingest._upsert import as_int, parse_date
from bifrost_market_data.ingest.financials_tables import (
    replace_symbol_statement,
    upsert_financials_rows,
)
from bifrost_market_data.symbol_void import clear_symbol_void, record_symbol_void
from bifrost_market_data.worker.claim import JobRow

# (v1 statement, report_type). Wave 1 hygiene: comprehensive_income is not
# written (unused by SEPA / dbt).
_STATEMENTS = (
    ("income-statements", "income_statement"),
    ("balance-sheets", "balance_sheet"),
    ("cash-flow-statements", "cash_flow_statement"),
)

# v1 spells trailing twelve months out; the tables and every reader say ttm.
_PERIOD_TYPES = {
    "quarterly": "quarterly",
    "annual": "annual",
    "trailing_twelve_months": "ttm",
}
_TIMEFRAMES = {period_type: timeframe for timeframe, period_type in _PERIOD_TYPES.items()}


def _current_cik(records: list[dict[str, Any]]) -> str | None:
    """The company that holds the ticker now: the one with the latest period.

    The vendor answers a ticker with every company that ever traded under it
    (ACIC: a SPAC in 2021, an insurer since 2023, and one quarter filed under a
    third CIK). The symbol is the table key, so one company fits under it;
    two would make every year-over-year figure compare different businesses.
    """
    best: tuple[str, str, str] | None = None
    for r in records:
        cik = r.get("cik")
        if not cik:
            continue
        key = (str(r.get("period_end") or ""), str(r.get("filing_date") or ""), str(cik))
        if best is None or key > best:
            best = key
    return best[2] if best else None


def _statement_rows(
    symbol: str, report_type: str, records: list[dict[str, Any]], cik: str | None
) -> tuple[list[tuple[Any, ...]], int]:
    """One row per (period_date, period_type); returns (rows, other-company records)."""
    kept: dict[tuple[Any, str], dict[str, Any]] = {}
    other_company = 0
    for r in records:
        if cik and r.get("cik") and str(r.get("cik")) != cik:
            other_company += 1
            continue
        period_date = parse_date(r.get("period_end"))
        period_type = _PERIOD_TYPES.get(str(r.get("timeframe") or ""))
        if period_date is None or period_type is None:
            continue
        # A fiscal-year relabel files one period twice with the same figures
        # (DECK 2013, NVDA 2010); keep the later filing.
        seen = kept.get((period_date, period_type))
        if seen is None or str(r.get("filing_date") or "") > str(seen.get("filing_date") or ""):
            kept[(period_date, period_type)] = r
    rows = [
        (
            symbol,
            report_type,
            period_date,
            period_type,
            as_int(r.get("fiscal_year")),
            as_int(r.get("fiscal_quarter")),
            r,
            # The latest filing that reported the period, often a restatement
            # a year later, not the first. A symbol's set of filing dates still
            # matches its 10-Q / 10-K dates (768 of 778 across 24 symbols), and
            # that set is what the earnings-event study reads.
            parse_date(r.get("filing_date")),
        )
        for (period_date, period_type), r in sorted(kept.items())
    ]
    return rows, other_company


async def handle_financials(job: JobRow, client: Any, conn: Any) -> Mapping[str, Any]:
    payload = job.payload or {}
    symbol = str(payload.get("symbol") or payload.get("ticker") or "").strip().upper()
    if not symbol:
        raise ValueError("financials payload requires symbol")
    period_type = str(payload.get("timeframe") or "").strip() or None
    timeframe = _TIMEFRAMES.get(period_type, period_type) if period_type else None

    answers: list[tuple[str, list[dict[str, Any]], Mapping[str, Any]]] = []
    for kind, report_type in _STATEMENTS:
        data = await client.fetch_financial_statements(kind, symbol, timeframe=timeframe)
        records = [r for r in (data.get("results") or []) if isinstance(r, dict)]
        answers.append((report_type, records, data))
    every_record = [r for _, records, _ in answers for r in records]
    cik = _current_cik(every_record)

    written = deleted = other_company = pages = 0
    truncated = False
    for report_type, records, data in answers:
        rows, dropped = _statement_rows(symbol, report_type, records, cik)
        other_company += dropped
        pages += int(data.get("pages") or 0)
        truncated = truncated or bool(data.get("truncated"))
        if timeframe is None and not data.get("truncated"):
            n, d = replace_symbol_statement(conn, symbol, report_type, rows)
        else:
            # Part of the history: what it leaves out is not gone.
            n, d = upsert_financials_rows(conn, rows), 0
        written += n
        deleted += d

    # An empty answer is a vendor void, not a transient miss: remember it so the
    # rotate stops putting this name first in line every day.
    if every_record:
        clear_symbol_void(conn, symbol, "financials")
    else:
        record_symbol_void(conn, symbol, "financials", note="vendor returned no statements")
    return {
        "rows_written": written,
        "rows_deleted": deleted,
        "other_company_rows": other_company,
        "cik": cik,
        "symbol": symbol,
        "void": not every_record,
        "truncated": truncated,
        "pages": pages,
    }
