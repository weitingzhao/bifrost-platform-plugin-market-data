"""No symbol column is compared through ``UPPER(TRIM())``; inputs are normalised instead.

Every ``raw_market`` symbol column the API filters on leads an index, and the
wrapper hid it: a point question became a scan of the table, or of every
partition in range. On 2026-09-26 the premise was read from the data, not the
DDL — through each column's own index, every distinct value already equals its
``UPPER(TRIM())`` form (stock_daily, stock_minute, stock_snapshot, ticker,
ticker_related, corporate_action, the six financials tables behind
``stock_financials``, and the option tables' ``underlying``); ``option_right``
holds only 'C' and 'P' in option_contract, option_open_interest and
option_daily; and not one of option_daily's 38.6M ``option_ticker`` values
differs. So the bare comparison is the same answer, provided the input is
normalised — which is the half these tests pin.
"""

from __future__ import annotations

import pathlib
import re
from typing import Any, Self

import pytest

import bifrost_market_data
from bifrost_market_data.api import (
    chain_by_expiry,
    corp_actions,
    deps,
    fundamentals_db,
    fundamentals_sepa,
    pcr,
    readiness_data,
    reference_db,
    stocks_db,
)

SRC = pathlib.Path(bifrost_market_data.__file__).resolve().parent

#: ``UPPER(TRIM(col)) =`` / ``IN`` on the left of a comparison, or ``= UPPER(TRIM(``
#: on the right of one (a join).
WRAPPED_EQUALITY = re.compile(
    r"UPPER\(TRIM\(\s*[\w.{}]+\s*\)\)\s*(=|IN\b)|(=|\bIN)\s*\(?\s*UPPER\(TRIM\(",
    re.IGNORECASE,
)


def test_no_wrapped_symbol_comparison_anywhere() -> None:
    offenders = []
    for path in sorted(SRC.rglob("*.py")):
        lines = path.read_text().splitlines()
        for i, line in enumerate(lines, 1):
            if line.lstrip().startswith("#"):
                continue
            if WRAPPED_EQUALITY.search(line):
                offenders.append(f"{path.relative_to(SRC)}:{i}: {line.strip()}")
    assert not offenders, (
        "compare the bare column and normalise the input (normalize_symbol / "
        "normalize_symbols) — after checking the column is clean and indexed:\n"
        + "\n".join(offenders)
    )


def test_normalize_symbols_drops_blanks_and_duplicates_in_order() -> None:
    assert deps.normalize_symbols([" msft", "aapl ", "MSFT", "", "  "]) == ["MSFT", "AAPL"]
    assert deps.normalize_symbols(None) == []


# ── the calls themselves: bare SQL, normalised parameters ────────────────────


class _Cursor:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def execute(self, query: str, params: Any = None) -> None:
        self._conn.executed.append((" ".join(query.split()), params))

    def fetchone(self) -> Any:
        return None

    def fetchall(self) -> list[Any]:
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_a: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def rollback(self) -> None:
        return None


def _flat(params: Any) -> list[Any]:
    out: list[Any] = []
    for p in params or ():
        out.extend(p if isinstance(p, list) else [p])
    return out


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> _Conn:
    for module in (
        chain_by_expiry,
        corp_actions,
        fundamentals_db,
        fundamentals_sepa,
        pcr,
        readiness_data,
        stocks_db,
    ):
        if hasattr(module, "table_exists"):
            monkeypatch.setattr(module, "table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr(reference_db, "_related_table_exists", lambda *_a, **_k: True)
    return _Conn()


MIXED = [" aapl", "AAPL", "msft "]

CALLS = {
    "chain_by_expiry": lambda c: chain_by_expiry.query_chain_by_expiry(
        c, symbol=" aapl", fallback_date="2026-09-25"
    ),
    "pcr_oi": lambda c: pcr.query_pcr_aggregate(c, symbol=" aapl", pcr_type="oi"),
    "pcr_volume": lambda c: pcr.query_pcr_aggregate(c, symbol="aapl ", pcr_type="volume"),
    "corporate_actions": lambda c: corp_actions.query_corporate_actions(c, symbol=" aapl"),
    "short_interest": lambda c: fundamentals_db.query_short_interest(c, symbols=MIXED, settlements=3),
    "short_volume": lambda c: fundamentals_db.query_short_volume(c, symbols=MIXED, trade_days=5),
    "financials": lambda c: fundamentals_db.query_financials(
        c, symbol=" aapl", report_type=None, timeframe=None, limit=5
    ),
    "financials_batch": lambda c: fundamentals_sepa.query_financials_batch(
        c, symbols=MIXED, report_type="income_statement"
    ),
    "income_rows_for_sepa": lambda c: fundamentals_sepa.query_income_rows_for_sepa(c, symbol=" aapl"),
    "financials_ext_batch": lambda c: fundamentals_sepa.query_financials_ext_batch(
        c, symbols=MIXED, report_type="ratios"
    ),
    "ratios_latest_batch": lambda c: fundamentals_sepa.query_ratios_latest_batch(c, symbols=MIXED),
    "short_interest_latest": lambda c: fundamentals_sepa.query_short_interest_latest_batch(
        c, symbols=MIXED
    ),
    "short_volume_recent": lambda c: fundamentals_sepa.query_short_volume_recent_batch(
        c, symbols=MIXED
    ),
    "bars_coverage": lambda c: stocks_db.query_bars_coverage(c, symbols=MIXED),
    "latest_bar_per_symbol": lambda c: readiness_data.query_latest_bar_per_symbol(
        c, symbols=MIXED
    ),
    "latest_bar_full_history": lambda c: readiness_data.query_latest_bar_full_history(
        c, symbols=MIXED
    ),
    "financials_fill_rate": lambda c: readiness_data.query_financials_fill_rate(
        c, universe_symbols=MIXED
    ),
    "ticker_related": lambda c: reference_db.query_ticker_related(c, ticker=" aapl"),
}


@pytest.mark.parametrize("name", sorted(CALLS))
def test_reads_compare_bare_with_normalised_input(conn: _Conn, name: str) -> None:
    CALLS[name](conn)
    reads = [(q, p) for q, p in conn.executed if "information_schema" not in q.lower()]
    assert reads, "the call issued no query"
    sql = "\n".join(q for q, _ in reads)
    assert "UPPER(TRIM(" not in sql
    values = [v for _, p in reads for v in _flat(p) if isinstance(v, str)]
    assert "AAPL" in values
    assert not any(v != v.strip().upper() and v.strip().upper() in ("AAPL", "MSFT") for v in values)
    if name in {"short_interest", "short_volume", "financials_batch", "bars_coverage"}:
        # Duplicates collapse, order kept.
        lists = [p for _, p in reads for p in (p or ()) if isinstance(p, list)]
        assert ["AAPL", "MSFT"] in lists


@pytest.mark.parametrize("name", ["chain_by_expiry", "pcr_oi", "pcr_volume"])
def test_option_right_is_compared_as_stored(conn: _Conn, name: str) -> None:
    CALLS[name](conn)
    sql = "\n".join(q for q, _ in conn.executed)
    # char(1), 'C' / 'P' only — the 'CALL' / 'PUT' alternatives could never match.
    assert "option_right = 'P'" in sql
    assert "option_right = 'C'" in sql
    assert "'PUT'" not in sql and "'CALL'" not in sql
