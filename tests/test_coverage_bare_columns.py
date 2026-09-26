"""Coverage reads compare and group symbol columns bare, and normalise the input.

Every ``raw_market`` table these routes read carries an index leading with its
symbol column; ``UPPER(TRIM(col))`` hid it, turning point questions into scans
and whole-table counts into on-disk sorts. On 2026-09-26 the distinct values of
all four columns were enumerated through those indexes and none differed from
its ``UPPER(TRIM())`` form, so the wrapper bought nothing.
"""

from __future__ import annotations

import inspect
import pathlib
import re
from datetime import date
from typing import Any, Self

import pytest

from bifrost_market_data.api import coverage

SRC = pathlib.Path(coverage.__file__).resolve().parents[1]

#: ``UPPER(TRIM(col)) = …`` / ``IN`` anywhere in the package. Only allowed to
#: fall: the sites left are outside coverage.py and each needs its own check
#: that the column is clean and indexed before it goes.
WRAPPED_EQUALITY = re.compile(r"UPPER\(TRIM\(\s*[\w.{}]+\s*\)\)\s*(=|IN\b)", re.IGNORECASE)
PACKAGE_BASELINE = 42


class _Cursor:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn
        self._row: Any = None
        self._rows: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        self._conn.executed.append((" ".join(query.split()), params))
        self._row, self._rows = self._conn.answer(query, params)

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        return self._rows

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

    def answer(self, query: str, params: Any) -> tuple[Any, list[Any]]:
        if "option_contract" in query and "= ANY" in query:
            return None, [("AAPL", 10, 3, None)]
        return (0, None, None), []

    def sql(self) -> str:
        return "\n".join(q for q, _ in self.executed)


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> _Conn:
    monkeypatch.setattr(coverage, "table_exists", lambda *_a, **_k: True)
    return _Conn()


def _code_without_comments(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def test_coverage_wraps_no_raw_market_column() -> None:
    body = _code_without_comments(inspect.getsource(coverage))
    sites = [m.start() for m in re.finditer(r"UPPER\(TRIM\(", body)]
    # The one left reads features.* — Research's tables, not verified here.
    assert len(sites) == 1
    assert "UPPER(TRIM(" in inspect.getsource(coverage._analytics_metric_summary)


def test_package_wrapped_equality_only_falls() -> None:
    total = sum(
        len(WRAPPED_EQUALITY.findall(path.read_text()))
        for path in SRC.rglob("*.py")
    )
    assert total <= PACKAGE_BASELINE, (
        f"{total} wrapped equality predicates (baseline {PACKAGE_BASELINE}); "
        "compare the bare column and normalise the input instead"
    )


@pytest.mark.parametrize(
    ("call", "expected_param"),
    [
        (lambda c: coverage.query_option_contracts_reference_gap(c, symbol=" aapl "), "AAPL"),
        (lambda c: coverage.query_option_snapshots_contracts_gap(c, symbol="aapl"), "AAPL"),
        (lambda c: coverage.query_option_bars_contracts_gap(c, symbol="Aapl "), "AAPL"),
        (lambda c: coverage.query_snapshot_quality_detail(c, symbol=" msft"), "MSFT"),
        (lambda c: coverage.query_bar_quality_detail(c, symbol="msft"), "MSFT"),
        (lambda c: coverage.query_greeks_coverage(c, symbol=" nvda "), "NVDA"),
    ],
)
def test_point_reads_compare_bare_with_normalised_input(
    conn: _Conn, call: Any, expected_param: str
) -> None:
    call(conn)
    assert conn.executed
    assert "UPPER(TRIM(" not in conn.sql()
    for _query, params in conn.executed:
        assert params is not None and expected_param in params


def test_stock_day_gap_compares_bare(conn: _Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    import bifrost_market_data.trading_calendar as tc

    monkeypatch.setattr(tc, "expected_trading_days", lambda *_a, **_k: [date(2026, 9, 25)])
    coverage.query_stock_day_gap(conn, symbol=" spy ")
    assert "WHERE symbol = %s" in conn.sql()
    assert conn.executed[-1][1][0] == "SPY"


def test_watchlist_coverage_normalises_whatever_the_source_returned(
    conn: _Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        coverage,
        "resolve_watchlist_with_source",
        lambda *_a, **_k: (["MSFT ", " aapl", "msft"], "option_contract_underlyings"),
    )
    out = coverage.query_watchlist_coverage(conn)
    query, params = conn.executed[-1]
    assert "WHERE underlying = ANY(%s)" in query
    # Normalised, de-duplicated, and still in the order the source ranked them.
    assert params == (["MSFT", "AAPL"],)
    assert [row["symbol"] for row in out["symbols"]] == ["MSFT", "AAPL"]
    by_symbol = {row["symbol"]: row for row in out["symbols"]}
    assert by_symbol["AAPL"]["contract_count"] == 10
    assert by_symbol["MSFT"]["contract_count"] == 0


@pytest.mark.parametrize("table", ["option_contract", "stock_daily"])
def test_distributions_group_bare(
    conn: _Conn, monkeypatch: pytest.MonkeyPatch, table: str
) -> None:
    monkeypatch.setattr(coverage, "resolve_market_schema", lambda *_a, **_k: "raw_market")
    coverage.query_distributions(conn, table=table)
    sql = conn.sql()
    assert "UPPER(TRIM(" not in sql
    assert "TRIM(" not in sql
