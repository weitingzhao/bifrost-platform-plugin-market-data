"""Tests for query_date_coverage — thin sessions, and the ones held not at all.

The check used to end in ``HAVING count < threshold``, which ranks only dates
the table already has rows for. A session with no row could not appear in its
own answer at all — so the answer could not distinguish "held thinly" from
"not held", and only ever spoke about the first.

Measured 2026-09-25 against the live store: no session in 500 days is fully
absent. The fix closes the blind spot before something moves into it.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from bifrost_market_data.api import readiness_data as mod


def _conn(
    *,
    relations: set[tuple[str, str]],
    held: dict[str, int] | None = None,
    holidays: tuple[date, ...] = (),
) -> Any:
    """A connection that answers existence per ``(schema, name)``.

    ``held`` maps ``YYYY-MM-DD`` to the distinct-symbol count ``stock_daily``
    holds for that session; a date absent from it holds nothing, which is
    exactly the case the old query could not express.
    """
    rows = sorted((d, n) for d, n in (held or {}).items())

    class _Cursor:
        def __init__(self) -> None:
            self._rows: list[Any] = []

        def execute(self, query: str, params: Any = None) -> None:
            q = " ".join(query.split()).lower()
            if "information_schema.tables" in q:
                self._rows = [(1,)] if tuple(params or ()) in relations else []
            elif "from raw_market.stock_daily" in q:
                self._rows = list(rows)
            elif "from raw_market.us_market_holiday" in q:
                self._rows = [(d,) for d in holidays]
            else:
                self._rows = []

        def fetchone(self) -> Any:
            return self._rows[0] if self._rows else None

        def fetchall(self) -> list[Any]:
            return self._rows

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *a: object) -> None:
            return None

    class _Conn:
        def cursor(self) -> "_Cursor":
            return _Cursor()

        def close(self) -> None:
            return None

    return _Conn()


_WITH_CALENDAR = {("raw_market", "stock_daily"), ("raw_market", "us_market_holiday")}


def _weekdays(days_back: int) -> list[date]:
    """The window query_date_coverage measures: ``today − days_back`` … yesterday."""
    end = date.today() - timedelta(days=1)
    start = date.today() - timedelta(days=days_back)
    out: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def test_absent_sessions_are_reported_even_with_no_rows() -> None:
    sessions = _weekdays(30)
    assert len(sessions) > 4
    missing = sessions[2:5]
    held = {d.isoformat(): 9_000 for d in sessions if d not in missing}

    result = mod.query_date_coverage(
        _conn(relations=_WITH_CALENDAR, held=held), days_back=30, min_symbol_threshold=1_000
    )

    assert result["absent_count"] == len(missing)
    assert result["absent_dates"] == [d.isoformat() for d in missing]
    # Every session the table does hold is well over the threshold.
    assert result["low_coverage_dates"] == []
    assert result["count"] == 0


def test_thin_sessions_are_still_reported() -> None:
    sessions = _weekdays(30)
    held = {d.isoformat(): 9_000 for d in sessions}
    held[sessions[1].isoformat()] = 18

    result = mod.query_date_coverage(
        _conn(relations=_WITH_CALENDAR, held=held), days_back=30, min_symbol_threshold=1_000
    )

    assert result["low_coverage_dates"] == [{"date": sessions[1].isoformat(), "symbol_count": 18}]
    assert result["count"] == 1
    assert result["absent_count"] == 0


def test_a_holiday_is_not_an_absent_session() -> None:
    sessions = _weekdays(30)
    closed = sessions[3]
    held = {d.isoformat(): 9_000 for d in sessions if d != closed}

    result = mod.query_date_coverage(
        _conn(relations=_WITH_CALENDAR, held=held, holidays=(closed,)),
        days_back=30,
        min_symbol_threshold=1_000,
    )

    assert result["absent_count"] == 0
    assert result["absent_dates"] == []


def test_absent_is_unknown_without_the_calendar() -> None:
    """No calendar means no denominator — and an unknown is not a finding."""
    sessions = _weekdays(30)
    held = {d.isoformat(): 9_000 for d in sessions[2:]}

    result = mod.query_date_coverage(
        _conn(relations={("raw_market", "stock_daily")}, held=held),
        days_back=30,
        min_symbol_threshold=1_000,
    )

    assert result["absent_dates"] is None
    assert result["absent_count"] is None
    assert result["ok"] is True


def test_absent_is_unknown_without_the_table() -> None:
    result = mod.query_date_coverage(_conn(relations=set()), days_back=30)

    assert result["low_coverage_dates"] == []
    assert result["absent_dates"] is None
    assert result["absent_count"] is None
