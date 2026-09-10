"""The fourth axis: a step is not a hole, and an absent day is not a thin one."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from bifrost_market_data import continuity as cont
from bifrost_market_data.contracts import contract_for


def _series(start: date, values: list[int]) -> list[tuple[date, int]]:
    return [(start + timedelta(days=i), v) for i, v in enumerate(values)]


def test_a_lone_collapse_is_a_hole() -> None:
    """2026-08-11 held 18 rows between neighbours holding 12,400."""
    series = _series(date(2026, 8, 1), [12400] * 10 + [18] + [12400] * 10)
    holes = cont.thin_days(series)
    assert [(d.isoformat(), n) for d, n, _m in holes] == [("2026-08-11", 18)]
    assert holes[0][2] == 12400  # what the feed had been producing before it


def test_a_step_is_not_a_hole() -> None:
    """Option coverage went from 26 underlyings to 575 on one day by design.

    Judged against the window's median that reads as twenty-five holes; judged
    against a centred neighbourhood the days either side of the step still trip,
    because their neighbourhood straddles it. Trailing reads it as what it is.
    """
    series = _series(date(2026, 8, 1), [26] * 25 + [575] * 10)
    assert cont.thin_days(series) == []


def test_a_gradual_slope_is_not_a_hole() -> None:
    """Growth is not loss, however steep."""
    series = _series(date(2026, 8, 1), list(range(100, 3100, 100)))
    assert cont.thin_days(series) == []


def test_a_sustained_collapse_is_reported_once_it_starts() -> None:
    """A feed that halves and stays halved is a regression, not a shape."""
    series = _series(date(2026, 8, 1), [1000] * 12 + [100] * 12)
    holes = cont.thin_days(series)
    assert holes, "a sustained drop must not be normalised away"
    assert holes[0][0] == date(2026, 8, 13)


def test_the_first_days_of_a_window_are_not_judged() -> None:
    """There is nothing behind them to compare against."""
    series = _series(date(2026, 8, 1), [5, 5000, 5000, 5000, 5000])
    assert [d for d, _n, _m in cont.thin_days(series)] == []


def test_the_threshold_is_a_share_of_the_neighbourhood() -> None:
    below = _series(date(2026, 8, 1), [1000] * 10 + [400] + [1000] * 10)
    above = _series(date(2026, 8, 1), [1000] * 10 + [600] + [1000] * 10)
    assert len(cont.thin_days(below)) == 1
    assert cont.thin_days(above) == []


def test_which_datasets_have_a_session_cadence() -> None:
    """Derived from the contract, not a hand list: a catalogue has no cadence."""
    assert cont.has_continuity(contract_for("raw_market.stock_daily"))
    assert cont.has_continuity(contract_for("raw_market.option_snapshot"))
    assert cont.has_continuity(contract_for("raw_market.ratios"))
    assert not cont.has_continuity(contract_for("raw_market.ticker"))
    assert not cont.has_continuity(contract_for("raw_market.option_contract"))
    assert not cont.has_continuity(contract_for("raw_market.income_statement"))
    assert not cont.has_continuity(contract_for("raw_market.stock_snapshot"))


class _Cur:
    def __init__(self, rows: list[Any], boom: bool = False) -> None:
        self._rows, self._boom = rows, boom

    def execute(self, sql: str, params: Any = None) -> None:
        if self._boom and "count(*)" in sql:
            raise RuntimeError("statement timeout")

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *a: object) -> None:
        return None


class _Conn:
    def __init__(self, rows: list[Any], boom: bool = False) -> None:
        self.rows, self.boom = rows, boom

    def cursor(self) -> _Cur:
        return _Cur(self.rows, self.boom)

    def rollback(self) -> None:
        return None


def test_an_absent_session_is_reported_apart_from_a_thin_one() -> None:
    """A blank day contributes no row to a per-day count, so nothing sees it."""
    days = [date(2026, 8, 3) + timedelta(days=i) for i in range(5)]
    rows = [(d, 12400) for d in days if d != date(2026, 8, 5)]
    out = cont.measure(
        _Conn(rows), contract_for("raw_market.stock_daily"), expected_days=days
    )
    assert out["measured"] is True
    assert out["days_present"] == 4
    assert out["days_absent"] == 1
    assert out["absent_sample"] == ["2026-08-05"]
    assert out["days_thin"] == 0


def test_days_before_the_dataset_existed_are_not_absences() -> None:
    """A dataset that starts midway through the window has lost nothing."""
    days = [date(2026, 8, 3) + timedelta(days=i) for i in range(10)]
    rows = [(d, 100) for d in days[5:]]
    out = cont.measure(_Conn(rows), contract_for("raw_market.stock_daily"), expected_days=days)
    assert out["days_absent"] == 0


def test_a_dataset_without_a_session_cadence_says_so() -> None:
    out = cont.measure(_Conn([]), contract_for("raw_market.ticker"))
    assert out["measured"] is False
    assert "catalogue" in out["why"]


def test_a_failed_read_does_not_sink_the_page() -> None:
    out = cont.measure(_Conn([], boom=True), contract_for("raw_market.stock_daily"))
    assert out["measured"] is False
    assert out["why"] == "read failed"


def test_a_settlement_series_is_not_judged_against_trading_days() -> None:
    """short_interest settles twice a month; against sessions it read as 56 gaps."""
    days = [date(2026, 8, 3) + timedelta(days=i) for i in range(20)]
    rows = [(date(2026, 8, 3), 5000), (date(2026, 8, 18), 5000)]
    out = cont.measure(
        _Conn(rows), contract_for("raw_market.short_interest"), expected_days=days
    )
    assert out["measured"] is True
    assert out["cadence"] == "settlement"
    assert out["days_absent"] == 0


def test_a_daily_series_still_is() -> None:
    """short_volume publishes every trading day, and had a 99-day hole."""
    assert contract_for("raw_market.short_volume").cadence == "session"
    assert contract_for("raw_market.ratios").cadence == "session"
    assert contract_for("raw_market.stock_daily").cadence == "session"


def test_short_volume_history_is_bought_not_merely_accrued() -> None:
    """forward_only was ratios' constraint, copied here without being checked.

    ratios' endpoint ignores ?date and always returns the latest, so its history
    can only accumulate. Short volume for 2026-06-15 comes back dated
    2026-06-15, measured 2026-09-09 — which is what made a two-year gap fixable
    rather than permanent.
    """
    c = contract_for("raw_market.short_volume")
    assert c.depth.kind == "rolling_days"
    assert c.depth.value == 730
    assert contract_for("raw_market.ratios").depth.kind == "forward_only"


def test_a_weekend_row_is_not_a_thin_session() -> None:
    """Measured 2026-09-10: four of option_open_interest's five worst "thin"
    days were a Saturday or a Sunday. A day the market was shut cannot be a
    session that came up short, and its handful of rows must not sit in the
    trailing median either — that drags the floor down where a real weekday
    hole should trip it.
    """
    weekdays = [d for d in (date(2026, 8, 3) + timedelta(days=i) for i in range(28))
                if d.weekday() < 5]
    rows = [(d, 60_000) for d in weekdays]
    # Saturdays and Sundays carrying a trickle of rows.
    weekend = [d for d in (date(2026, 8, 3) + timedelta(days=i) for i in range(28))
               if d.weekday() >= 5]
    rows += [(d, 900) for d in weekend]

    without = cont.measure(_Conn(sorted(rows)), contract_for("raw_market.option_open_interest"))
    assert without["days_thin"] == len(weekend), "no calendar: every weekend reads as a hole"

    out = cont.measure(
        _Conn(sorted(rows)),
        contract_for("raw_market.option_open_interest"),
        expected_days=weekdays,
    )
    assert out["days_thin"] == 0
    assert out["days_present"] == len(weekdays)
    assert out["days_off_calendar"] == len(weekend)
    assert out["off_calendar_sample"][0] == weekend[0].isoformat()


def test_a_real_weekday_hole_still_trips_with_the_calendar_on() -> None:
    """Filtering weekends must not blunt the measure it was meant to sharpen."""
    weekdays = [d for d in (date(2026, 8, 3) + timedelta(days=i) for i in range(28))
                if d.weekday() < 5]
    rows = [(d, 60_000) for d in weekdays]
    hole = weekdays[15]
    rows[15] = (hole, 300)
    out = cont.measure(
        _Conn(rows), contract_for("raw_market.option_open_interest"), expected_days=weekdays
    )
    assert out["days_thin"] == 1
    assert out["worst"][0]["date"] == hole.isoformat()


def test_an_unreadable_calendar_does_not_blank_the_axis() -> None:
    """expected_days == [] means the calendar read failed, not "no trading days"."""
    days = [date(2026, 8, 3) + timedelta(days=i) for i in range(10)]
    rows = [(d, 5_000) for d in days]
    out = cont.measure(_Conn(rows), contract_for("raw_market.option_open_interest"), expected_days=[])
    assert out["measured"] is True
    assert out["days_present"] == len(days)
    assert out["days_off_calendar"] == 0


def test_missing_sessions_asks_once_for_the_whole_calendar() -> None:
    """One statement, not one per day.

    Counting to answer a presence question cost the doctor ~10s across five
    datasets; a probe per day then traded that for 210 round trips and the
    median barely moved. The calendar goes to the server instead.
    """
    class _Cur:
        def __init__(self, has: set[date]) -> None:
            self.has = has
            self.sql: list[str] = []
            self.params: list[object] = []
            self._rows: list[tuple[date]] = []

        def __enter__(self) -> "_Cur":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def execute(self, sql: str, params: object = None) -> None:
            q = " ".join(str(sql).split())
            self.sql.append(q)
            if q.startswith("SET LOCAL"):
                return
            self.params.append(params)
            self._rows = [(d,) for d in (params or ()) if d not in self.has]

        def fetchall(self) -> list[tuple[date]]:
            return self._rows

    class _C:
        def __init__(self, cur: _Cur) -> None:
            self._cur = cur

        def cursor(self) -> _Cur:
            return self._cur

    days = [date(2026, 9, 1) + timedelta(days=i) for i in range(5)]
    cur = _Cur(set(days) - {days[2]})
    out = cont.missing_sessions(_C(cur), "raw_market.option_daily", "bar_date", days)
    assert out == [days[2]]

    probes = [q for q in cur.sql if not q.startswith("SET LOCAL")]
    assert len(probes) == 1, "the whole calendar in one statement"
    q = probes[0]
    assert "VALUES" in q and q.count("(%s)") == len(days) - 1
    # A lateral with a limit, not an anti-join: against a forty-row outer side
    # the planner hashes the whole inner relation and option_daily blew the
    # 30s budget, so the check skipped the table it exists for.
    assert "LEFT JOIN LATERAL" in q and "LIMIT 1" in q
    assert "NOT EXISTS" not in q
    assert "count(" not in q.lower(), "presence, not volume"
    # Half-open bounds, so a timestamp column rides the index instead of ::date.
    assert ">= v.d AND" in q and "< v.d + 1" in q
    assert "::date" not in q.split("VALUES")[1].split(")")[-1]


def test_missing_sessions_on_an_empty_calendar_asks_nothing() -> None:
    class _Boom:
        def cursor(self) -> object:
            raise AssertionError("must not query")

    assert cont.missing_sessions(_Boom(), "t", "c", []) == []


def test_missing_sessions_says_none_when_the_read_fails() -> None:
    """A failed read is not "no days are missing" — the doctor must skip, not prescribe."""
    class _Boom:
        def cursor(self) -> object:
            raise RuntimeError("statement timeout")

        def rollback(self) -> None:
            return None

    assert cont.missing_sessions(_Boom(), "t", "c", [date(2026, 9, 1)]) is None
