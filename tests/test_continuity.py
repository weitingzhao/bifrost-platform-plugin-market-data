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
