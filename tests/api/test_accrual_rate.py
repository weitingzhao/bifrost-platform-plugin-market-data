"""A fraction alone cannot be acted on.

"60% of the way to 90 sessions" reads the same whether the dataset gained a
session last night or stopped three weeks ago — and those are the only two
states worth telling apart when the goal is growing the estate.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from bifrost_market_data.api import coverage_dimensions as mod
from bifrost_market_data.contracts import contract_for

TODAY = date(2026, 9, 10)


def _trading_days(n: int, end: date = TODAY) -> list[date]:
    """`n` weekdays ending on `end`, newest last."""
    out: list[date] = []
    d = end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return sorted(out)


def _snapshot() -> Any:
    """option_snapshot: forward_only, accrues to 90 sessions."""
    c = contract_for("raw_market.option_snapshot")
    assert c.depth.accrues_to_sessions, "the fixture assumes an accruing contract"
    return c


def test_a_pile_gaining_every_session_reports_a_full_rate() -> None:
    c = _snapshot()
    days = _trading_days(30)
    out = mod._accrual_rate(c, held=45, gained=10, expected_days=days, today=TODAY)
    assert out["rate"] == 1.0
    assert out["stalled"] is False
    # 45 short of 90, one per session → 45 sessions to go.
    assert out["sessions_remaining"] == 45


def test_a_pile_that_stopped_is_named_not_left_at_a_percentage() -> None:
    """The state the panel could not show: 60% and going nowhere."""
    c = _snapshot()
    out = mod._accrual_rate(c, held=54, gained=0, expected_days=_trading_days(30), today=TODAY)
    assert out["stalled"] is True
    assert out["rate"] == 0.0
    # No rate, no honest projection. None rather than infinity or a large number.
    assert out["sessions_remaining"] is None


def test_a_half_speed_pile_takes_twice_as_long() -> None:
    c = _snapshot()
    out = mod._accrual_rate(c, held=70, gained=5, expected_days=_trading_days(30), today=TODAY)
    assert out["rate"] == 0.5
    assert out["sessions_remaining"] == 40  # (90-70)/0.5


def test_a_closed_market_is_not_a_stall() -> None:
    """A week the market was shut is not a week of standing still, and a rate
    that said so would put every dataset on report each Christmas."""
    c = _snapshot()
    # Calendar has nothing inside the rate window.
    old_days = _trading_days(10, end=TODAY - timedelta(days=60))
    out = mod._accrual_rate(c, held=54, gained=0, expected_days=old_days, today=TODAY)
    assert out["sessions_due_recent"] == 0
    assert out["rate"] is None
    assert out["stalled"] is None


def test_a_pile_at_its_ceiling_is_not_stalled() -> None:
    """It is not growing because there is nowhere left to grow. Whether it is
    still being written is the freshness axis's question, not this one."""
    c = _snapshot()
    out = mod._accrual_rate(c, held=90, gained=0, expected_days=_trading_days(30), today=TODAY)
    assert out["stalled"] is False
    assert out["sessions_remaining"] is None


def test_no_calendar_means_no_rate_rather_than_a_guessed_one() -> None:
    c = _snapshot()
    out = mod._accrual_rate(c, held=54, gained=0, expected_days=None, today=TODAY)
    assert out["rate"] is None
    assert out["stalled"] is None


def test_gaining_more_than_the_calendar_allows_is_clamped() -> None:
    """Two rows on one date, or a row dated on a Saturday, must not read as
    faster than the market runs."""
    c = _snapshot()
    out = mod._accrual_rate(c, held=45, gained=14, expected_days=_trading_days(30), today=TODAY)
    assert out["rate"] == 1.0
