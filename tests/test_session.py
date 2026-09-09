"""One definition of the session, and deadlines measured from its close.

There were four: the doctor's 19:30 New York anchor, the quality gate's weekday
rule (24 hours, or 72 across a weekend), the ingest dashboard's 22:30 ET cron
grace and readiness's flat seven days. One dataset could read fresh on one
panel and stale on the next — the failure C-F1 and C-F3 name is not a wrong
threshold but four of them.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from bifrost_market_data import session as mod

SESSION = date(2026, 8, 26)  # a Wednesday


def _conn() -> Any:
    return object()


def _at(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def test_a_session_closes_at_the_bell_not_at_midnight() -> None:
    """Deadlines run from the close; measuring from midnight makes a feed that
    landed at 22:00 look most of a day old the next morning."""
    close = mod.session_close(SESSION)
    assert close == _at("2026-08-26T20:00:00")  # 16:00 New York, in August
    assert mod.deadline(SESSION, 2) == close + timedelta(hours=2)


def test_today_counts_only_once_its_batch_should_have_drained() -> None:
    trading = lambda conn, d: d.weekday() < 5  # noqa: E731
    completed = lambda conn, n, as_of=None: [date(2026, 8, 25)]  # noqa: E731

    # 11:00 New York on a trading day: the session in hand is still yesterday's.
    early = mod.resolve_session(
        _conn(),
        _at("2026-08-26T15:00:00"),
        is_trading_day=trading,
        fetch_completed_trading_days=completed,
    )
    assert early == (date(2026, 8, 25), False)

    # 20:00 New York, past the anchor: today's rows must exist.
    late = mod.resolve_session(
        _conn(),
        _at("2026-08-27T00:00:00"),
        is_trading_day=trading,
        fetch_completed_trading_days=completed,
    )
    assert late == (date(2026, 8, 26), True)


def test_a_weekend_falls_back_to_the_last_completed_session() -> None:
    trading = lambda conn, d: d.weekday() < 5  # noqa: E731
    completed = lambda conn, n, as_of=None: [SESSION]  # noqa: E731

    got = mod.resolve_session(
        _conn(),
        _at("2026-08-30T18:00:00"),
        is_trading_day=trading,
        fetch_completed_trading_days=completed,
    )
    assert got == (SESSION, False)


def test_lateness_needs_both_a_passed_deadline_and_nothing_since_the_close() -> None:
    close = mod.session_close(SESSION)

    # Before the deadline: pending, whatever the age.
    assert mod.is_late(close - timedelta(days=3), SESSION, 2, close + timedelta(hours=1)) is False
    # After it, with nothing run since the close: late.
    assert mod.is_late(close - timedelta(days=3), SESSION, 2, close + timedelta(hours=3)) is True
    # After it, but something ran once the session closed: covered.
    assert (
        mod.is_late(close + timedelta(minutes=30), SESSION, 2, close + timedelta(hours=3)) is False
    )
    # Never run at all, past the deadline.
    assert mod.is_late(None, SESSION, 2, close + timedelta(hours=3)) is True


def test_a_naive_timestamp_is_read_as_utc_rather_than_crashing() -> None:
    close = mod.session_close(SESSION)
    naive = (close + timedelta(minutes=30)).replace(tzinfo=None)
    assert mod.is_late(naive, SESSION, 2, close + timedelta(hours=3)) is False
