"""One definition of "the session the tables should hold", and the deadline math.

There were four. ``doctor`` anchored on 19:30 New York of a trading day,
``quality`` approximated the same thing with a weekday rule (24 hours, or 72
across a weekend), ``ingest_dashboard`` used a 22:30 ET cron grace, and
``readiness_summary`` used a flat seven days. One dataset could therefore be
fresh on one panel and stale on another, which is the failure C-F1 and C-F3
name: not a wrong threshold, but four of them.

The deadline itself comes from the dataset's contract, never from here — this
module only knows which session is current and how to measure against it.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

#: The EOD chain fires at 22:00 UTC and drains in minutes; by this wall-clock
#: time in New York the session's rows must exist.
EOD_EXPECTED_BY_NY = time(19, 30)
#: When the market closes — deadlines are measured from here, not from midnight.
MARKET_CLOSE_NY = time(16, 0)


def resolve_session(
    conn: Any,
    now: datetime,
    *,
    is_trading_day: Any = None,
    fetch_completed_trading_days: Any = None,
) -> tuple[date, bool]:
    """The session the tables should hold by now, and whether that is today.

    Today counts once its EOD batch should have drained; before that, or on a
    non-trading day, the last completed session.

    ``is_trading_day`` is injectable so a caller that already owns that seam —
    the doctor's tests drive the calendar through it — keeps working without a
    second copy of this function.
    """
    from bifrost_market_data.quality import (
        fetch_completed_trading_days as _default_completed,
    )
    from bifrost_market_data.trading_calendar import is_trading_day as _default_probe

    is_trading_day = is_trading_day or _default_probe
    fetch_completed_trading_days = fetch_completed_trading_days or _default_completed
    now_ny = now.astimezone(NY)
    today_ny = now_ny.date()
    try:
        trading_today = is_trading_day(conn, today_ny)
    except Exception:
        trading_today = today_ny.weekday() < 5
    if trading_today and now_ny.time() >= EOD_EXPECTED_BY_NY:
        return today_ny, True
    completed = fetch_completed_trading_days(conn, 1, as_of=today_ny)
    if completed:
        return completed[-1], False
    return today_ny, trading_today


def session_close(session: date) -> datetime:
    """The instant a session closed, in UTC — the zero point for every deadline."""
    return datetime.combine(session, MARKET_CLOSE_NY, tzinfo=NY).astimezone(timezone.utc)


def deadline(session: date, hours: float) -> datetime:
    """When a dataset with this contract should have landed the session."""
    return session_close(session) + timedelta(hours=float(hours))


def is_late(last_run_at: datetime | None, session: date, hours: float, now: datetime) -> bool:
    """Late only once its own deadline has passed and nothing has run since the close.

    A feed published the morning after its session is not missing on the night
    of it; a flat 24-hour rule said otherwise and needed a weekend exception to
    stop failing every Friday.
    """
    due = deadline(session, hours)
    if now < due:
        return False
    if last_run_at is None:
        return True
    if last_run_at.tzinfo is None:
        last_run_at = last_run_at.replace(tzinfo=timezone.utc)
    return last_run_at < session_close(session)


__all__ = [
    "NY",
    "EOD_EXPECTED_BY_NY",
    "MARKET_CLOSE_NY",
    "resolve_session",
    "session_close",
    "deadline",
    "is_late",
]
