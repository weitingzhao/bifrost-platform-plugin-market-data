"""Minimal 5-field cron helpers for the schedule plan / adherence UI.

The crons are the ones in ``schedule.yaml``, which copies the Dagster roster
(bifrost-research ``orchestration/market_slot_schedules.py``) slot by slot.
A slot may carry several crons (``intraday-chain`` fires three times a session)
and an execution timezone (``America/New_York`` for the market-clock fires);
both are honoured, so a DST change moves the expected fire with Dagster's.

Fields: minute, hour, day of month, month, day of week (0=Sun..6=Sat), each
``*``, ``*/N``, ``A``, ``A-B``, ``A-B/N`` or a comma list of those. When both
day of month and day of week are restricted a time matches either (cron's rule).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

#: One slot's crons: a single expression or several.
CronSpec = str | Sequence[str]

_Fields = tuple[
    set[int] | None, set[int] | None, set[int] | None, set[int] | None, set[int] | None
]


def _parse_field(field: str, minimum: int, maximum: int) -> set[int] | None:
    """Return allowed values, or None if field is ``*`` (all).

    Accepts ``*``, ``*/N``, ``A``, ``A-B``, ``A-B/N`` and comma lists of those —
    the forms Dagster crons in the multi-schedule use (``30 4 * * 2-6``).
    """
    f = field.strip()
    if f == "*":
        return None
    out: set[int] = set()
    for part in f.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            step = int(step_s)
            if step <= 0:
                raise ValueError(f"invalid step in cron field: {field!r}")
        else:
            base = part
        if base == "*":
            lo, hi = minimum, maximum
        elif "-" in base:
            lo_s, hi_s = base.split("-", 1)
            lo, hi = int(lo_s), int(hi_s)
        else:
            lo = hi = int(base)
        if lo > hi or lo < minimum or hi > maximum:
            raise ValueError(f"cron field out of range: {field!r}")
        out.update(v for v in range(lo, hi + 1) if (v - lo) % step == 0)
    return out


def _parse_all(expr: str) -> _Fields:
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"expected 5-field cron, got {expr!r}")
    minute_s, hour_s, dom_s, month_s, dow_s = parts
    return (
        _parse_field(minute_s, 0, 59),
        _parse_field(hour_s, 0, 23),
        _parse_field(dom_s, 1, 31),
        _parse_field(month_s, 1, 12),
        _parse_field(dow_s, 0, 6),
    )


def parse_cron(expr: str) -> tuple[set[int] | None, set[int] | None, set[int] | None]:
    """Parse ``minute hour dow`` of a cron whose day of month and month are ``*``."""
    minutes, hours, doms, months, dows = _parse_all(expr)
    if doms is not None or months is not None:
        raise ValueError(f"day of month / month restricted; use the fire helpers: {expr!r}")
    return minutes, hours, dows


def _day_matches(day: date, fields: _Fields) -> bool:
    """Whether a local calendar day can carry a fire (month, day of month, day of week)."""
    _, _, doms, months, dows = fields
    if months is not None and day.month not in months:
        return False
    cron_dow = (day.weekday() + 1) % 7  # Python: Mon=0..Sun=6; cron: Sun=0..Sat=6
    dom_ok = doms is None or day.day in doms
    dow_ok = dows is None or cron_dow in dows
    if doms is not None and dows is not None:
        return dom_ok or dow_ok
    return dom_ok and dow_ok


def _matches(dt: datetime, fields: _Fields) -> bool:
    minutes, hours = fields[0], fields[1]
    if minutes is not None and dt.minute not in minutes:
        return False
    if hours is not None and dt.hour not in hours:
        return False
    return _day_matches(dt.date(), fields)


def _exprs(spec: CronSpec) -> list[str]:
    if isinstance(spec, str):
        return [spec]
    return [str(e) for e in spec]


def _zone(tz: str | tzinfo | None) -> tzinfo:
    if tz is None or tz == "" or tz == "UTC":
        return timezone.utc
    if isinstance(tz, str):
        return ZoneInfo(tz)
    return tz


def _restricts_day(spec: CronSpec) -> bool:
    return any(f[2] is not None or f[3] is not None for f in map(_parse_all, _exprs(spec)))


def _instants(wall: datetime, zone: tzinfo) -> list[datetime]:
    """The UTC instants a local wall-clock minute names: none in a spring-forward
    gap, two in a fall-back hour, otherwise one — the minutes a clock walking UTC
    minute by minute would see reading that wall time."""
    if zone is timezone.utc:
        return [wall.replace(tzinfo=timezone.utc)]
    out: list[datetime] = []
    for fold in (0, 1):
        instant = wall.replace(tzinfo=zone, fold=fold).astimezone(timezone.utc)
        back = instant.astimezone(zone).replace(tzinfo=None, fold=0)
        if back == wall and instant.second == 0 and instant not in out:
            out.append(instant)
    return out


def iter_cron_fires(
    expr: CronSpec,
    *,
    start: datetime,
    end: datetime,
    tz: str | tzinfo | None = None,
) -> list[datetime]:
    """Return UTC fire times in ``[start, end)`` at minute resolution.

    ``expr`` is one cron or several (their union); ``tz`` is the zone the crons
    are read in, as Dagster's ``execution_timezone`` (default UTC).

    Walks local days and only the hours and minutes a cron names. This used to
    test every UTC minute of the window: the queue dashboard asks for 14 days
    back and 14 forward per slot (62 / 32 for a monthly cron), about a million
    minute tests per call — 1.6 s of CPU, 3.3 s on average behind the API pod's
    half-core limit, and the reason ``/ingest/queue-dashboard`` was the plugin's
    slowest read (TD-242). The answer is the same minute walk's answer, DST gaps
    and repeated fall-back hours included (``tests/test_cronutil_fires.py``).
    """
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    if end <= start:
        return []
    parsed = [_parse_all(e) for e in _exprs(expr)]
    zone = _zone(tz)
    first_day = start.astimezone(zone).date()
    last_day = end.astimezone(zone).date()
    fires: set[datetime] = set()
    day = first_day
    while day <= last_day:
        for fields in parsed:
            if not _day_matches(day, fields):
                continue
            minutes = sorted(fields[0]) if fields[0] is not None else range(60)
            hours = sorted(fields[1]) if fields[1] is not None else range(24)
            for hour in hours:
                for minute in minutes:
                    wall = datetime(day.year, day.month, day.day, hour, minute)
                    for instant in _instants(wall, zone):
                        if start <= instant < end:
                            fires.add(instant)
        day += timedelta(days=1)
    return sorted(fires)


def next_fires(
    expr: CronSpec,
    *,
    after: datetime,
    count: int = 3,
    horizon_days: int = 14,
    tz: str | tzinfo | None = None,
) -> list[datetime]:
    if _restricts_day(expr):
        horizon_days = max(horizon_days, 62)
    start = after + timedelta(minutes=1)
    end = after + timedelta(days=horizon_days)
    fires = iter_cron_fires(expr, start=start, end=end, tz=tz)
    return fires[: max(0, int(count))]


def previous_fire(
    expr: CronSpec,
    *,
    before: datetime,
    lookback_days: int = 14,
    tz: str | tzinfo | None = None,
) -> datetime | None:
    if _restricts_day(expr):
        # A monthly cron (``0 7 1 * *``) has no fire in a two-week lookback.
        lookback_days = max(lookback_days, 32)
    start = before - timedelta(days=lookback_days)
    fires = iter_cron_fires(expr, start=start, end=before, tz=tz)
    return fires[-1] if fires else None


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
