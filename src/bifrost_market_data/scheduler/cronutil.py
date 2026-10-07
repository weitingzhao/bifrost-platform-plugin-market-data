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
from datetime import datetime, timedelta, timezone, tzinfo
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


def _matches(dt: datetime, fields: _Fields) -> bool:
    minutes, hours, doms, months, dows = fields
    if minutes is not None and dt.minute not in minutes:
        return False
    if hours is not None and dt.hour not in hours:
        return False
    if months is not None and dt.month not in months:
        return False
    cron_dow = (dt.weekday() + 1) % 7  # Python: Mon=0..Sun=6; cron: Sun=0..Sat=6
    dom_ok = doms is None or dt.day in doms
    dow_ok = dows is None or cron_dow in dows
    if doms is not None and dows is not None:
        return dom_ok or dow_ok
    return dom_ok and dow_ok


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
    """
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    parsed = [_parse_all(e) for e in _exprs(expr)]
    zone = _zone(tz)
    utc = zone is timezone.utc
    cur = start.astimezone(timezone.utc).replace(second=0, microsecond=0)
    if cur < start:
        cur += timedelta(minutes=1)
    out: list[datetime] = []
    while cur < end:
        local = cur if utc else cur.astimezone(zone)
        if any(_matches(local, f) for f in parsed):
            out.append(cur)
        cur += timedelta(minutes=1)
    return out


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
