"""Minimal 5-field cron helpers (UTC) for schedule plan / adherence UI.

Supports common forms used in ``schedule.yaml``:
``M H * * *``, ``M */N * * *``, ``M H * * D`` (dow 0=Sun..6=Sat).
Does not implement full cron semantics (lists/ranges beyond ``*/N``).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _parse_field(field: str, minimum: int, maximum: int) -> set[int] | None:
    """Return allowed values, or None if field is ``*`` (all)."""
    f = field.strip()
    if f == "*":
        return None
    if f.startswith("*/"):
        step = int(f[2:])
        if step <= 0:
            raise ValueError(f"invalid step in cron field: {field!r}")
        return {v for v in range(minimum, maximum + 1) if v % step == 0}
    if "," in f:
        return {int(p.strip()) for p in f.split(",") if p.strip() != ""}
    return {int(f)}


def parse_cron(expr: str) -> tuple[set[int] | None, set[int] | None, set[int] | None]:
    """Parse ``minute hour dow`` (day-of-month/month must be ``*`` for our slots)."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"expected 5-field cron, got {expr!r}")
    minute_s, hour_s, dom, month, dow_s = parts
    if dom != "*" or month != "*":
        # Still allow; we only match minute/hour/dow and ignore dom/month filters
        # when they are not ``*`` by treating as unsupported → empty fires.
        if dom != "*" or month != "*":
            raise ValueError(f"unsupported cron (dom/month must be *): {expr!r}")
    minutes = _parse_field(minute_s, 0, 59)
    hours = _parse_field(hour_s, 0, 23)
    dows = _parse_field(dow_s, 0, 6)
    return minutes, hours, dows


def _matches(dt: datetime, minutes: set[int] | None, hours: set[int] | None, dows: set[int] | None) -> bool:
    if minutes is not None and dt.minute not in minutes:
        return False
    if hours is not None and dt.hour not in hours:
        return False
    if dows is not None and dt.weekday() == 6:  # Python: Mon=0..Sun=6; cron: Sun=0
        cron_dow = 0
    elif dows is not None:
        cron_dow = dt.weekday() + 1  # Mon=1 .. Sat=6
    else:
        cron_dow = None
    if dows is not None and cron_dow not in dows:
        return False
    return True


def iter_cron_fires(
    expr: str,
    *,
    start: datetime,
    end: datetime,
) -> list[datetime]:
    """Return fire times in ``[start, end)`` at minute resolution (UTC)."""
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    minutes, hours, dows = parse_cron(expr)
    # Align to minute
    cur = start.replace(second=0, microsecond=0)
    if cur < start:
        cur += timedelta(minutes=1)
    out: list[datetime] = []
    while cur < end:
        if _matches(cur, minutes, hours, dows):
            out.append(cur)
        cur += timedelta(minutes=1)
    return out


def next_fires(expr: str, *, after: datetime, count: int = 3, horizon_days: int = 14) -> list[datetime]:
    start = after + timedelta(minutes=1)
    end = after + timedelta(days=horizon_days)
    fires = iter_cron_fires(expr, start=start, end=end)
    return fires[: max(0, int(count))]


def previous_fire(expr: str, *, before: datetime, lookback_days: int = 14) -> datetime | None:
    start = before - timedelta(days=lookback_days)
    fires = iter_cron_fires(expr, start=start, end=before)
    return fires[-1] if fires else None


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
