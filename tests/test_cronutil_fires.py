"""``iter_cron_fires`` walks days, not minutes — and must answer as the minute walk did (TD-242).

The queue dashboard scores every slot's adherence from the fires these helpers
return. Until 0.84.0 ``iter_cron_fires`` tested every UTC minute of its window:
about a million minute tests per dashboard call, 1.6 s of CPU measured in the
PROD API pod on 2026-10-07 (3.3 s average wall time behind the pod's half-core
limit). The day walk must give the same fires, including the New York DST gaps
(no 02:30 on the spring-forward Sunday) and repeats (01:30 twice in November).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from bifrost_market_data.scheduler import cronutil
from bifrost_market_data.scheduler.cronutil import (
    _matches,
    _parse_all,
    _zone,
    iter_cron_fires,
    next_fires,
    previous_fire,
)

ROOT = Path(__file__).resolve().parents[1]


def _minute_walk(expr, *, start, end, tz=None):
    """The pre-0.84.0 implementation, kept as the reference answer."""
    parsed = [_parse_all(e) for e in ([expr] if isinstance(expr, str) else list(expr))]
    zone = _zone(tz)
    cur = start.astimezone(timezone.utc).replace(second=0, microsecond=0)
    if cur < start:
        cur += timedelta(minutes=1)
    out = []
    while cur < end:
        local = cur if zone is timezone.utc else cur.astimezone(zone)
        if any(_matches(local, f) for f in parsed):
            out.append(cur)
        cur += timedelta(minutes=1)
    return out


def _schedule_crons() -> list[tuple[list[str], str | None]]:
    doc = yaml.safe_load((ROOT / "config" / "schedule.yaml").read_text())
    out = []
    for scfg in doc["scheduler"]["slots"].values():
        raw = scfg.get("cron")
        if not raw:
            continue
        crons = [raw] if isinstance(raw, str) else list(raw)
        out.append((crons, scfg.get("timezone")))
    return out


EDGE_CRONS: list[tuple[list[str], str | None]] = [
    (["30 2 * * *"], "America/New_York"),  # missing on the spring-forward Sunday
    (["30 1 * * *"], "America/New_York"),  # twice on the fall-back Sunday
    (["*/20 0-3 * * 0"], "America/New_York"),
    (["0 7 1 * *"], None),  # monthly
    (["0 7 1 * 1"], "America/New_York"),  # day of month OR day of week
    (["15 3 * 3,11 *"], "Europe/London"),
    (["0 12 * * 1-5", "30 15 * * 1-5"], "America/New_York"),
    (["*/15 * * * *"], None),
]

# Windows across both 2026 New York DST changes (Mar 8, Nov 1) and London's.
WINDOWS = [
    (
        datetime(2026, 3, 1, 0, 0, 30, tzinfo=timezone.utc),
        datetime(2026, 3, 16, tzinfo=timezone.utc),
    ),
    (datetime(2026, 3, 25, tzinfo=timezone.utc), datetime(2026, 4, 3, 7, 1, tzinfo=timezone.utc)),
    (
        datetime(2026, 10, 24, 5, 59, 59, tzinfo=timezone.utc),
        datetime(2026, 11, 9, tzinfo=timezone.utc),
    ),
    (datetime(2026, 12, 25, tzinfo=timezone.utc), datetime(2027, 1, 3, tzinfo=timezone.utc)),
]


@pytest.mark.parametrize("crons,tz", _schedule_crons() + EDGE_CRONS)
def test_the_day_walk_gives_the_minute_walks_fires(crons: list[str], tz: str | None) -> None:
    for start, end in WINDOWS:
        assert iter_cron_fires(crons, start=start, end=end, tz=tz) == _minute_walk(
            crons, start=start, end=end, tz=tz
        ), (crons, tz, start)


def test_dst_gap_and_repeat() -> None:
    ny = ZoneInfo("America/New_York")
    spring = iter_cron_fires(
        "30 2 * * *",
        start=datetime(2026, 3, 7, tzinfo=timezone.utc),
        end=datetime(2026, 3, 10, tzinfo=timezone.utc),
        tz="America/New_York",
    )
    assert [f.astimezone(ny).day for f in spring] == [7, 9]  # no 02:30 on Mar 8
    fall = iter_cron_fires(
        "30 1 * * *",
        start=datetime(2026, 11, 1, tzinfo=timezone.utc),
        end=datetime(2026, 11, 2, tzinfo=timezone.utc),
        tz="America/New_York",
    )
    assert fall == [
        datetime(2026, 11, 1, 5, 30, tzinfo=timezone.utc),
        datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc),
    ]


def test_an_empty_or_inverted_window_has_no_fires() -> None:
    at = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
    assert iter_cron_fires("* * * * *", start=at, end=at) == []
    assert iter_cron_fires("* * * * *", start=at, end=at - timedelta(hours=1)) == []


def test_the_dashboard_lookups_cost_milliseconds() -> None:
    """The dashboard's per-slot asks — previous fire, next three, the 30-hour
    swimlane — for every slot in schedule.yaml. The minute walk spent ~1.6 s of
    CPU here in the PROD pod; the bound is generous against slow CI runners."""
    now = datetime(2026, 10, 7, 3, 10, tzinfo=timezone.utc)
    started = time.process_time()
    for crons, tz in _schedule_crons():
        previous_fire(crons, before=now, tz=tz)
        next_fires(crons, after=now, count=3, tz=tz)
        iter_cron_fires(crons, start=now - timedelta(hours=24), end=now + timedelta(hours=6), tz=tz)
    assert time.process_time() - started < 0.1


def test_matches_is_the_reference_for_one_minute() -> None:
    fields = cronutil._parse_all("30 21 * * 1-5")
    assert cronutil._matches(datetime(2026, 8, 17, 21, 30), fields)  # Monday
    assert not cronutil._matches(datetime(2026, 8, 16, 21, 30), fields)  # Sunday
