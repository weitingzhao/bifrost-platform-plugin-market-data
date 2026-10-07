"""The slot crons in schedule.yaml are Dagster's, copied — and must stay equal (TD-191).

Dagster fires every slot (bifrost-research ``orchestration/market_slot_schedules.py``);
nothing in this plugin schedules anything. But the queue dashboard scores each
slot's adherence against the ``cron`` / ``timezone`` in ``config/schedule.yaml``
(``api/ingest_dashboard._slot_adherence``), its verdict is the platform's
"Market batch" lane, and the Console draws the swimlane from it. So the copy is
not documentation: a slot whose cron differs from Dagster's is judged against a
fire that never happens. Until 0.83.0 ``intraday-chain`` read ``30 14 * * 1-5``
UTC for Dagster's three New York fires (wrong by an hour once DST ends) and
``corporate-backfill`` had no cron though Dagster fires it monthly.

``tests/fixtures/dagster_slot_roster.json`` is a snapshot of the roster
(``scripts/snapshot_dagster_roster.py``); research's own test holds the roster to
its ScheduleDefinitions. ``test_schedule_config_parity`` holds the cluster copy
to this one.

bifrost-research keeps the same file at ``tests/fixtures/dagster_slot_roster.json``
and its ``test_market_slot_roster_snapshot.py`` fails when a market schedule
changes without it (TD-200), pointing here. The two files are byte-identical;
``test_the_research_copy_is_this_file`` checks that when a sibling
bifrost-research checkout is present and skips otherwise, so neither CI needs
the other's checkout.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from bifrost_market_data.api.ingest_dashboard import slot_crons
from bifrost_market_data.scheduler.cronutil import iter_cron_fires, previous_fire

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "tests" / "fixtures" / "dagster_slot_roster.json"
RESEARCH_COPY = ROOT.parent / "bifrost-research" / "tests" / "fixtures" / "dagster_slot_roster.json"


def _snapshot() -> list[dict[str, Any]]:
    return json.loads(SNAPSHOT.read_text())["schedules"]


def _dagster_by_slot() -> dict[str, tuple[set[str], set[str]]]:
    out: dict[str, tuple[set[str], set[str]]] = {}
    for s in _snapshot():
        for slot in s["slots"]:
            crons, zones = out.setdefault(slot, (set(), set()))
            crons.add(s["cron"])
            zones.add(s["timezone"])
    return out


def _slots(path: Path, cluster: bool) -> dict[str, dict[str, Any]]:
    doc = yaml.safe_load(path.read_text())
    if cluster:
        doc = yaml.safe_load(doc["data"]["schedule.yaml"])
    return doc["scheduler"]["slots"]


COPIES = {
    "config/schedule.yaml": _slots(ROOT / "config" / "schedule.yaml", cluster=False),
    "k8s/base/configmap-schedule.yaml": _slots(
        ROOT / "k8s" / "base" / "configmap-schedule.yaml", cluster=True
    ),
}


def test_the_snapshot_is_not_empty() -> None:
    dagster = _dagster_by_slot()
    assert len(dagster) >= 20, (
        "the snapshot lost its market slots; this test would pass by accident"
    )
    for slot, (_, zones) in dagster.items():
        assert len(zones) == 1, f"{slot}: one slot is fired in one zone ({zones})"


def test_every_dagster_slot_has_dagsters_crons_and_zone() -> None:
    dagster = _dagster_by_slot()
    for name, slots in COPIES.items():
        drift: dict[str, Any] = {}
        for slot, (crons, zones) in dagster.items():
            mine, tz = slot_crons(slots.get(slot) or {})
            want_tz = next(iter(zones))
            if set(mine) != crons or len(mine) != len(crons) or (tz or "UTC") != want_tz:
                drift[slot] = {"plugin": (mine, tz or "UTC"), "dagster": (sorted(crons), want_tz)}
        assert drift == {}, f"{name}: slot crons differ from the Dagster roster: {drift}"


def test_no_slot_has_a_cron_dagster_does_not_fire() -> None:
    """A cron without a Dagster schedule is a fire the dashboard waits for in vain."""
    dagster = _dagster_by_slot()
    for name, slots in COPIES.items():
        extra = sorted(
            s for s, cfg in slots.items() if slot_crons(cfg or {})[0] and s not in dagster
        )
        assert extra == [], f"{name}: cron on slots Dagster does not schedule: {extra}"


def test_intraday_fires_follow_new_york_across_the_dst_change() -> None:
    cfg = COPIES["config/schedule.yaml"]["intraday-chain"]
    crons, tz = slot_crons(cfg)
    # EDT on Fri 2026-10-30, EST on Mon 2026-11-02.
    summer = iter_cron_fires(
        crons,
        start=datetime(2026, 10, 30, tzinfo=timezone.utc),
        end=datetime(2026, 10, 31, tzinfo=timezone.utc),
        tz=tz,
    )
    winter = iter_cron_fires(
        crons,
        start=datetime(2026, 11, 2, tzinfo=timezone.utc),
        end=datetime(2026, 11, 3, tzinfo=timezone.utc),
        tz=tz,
    )
    assert [t.strftime("%H:%M") for t in summer] == ["14:30", "17:00", "19:30"]
    assert [t.strftime("%H:%M") for t in winter] == ["15:30", "18:00", "20:30"]


def test_a_monthly_cron_has_a_previous_fire() -> None:
    crons, tz = slot_crons(COPIES["config/schedule.yaml"]["corporate-backfill"])
    last = previous_fire(crons, before=datetime(2026, 10, 20, tzinfo=timezone.utc), tz=tz)
    assert last == datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc)


def test_the_row_names_the_zone_so_the_console_does_not_read_it_as_utc() -> None:
    from bifrost_market_data.api.ingest_dashboard import cron_label

    crons, tz = slot_crons(COPIES["config/schedule.yaml"]["intraday-chain"])
    assert (
        cron_label(crons, tz) == "30 10 * * 1-5 | 0 13 * * 1-5 | 30 15 * * 1-5 (America/New_York)"
    )
    assert cron_label("0 22 * * *", None) == "0 22 * * *"


def test_the_research_copy_is_this_file() -> None:
    if not RESEARCH_COPY.is_file():
        pytest.skip("no sibling bifrost-research checkout with the TD-200 fixture")
    assert RESEARCH_COPY.read_bytes() == SNAPSHOT.read_bytes(), (
        "bifrost-research tests/fixtures/dagster_slot_roster.json differs from this snapshot: "
        "run scripts/snapshot_dagster_roster.py, then copy tests/fixtures/dagster_slot_roster.json "
        "over the research copy (or pull both repos if one checkout is behind)"
    )
