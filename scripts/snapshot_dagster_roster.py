#!/usr/bin/env python3
"""Write tests/fixtures/dagster_slot_roster.json from the Dagster roster.

Dagster fires every market-data slot (bifrost-research
``orchestration/market_slot_schedules.py``). ``config/schedule.yaml`` keeps a
copy of each slot's crons for the queue dashboard's adherence scoring, and
``tests/test_dagster_slot_roster.py`` holds that copy to this snapshot. The
plugin never imports research at run time; this script is the only bridge, run
by hand after a Dagster schedule changes.

Sources (one of):
  --research-src PATH   a bifrost-research checkout (default: the sibling
                        ../bifrost-research); reads src/bifrost_research/api/
                        schedule_roster.py, which research's own test holds to
                        its ScheduleDefinitions.
  --status-json FILE    a saved response of research-api
                        GET /research/orchestration/status (``-`` for stdin),
                        i.e. what the deployed Dagster actually runs.

``--check`` compares instead of writing and exits 1 on a difference.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "dagster_slot_roster.json"
ROSTER_REL = Path("src/bifrost_research/api/schedule_roster.py")


def _from_research_src(src: Path) -> list[dict[str, Any]]:
    path = src / ROSTER_REL
    spec = importlib.util.spec_from_file_location("_dagster_schedule_roster", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return [
        {"schedule": s.name, "timezone": s.tz, "cron": s.cron, "slots": list(s.market_slots)}
        for s in mod.SCHEDULE_ROSTER
        if s.market_slots
    ]


def _from_status(raw: str) -> list[dict[str, Any]]:
    body = json.loads(raw)
    data = body.get("data", body)
    return [
        {
            "schedule": s["name"],
            "timezone": s.get("execution_timezone") or "UTC",
            "cron": s["cron_schedule"],
            "slots": list(s.get("market_slots") or []),
        }
        for s in data["schedules"]
        if s.get("market_slots")
    ]


def build(args: argparse.Namespace) -> dict[str, Any]:
    if args.status_json:
        raw = sys.stdin.read() if args.status_json == "-" else Path(args.status_json).read_text()
        rows, source = _from_status(raw), "GET /research/orchestration/status"
    else:
        rows, source = _from_research_src(Path(args.research_src)), f"bifrost-research {ROSTER_REL}"
    return {
        "_source": source,
        "schedules": sorted(rows, key=lambda r: r["schedule"]),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--research-src", default=str(ROOT.parent / "bifrost-research"))
    ap.add_argument("--status-json", default=None)
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    snap = build(args)
    if args.check:
        current = json.loads(FIXTURE.read_text())
        same = current["schedules"] == snap["schedules"]
        print("in step" if same else f"drifted from {snap['_source']}")
        return 0 if same else 1
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(snap, indent=2) + "\n")
    print(f"wrote {FIXTURE.relative_to(ROOT)}: {len(snap['schedules'])} schedules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
