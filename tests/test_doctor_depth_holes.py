"""Doctor: whole months of option_daily empty after a name's oldest bar.

2026-09-28: KLAC's first bar was 2024-09 and 2025-10..2026-05 held nothing;
the oldest-bar depth test read it at depth. The finding and the option-depth
slot share ``plan_option_depth``, so what is reported is what gets planned.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data import doctor as doc
from bifrost_market_data.doctor import _depth_hole_findings

TODAY = date(2026, 9, 28)
CFG: dict[str, Any] = {
    "slots": {"option-depth": {"universe": "research", "months": 24, "dte": 90}},
    "iv_radar_benchmarks": [],
}
KLAC_GAPS = [date(2025, m, 1) for m in (10, 11, 12)] + [date(2026, m, 1) for m in range(1, 6)]


@pytest.fixture
def wired(monkeypatch) -> dict[str, Any]:
    state: dict[str, Any] = {"plan": None, "planned": set(), "asked": {}}
    monkeypatch.setattr(
        doc,
        "load_research_universe",
        lambda conn: [{"symbol": s, "tier": "core", "history_months": 24} for s in ("KLAC", "AAPL")],
    )
    monkeypatch.setattr(doc, "load_voids_by_prefix", lambda conn, prefix, max_age_days: state["planned"])

    def _plan(conn, **kw):
        state["asked"] = kw
        return state["plan"]

    monkeypatch.setattr(doc, "plan_option_depth", _plan)
    return state


def test_an_unplanned_empty_month_is_a_warning_with_the_depth_slot_as_its_fix(wired) -> None:
    wired["plan"] = {
        "short": {},
        "holes": {"KLAC": [date(2025, 10, 1), date(2025, 11, 1)]},
        "gaps": {"KLAC": KLAC_GAPS},
        "empty_read": True,
    }
    [f] = _depth_hole_findings(None, cfg=CFG, today=TODAY)
    assert f.id == "depth_holes:option_daily" and f.severity == "warn"
    assert f.fix == {"action": "enqueue-slot", "slot": "option-depth", "force": False}
    assert f.auto_fixable is True
    assert "KLAC 8 (2025-10..2026-05)" in f.detail
    assert wired["asked"]["names"] == ["AAPL", "KLAC"]
    assert wired["asked"]["months_of"] == {"KLAC": 24, "AAPL": 24}


def test_empty_months_already_planned_are_not_a_standing_warning(wired) -> None:
    """AXTI 2025-06: every contract outside the strike band — the vendor has nothing."""
    wired["plan"] = {
        "short": {},
        "holes": {"AXTI": []},
        "gaps": {"AXTI": [date(2025, 6, 1)]},
        "empty_read": True,
    }
    [f] = _depth_hole_findings(None, cfg=CFG, today=TODAY)
    assert f.severity == "ok" and f.fix is None
    assert "planned within 30 days" in f.detail


def test_names_short_of_depth_are_not_this_finding(wired) -> None:
    """Short of depth is the coverage matrix's reading; this one is about holes."""
    wired["plan"] = {"short": {"CRWV": [date(2025, 1, 1)]}, "holes": {}, "gaps": {}, "empty_read": True}
    [f] = _depth_hole_findings(None, cfg=CFG, today=TODAY)
    assert f.severity == "ok" and f.actual == "none"


@pytest.mark.parametrize(
    ("plan", "which"),
    [(None, "oldest-bar"), ({"short": {}, "holes": {}, "gaps": {}, "empty_read": False}, "empty-months")],
)
def test_an_unreadable_measure_is_unprobed_not_dropped(wired, plan, which) -> None:
    """2026-09-28: the empty-months read timed out twice in six minutes, and the
    finding vanished from a report that read healthy. A failed read clears no
    month and plans none, and it says so."""
    wired["plan"] = plan
    [f] = _depth_hole_findings(None, cfg=CFG, today=TODAY)
    assert (f.id, f.severity, f.actual) == ("depth_holes:option_daily", "warn", "depth unprobed")
    assert f.fix is None and f.auto_fixable is False, "nothing is prescribed from a failed read"
    assert which in f.detail
