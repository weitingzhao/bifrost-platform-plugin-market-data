"""The rank moved out of the panel; these are the cases it has to keep answering.

Every test here mirrors one the console's own suite carries, because the console
keeps its copy as the fallback for an older plugin and the two must not drift.
The tail of the file covers what only the server-side copy can do: compare two
verdict maps and say which way an axis moved.
"""

from __future__ import annotations

from typing import Any

from bifrost_market_data import verdicts as v


def _row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "dataset": "raw_market.stock_daily",
        "error": None,
        "breadth": {"judged": True, "pct": 100.0},
        "depth": {
            "target": {"kind": "rolling_days", "value": 1825},
            "measured": True,
            "judged": True,
            "at_target": 100,
            "of": 100,
        },
        "freshness": {
            "measured": True,
            "newest": "2026-09-10",
            "days_behind": 0,
            "deadline_hours": 30,
            "cadence": "session",
            "judged": True,
        },
        "continuity": {"measured": True, "days_present": 60, "days_absent": 0, "days_thin": 0},
    }
    for k, val in over.items():
        if isinstance(val, dict) and isinstance(row.get(k), dict):
            row[k] = {**row[k], **val}
        else:
            row[k] = val
    return row


# ── breadth ───────────────────────────────────────────────────────────────


def test_breadth_thresholds() -> None:
    assert v.breadth_verdict(_row(breadth={"pct": 95.0})) == "ok"
    assert v.breadth_verdict(_row(breadth={"pct": 94.9})) == "partial"
    assert v.breadth_verdict(_row(breadth={"pct": 50.0})) == "partial"
    assert v.breadth_verdict(_row(breadth={"pct": 49.9})) == "thin"


def test_a_top_n_list_is_a_boundary_not_a_thin_market() -> None:
    """stock_movers is 20 names by design; 0.4% of the market is not a fault."""
    assert v.breadth_verdict(_row(breadth={"judged": False, "pct": 0.4})) == "boundary"


def test_an_unreadable_dataset_says_unknown_on_every_axis() -> None:
    row = _row(error="statement timeout")
    assert v.verdicts_for(row) == {a: "unknown" for a in v.AXES}


# ── depth ─────────────────────────────────────────────────────────────────


def test_an_unmeasured_depth_is_a_boundary_only_for_a_boundary_kind() -> None:
    boundary = _row(depth={"target": {"kind": "current_only"}, "measured": False})
    assert v.depth_verdict(boundary) == "boundary"
    unread = _row(depth={"target": {"kind": "rolling_days"}, "measured": False})
    assert v.depth_verdict(unread) == "unknown"


def test_an_absolute_start_is_not_judged_per_symbol() -> None:
    """Every one of income_statement's 4,467 symbols "failed" a 2009 target
    while the median held 9.7 years."""
    row = _row(depth={"judged": False, "at_target": None, "of": 4467})
    assert v.depth_verdict(row) == "boundary"


def test_depth_thresholds() -> None:
    assert v.depth_verdict(_row(depth={"at_target": 95, "of": 100})) == "ok"
    assert v.depth_verdict(_row(depth={"at_target": 50, "of": 100})) == "partial"
    assert v.depth_verdict(_row(depth={"at_target": 49, "of": 100})) == "thin"
    assert v.depth_verdict(_row(depth={"at_target": 0, "of": 0})) == "unknown"


# ── freshness ─────────────────────────────────────────────────────────────


def test_a_catalogue_has_no_clock() -> None:
    assert v.freshness_verdict(_row(freshness={"measured": False})) == "boundary"


def test_a_filing_arrives_when_the_company_files() -> None:
    row = _row(freshness={"cadence": "filing", "judged": False, "days_behind": 39})
    assert v.freshness_verdict(row) == "boundary"


def test_a_settlement_series_is_judged_on_its_own_interval() -> None:
    """short_interest read 27 days behind a 30-hour deadline while holding every
    settlement FINRA had released."""
    fresh = _row(freshness={"cadence": "settlement", "days_behind": 27, "overdue": False})
    assert v.freshness_verdict(fresh) == "ok"
    late = _row(freshness={"cadence": "settlement", "days_behind": 90, "overdue": True})
    assert v.freshness_verdict(late) == "thin"


def test_an_unmeasurable_interval_does_not_fall_back_to_the_hour_deadline() -> None:
    row = _row(freshness={"cadence": "settlement", "days_behind": 27, "overdue": None})
    assert v.freshness_verdict(row) == "unknown"


def test_a_session_feed_gets_the_session_plus_its_allowance() -> None:
    # 30h deadline → ceil(30/24) + 1 = 3 sessions before it is late at all.
    assert v.freshness_verdict(_row(freshness={"days_behind": 3})) == "ok"
    assert v.freshness_verdict(_row(freshness={"days_behind": 4})) == "partial"
    assert v.freshness_verdict(_row(freshness={"days_behind": 9})) == "partial"
    assert v.freshness_verdict(_row(freshness={"days_behind": 10})) == "thin"


# ── continuity ────────────────────────────────────────────────────────────


def test_continuity_counts_absent_and_thin_together() -> None:
    assert v.continuity_verdict(_row(continuity={"days_present": 81})) == "ok"
    d = _row(continuity={"days_present": 24, "days_absent": 1, "days_thin": 1})
    assert v.continuity_verdict(d) == "partial"
    assert v.continuity_verdict(_row(continuity={"days_present": 56, "days_thin": 8})) == "thin"


def test_no_session_cadence_is_a_boundary_with_a_reason_and_unknown_without() -> None:
    why = _row(continuity={"measured": False, "why": "catalogue has no session cadence"})
    assert v.continuity_verdict(why) == "boundary"
    silent = _row(continuity={"measured": False})
    assert v.continuity_verdict(silent) == "unknown"


# ── the map, and what moved ───────────────────────────────────────────────


def test_the_map_is_keyed_by_dataset_not_by_position() -> None:
    rows = [_row(dataset="a"), _row(dataset="b", breadth={"pct": 10.0})]
    m = v.verdict_map(rows)
    assert set(m) == {"a", "b"}
    assert m["b"]["breadth"] == "thin"


def test_the_map_prefers_a_verdict_the_row_already_carries() -> None:
    """A recorded payload is replayed as it was judged, not re-judged now."""
    row = _row(verdicts={"breadth": "thin", "depth": "ok", "freshness": "ok", "continuity": "ok"})
    assert v.verdict_map([row])["raw_market.stock_daily"]["breadth"] == "thin"


def test_direction_only_claims_one_between_ranked_verdicts() -> None:
    assert v.direction("ok", "thin") == "regressed"
    assert v.direction("thin", "partial") == "recovered"
    # option_snapshot depth becoming a declared plan boundary improved the
    # measurement; calling that a decline would be a lie about the data.
    assert v.direction("thin", "boundary") == "changed"
    assert v.direction("unknown", "ok") == "changed"


def test_diff_names_the_axis_that_moved_and_leaves_the_rest_alone() -> None:
    before = {"a": {"breadth": "ok", "depth": "ok"}}
    after = {"a": {"breadth": "thin", "depth": "ok"}}
    assert v.diff(before, after) == [
        {"dataset": "a", "axis": "breadth", "from": "ok", "to": "thin", "direction": "regressed"}
    ]


def test_a_dataset_that_stopped_being_computed_is_reported_not_skipped() -> None:
    """Silence is exactly what this record exists to break."""
    changes = v.diff({"gone": {"breadth": "ok"}}, {})
    assert [(c["dataset"], c["from"], c["to"]) for c in changes] == [("gone", "ok", None)]


def test_only_an_outright_read_failure_counts_as_unread() -> None:
    """treasury_yield answers unknown on depth because it has no symbol column
    to spread across — a stable fact about the dataset, not a failure to look."""
    rows = [
        _row(dataset="a", error="statement timeout"),
        _row(dataset="b", depth={"at_target": 0, "of": 0}),
    ]
    assert v.unread_datasets(rows) == {"a"}
