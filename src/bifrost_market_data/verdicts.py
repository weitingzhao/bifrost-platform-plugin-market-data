"""What colour a mark is — decided here, once.

The four axes are measured in this plugin and were, until now, *judged* in the
console: `dimensionsModel.ts` held the thresholds that turn 25/575 into "thin".
That was one ruler too many. C-G1 says the contract table is the only source of
denominators, thresholds and session definitions, and a threshold living in a
panel is a threshold no other reader can see — the doctor prescribes in Python
and could never have been told that a verdict went backwards.

So the rank moves here and the panel reads it. The console keeps its own copy
only as the fallback for a payload from an older plugin; the two are held to the
same answers by `tests/test_verdicts.py`, which carries the cases the console's
suite carries.

Five values, and only three of them are ranked. `boundary` says the instrument
is pointed at the wrong question — a top-N list is not partial coverage of the
market — and `unknown` says the read failed. Neither is a position on a scale,
so neither may be compared against `ok`; `RANK` leaves them out and every
caller has to decide what to do about that rather than accidentally calling a
boundary an improvement.
"""

from __future__ import annotations

from typing import Any, Mapping

#: Depth kinds that name a plan boundary rather than a target to reach.
BOUNDARY_KINDS = frozenset({"current_only", "catalogue", "forward_only"})

#: Severity order, worst last. Absent keys are deliberately unranked: a
#: boundary and a failed read are not points on this scale.
RANK: dict[str, int] = {"ok": 0, "partial": 1, "thin": 2}

AXES = ("breadth", "depth", "freshness", "continuity")

#: How many of a dataset's own publication intervals may pass before the newest
#: row counts as late. Mirrors coverage_dimensions.OVERDUE_INTERVALS, which
#: computes the `overdue` flag this module only reads.
_OK, _PARTIAL, _THIN, _BOUNDARY, _UNKNOWN = "ok", "partial", "thin", "boundary", "unknown"


def _f(row: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    v = row.get(key)
    return v if isinstance(v, Mapping) else {}


def breadth_verdict(row: Mapping[str, Any]) -> str:
    """How much of what this tier asked for we hold."""
    if row.get("error"):
        return _UNKNOWN
    b = _f(row, "breadth")
    # A top-N list is not partial coverage of the market, and a catalogue of
    # events that happened is not partial coverage of the instruments they
    # could have happened to. Both rendered red — 0.4% and 14.2% — with
    # nothing wrong.
    if b.get("judged") is False:
        return _BOUNDARY
    pct = b.get("pct")
    if pct is None:
        return _UNKNOWN
    if pct >= 95:
        return _OK
    return _PARTIAL if pct >= 50 else _THIN


def depth_verdict(row: Mapping[str, Any]) -> str:
    """How many symbols reach the contract's history target."""
    if row.get("error"):
        return _UNKNOWN
    d = _f(row, "depth")
    target = d.get("target") if isinstance(d.get("target"), Mapping) else {}
    if not d.get("measured"):
        # A boundary is not a gap: the vendor cannot backfill it, or it is not
        # a series at all.
        return _BOUNDARY if target.get("kind") in BOUNDARY_KINDS else _UNKNOWN
    # An absolute start cannot be judged per symbol without knowing when each
    # instrument began: every one of income_statement's 4,467 symbols "failed"
    # a 2009 target while the median held 9.7 years.
    if d.get("judged") is False:
        return _BOUNDARY
    at = d.get("at_target") or 0
    of = d.get("of") or 0
    if not of:
        return _UNKNOWN
    pct = 100.0 * at / of
    if pct >= 95:
        return _OK
    return _PARTIAL if pct >= 50 else _THIN


def freshness_verdict(row: Mapping[str, Any]) -> str:
    """Whether the newest row is late, in the unit this dataset publishes in.

    Counted in sessions rather than hours. `newest` is a date, so measuring from
    its midnight makes a feed that landed at 22:00 look 44 hours old the next
    evening — which is how one blanket 24-hour rule produced four different
    verdicts for one dataset.
    """
    if row.get("error"):
        return _UNKNOWN
    f = _f(row, "freshness")
    # measured:false only where the contract has no date column at all — a
    # catalogue lists what exists, it does not observe it. A plan boundary, the
    # way depth already treats one, not ignorance.
    if not f.get("measured"):
        return _BOUNDARY
    if not f.get("newest"):
        return _UNKNOWN
    # A company files when it files. Against a 48-hour deadline the three
    # statements read 39 days late with nothing wrong.
    if f.get("judged") is False:
        return _BOUNDARY
    cadence = f.get("cadence")
    if cadence is not None and cadence != "session":
        overdue = f.get("overdue")
        # None means the interval was not measurable — not a licence to fall
        # back on an hour deadline that does not apply to this cadence.
        if overdue is None:
            return _UNKNOWN
        return _THIN if overdue else _OK
    behind = f.get("days_behind")
    if behind is None:
        return _UNKNOWN
    # The session itself, plus whatever the contract allows after it.
    hours = f.get("deadline_hours") or 0
    allowed = -(-int(hours) // 24) + 1
    if behind <= allowed:
        return _OK
    return _PARTIAL if behind <= allowed * 3 else _THIN


def continuity_verdict(row: Mapping[str, Any]) -> str:
    """Whether the middle is solid.

    A session that never landed and a session that landed nearly empty are
    counted together here, because for a reader both are a day of missing
    data — the label keeps them apart, because for whoever fixes it they are
    different faults.
    """
    if row.get("error"):
        return _UNKNOWN
    c = _f(row, "continuity")
    if not c.get("measured"):
        # A catalogue has no cadence and a quarterly filing is not a daily
        # series; neither can have a gap, so neither is a gap.
        return _BOUNDARY if c.get("why") else _UNKNOWN
    present = c.get("days_present") or 0
    absent = c.get("days_absent") or 0
    holes = absent + (c.get("days_thin") or 0)
    sessions = present + absent
    if sessions == 0:
        return _UNKNOWN
    if holes == 0:
        return _OK
    return _PARTIAL if holes / sessions <= 0.1 else _THIN


_BY_AXIS = {
    "breadth": breadth_verdict,
    "depth": depth_verdict,
    "freshness": freshness_verdict,
    "continuity": continuity_verdict,
}


def verdicts_for(row: Mapping[str, Any]) -> dict[str, str]:
    """The four verdicts for one dataset row of the dimensions payload."""
    return {axis: fn(row) for axis, fn in _BY_AXIS.items()}


def verdict_map(datasets: Any) -> dict[str, dict[str, str]]:
    """`{dataset: {axis: verdict}}` for a whole payload — the recorded unit.

    Keyed by dataset name rather than by position, so a contract added or
    removed between two samples shows up as an appearance or a disappearance
    instead of shifting every verdict after it by one.
    """
    out: dict[str, dict[str, str]] = {}
    for row in datasets or []:
        if not isinstance(row, Mapping):
            continue
        name = row.get("dataset")
        if not name:
            continue
        out[str(name)] = row.get("verdicts") or verdicts_for(row)
    return out


def direction(before: str, after: str) -> str:
    """`regressed` / `recovered` / `changed` — never a number.

    Only `ok`, `partial` and `thin` sit on a scale. A move to or from a
    boundary or an unreadable dataset is a change worth showing and not a
    direction worth claiming: `option_snapshot` depth becoming a declared plan
    boundary was an improvement in the measurement, not a decline in the data.
    """
    a, b = RANK.get(before), RANK.get(after)
    if a is None or b is None:
        return "changed"
    if b > a:
        return "regressed"
    return "recovered" if b < a else "changed"


def diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[dict[str, str]]:
    """Every axis whose verdict differs between two recorded maps.

    Datasets present in only one of the two are reported with `None` on the
    missing side rather than skipped — a contract that stopped being computed
    is exactly the kind of silence this record exists to break.
    """
    out: list[dict[str, str]] = []
    for name in sorted(set(before) | set(after)):
        b = before.get(name) or {}
        a = after.get(name) or {}
        if not isinstance(b, Mapping) or not isinstance(a, Mapping):
            continue
        for axis in AXES:
            was, now = b.get(axis), a.get(axis)
            if was == now:
                continue
            out.append(
                {
                    "dataset": name,
                    "axis": axis,
                    "from": was,
                    "to": now,
                    "direction": direction(str(was), str(now)),
                }
            )
    return out


__all__ = [
    "AXES",
    "BOUNDARY_KINDS",
    "RANK",
    "breadth_verdict",
    "depth_verdict",
    "freshness_verdict",
    "continuity_verdict",
    "verdicts_for",
    "verdict_map",
    "direction",
    "diff",
]
