"""The doctor learns to see a hole it can still fix.

Before this the doctor was a per-session check with no memory: it reported
2026-08-11 as a problem on 2026-08-11 and forgot by the next morning, so the
hole sat there for a month. The fourth axis had the memory but was a read-only
measure. What found a hole could not fix it; what fixed could not find it.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

from bifrost_market_data import continuity as cont
from bifrost_market_data import trading_calendar as cal
from bifrost_market_data.doctor import CONTINUITY_MAX_PRESCRIBED, _continuity_findings

TODAY = date(2026, 9, 10)


def _sessions(n: int = 40) -> list[date]:
    out, d = [], TODAY - timedelta(days=60)
    while len(out) < n and d <= TODAY:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


@pytest.fixture()
def wired(monkeypatch: pytest.MonkeyPatch):
    """Answer the calendar and the presence probe; the SQL itself is not the subject.

    ``gaps[table]`` is the days that table holds no row for — None means the
    read failed, which is not the same as "no days are missing".
    """
    sessions = _sessions()
    gaps: dict[str, list[date] | None] = {}

    def fake_days(conn: Any, *, start: date, end: date) -> list[date]:
        return [d for d in sessions if start <= d <= end]

    def fake_missing(conn: Any, table: str, column: str, days: Any, **kw: Any):
        if table in gaps:
            return gaps[table]
        return []

    monkeypatch.setattr(cal, "expected_trading_days", fake_days)
    monkeypatch.setattr(cont, "missing_sessions", fake_missing)
    return sessions, gaps


def test_a_clean_window_prescribes_nothing(wired) -> None:
    _sess, _gaps = wired
    out = _continuity_findings(None, today=TODAY)
    assert out, "every backfillable dataset should still report"
    assert {f.severity for f in out} == {"ok"}
    assert not [f for f in out if f.fix]


def test_a_missing_session_becomes_a_prescription(wired) -> None:
    sessions, gaps = wired
    hole = sessions[20]
    gaps["raw_market.option_daily"] = [hole]

    out = _continuity_findings(None, today=TODAY)
    hit = [f for f in out if f.id == f"continuity:option_daily:{hole}"]
    assert len(hit) == 1
    f = hit[0]
    assert f.severity == "warn"
    assert f.auto_fixable is True
    assert f.fix == {
        "action": "enqueue-slot",
        "slot": "option-bars",
        "force": True,
        "date": hole.isoformat(),
    }
    assert str(hole) in f.detail


def test_a_dataset_that_cannot_be_refilled_is_never_prescribed_for(wired) -> None:
    """An EOD chain download only returns the current session. A prescription
    for a past one would be a lie, not a repair — 2026-08-11 is gone."""
    sessions, gaps = wired
    gaps["raw_market.option_snapshot"] = list(sessions[-5:])
    out = _continuity_findings(None, today=TODAY)
    assert not [f for f in out if "option_snapshot" in f.id]
    assert not [f for f in out if "ratios" in f.id]
    assert not [f for f in out if "short_interest" in f.id]


def test_the_prescription_cap_is_stated_not_silent(wired) -> None:
    """One option-bars day is ~70,000 jobs; an unbounded run would enqueue millions."""
    sessions, gaps = wired
    holes = set(sessions[10:20])
    gaps["raw_market.option_daily"] = sorted(holes)

    out = [f for f in _continuity_findings(None, today=TODAY) if f.id.startswith("continuity:option_daily:")]
    assert len(out) == CONTINUITY_MAX_PRESCRIBED
    assert all(str(len(holes)) in f.detail for f in out)
    assert any("left for the next" in f.detail for f in out)
    # The oldest first, so a backlog drains in order rather than at random.
    assert [f.session for f in out] == [d.isoformat() for d in sorted(holes)[:CONTINUITY_MAX_PRESCRIBED]]


def test_days_before_the_dataset_existed_are_not_holes(wired) -> None:
    """A dataset that starts midway through the window has not lost anything."""
    sessions, gaps = wired
    # Nothing before session 25 — the dataset did not exist yet.
    gaps["raw_market.option_daily"] = list(sessions[:25])
    out = [f for f in _continuity_findings(None, today=TODAY) if "option_daily" in f.id]
    assert [f.severity for f in out] == ["ok"]


def test_an_unreadable_dataset_is_skipped_not_guessed_at(wired) -> None:
    sessions, gaps = wired
    gaps["raw_market.option_daily"] = None  # a failed read, not an empty answer
    out = _continuity_findings(None, today=TODAY)
    assert not [f for f in out if "option_daily" in f.id]
    assert [f for f in out if "stock_daily" in f.id], "one bad read must not sink the rest"


def test_it_stays_out_of_the_gate_that_blocks_research(wired) -> None:
    from bifrost_market_data.doctor import EOD_CRITICAL_CHECKS

    sessions, gaps = wired
    gaps["raw_market.option_daily"] = [sessions[20]]
    for f in _continuity_findings(None, today=TODAY):
        assert f.id.split(":", 1)[0] not in EOD_CRITICAL_CHECKS
