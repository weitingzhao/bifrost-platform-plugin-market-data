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

    # A steady breadth, so a test about presence is not also a test about a
    # failed breadth read -- which is a finding of its own now.
    def flat_breadth(conn: Any, table: str, date_col: str, sym_col: str, **kw: Any):
        return [(d, 340) for d in sessions if d not in (gaps.get(table) or [])]

    monkeypatch.setattr(cal, "expected_trading_days", fake_days)
    monkeypatch.setattr(cont, "missing_sessions", fake_missing)
    monkeypatch.setattr(cont, "per_day_breadth", flat_breadth)
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


def test_a_self_healing_dataset_gets_no_prescription(wired) -> None:
    """treasury_yield's slot re-pulls thirty days on every run.

    Prescribing an enqueue for a session it will fix by itself is noise, and
    telling a reader it is unrecoverable — which the brief did — is wrong.
    """
    sessions, gaps = wired
    gaps["raw_market.treasury_yield"] = [sessions[20]]
    out = _continuity_findings(None, today=TODAY)
    assert not [f for f in out if "treasury_yield" in f.id]


@pytest.fixture()
def breadth(wired, monkeypatch: pytest.MonkeyPatch):
    """Distinct symbols per session: ``series[table]`` overrides a flat 340."""
    sessions, gaps = wired
    series: dict[str, dict[date, int] | None] = {}

    def fake_breadth(conn: Any, table: str, date_col: str, sym_col: str, **kw: Any):
        if table in series and series[table] is None:
            return None
        override = series.get(table) or {}
        return [(d, override.get(d, 340)) for d in sessions if d not in (gaps.get(table) or [])]

    monkeypatch.setattr(cont, "per_day_breadth", fake_breadth)
    return sessions, gaps, series


def test_a_narrow_session_is_prescribed_like_a_missing_one(breadth) -> None:
    """2026-09-09: 26 underlyings against ~340 either side. Present, so the
    presence probe passed it, and nothing prescribed the refill."""
    sessions, _gaps, series = breadth
    day = sessions[20]
    series["raw_market.option_daily"] = {day: 26}
    out = _continuity_findings(None, today=TODAY, session=sessions[-1])
    hit = [f for f in out if f.id == f"continuity:option_daily:{day}"]
    assert len(hit) == 1
    f = hit[0]
    assert f.severity == "warn" and f.auto_fixable is True
    assert f.title == "Narrow session: option_daily"
    assert f.fix == {"action": "enqueue-slot", "slot": "option-bars", "force": True, "date": day.isoformat()}
    assert "26 symbols" in f.detail and "340" in f.detail


def test_option_daily_breadth_counts_only_the_expiries_atm_iv_reads(wired, monkeypatch) -> None:
    """2026-08-17..21: ~650 underlyings, ~230 of them only through that week's expiries."""
    sessions, _gaps = wired
    asked: dict[str, Any] = {}

    def fake_breadth(conn: Any, table: str, date_col: str, sym_col: str, **kw: Any):
        asked[table] = kw.get("where")
        return [(d, 420 if d == sessions[20] and kw.get("where") else 650) for d in sessions]

    monkeypatch.setattr(cont, "per_day_breadth", fake_breadth)
    out = _continuity_findings(None, today=TODAY, session=sessions[-1])
    assert asked["raw_market.option_daily"] == "expiry BETWEEN bar_date + 5 AND bar_date + 90"
    assert asked["raw_market.stock_daily"] is None
    [f] = [f for f in out if f.id == f"continuity:option_daily:{sessions[20]}"]
    assert f.title == "Narrow session: option_daily"
    assert "420 symbols with rows where expiry BETWEEN bar_date + 5 AND bar_date + 90" in f.detail


def test_breadth_where_is_added_to_the_count() -> None:
    seen: list[str] = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            seen.append(sql)

        def fetchall(self):
            return []

    class _Conn:
        def cursor(self):
            return _Cur()

    cont.per_day_breadth(_Conn(), "raw_market.option_daily", "bar_date", "underlying", window_days=60, where="x > 1")
    assert "AND (x > 1)" in seen[-1]
    cont.per_day_breadth(_Conn(), "raw_market.stock_daily", "bar_date", "symbol", window_days=60)
    assert "AND (" not in seen[-1]


def test_breadth_bounds_are_dates_the_planner_can_prune_on() -> None:
    """2026-09-28: ``current_date - n`` with no upper bound kept all eighteen
    option_daily partitions in the plan -- the DEFAULT one's index was read for
    nothing and the cost crossed the JIT inlining threshold, 32.5s in all."""
    seen: list[tuple[str, Any]] = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            seen.append((sql, params))

        def fetchall(self):
            return [(date(2026, 9, 25), 650)]

    class _Conn:
        def cursor(self):
            return _Cur()

    out = cont.per_day_breadth(
        _Conn(), "raw_market.option_daily", "bar_date", "underlying", window_days=81, today=date(2026, 9, 28)
    )
    assert out == [(date(2026, 9, 25), 650)]
    sql, params = seen[-1]
    assert "current_date" not in sql
    assert "bar_date >= %s AND bar_date < %s" in sql
    assert params == (date(2026, 7, 9), date(2026, 9, 29))
    # Pairs first, then a count: count(DISTINCT) sorts every row it reads.
    assert "count(DISTINCT" not in sql
    assert "SELECT DISTINCT bar_date::date AS d, underlying" in sql


def test_an_unread_breadth_is_unprobed_not_a_clean_bill(breadth) -> None:
    """2026-09-28 the breadth reads timed out and the finding still said "none narrow"."""
    sessions, _gaps, series = breadth
    series["raw_market.short_volume"] = None
    out = [f for f in _continuity_findings(None, today=TODAY, session=sessions[-1]) if "short_volume" in f.id]
    assert [(f.id, f.severity) for f in out] == [("continuity:short_volume", "warn")]
    assert "narrow unprobed" in out[0].actual
    assert "none narrow" not in out[0].actual
    assert out[0].fix is None


def test_an_unread_breadth_is_named_beside_the_missing_sessions(breadth) -> None:
    sessions, gaps, series = breadth
    gaps["raw_market.option_daily"] = [sessions[20]]
    series["raw_market.option_daily"] = None
    out = [f for f in _continuity_findings(None, today=TODAY, session=sessions[-1]) if "option_daily" in f.id]
    assert [f.id for f in out] == ["continuity:option_daily", f"continuity:option_daily:{sessions[20]}"]
    assert out[0].severity == "warn" and "1 missing; narrow unprobed" in out[0].actual


def test_a_step_up_in_breadth_is_not_narrow(breadth) -> None:
    """The universe grew from 337 to 575 on 2026-09-08; the days before are not holes."""
    sessions, _gaps, series = breadth
    series["raw_market.option_daily"] = {d: (337 if i < 25 else 575) for i, d in enumerate(sessions)}
    out = [f for f in _continuity_findings(None, today=TODAY, session=sessions[-1]) if "option_daily" in f.id]
    assert [f.severity for f in out] == ["ok"]


def test_the_session_being_written_is_not_judged_narrow(breadth) -> None:
    """At 22:05 UTC tonight's rows are half written; the heal must not refill them mid-write."""
    sessions, _gaps, series = breadth
    series["raw_market.stock_daily"] = {sessions[-1]: 12}
    out = [f for f in _continuity_findings(None, today=TODAY, session=sessions[-1]) if "stock_daily" in f.id]
    assert [f.severity for f in out] == ["ok"]


def test_minute_tables_are_not_judged_on_breadth(breadth) -> None:
    """They rotate by design; a narrow minute session is the rotation, not a hole."""
    sessions, _gaps, series = breadth
    for t in ("raw_market.stock_minute", "raw_market.option_minute"):
        series[t] = {sessions[20]: 1}
    out = _continuity_findings(None, today=TODAY, session=sessions[-1])
    assert not [f for f in out if "minute" in f.id and f.severity != "ok"]


def test_narrow_and_missing_share_one_cap_oldest_first(breadth) -> None:
    sessions, gaps, series = breadth
    gaps["raw_market.option_daily"] = [sessions[22], sessions[24]]
    series["raw_market.option_daily"] = {sessions[21]: 5, sessions[23]: 5}
    out = [f for f in _continuity_findings(None, today=TODAY, session=sessions[-1])
           if f.id.startswith("continuity:option_daily:")]
    assert [f.session for f in out] == [d.isoformat() for d in sessions[21:24]]
    assert any("left for the next" in f.detail for f in out)


def test_an_unreadable_breadth_still_reports_missing_sessions(breadth) -> None:
    sessions, gaps, series = breadth
    gaps["raw_market.option_daily"] = [sessions[20]]
    series["raw_market.option_daily"] = None
    out = [f for f in _continuity_findings(None, today=TODAY, session=sessions[-1])
           if f.id.startswith("continuity:option_daily:")]
    assert [f.session for f in out] == [sessions[20].isoformat()]


def test_short_volume_is_prescribed_by_kind_not_by_slot(wired) -> None:
    """Its slot would also fire ratios_market, whose endpoint ignores the date."""
    sessions, gaps = wired
    hole = sessions[20]
    gaps["raw_market.short_volume"] = [hole]
    hit = [
        f for f in _continuity_findings(None, today=TODAY)
        if f.id == f"continuity:short_volume:{hole}"
    ]
    assert len(hit) == 1
    assert hit[0].fix == {
        "action": "enqueue",
        "kind": "short_volume_market",
        "payload": {"date": hole.isoformat()},
    }
