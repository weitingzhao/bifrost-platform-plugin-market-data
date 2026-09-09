"""Doctor: findings for the session the tables should hold, and heal executes them."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from bifrost_market_data import doctor as doc

SESSION = date(2026, 9, 4)  # Friday
NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)  # Saturday noon UTC


class _Cur:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent
        self._rows: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        q = " ".join(query.lower().split())
        self.parent.statements.append((q, params))
        d = self.parent.data
        # The option tables are now asked twice with different symbol lists —
        # a ratio for the whole chains, presence for the windowed tiers — so the
        # fake has to honour ``ANY(%s)`` the way PG does, or both reads return
        # the same rows and the split is untestable.
        scope: set[str] | None = None
        if isinstance(params, (list, tuple)) and params and isinstance(params[0], (list, tuple)):
            scope = {str(x).strip().upper() for x in params[0]}

        def _scoped(key: str) -> list[Any]:
            return [(u, n) for u, n in d.get(key, {}).items() if scope is None or u in scope]

        if "from research.option_universe" in q:
            self._rows = list(d.get("universe", []))
        elif "from raw_market.option_contract" in q:
            self._rows = _scoped("live")
        elif "from raw_market.option_snapshot" in q:
            self._rows = _scoped("snapshot")
        elif "from raw_market.option_open_interest" in q:
            self._rows = _scoped("oi")
        elif "from raw_market.stock_daily" in q and "distinct symbol" in q:
            self._rows = [{"symbol": s} for s in d.get("daily_watch", [])]
        elif "from raw_market.stock_daily" in q:
            self._rows = [{"count": d.get("daily", 0)}]
        elif "from raw_market.stock_snapshot" in q:
            self._rows = [{"count": d.get("snap_rows", 0)}]
        elif "from raw_market.ratios" in q:
            self._rows = [{"count": d.get("ratios", 0)}]
        elif "from raw_market.short_volume" in q:
            self._rows = [{"count": d.get("short_volume", 0)}]
        elif "from ops_jobs.ingest_freshness" in q:
            self._rows = list(d.get("freshness", []))
        elif "relpartbound" in q:
            self._rows = list(d.get("partitions", []))
        elif "from ops_jobs.queue_sample" in q:
            # The recorded failure counts, which outlive the job rows.
            self._rows = list(d.get("failed_samples", []))
        elif "status = 'failed'" in q and "group by kind" in q:
            self._rows = list(d.get("failed", []))
        elif "status = 'failed'" in q:
            self._rows = list(d.get("failed_jobs", []))
        elif "status = 'running'" in q:
            self._rows = [{"count": d.get("stuck", 0)}]
        else:
            self._rows = []

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self.statements: list[tuple[str, Any]] = []

    def cursor(self) -> _Cur:
        return _Cur(self)

    def rollback(self) -> None:
        return None


UNIVERSE = ["AAPL", "MSFT", "NVDA", "SPY"]


@pytest.fixture(autouse=True)
def _pin_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(doc, "is_trading_day", lambda conn, d: d.weekday() < 5)
    monkeypatch.setattr(doc, "chain_session", lambda conn, now=None: SESSION)
    monkeypatch.setattr(doc, "fetch_completed_trading_days", lambda conn, n, as_of=None: [SESSION])
    monkeypatch.setattr(doc, "union_iv_radar_benchmarks", lambda syms, cfg=None: list(syms))
    monkeypatch.setattr(doc, "filter_optionable_underlyings", lambda conn, syms: list(syms))


def _fresh(hours: float) -> datetime:
    return NOW - timedelta(hours=hours)


def _healthy_data() -> dict[str, Any]:
    return {
        "live": {u: 1000 for u in UNIVERSE},
        "snapshot": {u: 1000 for u in UNIVERSE},
        "oi": {u: 1000 for u in UNIVERSE},
        "daily": 12496,
        "daily_watch": UNIVERSE,
        "snap_rows": 13157,
        "ratios": 5000,
        "short_volume": 15000,
        "freshness": [
            {"dimension": "calendar", "last_run_at": _fresh(5)},
            {"dimension": "ticker_sync", "last_run_at": _fresh(5)},
            {"dimension": "option_contract", "last_run_at": _fresh(5)},
            {"dimension": "dividends", "last_run_at": _fresh(5)},
            {"dimension": "financials", "last_run_at": _fresh(5)},
        ],
    }


def test_healthy_session_has_no_prescriptions() -> None:
    conn = _Conn(_healthy_data())
    rep = doc.run_doctor(conn, now=NOW, watchlist=UNIVERSE)
    assert rep["session"] == SESSION.isoformat()
    assert rep["session_is_today"] is False
    assert rep["verdict"] == "healthy"
    assert rep["prescriptions"] == []
    assert {f["severity"] for f in rep["findings"]} == {"ok"}
    assert "option-trades" in rep["retired_slots"]
    # The Research gate reads this: EOD data fit for dbt, regardless of rotates.
    assert rep["eod_critical"]["verdict"] == "healthy"
    assert rep["eod_critical"]["findings"] == []


def test_partial_chain_coverage_is_a_finding_not_a_pass() -> None:
    """A session holding 40% of the live chain is broken, even though rows exist."""
    data = _healthy_data()
    data["snapshot"] = {"AAPL": 1000, "MSFT": 400, "NVDA": 300, "SPY": 350}
    data["oi"] = {"AAPL": 1000, "MSFT": 1000, "NVDA": 1000, "SPY": 700}
    data["daily"] = 12
    conn = _Conn(data)
    rep = doc.run_doctor(conn, now=NOW, watchlist=UNIVERSE)
    by_id = {f["id"]: f for f in rep["findings"]}
    snap = by_id[f"option_snapshot:{SESSION.isoformat()}"]
    assert snap["severity"] == "crit"  # 3 of 4 underlyings below 95%
    assert snap["actual"] == "51% (2050)"
    assert snap["missing_sample"] == ["NVDA", "SPY", "MSFT"]
    assert "Worst: NVDA 30%" in snap["detail"]
    assert snap["fix"] == {"action": "enqueue-slot", "slot": "eod-pipeline", "force": True, "date": "2026-09-04"}
    assert by_id[f"option_open_interest:{SESSION.isoformat()}"]["severity"] == "warn"  # 1 of 4
    # A contract listed after the session must not count against that session.
    contract_sql = next(q for q, _p in conn.statements if "option_contract" in q)
    assert "first_seen_at <" in contract_sql
    assert rep["eod_critical"]["verdict"] == "critical"
    assert "Option chain snapshot" in rep["eod_critical"]["detail"]
    assert by_id[f"stock_daily:{SESSION.isoformat()}"]["severity"] == "crit"
    assert rep["verdict"] == "critical"
    slots = [(p["action"], p["slot"]) for p in rep["prescriptions"]]
    # snapshot + OI share one eod-pipeline prescription
    assert slots.count(("enqueue-slot", "eod-pipeline")) == 1
    assert ("enqueue-slot", "universe-daily") in slots
    eod = next(p for p in rep["prescriptions"] if p["slot"] == "eod-pipeline")
    assert set(eod["finding_ids"]) == {f"option_snapshot:{SESSION.isoformat()}", f"option_open_interest:{SESSION.isoformat()}"}


def test_stale_reference_slots_get_dateless_enqueue() -> None:
    data = _healthy_data()
    data["freshness"] = [{"dimension": "calendar", "last_run_at": _fresh(80)}]  # others never written
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    stale = {f["slot"]: f for f in rep["findings"] if f["id"].startswith("stale:")}
    assert stale["calendar"]["severity"] == "warn"
    assert stale["calendar"]["actual"] == 80.0
    assert stale["option-refresh"]["actual"] is None
    assert stale["calendar"]["fix"] == {"action": "enqueue-slot", "slot": "calendar", "force": True}
    assert rep["verdict"] == "degraded"
    # Stale rotates must not block the Research batch — the session's EOD is fine.
    assert rep["eod_critical"]["verdict"] == "healthy"


def test_failed_jobs_prescribe_retry_unless_unentitled() -> None:
    data = _healthy_data()
    data["failed"] = [
        {"kind": "option_bars", "n": 3, "sample_error": "HTTP 500 upstream", "ids": [30, 20, 10]},
        {"kind": "option_trades", "n": 2, "sample_error": "403 NOT_AUTHORIZED: You are not entitled to this data", "ids": [5, 4]},
    ]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    by_id = {f["id"]: f for f in rep["findings"]}
    assert by_id["failed:option_bars"]["fix"] == {"action": "retry-jobs", "kind": "option_bars", "job_ids": [30, 20, 10]}
    assert by_id["failed:option_bars"]["auto_fixable"] is True
    assert by_id["failed:option_trades"]["fix"] is None
    assert "plan does not cover" in by_id["failed:option_trades"]["detail"]
    assert [p for p in rep["prescriptions"] if p["action"] == "retry-jobs"][0]["job_ids"] == [30, 20, 10]


def test_workers_and_vendor_findings_are_informational() -> None:
    rep = doc.run_doctor(
        _Conn(_healthy_data()),
        now=NOW,
        watchlist=UNIVERSE,
        worker_health={
            "stocks": {"jobs_done": 5, "jobs_failed": 0, "last_claim_at": "x", "uptime_sec": 9, "loop_lag_sec": 0.2},
            "options": None,
        },
        vendor={"reachable": True, "status_code": 401, "detail": "vendor answered HTTP 401"},
    )
    by_id = {f["id"]: f for f in rep["findings"]}
    assert by_id["worker:stocks"]["severity"] == "ok"
    assert by_id["worker:options"]["severity"] == "crit"
    assert by_id["worker:options"]["fix"] == {"action": "rollout-restart", "deployment": "polygon-worker-options"}
    assert by_id["worker:options"]["auto_fixable"] is False
    assert by_id["vendor"]["severity"] == "warn"
    assert rep["prescriptions"] == []  # nothing the plugin can execute itself
    assert rep["verdict"] == "critical"
    # An unreachable worker pool is an ops problem, not a reason to fail dbt.
    assert rep["eod_critical"]["verdict"] == "healthy"


def test_session_is_today_after_eod_window_on_trading_day() -> None:
    conn = _Conn(_healthy_data())
    # Friday 20:00 New York = 00:00 UTC Saturday
    late = datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc)
    assert doc.resolve_session(conn, late) == (date(2026, 9, 4), True)
    # Friday 15:00 New York → last completed session (Thursday per the pinned helper → SESSION)
    early = datetime(2026, 9, 4, 19, 0, tzinfo=timezone.utc)
    assert doc.resolve_session(conn, early) == (SESSION, False)


def test_today_session_missing_fundamentals_is_only_a_warning() -> None:
    data = _healthy_data()
    data["ratios"] = 0
    data["short_volume"] = 0
    late = datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc)  # Fri 20:00 NY
    monkeypatch_session = date(2026, 9, 4)
    assert monkeypatch_session == SESSION
    rep = doc.run_doctor(_Conn(data), now=late, watchlist=UNIVERSE)
    f = next(x for x in rep["findings"] if x["id"].startswith("fundamentals_market:"))
    assert rep["session_is_today"] is True
    assert f["severity"] == "warn"
    assert f["auto_fixable"] is False
    assert "morning after" in f["detail"]


def test_heal_dry_run_lists_actions_without_touching_the_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(doc, "enqueue_slot", lambda *a, **k: calls.append((a, k)) or {"enqueued": 1})
    report = {
        "session": "2026-09-04",
        "verdict": "critical",
        "prescriptions": [
            {"finding_ids": ["option_snapshot:2026-09-04"], "action": "enqueue-slot", "slot": "eod-pipeline", "force": True, "date": "2026-09-04"},
            {"finding_ids": ["failed:option_bars"], "action": "retry-jobs", "kind": "option_bars", "job_ids": [1]},
        ],
    }
    out = doc.heal(_Conn({}), report=report, dry_run=True)
    assert out["dry_run"] is True
    assert [a["result"] for a in out["actions"]] == ["dry_run", "dry_run"]
    assert calls == []


def test_heal_executes_selected_findings_only(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []

    def fake_enqueue(conn: Any, slot: str, **kw: Any) -> dict[str, Any]:
        calls.append((slot, kw))
        return {"enqueued": 7, "deduped": 1, "target_date": kw["target_date"].isoformat()}

    monkeypatch.setattr(doc, "enqueue_slot", fake_enqueue)
    report = {
        "session": "2026-09-04",
        "verdict": "critical",
        "prescriptions": [
            {"finding_ids": ["option_snapshot:2026-09-04"], "action": "enqueue-slot", "slot": "eod-pipeline", "force": True, "date": "2026-09-04"},
            {"finding_ids": ["stale:calendar"], "action": "enqueue-slot", "slot": "calendar", "force": True},
        ],
    }
    out = doc.heal(_Conn({}), report=report, finding_ids=["option_snapshot:2026-09-04"], scheduler_cfg={"x": 1})
    assert len(out["actions"]) == 1
    assert out["actions"][0]["result"]["enqueued"] == 7
    assert out["enqueued"] == 7
    assert calls == [("eod-pipeline", {"target_date": date(2026, 9, 4), "scheduler_cfg": {"x": 1}, "force": True})]


def test_heal_retries_failed_jobs_with_original_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    specs_seen: list[Any] = []

    def fake_bulk(conn: Any, specs: Any) -> list[int | None]:
        specs_seen.extend(specs)
        return [1, None]

    monkeypatch.setattr(doc, "insert_jobs_bulk", fake_bulk)
    conn = _Conn({"failed_jobs": [
        {"kind": "option_bars", "payload": {"ticker": "O:X"}, "priority": 2},
        {"kind": "option_bars", "payload": '{"ticker": "O:Y"}', "priority": 0},
    ]})
    report = {"prescriptions": [{"finding_ids": ["failed:option_bars"], "action": "retry-jobs", "kind": "option_bars", "job_ids": [10, 11]}]}
    out = doc.heal(conn, report=report)
    assert out["actions"][0]["result"] == {"enqueued": 1, "deduped": 1}
    assert specs_seen == [("option_bars", {"ticker": "O:X"}, 2, 3), ("option_bars", {"ticker": "O:Y"}, 0, 3)]


def test_heal_reports_action_errors_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any, **k: Any) -> dict[str, Any]:
        raise RuntimeError("db down")

    monkeypatch.setattr(doc, "enqueue_slot", boom)
    report = {"prescriptions": [
        {"finding_ids": ["a"], "action": "enqueue-slot", "slot": "calendar", "force": True},
        {"finding_ids": ["b"], "action": "rollout-restart", "deployment": "polygon-worker-options"},
    ]}
    out = doc.heal(_Conn({}), report=report)
    assert out["actions"][0]["result"] == "error: db down"
    assert out["actions"][1]["result"] == "not executable by the plugin"


def test_doctor_routes_are_registered() -> None:
    from bifrost_market_data.api.app import create_app

    paths = {r.path for r in create_app().routes}
    assert "/market/doctor" in paths
    assert "/market/doctor/heal" in paths


def test_lost_session_is_reported_as_lost_not_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """Once the chain moved on, a gap is not something Fix can repair."""
    monkeypatch.setattr(doc, "chain_session", lambda conn, now=None: date(2026, 9, 8))
    data = _healthy_data()
    data["snapshot"] = {"AAPL": 1000, "MSFT": 10, "NVDA": 10, "SPY": 10}
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    snap = next(f for f in rep["findings"] if f["id"].startswith("option_snapshot:"))
    assert snap["severity"] == "crit"
    assert snap["auto_fixable"] is False
    assert snap["fix"] is None
    assert "lost, not pending" in snap["detail"]
    assert not [p for p in rep["prescriptions"] if p.get("slot") == "eod-pipeline"]


def test_a_normal_vendor_shortfall_is_not_an_alarm() -> None:
    """The vendor returns ~95% of the catalogue; that must read as healthy."""
    data = _healthy_data()
    data["snapshot"] = {u: 940 for u in UNIVERSE}
    data["oi"] = {u: 940 for u in UNIVERSE}
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    snap = next(f for f in rep["findings"] if f["id"].startswith("option_snapshot:"))
    assert snap["severity"] == "ok"
    assert rep["eod_critical"]["verdict"] == "healthy"


def test_a_saturated_pool_reads_as_busy_not_dead() -> None:
    """A backfill holds the worker loop; that is degraded, not a dead pod."""
    rep = doc.run_doctor(
        _Conn(_healthy_data()),
        now=NOW,
        watchlist=UNIVERSE,
        worker_health={
            "stocks": {"jobs_done": 900, "jobs_failed": 0, "last_claim_at": "x", "uptime_sec": 99, "loop_lag_sec": 240.0}
        },
    )
    f = next(x for x in rep["findings"] if x["id"] == "worker:stocks")
    assert f["severity"] == "warn"
    assert "saturated, not down" in f["detail"]
    assert rep["eod_critical"]["verdict"] == "healthy"


# ── The three-tier universe: what the collector collects, checked how it collects ──


def _ruled(data: dict[str, Any], rows: list[tuple[str, str, int]]) -> dict[str, Any]:
    """Publish a research.option_universe rule into the fake."""
    data["universe"] = rows
    return data


def test_universe_widens_to_the_research_rule() -> None:
    """The doctor checks what `eod-pipeline` enqueues, not the watchlist it predates."""
    data = _ruled(
        _healthy_data(), [("AAPL", "resident", 24), ("PLTR", "core", 24), ("SOFI", "edge", 12)]
    )
    data["live"].update({"PLTR": 4000, "SOFI": 900})
    data["snapshot"].update({"PLTR": 120, "SOFI": 40})
    data["oi"].update({"PLTR": 120, "SOFI": 40})
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    # Four watchlist names plus the two the rule added, split by how they are collected.
    assert rep["universe"]["optionable"] == 6
    assert rep["universe"]["whole_chain"] == 4
    assert rep["universe"]["windowed"] == 2


def test_windowed_names_are_judged_on_presence_not_share() -> None:
    """Core and edge chains hold a strike band by design; 3% of the catalogue is not a gap."""
    data = _ruled(_healthy_data(), [("PLTR", "core", 24)])
    data["live"]["PLTR"] = 4000
    data["snapshot"]["PLTR"] = 120
    data["oi"]["PLTR"] = 120
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    by = {f["id"].split(":", 1)[0]: f for f in rep["findings"]}
    assert by["option_chain_windowed"]["severity"] == "ok"
    assert by["option_oi_windowed"]["severity"] == "ok"
    # PLTR's 4,000 live contracts stay out of the ratio's denominator.
    assert by["option_snapshot"]["expected"] == ">= 90% of 4000"
    assert rep["verdict"] == "healthy"


def test_unreached_windowed_name_is_a_finding() -> None:
    """A name with a live chain and no rows for the session is the failure this exists to see."""
    data = _ruled(_healthy_data(), [("PLTR", "core", 24), ("SOFI", "core", 24)])
    data["live"].update({"PLTR": 4000, "SOFI": 900})
    data["snapshot"]["PLTR"] = 120  # SOFI was never reached
    data["oi"]["PLTR"] = 120
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(f for f in rep["findings"] if f["id"].startswith("option_chain_windowed"))
    assert f["severity"] == "crit"  # one of two missing, well past 10%
    assert f["missing_sample"] == ["SOFI"]
    assert f["fix"] == {
        "action": "enqueue-slot",
        "slot": "eod-pipeline",
        "force": True,
        "date": SESSION.isoformat(),
    }


def test_one_absent_name_in_a_large_universe_is_a_warning() -> None:
    """A single name the vendor answered nothing for is not a failed session."""
    ruled = [(f"SYM{i:03d}", "core", 24) for i in range(20)]
    data = _ruled(_healthy_data(), ruled)
    for sym, _tier, _months in ruled:
        data["live"][sym] = 900
        data["snapshot"][sym] = 40
        data["oi"][sym] = 40
    del data["snapshot"]["SYM007"]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(f for f in rep["findings"] if f["id"].startswith("option_chain_windowed"))
    assert f["severity"] == "warn"
    assert f["missing_sample"] == ["SYM007"]


def test_windowed_name_without_a_live_chain_is_not_a_gap() -> None:
    """No unexpired contracts means nothing to collect — the C-B3 attribution."""
    data = _ruled(_healthy_data(), [("PLTR", "core", 24), ("SENEA", "edge", 12)])
    data["live"]["PLTR"] = 4000  # SENEA lists no live contracts at all
    data["snapshot"]["PLTR"] = 120
    data["oi"]["PLTR"] = 120
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(f for f in rep["findings"] if f["id"].startswith("option_chain_windowed"))
    assert f["severity"] == "ok"
    assert f["expected"] == "1 with a live chain"


def test_presence_checks_do_not_gate_the_research_batch() -> None:
    """Widening what blocks dbt is a decision about the gate, not a new check's side effect."""
    data = _ruled(_healthy_data(), [("PLTR", "core", 24), ("SOFI", "core", 24)])
    data["live"].update({"PLTR": 4000, "SOFI": 900})
    data["snapshot"]["PLTR"] = 120
    data["oi"]["PLTR"] = 120
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    assert rep["verdict"] == "critical"
    assert rep["eod_critical"]["verdict"] == "healthy"
    assert not [i for i in rep["eod_critical"]["findings"] if "windowed" in i]


# ── C-F2: overdue is the contract's deadline, not a calendar rollover ──


def _no_fundamentals() -> dict[str, Any]:
    data = _healthy_data()
    data["ratios"] = 0
    data["short_volume"] = 0
    return data


def test_fundamentals_are_not_critical_before_their_deadline() -> None:
    """New York midnight is 04:00 UTC in daylight time; the slot publishes at 04:30.

    The old rule escalated on `session_is_today`, so every summer night had a
    half-hour window where the whole session read critical for data not yet due.
    """
    just_after_ny_midnight = datetime(2026, 9, 5, 4, 10, tzinfo=timezone.utc)
    rep = doc.run_doctor(_Conn(_no_fundamentals()), now=just_after_ny_midnight, watchlist=UNIVERSE)
    f = next(f for f in rep["findings"] if f["id"].startswith("fundamentals_market"))
    assert f["severity"] == "warn"
    assert f["auto_fixable"] is False
    assert "due" in f["detail"]


def test_fundamentals_are_critical_once_the_deadline_passes() -> None:
    past_the_deadline = datetime(2026, 9, 6, 3, 0, tzinfo=timezone.utc)
    rep = doc.run_doctor(_Conn(_no_fundamentals()), now=past_the_deadline, watchlist=UNIVERSE)
    f = next(f for f in rep["findings"] if f["id"].startswith("fundamentals_market"))
    assert f["severity"] == "crit"
    assert f["auto_fixable"] is True


# ── Failure counts come from the record; retries come from what is still there ──


def _failing(**kw: Any) -> dict[str, Any]:
    data = _healthy_data()
    data.update(kw)
    return data


def test_failure_counts_come_from_the_samples_not_the_surviving_rows() -> None:
    """40,000 finished rows was fifteen minutes at 2,700 jobs a minute.

    Counting the rows still on the queue answered "how many failed today" with
    however many happened to survive the trim.
    """
    rep = doc.run_doctor(
        _Conn(
            _failing(
                failed_samples=[("option_daily", 1956)],
                failed=[{"kind": "option_daily", "n": 12, "sample_error": "invalid ticker", "ids": [7, 8]}],
            )
        ),
        now=NOW,
        watchlist=UNIVERSE,
    )
    f = next(x for x in rep["findings"] if x["id"] == "failed:option_daily")
    assert f["actual"] == 1956  # what the record says
    assert "12 still on the queue and retryable" in f["detail"]
    assert f["fix"] == {"action": "retry-jobs", "kind": "option_daily", "job_ids": [7, 8]}


def test_a_kind_whose_rows_were_all_trimmed_still_reports_but_offers_no_retry() -> None:
    rep = doc.run_doctor(
        _Conn(_failing(failed_samples=[("stock_daily", 41)], failed=[])),
        now=NOW,
        watchlist=UNIVERSE,
    )
    f = next(x for x in rep["findings"] if x["id"] == "failed:stock_daily")
    assert f["actual"] == 41
    assert "no error text or retry survives" in f["detail"]
    assert f["fix"] is None
    assert f["auto_fixable"] is False


def test_without_the_sample_table_the_queue_rows_still_answer() -> None:
    """An older database has no samples; the check degrades, it does not vanish."""
    rep = doc.run_doctor(
        _Conn(
            _failing(
                failed_samples=[],
                failed=[{"kind": "option_daily", "n": 6, "sample_error": "boom", "ids": [1]}],
            )
        ),
        now=NOW,
        watchlist=UNIVERSE,
    )
    f = next(x for x in rep["findings"] if x["id"] == "failed:option_daily")
    assert f["actual"] == 6
    assert "still on the queue" not in f["detail"]


# ── Partition runway: running out is an outage, not a degradation ──


def _bound(to_date: str) -> str:
    return f"FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('{to_date} 00:00:00+00')"


def test_a_table_running_out_of_partitions_is_reported() -> None:
    """A row with no partition to land in is rejected outright."""
    data = _healthy_data()
    data["partitions"] = [("option_snapshot", True, _bound("2026-09-20"))]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(x for x in rep["findings"] if x["id"] == "partition_runway:option_snapshot")
    # Sixteen days from the 2026-09-04 session: short of the runway, but there
    # is still time for the nightly build-ahead to do it.
    assert f["severity"] == "warn"
    assert f["actual"] == "16 days"
    assert "through 2026-09-20" in f["detail"]
    assert "builds them ahead automatically" in f["detail"]


def test_two_weeks_of_runway_is_critical() -> None:
    """Close enough that the next build-ahead is the last chance to matter."""
    data = _healthy_data()
    data["partitions"] = [("option_snapshot", True, _bound("2026-09-12"))]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(x for x in rep["findings"] if x["id"] == "partition_runway:option_snapshot")
    assert f["severity"] == "crit"


def test_not_owning_the_table_is_critical_however_far_out_it_reaches() -> None:
    """The plugin cannot extend a table it does not own, so time does not help."""
    data = _healthy_data()
    data["partitions"] = [("option_snapshot", False, _bound("2026-10-10"))]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(x for x in rep["findings"] if x["id"] == "partition_runway:option_snapshot")
    assert f["severity"] == "crit"
    assert "fix_object_ownership.sql" in f["detail"]


def test_a_long_runway_is_not_a_finding() -> None:
    data = _healthy_data()
    data["partitions"] = [("option_snapshot", True, _bound("2027-06-01"))]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    assert not [x for x in rep["findings"] if x["id"].startswith("partition_runway:")]


def test_the_furthest_partition_is_the_one_that_counts() -> None:
    """Several partitions per table; the runway is the last one, not the first."""
    data = _healthy_data()
    data["partitions"] = [
        ("option_snapshot", True, _bound("2026-09-10")),
        ("option_snapshot", True, _bound("2027-06-01")),
    ]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    assert not [x for x in rep["findings"] if x["id"].startswith("partition_runway:")]
