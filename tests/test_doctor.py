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
        if "from raw_market.option_contract" in q:
            self._rows = [(u, n) for u, n in d.get("live", {}).items()]
        elif "from raw_market.option_snapshot" in q:
            self._rows = [(u, n) for u, n in d.get("snapshot", {}).items()]
        elif "from raw_market.option_open_interest" in q:
            self._rows = [(u, n) for u, n in d.get("oi", {}).items()]
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
        worker_health={"stocks": {"jobs_done": 5, "jobs_failed": 0, "last_claim_at": "x", "uptime_sec": 9}, "options": None},
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
