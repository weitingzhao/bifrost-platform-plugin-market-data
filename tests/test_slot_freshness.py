"""TD-101: a policed slot is judged by its own deliveries, not a shared dimension row.

``ops_jobs.ingest_freshness`` is keyed by dimension, and three policed slots
share theirs with a sibling: reference with ticker-details (``ticker_sync``),
option-refresh with option-contract-expired (``option_contract``), corporate
with corporate-backfill (``dividends``). Measured 2026-10-06: ``ticker_sync``
read 12.7h old off a detail job while the last reference walk was 18.7h old —
had the walk stopped, the doctor would have gone on calling it fresh.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Self

import pytest
from test_daily import _DailyConn
from test_doctor import NOW, UNIVERSE, _Conn, _healthy_data

from bifrost_market_data import doctor as doc
from bifrost_market_data.api import ingest_dashboard as dash
from bifrost_market_data.api.ingest_dashboard import SLOT_EVIDENCE
from bifrost_market_data.contracts import staleness_by_slot
from bifrost_market_data.freshness import (
    JOB_SHAPE_COLUMNS_SQL,
    POLICED_SLOT_KINDS,
    SHAPE_NAMED_SLOTS,
    payload_from_shape,
    policed_slot_for_job,
    slot_freshness_key,
)
from bifrost_market_data.scheduler import daily
from bifrost_market_data.scheduler.daily import (
    MIGRATED_ANALYTICS_SLOTS,
    SLOT_NAMES,
    UNENTITLED_SLOTS,
    enqueue_slot,
)
from bifrost_market_data.worker.claim import JobRow
from bifrost_market_data.worker.health import HealthState
from bifrost_market_data.worker.loop import process_one_job

TUESDAY = date(2026, 9, 15)


def _jobs_of(slot: str, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """What ``slot`` really enqueues, through the scheduler's own code."""
    monkeypatch.setattr(daily, "load_pinned_contracts", lambda conn: [])
    monkeypatch.setattr(daily, "load_pinned_underlyings", lambda conn: set())
    monkeypatch.setattr(daily, "tickers_needing_detail", lambda conn, limit=200: ["AAPL", "MSFT"])
    conn = _DailyConn(
        research_universe=[("SPY", "resident", 24), ("AAPL", "core", 24), ("HALO", "edge", 12)],
        cs_universe=["AAPL", "MSFT"],
    )
    r = enqueue_slot(
        conn,
        slot,
        target_date=TUESDAY,
        fire_date=TUESDAY,
        watchlist_symbols=["AAPL", "MSFT"],
        scheduler_cfg={"slots": {slot: {"universe": "research"}}},
        force=True,
    )
    return list(r.get("jobs") or [])


_RUNNABLE = [
    s
    for s in SLOT_NAMES
    if s not in MIGRATED_ANALYTICS_SLOTS and s not in UNENTITLED_SLOTS and s != "trim"
]


@pytest.mark.parametrize("slot", _RUNNABLE)
def test_no_slot_enqueues_a_job_shaped_like_another_policed_slot(
    slot: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ratchet: only a policed slot's own jobs can evidence it."""
    try:
        jobs = _jobs_of(slot, monkeypatch)
    except Exception as exc:  # noqa: BLE001 — a slot the fake cannot drive
        pytest.skip(f"{slot}: {exc}")
    named = {policed_slot_for_job(j["kind"], j["payload"]) for j in jobs} - {None}
    assert named <= {slot}, f"{slot} enqueues jobs the doctor would credit to {named - {slot}}"
    if slot in SHAPE_NAMED_SLOTS:
        assert named == {slot}, f"{slot}'s own jobs must evidence it"


@pytest.mark.parametrize("slot", sorted(SHAPE_NAMED_SLOTS))
def test_every_policed_slot_is_driven_and_named(
    slot: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A policed slot the fake could not drive would pass the ratchet by skipping."""
    jobs = _jobs_of(slot, monkeypatch)
    assert any(policed_slot_for_job(j["kind"], j["payload"]) == slot for j in jobs)
    assert {j["kind"] for j in jobs} & set(POLICED_SLOT_KINDS)


def test_the_slots_that_share_a_dimension_are_the_ones_this_guards() -> None:
    """If the sharing changes, revisit ``policed_slot_for_job``."""
    by_slot = staleness_by_slot()
    shared: dict[str, set[str]] = {}
    for slot in doc.POLICED_SLOTS:
        dim = by_slot[slot][0]
        others = {
            s for s, ev in SLOT_EVIDENCE.items() if ev.get("freshness") == dim and s != slot
        }
        if others:
            shared[slot] = others
    assert shared == {
        "reference": {"ticker-details"},
        "option-refresh": {"option-contract-expired"},
        "corporate": {"corporate-backfill"},
    }


# ── the worker writes the slot row ──────────────────────────────────────────


class _Cur:
    def __init__(self, parent: _FreshConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FreshConn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def _job(kind: str, payload: dict[str, Any]) -> JobRow:
    return JobRow(
        id=7,
        kind=kind,
        payload=payload,
        payload_hash=None,
        priority=0,
        status="running",
        result=None,
        attempts=1,
        max_attempts=3,
        created_at=None,
        updated_at=None,
        started_at=None,
        finished_at=None,
    )


async def _freshness_writes(
    monkeypatch: pytest.MonkeyPatch, kind: str, payload: dict[str, Any], rows: int
) -> list[tuple[str, int, str]]:
    monkeypatch.setattr("bifrost_market_data.worker.loop.mark_done", lambda *a, **k: None)

    async def handler(_job: JobRow) -> dict[str, Any]:
        return {"rows_written": rows}

    conn = _FreshConn()
    await process_one_job(
        conn, _job(kind, payload), handlers={kind: handler}, health=HealthState(pool="stocks")
    )
    return [p for q, p in conn.statements if "ingest_freshness" in q.lower()]


@pytest.mark.asyncio
async def test_a_detail_job_bumps_the_dimension_and_its_own_slot_not_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes = await _freshness_writes(
        monkeypatch, "ticker_sync", {"mode": "detail", "symbol": "AAPL"}, 1
    )
    assert writes == [("ticker_sync", 1, "ok"), ("slot:ticker-details", 1, "ok")]


@pytest.mark.asyncio
async def test_a_delisted_lookup_is_nobody_s_slot_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes = await _freshness_writes(
        monkeypatch, "ticker_sync", {"mode": "delisted", "symbol": "XYZ"}, 1
    )
    assert writes == [("ticker_sync", 1, "ok")]


@pytest.mark.asyncio
async def test_the_universe_walk_bumps_its_slot_row(monkeypatch: pytest.MonkeyPatch) -> None:
    writes = await _freshness_writes(monkeypatch, "ticker_sync", {"mode": "universe"}, 5300)
    assert writes == [("ticker_sync", 5300, "ok"), ("slot:reference", 5300, "ok")]


@pytest.mark.asyncio
async def test_a_slot_job_that_delivered_nothing_is_not_slot_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes = await _freshness_writes(
        monkeypatch, "option_contract", {"underlying": "ESQ", "expired": False}, 0
    )
    assert writes == [("option_contract", 0, "ok")]


# ── the doctor reads it ─────────────────────────────────────────────────────


def _reference(data: dict[str, Any]) -> dict[str, Any]:
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    return next(f for f in rep["findings"] if f["id"] == "stale:reference")


def _without(data: dict[str, Any], dimension: str) -> dict[str, Any]:
    data["freshness"] = [r for r in data["freshness"] if r["dimension"] != dimension]
    return data


def test_a_stopped_reference_walk_reads_stale_while_ticker_details_keeps_the_row_fresh() -> None:
    """The TD-101 ratchet: the sibling's fresh ``ticker_sync`` row does not save it."""
    data = _without(_healthy_data(), "slot:reference")
    data["freshness"] = [
        {"dimension": "ticker_sync", "last_run_at": NOW - timedelta(hours=1)},
        *[r for r in data["freshness"] if r["dimension"] != "ticker_sync"],
        {"dimension": "slot:reference", "last_run_at": NOW - timedelta(hours=60)},
    ]
    f = _reference(data)
    assert f["severity"] == "warn"
    assert f["actual"] == 60.0
    assert "freshness.slot:reference" in f["detail"]
    assert "freshness.ticker_sync, which ticker-details also bumps, is 1.0h old" in f["detail"]
    assert f["fix"] == {"action": "enqueue-slot", "slot": "reference", "force": True}


def test_before_its_row_exists_the_slot_is_judged_by_its_own_jobs() -> None:
    data = _without(_healthy_data(), "slot:reference")
    # Only detail jobs on the queue: the sibling's, so the walk has not run.
    data["slot_jobs"] = [("ticker_sync", "detail", None, False, NOW - timedelta(hours=1))]
    f = _reference(data)
    assert f["severity"] == "warn" and f["actual"] is None
    assert "no slot:reference row" in f["detail"]

    data["slot_jobs"].append(("ticker_sync", "universe", None, False, NOW - timedelta(hours=7)))
    f = _reference(data)
    assert f["severity"] == "ok" and f["actual"] == 7.0
    assert "its own last delivering job" in f["detail"]


def test_an_unreadable_queue_falls_back_to_the_shared_row_and_says_so(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed read must not raise a stale that sends the self-heal to force a slot."""
    monkeypatch.setattr(doc, "_slot_job_evidence", lambda conn: None)
    data = _without(_healthy_data(), "slot:reference")
    f = _reference(data)
    assert f["severity"] == "ok"
    assert "slot evidence unreadable" in f["detail"]


def test_the_queue_is_not_read_when_every_slot_has_its_row() -> None:
    conn = _Conn(_healthy_data())
    doc.run_doctor(conn, now=NOW, watchlist=UNIVERSE)
    assert not any("/* doctor: slot-evidence */" in q for q, _p in conn.statements)


def test_slot_rows_are_named_for_the_slot() -> None:
    assert slot_freshness_key("reference") == "slot:reference"


def test_the_doctor_polices_every_shape_named_slot_but_ticker_details() -> None:
    """The Console adherence (TD-167, TD-175) and the doctor (TD-101) judge the same
    slots, except ticker-details: it has no staleness contract, and naming it for
    the Console must not add a doctor finding."""
    assert set(doc.POLICED_SLOTS) == SHAPE_NAMED_SLOTS - {"ticker-details"}
    assert "ticker-details" not in staleness_by_slot()


def test_naming_ticker_details_adds_no_doctor_finding() -> None:
    """Even when the doctor reads the queue for slot evidence and a detail job is on it."""
    data = _without(_healthy_data(), "slot:reference")
    data["slot_jobs"] = [
        ("ticker_sync", "universe", None, False, NOW - timedelta(hours=7)),
        ("ticker_sync", "detail", None, False, NOW - timedelta(hours=1)),
    ]
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    stale = {f["id"] for f in rep["findings"] if f["id"].startswith("stale:")}
    assert stale == {f"stale:{s}" for s in doc.POLICED_SLOTS}
    assert not any("ticker-details" in f["id"] for f in rep["findings"])


def test_the_shape_columns_rebuild_what_the_function_reads() -> None:
    """One SQL fragment feeds both readers; it must carry every field the function keys on."""
    for col in ("'mode'", "'expired'", "'expiration_date'", "'expiration_date_gte'"):
        assert col in JOB_SHAPE_COLUMNS_SQL
    assert payload_from_shape("universe", None, False) == {"mode": "universe"}
    live = payload_from_shape(None, "false", False)
    assert policed_slot_for_job("option_contract", live) == "option-refresh"
    dated = payload_from_shape(None, "false", True)
    assert policed_slot_for_job("option_contract", dated) is None


# ── TD-168: the doctor mentions sharing only where it is real ──────────────


@pytest.mark.parametrize("slot", doc.POLICED_SLOTS)
def test_the_sharing_clause_appears_only_for_a_shared_dimension(slot: str) -> None:
    data = _healthy_data()
    rep = doc.run_doctor(_Conn(data), now=NOW, watchlist=UNIVERSE)
    f = next(x for x in rep["findings"] if x["id"] == f"stale:{slot}")
    dim = staleness_by_slot()[slot][0]
    shared = slot in {"reference", "option-refresh", "corporate"}
    assert f"freshness.{dim}" in f["detail"]
    assert ("also bumps" in f["detail"]) is shared, f["detail"]
    assert "other slots also write" not in f["detail"]


# ── TD-167: the Console's adherence credits a policed slot with its own jobs ─


class _AdhCur:
    def __init__(self, parent: _AdhConn) -> None:
        self.parent = parent
        self._rows: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "from ops_jobs.ingest_freshness" in q:
            self._rows = [(d, t, 1, "ok") for d, t in self.parent.freshness.items()]
            return
        kinds, start, end = params
        jobs = [
            j
            for j in self.parent.jobs
            if j["kind"] in kinds and start <= j["created_at"] < end
        ]
        if "payload->>'mode'" in q:
            groups: dict[tuple[Any, ...], int] = {}
            for j in jobs:
                p = j["payload"]
                dated = any(
                    p.get(k)
                    for k in ("expiration_date", "expiration_date_gte", "expiration_date_lte")
                )
                expired = None if p.get("expired") is None else str(p["expired"]).lower()
                key = (j["status"], j["kind"], p.get("mode"), expired, dated)
                groups[key] = groups.get(key, 0) + 1
            self._rows = [(*k, n) for k, n in groups.items()]
        else:
            by: dict[str, int] = {}
            for j in jobs:
                by[j["status"]] = by.get(j["status"], 0) + 1
            self._rows = list(by.items())

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _AdhConn:
    def __init__(self, jobs: list[dict[str, Any]], freshness: dict[str, datetime]) -> None:
        self.jobs = jobs
        self.freshness = freshness
        self.statements: list[tuple[str, Any]] = []

    def cursor(self) -> _AdhCur:
        return _AdhCur(self)

    def rollback(self) -> None:
        return None


def _adherence(slot: str, cron: str, conn: _AdhConn, now: datetime) -> dict[str, Any]:
    return dash._slot_adherence(
        conn,
        slot_id=slot,
        cron=cron,
        now=now,
        grace_minutes=45,
        freshness=dash._freshness_map(conn),
    )


# Tuesday 2026-09-15: reference fired 21:30 UTC on the 14th, ticker-details 03:30 on the 15th.
_REF_FIRE = datetime(2026, 9, 14, 21, 30, tzinfo=UTC)
_DETAIL_AT = datetime(2026, 9, 15, 3, 30, 5, tzinfo=UTC)
_NOW = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


def _detail_jobs(n: int = 3) -> list[dict[str, Any]]:
    return [
        {
            "status": "done",
            "kind": "ticker_sync",
            "payload": {"mode": "detail", "symbol": f"S{i}"},
            "created_at": _DETAIL_AT,
        }
        for i in range(n)
    ]


def test_a_stopped_reference_walk_is_missed_although_ticker_details_ran() -> None:
    """The TD-167 ratchet: a ticker-details job is not evidence that reference ran."""
    conn = _AdhConn(_detail_jobs(), {"ticker_sync": _DETAIL_AT})
    row = _adherence("reference", "30 21 * * *", conn, _NOW)
    assert row["adherence"] == "missed", row
    assert row["freshness_dimension"] == "slot:reference"
    assert row["jobs_in_window"]["created"] == 0


def test_a_sibling_job_inside_the_window_is_not_counted_either() -> None:
    """Even a detail job created inside reference's own window does not count."""
    jobs = _detail_jobs()
    for j in jobs:
        j["created_at"] = _REF_FIRE + timedelta(minutes=5)
    conn = _AdhConn(jobs, {"ticker_sync": _REF_FIRE + timedelta(minutes=6)})
    row = _adherence("reference", "30 21 * * *", conn, _NOW)
    assert row["adherence"] == "missed", row


def test_the_universe_walk_is_reference_evidence() -> None:
    walk = {
        "status": "done",
        "kind": "ticker_sync",
        "payload": {"mode": "universe"},
        "created_at": _REF_FIRE + timedelta(seconds=7),
    }
    delisted = {
        "status": "done",
        "kind": "ticker_sync",
        "payload": {"mode": "delisted", "symbol": "XYZ"},
        "created_at": _REF_FIRE + timedelta(seconds=7),
    }
    conn = _AdhConn([walk, delisted, *_detail_jobs()], {"ticker_sync": _DETAIL_AT})
    row = _adherence("reference", "30 21 * * *", conn, _NOW)
    assert row["adherence"] == "on_plan", row
    assert row["jobs_in_window"]["created"] == 1


def test_after_trim_the_slot_row_carries_the_fire_and_the_shared_row_does_not() -> None:
    walked = _AdhConn([], {slot_freshness_key("reference"): _REF_FIRE + timedelta(minutes=2)})
    row = _adherence("reference", "30 21 * * *", walked, _NOW)
    assert row["adherence"] == "on_plan" and "freshness.slot:reference" in row["detail"]

    shared_only = _AdhConn([], {"ticker_sync": _DETAIL_AT})
    assert _adherence("reference", "30 21 * * *", shared_only, _NOW)["adherence"] == "missed"


def test_option_refresh_is_not_credited_with_the_expired_catalogue() -> None:
    fire = datetime(2026, 9, 15, 6, 20, tzinfo=UTC)
    expired = {
        "status": "done",
        "kind": "option_contract",
        "payload": {
            "underlying": "SPY",
            "expired": True,
            "expiration_date_gte": "2025-01-01",
            "expiration_date_lte": "2025-03-31",
        },
        "created_at": fire + timedelta(minutes=1),
    }
    conn = _AdhConn([expired], {"option_contract": fire + timedelta(minutes=3)})
    assert _adherence("option-refresh", "20 */6 * * *", conn, _NOW)["adherence"] == "missed"
    conn.jobs.append(
        {
            "status": "pending",
            "kind": "option_contract",
            "payload": {"underlying": "AAPL", "expired": False},
            "created_at": fire + timedelta(minutes=1),
        }
    )
    assert _adherence("option-refresh", "20 */6 * * *", conn, _NOW)["adherence"] == "on_plan"


def test_a_slot_no_sibling_shares_keeps_counting_its_kind() -> None:
    """related-rotate is not shape-named: its jobs and dimension row still evidence it."""
    fire = datetime(2026, 9, 14, 22, 30, tzinfo=UTC)
    jobs = [
        {
            "status": "done",
            "kind": "ticker_related",
            "payload": {"symbol": "AAPL"},
            "created_at": fire + timedelta(seconds=5),
        }
    ]
    conn = _AdhConn(jobs, {"ticker_related": fire + timedelta(minutes=2)})
    row = _adherence("related-rotate", "30 22 * * *", conn, _NOW)
    assert row["adherence"] == "on_plan", row
    assert row["freshness_dimension"] == "ticker_related"


# ── TD-175: ticker-details is credited with its own jobs too ───────────────

# Tuesday 2026-09-15: ticker-details fired 03:30 UTC, reference walked at 21:30.
_TD_FIRE = datetime(2026, 9, 15, 3, 30, tzinfo=UTC)
_REF_WALK = datetime(2026, 9, 15, 21, 30, 7, tzinfo=UTC)
_EVENING = datetime(2026, 9, 15, 22, 0, tzinfo=UTC)


def _walk(at: datetime) -> list[dict[str, Any]]:
    return [
        {"status": "done", "kind": "ticker_sync", "payload": {"mode": "universe"}, "created_at": at},
        {
            "status": "done",
            "kind": "ticker_sync",
            "payload": {"mode": "delisted", "symbol": "XYZ"},
            "created_at": at,
        },
    ]


def test_a_stopped_ticker_details_is_missed_although_reference_ran() -> None:
    """The TD-175 ratchet: reference's walk bumps ``ticker_sync`` after the 03:30
    fire, and that is no evidence the detail rotation ran."""
    conn = _AdhConn(
        _walk(_REF_WALK),
        {"ticker_sync": _REF_WALK + timedelta(minutes=1), "slot:reference": _REF_WALK},
    )
    row = _adherence("ticker-details", "30 3 * * *", conn, _EVENING)
    assert row["adherence"] == "missed", row
    assert row["freshness_dimension"] == "slot:ticker-details"
    assert row["jobs_in_window"]["created"] == 0


def test_a_reference_job_inside_the_detail_window_is_not_counted() -> None:
    """A walk run by hand at 03:35 sits inside ticker-details' window; it still does not count."""
    conn = _AdhConn(_walk(_TD_FIRE + timedelta(minutes=5)), {})
    row = _adherence("ticker-details", "30 3 * * *", conn, _EVENING)
    assert row["adherence"] == "missed", row


def test_the_detail_rotation_is_ticker_details_evidence() -> None:
    jobs = [*_walk(_REF_WALK), *_detail_jobs()]
    for j in jobs[2:]:
        j["created_at"] = _TD_FIRE + timedelta(seconds=5)
    conn = _AdhConn(jobs, {"ticker_sync": _REF_WALK})
    row = _adherence("ticker-details", "30 3 * * *", conn, _EVENING)
    assert row["adherence"] == "on_plan", row
    assert row["jobs_in_window"]["created"] == 3


def test_after_trim_the_ticker_details_row_carries_the_fire() -> None:
    walked = _AdhConn([], {"slot:ticker-details": _TD_FIRE + timedelta(minutes=4)})
    row = _adherence("ticker-details", "30 3 * * *", walked, _EVENING)
    assert row["adherence"] == "on_plan" and "freshness.slot:ticker-details" in row["detail"]


def test_a_status_spread_over_several_shapes_is_added_up() -> None:
    """0.80.0 showed corporate as created=2 done=1: the second group overwrote the first."""
    fire = datetime(2026, 10, 5, 23, 0, tzinfo=UTC)
    jobs = [
        {"status": "done", "kind": k, "payload": {}, "created_at": fire + timedelta(seconds=30)}
        for k in ("dividends_market", "splits_market")
    ]
    row = _adherence("corporate", "0 23 * * *", _AdhConn(jobs, {}), fire + timedelta(hours=3))
    assert row["jobs_in_window"] == {
        "created": 2, "done": 2, "failed": 0, "pending": 0, "running": 0
    }
