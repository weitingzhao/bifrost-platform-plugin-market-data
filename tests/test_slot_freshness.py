"""TD-101: a policed slot is judged by its own deliveries, not a shared dimension row.

``ops_jobs.ingest_freshness`` is keyed by dimension, and three policed slots
share theirs with a sibling: reference with ticker-details (``ticker_sync``),
option-refresh with option-contract-expired (``option_contract``), corporate
with corporate-backfill (``dividends``). Measured 2026-10-06: ``ticker_sync``
read 12.7h old off a detail job while the last reference walk was 18.7h old —
had the walk stopped, the doctor would have gone on calling it fresh.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Self

import pytest
from test_daily import _DailyConn
from test_doctor import NOW, UNIVERSE, _Conn, _healthy_data

from bifrost_market_data import doctor as doc
from bifrost_market_data.api.ingest_dashboard import SLOT_EVIDENCE
from bifrost_market_data.contracts import staleness_by_slot
from bifrost_market_data.freshness import (
    POLICED_SLOT_KINDS,
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
    if slot in doc.POLICED_SLOTS:
        assert named == {slot}, f"{slot}'s own jobs must evidence it"


@pytest.mark.parametrize("slot", doc.POLICED_SLOTS)
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
async def test_a_detail_job_bumps_the_dimension_and_not_the_reference_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes = await _freshness_writes(
        monkeypatch, "ticker_sync", {"mode": "detail", "symbol": "AAPL"}, 1
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
    assert "freshness.ticker_sync, which other slots also write, is 1.0h old" in f["detail"]
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
