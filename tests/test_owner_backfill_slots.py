"""R9 C3 P5 / P7 / P8 and C4: the Owner-run history slots and the pin list.

Three gaps closed here, all of them "the daily slot is right about today and blind
to the past": corporate actions before the -7 day window, option contracts that
have already expired, and the bars of a contract that drifted away from spot.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data import research_pins
from bifrost_market_data.contracts import CONTRACTS
from bifrost_market_data.api.ingest_dashboard import (
    MAINTENANCE_SLOT_IDS,
    SLOT_EVIDENCE,
    SLOT_NOTES,
)
from bifrost_market_data.scheduler import daily
from bifrost_market_data.scheduler.daily import SLOT_NAMES, enqueue_slot
from bifrost_market_data.scheduler.enqueue import trim_option_snapshots

from tests.test_daily import _DailyConn


def _no_pins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily, "load_pinned_contracts", lambda conn: [])
    monkeypatch.setattr(daily, "load_pinned_underlyings", lambda conn: set())


# ── P5: corporate-backfill ────────────────────────────────────────────────


def test_corporate_backfill_asks_for_each_symbols_whole_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No date window: the daily slot owns the window, this slot owns the history."""
    _no_pins(monkeypatch)
    conn = _DailyConn(research_universe=[("SPY", "resident", 24), ("HALO", "edge", 12)])
    r = enqueue_slot(
        conn,
        "corporate-backfill",
        target_date=date(2026, 9, 15),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"corporate-backfill": {"priority": 1}}},
    )
    per_kind: dict[str, set[str]] = {}
    for job in r["jobs"]:
        per_kind.setdefault(job["kind"], set()).add(job["payload"]["symbol"])
        assert set(job["payload"]) == {"symbol"}, "a full history has no date filter"
        assert job["priority"] == 1
    assert per_kind == {
        "dividends": {"AAPL", "HALO", "SPY"},
        "splits": {"AAPL", "HALO", "SPY"},
    }, "the universe and the watchlist, both"


# ── C4: option-contract-expired ───────────────────────────────────────────


def test_expired_catalogue_walks_quarters_not_the_whole_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _no_pins(monkeypatch)
    conn = _DailyConn(research_universe=[("AAPL", "core", 24)])
    r = enqueue_slot(
        conn,
        "option-contract-expired",
        target_date=date(2026, 9, 15),
        watchlist_symbols=[],
        scheduler_cfg={
            "slots": {
                "option-contract-expired": {
                    "universe": "research",
                    "months": 6,
                    "months_per_batch": 3,
                }
            },
            "iv_radar_benchmarks": [],
        },
    )
    windows = sorted(
        (j["payload"]["expiration_date_gte"], j["payload"]["expiration_date_lte"])
        for j in r["jobs"]
    )
    assert windows == [
        ("2026-04-01", "2026-06-30"),
        ("2026-07-01", "2026-09-30"),
    ], "two quarters covering the six months up to and including the target month"
    assert all(j["payload"]["expired"] is True for j in r["jobs"])
    assert all(j["kind"] == "option_contract" for j in r["jobs"])


def test_expired_catalogue_covers_the_underlyings_research_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pin on an expired contract only resolves if its catalogue was walked."""
    monkeypatch.setattr(daily, "load_pinned_contracts", lambda conn: [])
    monkeypatch.setattr(daily, "load_pinned_underlyings", lambda conn: {"DAVE"})
    conn = _DailyConn(research_universe=[("AAPL", "core", 24)])
    r = enqueue_slot(
        conn,
        "option-contract-expired",
        target_date=date(2026, 9, 15),
        watchlist_symbols=[],
        scheduler_cfg={
            "slots": {
                "option-contract-expired": {"universe": "research", "months": 3},
            },
            "iv_radar_benchmarks": [],
        },
    )
    assert {j["payload"]["underlying"] for j in r["jobs"]} == {"AAPL", "DAVE"}


# ── P7: option-bars prices the pinned contracts ───────────────────────────


def test_option_bars_prices_a_pinned_contract_far_from_spot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The near-spot window is a scope, not a filter on what the Owner holds."""
    monkeypatch.setattr(
        daily, "load_pinned_contracts", lambda conn: [("O:DAVE260220C00090000", "DAVE")]
    )
    monkeypatch.setattr(daily, "load_pinned_underlyings", lambda conn: {"DAVE"})
    conn = _DailyConn(
        watchlist=["AAPL"],
        option_contracts=[("O:AAPL260117C00200000", "AAPL", date(2026, 1, 17), 200.0)],
        spots={"AAPL": 200.0},
    )
    r = enqueue_slot(
        conn,
        "option-bars",
        target_date=date(2026, 9, 15),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"option-bars": {}}, "iv_radar_benchmarks": []},
    )
    pinned = [j for j in r["jobs"] if j["payload"]["option_ticker"] == "O:DAVE260220C00090000"]
    assert pinned, "a pinned contract is priced wherever it sits relative to spot"
    assert all(j["payload"]["from"] == "2026-09-15" for j in pinned)


def test_pinned_history_days_buys_the_contracts_own_past_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        daily, "load_pinned_contracts", lambda conn: [("O:DAVE260220C00090000", "DAVE")]
    )
    monkeypatch.setattr(daily, "load_pinned_underlyings", lambda conn: {"DAVE"})
    conn = _DailyConn(watchlist=["AAPL"], option_contracts=[], spots={})
    r = enqueue_slot(
        conn,
        "option-bars",
        target_date=date(2026, 9, 15),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {"option-bars": {"pinned_history_days": 400}},
            "iv_radar_benchmarks": [],
        },
    )
    windows = sorted((j["payload"]["from"], j["payload"]["to"]) for j in r["jobs"])
    assert windows == [
        ("2025-08-11", "2026-09-15"),
        ("2026-09-15", "2026-09-15"),
    ], "the session, plus the contract's history behind it"


def test_no_pins_leaves_option_bars_as_it_was(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_pins(monkeypatch)
    conn = _DailyConn(watchlist=["AAPL"], option_contracts=[], spots={})
    r = enqueue_slot(
        conn,
        "option-bars",
        target_date=date(2026, 9, 15),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={
            "slots": {"option-bars": {"pinned_history_days": 400}},
            "iv_radar_benchmarks": [],
        },
    )
    assert r["jobs"] == []


# ── P8: the trim spares a pinned contract ─────────────────────────────────


class _TrimCursor:
    def __init__(self, parent: _TrimConn) -> None:
        self.parent = parent
        self.rowcount = 0

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append(query)
        if "to_regclass" in query:
            self.parent._fetchone = (self.parent.pin_table,)
        elif "delete from" in query.lower():
            self.rowcount = 0

    def fetchone(self) -> Any:
        return self.parent._fetchone

    def __enter__(self) -> _TrimCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _TrimConn:
    def __init__(self, pin_table: bool) -> None:
        self.pin_table = pin_table
        self.statements: list[str] = []
        self._fetchone: Any = None

    def cursor(self) -> _TrimCursor:
        return _TrimCursor(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_the_trim_spares_pinned_contracts_when_research_published_a_list() -> None:
    conn = _TrimConn(pin_table=True)
    trim_option_snapshots(conn, keep_days=90, budget_sec=1.0)
    deletes = [s for s in conn.statements if s.lstrip().upper().startswith("DELETE")]
    assert deletes and "option_pinned_contract" in deletes[0]
    assert "pin_until >= CURRENT_DATE" in deletes[0]


def test_no_pin_table_means_nothing_to_spare() -> None:
    conn = _TrimConn(pin_table=False)
    trim_option_snapshots(conn, keep_days=90, budget_sec=1.0)
    deletes = [s for s in conn.statements if s.lstrip().upper().startswith("DELETE")]
    assert deletes and "option_pinned_contract" not in deletes[0]


# ── the reader itself ─────────────────────────────────────────────────────


class _PinCursor:
    def __init__(self, parent: _PinConn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        if self.parent.raises:
            raise RuntimeError("permission denied for schema research")
        self.parent.last = query
        self.parent._rows = (
            [("DAVE",)] if "DISTINCT" in query else [("O:DAVE260220C00090000", "DAVE")]
        )

    def fetchall(self) -> list[Any]:
        return list(self.parent._rows)

    def __enter__(self) -> _PinCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _PinConn:
    def __init__(self, raises: bool = False) -> None:
        self.raises = raises
        self.last = ""
        self._rows: list[Any] = []
        self.rolled_back = 0

    def cursor(self) -> _PinCursor:
        return _PinCursor(self)

    def rollback(self) -> None:
        self.rolled_back += 1


def test_the_pin_reader_only_reads_and_only_reads_live_pins() -> None:
    conn = _PinConn()
    assert research_pins.load_pinned_contracts(conn) == [("O:DAVE260220C00090000", "DAVE")]
    assert "pin_until >= CURRENT_DATE" in conn.last
    assert "INSERT" not in research_pins.PINNED_QUERY.upper()
    assert research_pins.load_pinned_underlyings(conn) == {"DAVE"}


def test_an_unreadable_pin_table_is_empty_not_an_exception() -> None:
    conn = _PinConn(raises=True)
    assert research_pins.load_pinned_contracts(conn) == []
    assert research_pins.load_pinned_underlyings(conn) == set()
    assert conn.rolled_back == 2, "a failed read leaves the transaction usable"


# ── registration: a slot exists in every place that has to know ───────────


@pytest.mark.parametrize("slot", ["corporate-backfill", "option-contract-expired"])
def test_a_new_slot_is_registered_everywhere_it_has_to_be(slot: str) -> None:
    assert slot in SLOT_NAMES
    assert slot in SLOT_EVIDENCE and slot in SLOT_NOTES
    assert slot in MAINTENANCE_SLOT_IDS, "no cron, so it must not decide the gate verdict"
    assert any(slot in c.slots for c in CONTRACTS), "the dataset it fills has to claim it"


@pytest.mark.parametrize("slot", ["corporate-backfill", "option-contract-expired"])
def test_a_new_slot_has_no_cron_and_no_cronjob(slot: str) -> None:
    import pathlib

    import yaml

    cfg = yaml.safe_load(pathlib.Path("config/schedule.yaml").read_text())
    assert "cron" not in cfg["scheduler"]["slots"][slot], "Owner-run means Owner-triggered"
    manifests = list(pathlib.Path("k8s").rglob("cronjob-*.yaml"))
    assert manifests, "the manifests moved; this test would pass by accident"
    assert not [m for m in manifests if slot in m.name]
