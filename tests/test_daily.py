"""Tests for scheduler daily slot enqueue."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from bifrost_market_data.scheduler.daily import (
    SLOT_NAMES,
    enqueue_slot,
    resolve_target_date,
)
from bifrost_market_data.scheduler.enqueue import payload_hash


class _DailyCursor:
    def __init__(self, parent: _DailyConn) -> None:
        self.parent = parent
        self.rowcount = 0

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        q = query.lower()
        if "from watchlist" in q or "select distinct symbol" in q:
            self.parent._fetchall = [(s,) for s in self.parent.watchlist]
            self.parent._fetchone = None
        elif "returning id" in q:
            kind = params[0] if params else None
            ph = params[2] if params and len(params) > 2 else None
            key = (kind, ph)
            if key in self.parent.seen_keys:
                self.parent._fetchone = None
            else:
                self.parent.seen_keys.add(key)
                self.parent.next_id += 1
                self.parent._fetchone = (self.parent.next_id,)
        elif "delete from" in q:
            self.rowcount = 2
            self.parent._fetchone = None
        else:
            self.parent._fetchone = None
            self.parent._fetchall = []

    def fetchone(self) -> Any:
        return self.parent._fetchone

    def fetchall(self) -> list[Any]:
        return list(self.parent._fetchall)

    def __enter__(self) -> _DailyCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _DailyConn:
    def __init__(self, watchlist: list[str] | None = None) -> None:
        self.watchlist = watchlist or ["AAPL", "MSFT", "TSLA"]
        self.statements: list[tuple[str, Any]] = []
        self.seen_keys: set[tuple[Any, Any]] = set()
        self.next_id = 0
        self._fetchone: Any = None
        self._fetchall: list[Any] = []
        self.committed = 0

    def cursor(self) -> _DailyCursor:
        return _DailyCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        return None


def test_resolve_target_date_explicit() -> None:
    assert resolve_target_date("2024-06-20") == date(2024, 6, 20)
    assert resolve_target_date(date(2024, 1, 2)) == date(2024, 1, 2)


def test_enqueue_stock_eod() -> None:
    conn = _DailyConn(["AAPL", "MSFT", "TSLA"])
    result = enqueue_slot(
        conn,
        "stock-eod",
        target_date=date(2024, 6, 20),
        scheduler_cfg={"slots": {"stock-eod": {"priority": 5}}},
    )
    assert result["enqueued"] == 3
    assert result["deduped"] == 0
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["stock_daily", "stock_daily", "stock_daily"]
    assert all(j["payload"]["from"] == "2024-06-20" for j in result["jobs"])
    assert {j["payload"]["symbol"] for j in result["jobs"]} == {"AAPL", "MSFT", "TSLA"}


def test_enqueue_stock_eod_dedup() -> None:
    conn = _DailyConn(["AAPL"])
    cfg = {"slots": {"stock-eod": {"priority": 5}}, "watchlist_symbols": ["AAPL"]}
    r1 = enqueue_slot(conn, "stock-eod", target_date=date(2024, 6, 20), scheduler_cfg=cfg)
    r2 = enqueue_slot(conn, "stock-eod", target_date=date(2024, 6, 20), scheduler_cfg=cfg)
    assert r1["enqueued"] == 1
    assert r2["enqueued"] == 0
    assert r2["deduped"] == 1


def test_enqueue_eod_pipeline() -> None:
    conn = _DailyConn(["AAPL"])
    result = enqueue_slot(
        conn,
        "eod-pipeline",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL"],
        scheduler_cfg={"slots": {"eod-pipeline": {"priority": 5}}},
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["option_snapshot", "option_open_interest"]
    assert result["jobs"][1]["payload"]["trade_date"] == "2024-06-20"


def test_enqueue_universe_daily() -> None:
    conn = _DailyConn([])
    result = enqueue_slot(
        conn,
        "universe-daily",
        target_date=date(2024, 6, 20),
        watchlist_symbols=[],
        scheduler_cfg={
            "slots": {
                "universe-daily": {"priority": 3},
                "calendar": {"priority": 1},
            }
        },
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert "stock_daily" in kinds
    assert "calendar" in kinds
    stock = next(j for j in result["jobs"] if j["kind"] == "stock_daily")
    assert stock["payload"]["mode"] == "grouped"


def test_enqueue_corporate() -> None:
    conn = _DailyConn(["MSFT"])
    result = enqueue_slot(
        conn,
        "corporate",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["MSFT"],
        scheduler_cfg={"slots": {"corporate": {"priority": 2}}},
    )
    kinds = [j["kind"] for j in result["jobs"]]
    assert kinds == ["splits", "dividends"]


def test_enqueue_option_refresh_batch() -> None:
    conn = _DailyConn()
    result = enqueue_slot(
        conn,
        "option-refresh",
        target_date=date(2024, 6, 20),
        watchlist_symbols=["AAPL", "MSFT", "TSLA", "NVDA"],
        scheduler_cfg={"slots": {"option-refresh": {"priority": 4, "batch_size": 2}}},
    )
    # 2 symbols × (contract + expiration)
    assert result["enqueued"] == 4
    underlyings = {j["payload"]["underlying"] for j in result["jobs"]}
    assert underlyings == {"AAPL", "MSFT"}


def test_enqueue_calendar_and_trim() -> None:
    conn = _DailyConn([])
    cal = enqueue_slot(conn, "calendar", watchlist_symbols=[], scheduler_cfg={})
    assert cal["enqueued"] == 1
    assert cal["jobs"][0]["kind"] == "calendar"

    trim = enqueue_slot(
        conn,
        "trim",
        scheduler_cfg={"slots": {"trim": {"keep_days": 7, "keep_max": 100}}},
    )
    assert trim["trimmed"] == 4  # two DELETEs × rowcount 2
    assert trim["enqueued"] == 0


def test_unknown_slot() -> None:
    with pytest.raises(ValueError, match="unknown slot"):
        enqueue_slot(_DailyConn(), "nope")


def test_all_slot_names_covered() -> None:
    assert "stock-eod" in SLOT_NAMES
    assert "trim" in SLOT_NAMES
    # payload_hash stable for slot payloads
    assert payload_hash({"symbol": "AAPL"}) == payload_hash({"symbol": "AAPL"})
