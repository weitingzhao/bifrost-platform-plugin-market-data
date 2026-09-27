"""minute-bars: 1-hour bars for the whole research universe (2026-09-26).

Research settles every forecast session hour by hour against the session's
1-hour bars. The watchlist carried them for 18 of the 678 names it forecasts;
the rest were judged on the close alone.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Self

import pytest
from test_daily import _DailyConn

from bifrost_market_data.scheduler import daily
from bifrost_market_data.scheduler.daily import enqueue_slot, latest_hourly_bar_days

DAY = date(2026, 9, 28)
CFG = {
    "iv_radar_benchmarks": [],
    "slots": {"minute-bars": {"priority": 3, "batch_size": 0, "hourly_universe": "research", "hourly_backfill_days": 100}},
}


def _hourly_jobs(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        j["payload"]["symbol"]: j["payload"]
        for j in result["jobs"]
        if j["kind"] == "stock_minute" and j["payload"]["timespan"] == "hour"
    }


def test_each_universe_stock_gets_hourly_bars_from_after_its_newest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        daily,
        "latest_hourly_bar_days",
        lambda conn, symbols, *, since: {"PLTR": date(2026, 9, 25), "MU": DAY},
    )
    conn = _DailyConn(
        research_universe=[("SPX", "resident", 24), ("AAPL", "resident", 24), ("PLTR", "core", 24),
                           ("MU", "core", 24), ("HALO", "edge", 12)],
    )
    result = enqueue_slot(conn, "minute-bars", target_date=DAY, watchlist_symbols=["AAPL"], scheduler_cfg=CFG)
    hourly = _hourly_jobs(result)
    # AAPL keeps its watchlist job for the day; SPX is an index; MU already has today.
    assert set(hourly) == {"AAPL", "PLTR", "HALO"}
    assert hourly["AAPL"]["from"] == "2026-09-28"
    assert (hourly["PLTR"]["from"], hourly["PLTR"]["to"]) == ("2026-09-26", "2026-09-28")
    assert hourly["HALO"]["from"] == "2026-06-20", "no bars at all: the whole backfill window"
    minute_syms = {j["payload"]["symbol"] for j in result["jobs"] if j["payload"].get("timespan") == "minute"}
    assert minute_syms == {"AAPL"}, "minute bars stay the benchmark tier's (watchlist ∪ benchmarks)"


def test_off_by_default() -> None:
    conn = _DailyConn(research_universe=[("PLTR", "core", 24)])
    cfg = {"iv_radar_benchmarks": [], "slots": {"minute-bars": {"priority": 3, "batch_size": 0}}}
    result = enqueue_slot(conn, "minute-bars", target_date=DAY, watchlist_symbols=["AAPL"], scheduler_cfg=cfg)
    assert set(_hourly_jobs(result)) == {"AAPL"}


def test_an_unreadable_lookup_asks_for_the_whole_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(daily, "latest_hourly_bar_days", lambda conn, symbols, *, since: None)
    conn = _DailyConn(research_universe=[("PLTR", "core", 24)])
    result = enqueue_slot(conn, "minute-bars", target_date=DAY, watchlist_symbols=[], scheduler_cfg=CFG)
    assert _hourly_jobs(result)["PLTR"]["from"] == "2026-06-20"


class _Cur:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.params: Any = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.params = params

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _Conn:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.cur = _Cur(rows)

    def cursor(self) -> _Cur:
        return self.cur


def test_the_newest_bar_is_dated_in_new_york() -> None:
    # 19:00 ET on Friday 2026-12-04 (EST) is 00:00 UTC on Saturday.
    rows = [("PLTR", datetime(2026, 12, 5, 0, 0, tzinfo=UTC)), ("HALO", None)]
    conn = _Conn(rows)
    got = latest_hourly_bar_days(conn, ["PLTR", "HALO"], since=date(2026, 9, 1))
    assert got == {"PLTR": date(2026, 12, 4)}
    since_ts, names = conn.cur.params
    assert names == ["PLTR", "HALO"] and since_ts.utcoffset() is not None
