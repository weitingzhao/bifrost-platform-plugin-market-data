"""The overview fields have a handler and never had an enqueuer.

ticker_sync grew a `mode: "detail"` branch that fetches
/v3/reference/tickers/{ticker} and upserts list_date, sector, market_cap and
description. Nothing ever sent it a job. Measured 2026-09-11, list_date is null
for all 5,317 active tickers — and that absence is what blocks declaring "an
instrument listed after the window opened cannot reach a five-year target",
which is three of the four depth partials on the board.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from bifrost_market_data.scheduler.daily import (
    TICKERS_NEEDING_DETAIL_QUERY,
    tickers_needing_detail,
)


class _Cur:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.sql.append((" ".join(sql.split()), params))
        if self.conn.boom:
            raise RuntimeError("boom")

    def fetchall(self) -> list[tuple[str]]:
        return [(s,) for s in self.conn.rows]


class _Conn:
    def __init__(self, rows: list[str], boom: bool = False) -> None:
        self.rows = rows
        self.boom = boom
        self.sql: list[tuple[str, Any]] = []
        self.rollbacks = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def rollback(self) -> None:
        self.rollbacks += 1


def test_never_fetched_comes_before_ever_fetched() -> None:
    """`(list_date IS NOT NULL)` sorts false first, so the backlog drains before
    the refresh starts competing with it."""
    sql = " ".join(TICKERS_NEEDING_DETAIL_QUERY.split())
    assert "(list_date IS NOT NULL)" in sql
    assert sql.index("(list_date IS NOT NULL)") < sql.index("updated_at")
    assert "WHERE active" in sql


def test_it_returns_upper_cased_symbols_and_honours_the_limit() -> None:
    conn = _Conn(["aapl", " msft ", "NVDA"])
    assert tickers_needing_detail(conn, limit=3) == ["AAPL", "MSFT", "NVDA"]
    assert conn.sql[0][1] == (3,)


def test_a_failed_pick_enqueues_nothing_rather_than_guessing() -> None:
    conn = _Conn([], boom=True)
    assert tickers_needing_detail(conn) == []
    assert conn.rollbacks == 1


def test_the_slot_sends_the_detail_mode_the_handler_already_understands(monkeypatch) -> None:
    """The handler has existed since ticker_sync gained its detail branch; the
    job shape has to match it exactly or this stays a no-op for another month."""
    from bifrost_market_data.scheduler import daily as mod
    from test_daily import _DailyConn

    monkeypatch.setattr(mod, "tickers_needing_detail", lambda conn, limit: ["AAPL", "MSFT"])
    conn = _DailyConn(["AAPL", "MSFT"])
    result = mod.enqueue_slot(
        conn,
        "ticker-details",
        target_date=date(2026, 9, 11),
        scheduler_cfg={"slots": {"ticker-details": {"priority": 1, "batch_size": 2}}},
    )
    assert result["enqueued"] == 2
    jobs = result.get("jobs") or []
    assert {j["kind"] for j in jobs} == {"ticker_sync"}
    assert [j["payload"] for j in jobs] == [
        {"mode": "detail", "symbol": "AAPL"},
        {"mode": "detail", "symbol": "MSFT"},
    ]


def test_the_batch_size_is_what_bounds_the_rotation(monkeypatch) -> None:
    """200 a day drains 5,317 in about four weeks; an unbounded slot would ask
    the vendor for the whole universe in one run."""
    from bifrost_market_data.scheduler import daily as mod
    from test_daily import _DailyConn

    seen: list[int] = []
    monkeypatch.setattr(
        mod, "tickers_needing_detail", lambda conn, limit: seen.append(limit) or ["AAPL"]
    )
    mod.enqueue_slot(
        _DailyConn(["AAPL"]),
        "ticker-details",
        target_date=date(2026, 9, 11),
        scheduler_cfg={"slots": {"ticker-details": {"batch_size": 40}}},
    )
    assert seen == [40]
