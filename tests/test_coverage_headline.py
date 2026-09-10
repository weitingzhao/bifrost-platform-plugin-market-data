"""What the retired SEPA panel got wrong, and what replaces it."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api import coverage as mod
from bifrost_market_data.api.app import create_app
from bifrost_market_data.api.deps import estimated_rows


class _Cur:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn
        self.row: tuple[Any, ...] | None = None

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        s = " ".join(sql.split())
        self.conn.sql.append((s, params))
        if self.conn.boom:
            raise RuntimeError("boom")
        self.row = self.conn.answer(s)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class _Conn:
    def __init__(self, answer=None) -> None:
        self.sql: list[tuple[str, Any]] = []
        self.boom = False
        self.rollbacks = 0
        self._answer = answer or (lambda _s: None)

    def answer(self, s: str) -> Any:
        return self._answer(s)

    def cursor(self) -> _Cur:
        return _Cur(self)

    def rollback(self) -> None:
        self.rollbacks += 1


class _DummyConn:
    def close(self) -> None:
        return None


# ── the estimate ──────────────────────────────────────────────────────────


def test_an_estimate_sums_the_partitions_and_resolves_the_alias(monkeypatch) -> None:
    """option_daily is partitioned by month; the parent holds no rows of its own.

    Measured 2026-09-10: COUNT(*) did not finish inside 180s over 37.3M rows,
    while the planner estimate answered in under a millisecond.
    """
    conn = _Conn(lambda _s: (37_317_672,))
    monkeypatch.setattr(
        "bifrost_market_data.api.deps.resolve_market_schema",
        lambda *_a, **_k: "raw_market",
    )
    assert estimated_rows(conn, "market.option_daily") == 37_317_672
    sql, params = conn.sql[0]
    assert "pg_class" in sql
    # The alias is resolved for the *query*, not only for the guard — that gap
    # is what made every sepa-stats table read as empty.
    assert params[0] == "raw_market"
    assert params[2] == "option_daily\\_%"


def test_an_unreadable_estimate_is_none_not_zero(monkeypatch) -> None:
    conn = _Conn()
    conn.boom = True
    monkeypatch.setattr(
        "bifrost_market_data.api.deps.resolve_market_schema",
        lambda *_a, **_k: "raw_market",
    )
    assert estimated_rows(conn, "market.option_daily") is None
    assert conn.rollbacks == 1


# ── db-summary absorbs the retired panel's tables ─────────────────────────


def test_db_summary_carries_the_tables_the_retired_panel_held(monkeypatch) -> None:
    counted: list[str] = []
    estimated: list[str] = []
    monkeypatch.setattr(mod, "safe_count", lambda _c, t: counted.append(t) or 1)
    monkeypatch.setattr(mod, "estimated_rows", lambda _c, t: estimated.append(t) or 2)
    monkeypatch.setattr(mod, "table_exists", lambda *_a, **_k: False)

    out = mod.query_db_summary(_Conn())

    # The three that were only in sepa-stats and are cheap or estimable here.
    assert {"market.stock_minute", "market.stock_snapshot"} <= set(counted)
    assert estimated == ["market.option_daily"]
    # Named, never silently mixed in with the exact counts.
    assert out["estimated"] == ["option_daily"]
    assert out["counts"]["option_daily"] == 2


# ── the headline is over the whole population ─────────────────────────────


def test_the_headline_does_not_carry_a_page_limit() -> None:
    """The console asked for 500 underlyings and drew "301/500" while the estate
    held 570 with 379 at target — numerator and denominator both truncated by
    the same limit."""
    def answer(s: str) -> Any:
        if "option_contract" in s:
            return (570, 769_363, "2026-08-05", "2031-12-19")
        return (570, 236_644, 203_750, 379, 96)

    conn = _Conn(answer)
    out = mod.query_chain_headline(conn)
    assert out["contracts"]["underlyings"] == 570
    assert out["greeks"] == {
        "underlyings": 570,
        "contracts": 236_644,
        "with_full_greeks": 203_750,
        "pct_full": 86.1,
        "at_90": 379,
        "at_70": 96,
    }
    # The aggregates themselves take no page. (The existence probe that runs
    # first has its own LIMIT 1 and is not one of them.)
    aggregates = [sql for sql, _ in conn.sql if "COUNT(" in sql.upper()]
    assert len(aggregates) == 2
    assert all("LIMIT" not in sql.upper() for sql in aggregates)


def test_a_missing_table_leaves_its_half_none_rather_than_zero(monkeypatch) -> None:
    monkeypatch.setattr(mod, "table_exists", lambda _c, _s, t: t == "option_contract")
    conn = _Conn(lambda _s: (570, 769_363, None, None))
    out = mod.query_chain_headline(conn)
    assert out["contracts"] is not None
    # "no snapshot table" is not "no greeks" — an empty answer must not read as
    # a measured zero.
    assert out["greeks"] is None


def test_a_cold_headline_answers_at_once_without_a_verdict(monkeypatch) -> None:
    """40.5s for the greeks pass; the header must not wait on it."""
    mod.CHAIN_CACHE.clear()
    monkeypatch.setattr(mod.CHAIN_CACHE, "start_refresh", lambda key, compute: True)
    client = TestClient(create_app())
    body = client.get("/market/coverage/chain-headline").json()
    assert body["computing"] is True
    assert body["greeks"] is None and body["contracts"] is None


# ── the sibling with the same bug ─────────────────────────────────────────


def test_distributions_queries_the_schema_it_resolved(monkeypatch) -> None:
    """Same defect as the retired panel, but it 500s instead of answering null."""
    monkeypatch.setattr(mod, "resolve_market_schema", lambda *_a, **_k: "raw_market")

    class _ListCur(_Cur):
        def fetchall(self) -> list[Any]:
            return [("AAPL", 5)]

    conn = _Conn()
    conn.cursor = lambda: _ListCur(conn)  # type: ignore[method-assign]
    out = mod.query_distributions(conn, table="stock_daily")
    assert out["table"] == "raw_market.stock_daily"
    assert "raw_market.stock_daily" in conn.sql[0][0]
    assert "market.stock_daily" not in conn.sql[0][0].replace("raw_market.stock_daily", "")


def test_distributions_on_a_missing_table_is_empty_not_an_error(monkeypatch) -> None:
    monkeypatch.setattr(mod, "resolve_market_schema", lambda *_a, **_k: None)
    out = mod.query_distributions(_Conn(), table="stock_daily")
    assert out == {"ok": True, "table": "market.stock_daily", "distributions": [], "count": 0}


def test_the_retired_route_is_gone(monkeypatch) -> None:
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())
    assert client.get("/market/coverage/sepa-stats").status_code == 404
