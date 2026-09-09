"""Scopes: one population per tier, and the benchmark tier includes the watchlist."""

from __future__ import annotations

from typing import Any

import pytest

from bifrost_market_data import scopes


class _Cur:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def execute(self, sql: str, params: Any = None) -> None:
        return None

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def cursor(self) -> _Cur:
        return _Cur(self.rows)

    def rollback(self) -> None:
        return None


def test_benchmark_scope_is_the_union_the_slots_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dividing by the benchmarks alone reported 164% for a slot that also rotates the watchlist."""
    import bifrost_market_data.scheduler.daily as daily

    monkeypatch.setattr(daily, "resolve_scheduler_cfg", lambda: {"watchlist_source": "db"})
    monkeypatch.setattr(daily, "load_watchlist_symbols", lambda conn, cfg: ["nvda", "SPY"])
    assert scopes.benchmark_scope(_Conn([]), ["SPY", "QQQ"]) == {"SPY", "QQQ", "NVDA"}


def test_benchmark_scope_uses_the_resolved_config_not_an_empty_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty scheduler block sends the loader to a table Golden Source lacks."""
    import bifrost_market_data.scheduler.daily as daily

    seen: list[Any] = []
    monkeypatch.setattr(daily, "resolve_scheduler_cfg", lambda: {"watchlist_source": "platform-api"})

    def _load(conn: Any, cfg: Any) -> list[str]:
        seen.append(cfg)
        return []

    monkeypatch.setattr(daily, "load_watchlist_symbols", _load)
    scopes.benchmark_scope(_Conn([]), ["SPY"])
    assert seen == [{"watchlist_source": "platform-api"}]


def test_an_unreadable_scope_is_empty_not_an_exception() -> None:
    class _Boom(_Conn):
        def cursor(self) -> _Cur:
            raise RuntimeError("relation does not exist")

    assert scopes.active_tickers(_Boom([])) == set()
    assert scopes.universe_symbols(_Boom([])) == set()


def test_symbols_are_normalised_to_the_storage_form() -> None:
    assert scopes.active_tickers(_Conn([(" nvda ",), ("SPY",), (None,)])) == {"NVDA", "SPY"}
