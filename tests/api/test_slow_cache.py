"""The background cache: answer now, recompute behind it, say how old it is."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from bifrost_market_data.api.slow_cache import BackgroundCache


class _InlineThread:
    """Runs the refresh body where it was started, so a test needs no waiting."""

    def __init__(self, target: Any, name: str = "", daemon: bool = False) -> None:
        self.target = target
        self.name = name

    def start(self) -> None:
        self.target()


@pytest.fixture()
def inline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(threading, "Thread", _InlineThread)


def test_a_cold_read_answers_at_once_rather_than_holding_the_request() -> None:
    """141 seconds inside the pod against a 60-second gateway: nobody may wait."""
    cache = BackgroundCache("t")
    started: list[str] = []
    cache.start_refresh = lambda key, compute: (started.append(key), True)[1]  # type: ignore[method-assign]

    out = cache.read("k", lambda: {"rows": [1]}, empty={"rows": []})

    assert out == {"rows": [], "age_sec": None, "computing": True}
    assert started == ["k"]


def test_a_stale_answer_is_served_while_the_refresh_runs() -> None:
    cache = BackgroundCache("t", ttl_sec=100.0)
    cache.compute_now("k", lambda: {"n": 1})
    at, payload = cache._cache["k"]
    cache._cache["k"] = (at - 101.0, payload)
    cache.start_refresh = lambda key, compute: True  # type: ignore[method-assign]

    out = cache.read("k", lambda: {"n": 2})

    assert out["n"] == 1  # the last good answer, not an empty page
    assert out["computing"] is True
    assert out["age_sec"] > 100.0


def test_a_fresh_answer_starts_no_refresh() -> None:
    cache = BackgroundCache("t", ttl_sec=100.0)
    cache.compute_now("k", lambda: {"n": 1})
    started: list[str] = []
    cache.start_refresh = lambda key, compute: (started.append(key), True)[1]  # type: ignore[method-assign]

    out = cache.read("k", lambda: {"n": 2})

    assert out["computing"] is False
    assert started == []


def test_only_one_refresh_runs_per_key(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = BackgroundCache("t")
    names: list[str] = []
    release = threading.Event()

    class _Held(_InlineThread):
        def start(self) -> None:
            names.append(self.name)  # started but never joined: still in flight

    monkeypatch.setattr(threading, "Thread", _Held)
    assert cache.start_refresh("k", lambda: {"n": 1}) is True
    # A second caller is told a refresh is in flight, not that none is — the
    # question is whether a fresh answer is coming, not who asked for it.
    assert cache.start_refresh("k", lambda: {"n": 1}) is True
    assert names == ["t-k"]
    assert cache.is_refreshing("k") is True
    release.set()


def test_a_failed_refresh_keeps_the_last_good_answer(inline: None) -> None:
    cache = BackgroundCache("t", ttl_sec=0.0)
    cache.compute_now("k", lambda: {"n": 1})

    def _boom() -> dict[str, Any]:
        raise RuntimeError("statement timeout")

    out = cache.read("k", _boom)

    assert out["n"] == 1
    assert cache.is_refreshing("k") is False


def test_the_answer_carries_what_it_cost(inline: None) -> None:
    cache = BackgroundCache("t")
    out = cache.compute_now("k", lambda: {"n": 1})
    assert isinstance(out["computed_ms"], int)
    assert out["age_sec"] == 0.0
    assert out["computing"] is False
