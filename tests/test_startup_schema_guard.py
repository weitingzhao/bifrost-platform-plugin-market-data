"""The startup schema guard: a finding stays, a failure to ask is asked again.

On the 0.41.2 rollout (2026-09-26) the guard's one connection, 100 ms after
process start, was closed by the server; every database call after it worked,
yet /health reported ``degraded`` for the life of the pod because the guard
never ran again.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Self

import pytest

from bifrost_market_data.api import deps, health


class _Cursor:
    def __init__(self, found: list[str]) -> None:
        self._found = found

    def execute(self, query: str, params: Any = None) -> None:
        return None

    def fetchall(self) -> list[tuple[str]]:
        return [(name,) for name in self._found]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_a: object) -> None:
        return None


class _Conn:
    def __init__(self, found: list[str]) -> None:
        self._found = found

    def cursor(self) -> _Cursor:
        return _Cursor(self._found)

    def close(self) -> None:
        return None


class _Db:
    """connect_db stand-in: fails the first ``failures`` calls, then answers."""

    def __init__(self, *, failures: int = 0, found: list[str] | None = None) -> None:
        self.failures = failures
        self.found = found or []
        self.calls = 0

    def __call__(self, **_kw: Any) -> _Conn:
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("server closed the connection unexpectedly")
        return _Conn(self.found)


@pytest.fixture(autouse=True)
def _fresh_guard_state() -> Iterator[None]:
    saved = (deps._startup_ok, deps._startup_error, deps._guard_unverified)
    deps._startup_ok, deps._startup_error, deps._guard_unverified = True, None, False
    yield
    deps._startup_ok, deps._startup_error, deps._guard_unverified = saved


def _wait_for_recheck() -> None:
    # recheck_schema_guard takes the lock before starting its thread, and the
    # thread releases it when done — so acquiring it here waits for the result.
    assert deps._guard_lock.acquire(timeout=5)
    deps._guard_lock.release()


def test_unreachable_at_start_is_unverified_not_a_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deps, "connect_db", _Db(failures=1))
    deps.run_startup_schema_guard()
    assert deps.startup_ok() is False
    assert "closed the connection" in (deps.startup_error() or "")
    assert deps._guard_unverified is True


def test_recheck_clears_a_guard_that_could_not_run(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Db(failures=1)
    monkeypatch.setattr(deps, "connect_db", db)
    deps.run_startup_schema_guard()

    assert deps.recheck_schema_guard() is True
    _wait_for_recheck()

    assert deps.startup_ok() is True
    assert deps.startup_error() is None
    assert deps._guard_unverified is False
    assert db.calls == 2
    # Settled: nothing left to ask.
    assert deps.recheck_schema_guard() is False


def test_a_legacy_schema_is_a_finding_and_is_not_rechecked(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Db(found=["market_analytics"])
    monkeypatch.setattr(deps, "connect_db", db)
    deps.run_startup_schema_guard()

    assert deps.startup_ok() is False
    assert "market_analytics" in (deps.startup_error() or "")
    assert deps.recheck_schema_guard() is False
    assert db.calls == 1


def test_a_passed_guard_is_not_rechecked(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Db()
    monkeypatch.setattr(deps, "connect_db", db)
    deps.run_startup_schema_guard()
    assert deps.startup_ok() is True
    assert deps.recheck_schema_guard() is False
    assert db.calls == 1


def test_only_one_recheck_runs_at_a_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deps, "connect_db", _Db(failures=1))
    deps.run_startup_schema_guard()
    assert deps._guard_lock.acquire(blocking=False)  # a re-check already in flight
    try:
        assert deps.recheck_schema_guard() is False
    finally:
        deps._guard_lock.release()


def test_health_rechecks_once_the_database_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deps, "connect_db", _Db(failures=1))
    deps.run_startup_schema_guard()

    monkeypatch.setattr(health, "_probe_db", lambda: "ok")
    # Whether this response already sees the re-check depends on thread timing;
    # what is guaranteed is that the probe after it does.
    health.health()
    _wait_for_recheck()
    second = health.health()
    assert second["status"] == "ok"
    assert second["startup_ok"] is True
    assert second["startup_error"] is None


def test_health_does_not_recheck_while_the_database_is_down(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Db(failures=1)
    monkeypatch.setattr(deps, "connect_db", db)
    deps.run_startup_schema_guard()

    monkeypatch.setattr(health, "_probe_db", lambda: "unreachable")
    body = health.health()
    assert body["status"] == "degraded"
    assert db.calls == 1
    assert deps._guard_unverified is True
