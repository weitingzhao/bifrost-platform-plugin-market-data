"""Tests for /market/readiness/snapshot-coverage endpoint."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import readiness_data as mod


class _DummyConn:
    def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# query_snapshot_coverage unit tests (mock DB cursor)
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows_by_query: dict[str, list[Any]]) -> None:
        self._rows_by_query = rows_by_query
        self._rows: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        q = query.strip().lower()
        for key, rows in self._rows_by_query.items():
            if key in q:
                self._rows = list(rows)
                return
        self._rows = []

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[Any]:
        return self._rows

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, rows_by_query: dict[str, list[Any]], *, tables: set[str] | None = None) -> None:
        self._rows_by_query = rows_by_query
        self._tables = tables or set()

    def cursor(self) -> _FakeCursor:
        # Merge table-existence check into the query router
        merged = dict(self._rows_by_query)
        if self._tables:
            merged["information_schema"] = [(1,)]
        return _FakeCursor(merged)

    def close(self) -> None:
        return None


def test_snapshot_coverage_empty_table() -> None:
    conn = _FakeConn({}, tables={"market.stock_snapshot"})
    result = mod.query_snapshot_coverage(conn)
    assert result["ok"] is True
    assert result["row_count"] == 0


def test_snapshot_coverage_with_data() -> None:
    from datetime import date

    conn = _FakeConn(
        {
            "max(session_date)": [(date(2026, 8, 20),)],
            "count(*)": [(9800, "2026-08-20T15:30:00+00:00")],
            "coalesce(u.instrument_type": [
                ("CS", 8500, 9000),
                ("ETF", 1200, 1200),
            ],
        },
        tables={"market.stock_snapshot", "market.v_us_equity_universe"},
    )
    result = mod.query_snapshot_coverage(conn)
    assert result["ok"] is True
    assert result["row_count"] == 9800
    assert result["session_date"] == "2026-08-20"
    assert len(result["by_instrument_type"]) == 2
    assert result["by_instrument_type"][0]["code"] == "CS"
    assert result["by_instrument_type"][0]["snapshot_row_count"] == 8500


# ---------------------------------------------------------------------------
# HTTP route tests — the read is cached, so the route never runs the join
# ---------------------------------------------------------------------------

_SAMPLE: dict[str, Any] = {
    "ok": True,
    "row_count": 12345,
    "last_fetched_at": "2026-08-20T15:30:00+00:00",
    "session_date": "2026-08-20",
    "by_instrument_type": [
        {"code": "CS", "snapshot_row_count": 9800, "universe_ticker_count": 10200},
        {"code": "ETF", "snapshot_row_count": 2500, "universe_ticker_count": 2600},
    ],
}


def test_snapshot_coverage_endpoint(monkeypatch) -> None:
    mod.SNAPSHOT_COVERAGE_CACHE.clear()
    monkeypatch.setattr(mod, "_snapshot_coverage_payload", lambda: dict(_SAMPLE))
    client = TestClient(create_app())

    resp = client.get("/market/readiness/snapshot-coverage", params={"refresh": "true"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["row_count"] == 12345
    assert data["session_date"] == "2026-08-20"
    assert len(data["by_instrument_type"]) == 2
    assert data["by_instrument_type"][0]["code"] == "CS"


def test_snapshot_coverage_endpoint_empty(monkeypatch) -> None:
    mod.SNAPSHOT_COVERAGE_CACHE.clear()
    sample = {
        "ok": True,
        "row_count": 0,
        "last_fetched_at": None,
        "session_date": None,
        "by_instrument_type": [],
    }
    monkeypatch.setattr(mod, "_snapshot_coverage_payload", lambda: dict(sample))
    client = TestClient(create_app())

    resp = client.get("/market/readiness/snapshot-coverage", params={"refresh": "true"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["row_count"] == 0
    assert data["by_instrument_type"] == []


def test_a_second_read_does_not_run_the_join_again(monkeypatch) -> None:
    """Measured 2026-09-25 through the gateway: 60s (HTTP 502), 34s, 33s.

    The Readiness panel polls this every 60 seconds, which is shorter than the
    query's own runtime, so one open tab kept a copy permanently in flight and
    platform-api's status probe added another. Research counted six concurrent
    copies of the same statement on the shared database.
    """
    mod.SNAPSHOT_COVERAGE_CACHE.clear()
    calls: list[int] = []

    def _payload() -> dict[str, Any]:
        calls.append(1)
        return dict(_SAMPLE)

    monkeypatch.setattr(mod, "_snapshot_coverage_payload", _payload)
    client = TestClient(create_app())

    client.get("/market/readiness/snapshot-coverage", params={"refresh": "true"})
    assert len(calls) == 1
    body = client.get("/market/readiness/snapshot-coverage").json()
    assert len(calls) == 1, "a cached answer must not be recomputed"
    assert body["row_count"] == 12345
    assert body["age_sec"] is not None and body["computing"] is False


def test_a_cold_read_answers_at_once_without_a_count(monkeypatch) -> None:
    """Still counting must not be served as a count of zero.

    ``row_count`` is null rather than 0 in the empty shape, which is also what
    platform-api's rollup needs: with no session date, no rows and no universe
    it drops the KPI instead of rendering an untaken count.
    """
    mod.SNAPSHOT_COVERAGE_CACHE.clear()
    monkeypatch.setattr(mod, "_snapshot_coverage_payload", lambda: dict(_SAMPLE))
    client = TestClient(create_app())

    body = client.get("/market/readiness/snapshot-coverage").json()
    assert body["computing"] is True and body["age_sec"] is None
    assert body["row_count"] is None
    assert body["session_date"] is None
    assert body["by_instrument_type"] == []
