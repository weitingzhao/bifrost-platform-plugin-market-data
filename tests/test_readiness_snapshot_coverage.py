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
# HTTP route tests (monkeypatch query function)
# ---------------------------------------------------------------------------


def test_snapshot_coverage_endpoint(monkeypatch) -> None:
    sample = {
        "ok": True,
        "row_count": 12345,
        "last_fetched_at": "2026-08-20T15:30:00+00:00",
        "session_date": "2026-08-20",
        "by_instrument_type": [
            {"code": "CS", "snapshot_row_count": 9800, "universe_ticker_count": 10200},
            {"code": "ETF", "snapshot_row_count": 2500, "universe_ticker_count": 2600},
        ],
    }
    monkeypatch.setattr(mod, "query_snapshot_coverage", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())

    resp = client.get("/market/readiness/snapshot-coverage")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["row_count"] == 12345
    assert data["session_date"] == "2026-08-20"
    assert len(data["by_instrument_type"]) == 2
    assert data["by_instrument_type"][0]["code"] == "CS"


def test_snapshot_coverage_endpoint_empty(monkeypatch) -> None:
    sample = {
        "ok": True,
        "row_count": 0,
        "last_fetched_at": None,
        "session_date": None,
        "by_instrument_type": [],
    }
    monkeypatch.setattr(mod, "query_snapshot_coverage", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())

    resp = client.get("/market/readiness/snapshot-coverage")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["row_count"] == 0
    assert data["by_instrument_type"] == []
