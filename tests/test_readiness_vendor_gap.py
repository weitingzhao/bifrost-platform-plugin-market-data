"""Tests for /market/readiness/vendor-gap endpoint."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import readiness_data as mod


class _DummyConn:
    def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# HTTP route tests (monkeypatch query function)
# ---------------------------------------------------------------------------


def test_vendor_gap_endpoint_summary(monkeypatch) -> None:
    sample = {
        "ok": True,
        "gap_count": 42,
        "session_date": "2026-08-20",
    }
    monkeypatch.setattr(mod, "query_vendor_gap", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())

    resp = client.get("/market/readiness/vendor-gap")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["gap_count"] == 42
    assert data["session_date"] == "2026-08-20"
    assert "gaps" not in data


def test_vendor_gap_endpoint_detail(monkeypatch) -> None:
    sample = {
        "ok": True,
        "gap_count": 2,
        "session_date": "2026-08-20",
        "gaps": [
            {
                "symbol": "XYZ",
                "reason": "vendor_gap",
                "session_date": "2026-08-20",
                "last_bar_date": "2026-08-18",
                "last_bar_close": 50.0,
                "snapshot_close": 55.0,
                "bar_rows": 300,
                "agg_last_bar_date": "2026-08-18",
                "null_close_rows": 0,
                "null_volume_rows": 0,
            },
            {
                "symbol": "ABC",
                "reason": "fallback_gap",
                "session_date": "2026-08-20",
                "last_bar_date": None,
                "last_bar_close": None,
                "snapshot_close": 10.0,
                "bar_rows": 100,
                "agg_last_bar_date": "2026-07-01",
                "null_close_rows": 5,
                "null_volume_rows": 3,
            },
        ],
    }
    monkeypatch.setattr(mod, "query_vendor_gap", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())

    resp = client.get("/market/readiness/vendor-gap", params={"detail": "true", "limit": "100"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["gap_count"] == 2
    assert len(data["gaps"]) == 2
    assert data["gaps"][0]["reason"] == "vendor_gap"
    assert data["gaps"][1]["reason"] == "fallback_gap"


def test_vendor_gap_endpoint_no_gaps(monkeypatch) -> None:
    sample = {
        "ok": True,
        "gap_count": 0,
        "session_date": "2026-08-20",
    }
    monkeypatch.setattr(mod, "query_vendor_gap", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())

    resp = client.get("/market/readiness/vendor-gap")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["gap_count"] == 0


def test_vendor_gap_endpoint_no_snapshot_table(monkeypatch) -> None:
    sample = {"ok": True, "gap_count": 0, "session_date": None}
    monkeypatch.setattr(mod, "query_vendor_gap", lambda *_a, **_k: sample)
    monkeypatch.setattr(mod, "require_db", lambda: _DummyConn())
    client = TestClient(create_app())

    resp = client.get("/market/readiness/vendor-gap")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["session_date"] is None


# ---------------------------------------------------------------------------
# query_vendor_gap unit tests (table-missing guard)
# ---------------------------------------------------------------------------


class _NoTableCursor:
    def execute(self, query: str, params: Any = None) -> None:
        self._rows: list[Any] = []

    def fetchone(self) -> Any:
        return None

    def fetchall(self) -> list[Any]:
        return []

    def __enter__(self) -> "_NoTableCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _NoTableConn:
    def cursor(self) -> _NoTableCursor:
        return _NoTableCursor()

    def close(self) -> None:
        return None


def test_query_vendor_gap_no_tables(monkeypatch) -> None:
    monkeypatch.setattr(mod, "table_exists", lambda *_a, **_k: False)
    conn = _NoTableConn()
    result = mod.query_vendor_gap(conn)
    assert result["ok"] is True
    assert result["gap_count"] == 0
    assert result["session_date"] is None


def test_query_snapshot_coverage_no_table(monkeypatch) -> None:
    monkeypatch.setattr(mod, "table_exists", lambda *_a, **_k: False)
    conn = _NoTableConn()
    result = mod.query_snapshot_coverage(conn)
    assert result["ok"] is True
    assert result["row_count"] == 0
