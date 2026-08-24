"""Tests for /market/readiness/source-void and summary contract shape."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app
from bifrost_market_data.api import readiness_summary as summary_mod
from bifrost_market_data.api import source_void as void_mod


class _DummyConn:
    def close(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def cursor(self) -> Any:
        raise AssertionError("cursor should be mocked")


def test_source_void_get_empty(monkeypatch) -> None:
    monkeypatch.setattr(void_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(void_mod, "query_all_voids", lambda _c: {})
    client = TestClient(create_app())
    resp = client.get("/market/readiness/source-void")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["voids"] == {}
    assert data["acks"] == []


def test_source_void_post_roundtrip(monkeypatch) -> None:
    stored: dict[str, Any] = {}

    def _upsert(conn, *, data_type, is_void, gap_count=None, note=None):
        stored[data_type] = {
            "is_void": is_void,
            "acked_gap_count": gap_count,
            "note": note,
        }
        return {
            "data_type": data_type,
            "is_void": is_void,
            "acked_gap_count": gap_count,
            "note": note,
            "void_reason": note,
            "updated_at": "2026-08-23T12:00:00+00:00",
            "acked_at": "2026-08-23T12:00:00+00:00",
        }

    monkeypatch.setattr(void_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(void_mod, "table_exists", lambda *_a, **_k: True)
    monkeypatch.setattr(void_mod, "upsert_void", _upsert)
    monkeypatch.setattr(void_mod, "require_write_token", lambda: None)

    client = TestClient(create_app())
    # Bypass write-token dependency by overriding on app
    app = create_app()
    app.dependency_overrides[void_mod.require_write_token] = lambda: None
    client = TestClient(app)

    resp = client.post(
        "/market/readiness/source-void",
        json={
            "data_type": "ratios",
            "is_void": True,
            "gap_count": 42,
            "void_reason": "vendor N/A",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["data_type"] == "ratios"
    assert data["is_void"] is True
    assert data["acked_gap_count"] == 42
    assert stored["ratios"]["is_void"] is True


def test_source_void_post_rejects_bad_type(monkeypatch) -> None:
    monkeypatch.setattr(void_mod, "require_db", lambda: _DummyConn())
    app = create_app()
    app.dependency_overrides[void_mod.require_write_token] = lambda: None
    client = TestClient(app)
    resp = client.post(
        "/market/readiness/source-void",
        json={"data_type": "bogus", "is_void": True, "gap_count": 1},
    )
    assert resp.status_code == 400


def test_summary_contract_fields(monkeypatch) -> None:
    sample = {
        "ok": True,
        "universe_count": 100,
        "tickers_active_count": 100,
        "tickers_last_synced_at": None,
        "price_readiness_live": {"total_symbols": 90, "price_ready": 80},
        "fund_cache_valid_count": 70,
        "snapshot_populated": True,
        "snapshot_today": {
            "rows_total": 70,
            "included_in_universe": 70,
            "price_ready": 70,
        },
        "notes_breakdown": [],
        "stock_day_vendor_fill_gap_count": 5,
        "income_statements_gap_count": 10,
        "balance_sheets_gap_count": 10,
        "cash_flows_gap_count": 10,
        "ratios_gap_count": 10,
        "short_interest_gap_count": 10,
        "short_volume_gap_count": 10,
        "income_statements_source_void": True,
        "balance_sheets_source_void": False,
        "cash_flows_source_void": False,
        "ratios_source_void": False,
        "short_interest_source_void": False,
        "short_volume_source_void": False,
        "income_statements_acked_gap_count": 8,
        "balance_sheets_acked_gap_count": None,
        "cash_flows_acked_gap_count": None,
        "ratios_acked_gap_count": None,
        "short_interest_acked_gap_count": None,
        "short_volume_acked_gap_count": None,
        "income_statements_actionable_gap_count": 2,
        "balance_sheets_actionable_gap_count": 10,
        "cash_flows_actionable_gap_count": 10,
        "ratios_actionable_gap_count": 10,
        "short_interest_actionable_gap_count": 10,
        "short_volume_actionable_gap_count": 10,
    }
    monkeypatch.setattr(summary_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(summary_mod, "build_readiness_summary", lambda _c: sample)
    client = TestClient(create_app())
    resp = client.get("/market/readiness/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["stock_day_vendor_fill_gap_count"] == 5
    for dt in (
        "income_statements",
        "balance_sheets",
        "cash_flows",
        "ratios",
        "short_interest",
        "short_volume",
    ):
        assert f"{dt}_gap_count" in data
        assert f"{dt}_source_void" in data
        assert f"{dt}_acked_gap_count" in data
        assert f"{dt}_actionable_gap_count" in data
