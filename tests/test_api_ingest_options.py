"""Wave 5-C ingest + options + analytics compute route tests (offline mocks)."""

from __future__ import annotations

from typing import Any, List, Tuple

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app


class _DummyConn:
    def close(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def cursor(self) -> Any:
        raise AssertionError("cursor should be mocked via helpers")


# ──────────────────────────────────────────────────────────────────
# W0-P3: POST /market/options/expirations/replace
# ──────────────────────────────────────────────────────────────────


class _MockCursor:
    """Minimal cursor mock that records execute/executemany calls."""

    def __init__(self) -> None:
        self.executed: List[Tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def executemany(self, sql: str, params_seq: Any) -> None:
        for p in params_seq:
            self.executed.append((sql, p))

    def __enter__(self) -> "_MockCursor":
        return self

    def __exit__(self, *_: Any) -> None:
        pass


class _WritableConn:
    def __init__(self) -> None:
        self._cursor = _MockCursor()
        self.committed = False
        self.rolled_back = False

    def close(self) -> None:
        pass

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def cursor(self) -> _MockCursor:
        return self._cursor


def test_replace_expirations_success(monkeypatch) -> None:
    from bifrost_market_data.api import ingest_options as mod

    conn = _WritableConn()
    monkeypatch.setattr(mod, "require_db", lambda: conn)
    client = TestClient(create_app())
    resp = client.post(
        "/market/options/expirations/replace",
        json={
            "symbol": "NVDA",
            "expirations": ["2026-09-19", "2026-10-17", "2026-12-19"],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["symbol"] == "NVDA"
    assert data["replaced"] == 3
    assert conn.committed is True
    # Verify DELETE was issued
    delete_calls = [c for c in conn._cursor.executed if "DELETE" in c[0]]
    assert len(delete_calls) == 1
    assert delete_calls[0][1] == ("NVDA",)
    # Verify INSERTs
    insert_calls = [c for c in conn._cursor.executed if "INSERT" in c[0]]
    assert len(insert_calls) == 3


def test_replace_expirations_yyyymmdd_format(monkeypatch) -> None:
    from bifrost_market_data.api import ingest_options as mod

    conn = _WritableConn()
    monkeypatch.setattr(mod, "require_db", lambda: conn)
    client = TestClient(create_app())
    resp = client.post(
        "/market/options/expirations/replace",
        json={
            "symbol": "aapl",
            "expirations": ["20260919", "20261017"],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["symbol"] == "AAPL"
    assert data["replaced"] == 2


def test_replace_expirations_empty_list_rejected() -> None:
    client = TestClient(create_app())
    resp = client.post(
        "/market/options/expirations/replace",
        json={"symbol": "NVDA", "expirations": []},
    )
    assert resp.status_code == 400


def test_replace_expirations_invalid_date_rejected() -> None:
    client = TestClient(create_app())
    resp = client.post(
        "/market/options/expirations/replace",
        json={"symbol": "NVDA", "expirations": ["not-a-date"]},
    )
    assert resp.status_code == 422


def test_replace_expirations_missing_symbol_rejected() -> None:
    client = TestClient(create_app())
    resp = client.post(
        "/market/options/expirations/replace",
        json={"expirations": ["2026-09-19"]},
    )
    assert resp.status_code == 422


def test_ingest_kinds_lists_handlers() -> None:
    client = TestClient(create_app())
    resp = client.get("/market/ingest/kinds")
    assert resp.status_code == 200
    kinds = resp.json()["kinds"]
    assert "stock_daily" in kinds
    assert "option_snapshot" in kinds


def test_ingest_enqueue_writes_job(monkeypatch) -> None:
    from bifrost_market_data.api import ingest as ingest_mod

    monkeypatch.setattr(ingest_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(ingest_mod, "insert_job", lambda *_a, **_k: 42)
    client = TestClient(create_app())
    resp = client.post(
        "/market/ingest/enqueue",
        json={"kind": "ticker_sync", "payload": {}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["job_id"] == "42"
    assert data["deduplicated"] is False


def test_ingest_enqueue_rejects_unknown_kind() -> None:
    client = TestClient(create_app())
    resp = client.post(
        "/market/ingest/enqueue",
        json={"kind": "not_a_real_kind", "payload": {}},
    )
    assert resp.status_code == 400


def test_ingest_get_job(monkeypatch) -> None:
    from bifrost_market_data.api import ingest as ingest_mod

    sample = {
        "id": 7,
        "job_id": "7",
        "kind": "ticker_sync",
        "status": "pending",
        "payload": {},
    }
    monkeypatch.setattr(ingest_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(ingest_mod, "get_job", lambda *_a, **_k: sample)
    client = TestClient(create_app())
    resp = client.get("/market/ingest/jobs/7")
    assert resp.status_code == 200
    assert resp.json()["job"]["kind"] == "ticker_sync"


def test_options_expirations(monkeypatch) -> None:
    from bifrost_market_data.api import options as options_mod

    monkeypatch.setattr(options_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(
        options_mod,
        "query_expirations",
        lambda *_a, **_k: {
            "symbol": "AAPL",
            "expirations": ["2025-06-20"],
            "strikes": [100.0, 105.0],
            "provider": "db",
            "source": "option_expiration",
            "expiration_for_strikes": "2025-06-20",
        },
    )
    client = TestClient(create_app())
    resp = client.get("/market/options/expirations", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    assert resp.json()["expirations"] == ["2025-06-20"]


def test_options_snapshots_and_oi(monkeypatch) -> None:
    from bifrost_market_data.api import options as options_mod

    monkeypatch.setattr(options_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(
        options_mod,
        "query_snapshots",
        lambda *_a, **_k: [{"option_ticker": "O:AAPL", "iv": 0.25, "open_interest": 10}],
    )
    monkeypatch.setattr(
        options_mod,
        "query_oi",
        lambda *_a, **_k: [{"option_ticker": "O:AAPL", "open_interest": 10}],
    )
    client = TestClient(create_app())
    r1 = client.get("/market/options/snapshots", params={"symbol": "AAPL"})
    assert r1.status_code == 200
    assert r1.json()["count"] == 1
    r2 = client.get("/market/options/oi", params={"symbol": "AAPL"})
    assert r2.status_code == 200
    assert r2.json()["count"] == 1


def test_options_liquidity_summary(monkeypatch) -> None:
    from bifrost_market_data.api import options as options_mod

    monkeypatch.setattr(options_mod, "require_db", lambda: _DummyConn())
    monkeypatch.setattr(
        options_mod,
        "query_snapshots",
        lambda *_a, **_k: [
            {"open_interest": 10, "day_volume": 5},
            {"open_interest": 100, "day_volume": 50},
        ],
    )
    client = TestClient(create_app())
    resp = client.get("/market/options/liquidity-summary", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["contracts"] == 2
    assert data["oi"]["sum"] == 110


def test_max_pain_compute_route(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    monkeypatch.setattr(
        analytics_mod,
        "compute_max_pain_live",
        lambda *_a, **_k: {
            "ok": True,
            "symbol": "AAPL",
            "expiry": "2025-06-20",
            "trade_date": "2024-06-20",
            "max_pain_strike": 100.0,
            "total_pain_at_strike": 1.0,
            "total_oi": 10,
            "points": [],
            "source": "live_oi",
        },
    )
    client = TestClient(create_app())
    resp = client.get(
        "/market/analytics/max-pain/compute",
        params={"symbol": "AAPL", "expiry": "2025-06-20"},
    )
    assert resp.status_code == 200
    assert resp.json()["max_pain_strike"] == 100.0


def test_max_pain_compute_history_route(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    monkeypatch.setattr(
        analytics_mod,
        "compute_max_pain_history",
        lambda *_a, **_k: {
            "ok": True,
            "symbol": "AAPL",
            "expiry": "2025-06-20",
            "lookback_days": 30,
            "series": [{"trade_date": "2024-06-20", "max_pain_strike": 100.0}],
            "count": 1,
            "source": "live_oi",
        },
    )
    client = TestClient(create_app())
    resp = client.get(
        "/market/analytics/max-pain/compute/history",
        params={"symbol": "AAPL", "expiry": "20250620", "lookback_days": 30},
    )
    assert resp.status_code == 200
    assert resp.json()["count"] == 1


def test_atm_iv_term_route(monkeypatch) -> None:
    from bifrost_market_data.api import analytics as analytics_mod

    monkeypatch.setattr(analytics_mod, "_connect", lambda: _DummyConn())
    monkeypatch.setattr(
        analytics_mod,
        "query_atm_iv",
        lambda *_a, **_k: [
            {
                "symbol": "AAPL",
                "trade_date": "2024-06-20",
                "expiry": "2025-06-20",
                "atm_iv": 0.26,
            },
            {
                "symbol": "AAPL",
                "trade_date": "2024-06-20",
                "expiry": "2025-07-18",
                "atm_iv": 0.28,
            },
        ],
    )
    client = TestClient(create_app())
    resp = client.get("/market/analytics/atm-iv/term", params={"symbol": "AAPL"})
    assert resp.status_code == 200
    assert resp.json()["count"] == 2
