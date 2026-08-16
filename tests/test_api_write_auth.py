"""Write-path operator token: unarmed allows POST; armed requires Bearer."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bifrost_market_data.api.app import create_app


def test_write_unarmed_allows_post_without_token(monkeypatch) -> None:
    monkeypatch.delenv("MARKET_DATA_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("PLUGIN_OPERATOR_TOKEN", raising=False)
    monkeypatch.delenv("PLATFORM_OPERATOR_TOKEN", raising=False)
    client = TestClient(create_app())
    # Validation 400 (empty rows) proves we passed auth, not 401.
    resp = client.post("/market/stocks/bars/ingest", json={"rows": []})
    assert resp.status_code == 422


def test_write_armed_without_token_is_401(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_DATA_WRITE_TOKEN", "secret-op")
    client = TestClient(create_app())
    resp = client.post(
        "/market/stocks/bars/ingest",
        json={
            "rows": [
                {
                    "symbol": "NVDA",
                    "period": "1 D",
                    "bar_time": "2026-08-14",
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                }
            ]
        },
    )
    assert resp.status_code == 401
    assert "operator token" in str(resp.json()).lower()


def test_write_armed_with_token_passes_auth(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_DATA_WRITE_TOKEN", "secret-op")
    client = TestClient(create_app())
    resp = client.post(
        "/market/stocks/bars/ingest",
        headers={"Authorization": "Bearer secret-op"},
        json={"rows": []},
    )
    # Auth passed; pydantic rejects empty rows.
    assert resp.status_code == 422


def test_write_armed_with_proxy_header_passes_auth_when_authorization_is_kube_token(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_DATA_WRITE_TOKEN", "secret-op")
    client = TestClient(create_app())
    resp = client.post(
        "/market/ingest/enqueue",
        headers={
            "Authorization": "Bearer kube-apiserver-token",
            "X-Market-Data-Write-Token": "secret-op",
        },
        json={"kind": "__qa_auth_probe__", "payload": {}},
    )
    # Auth passed via X-header; kind is invalid → 400 not 401.
    assert resp.status_code == 400


def test_write_armed_wrong_token_is_401(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_DATA_WRITE_TOKEN", "secret-op")
    client = TestClient(create_app())
    resp = client.post(
        "/market/ingest/enqueue",
        headers={"Authorization": "Bearer no-match"},
        json={"kind": "stock_daily_grouped", "payload": {"from": "2026-08-14"}},
    )
    assert resp.status_code == 401


def test_get_jobs_unauthenticated_when_armed(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_DATA_WRITE_TOKEN", "secret-op")

    class _Conn:
        def cursor(self):
            return self

        def execute(self, *a, **k):
            return None

        def fetchall(self):
            return []

        def close(self):
            return None

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    from bifrost_market_data.api import ingest as ingest_mod

    monkeypatch.setattr(ingest_mod, "require_db", lambda: _Conn())
    client = TestClient(create_app())
    resp = client.get("/market/ingest/jobs")
    assert resp.status_code == 200
    assert resp.json().get("ok") is True
