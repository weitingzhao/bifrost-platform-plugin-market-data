"""A sibling resource is not a ticker; the doctor report is not recomputed per poll.

R9 C3-P3 / P4.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from bifrost_market_data.api import doctor as doc
from bifrost_market_data.api import stocks
from bifrost_market_data.api.app import create_app


@pytest.fixture()
def client() -> TestClient:
    return TestClient(create_app())


# ── P3: reserved path segments ────────────────────────────────────────────


def test_every_reserved_name_is_a_real_sibling_route() -> None:
    # The map must not rot into a list of paths that no longer exist.
    app = create_app()
    segments = {
        p[len("/market/"):].split("/")[0]
        for p in (getattr(r, "path", "") for r in app.routes)
        if p.startswith("/market/")
    }
    assert set(stocks.RESERVED_SEGMENTS) <= segments
    assert "stocks" not in stocks.RESERVED_SEGMENTS


def test_a_sibling_resource_addressed_as_a_ticker_gets_the_real_path(client: TestClient) -> None:
    res = client.get("/market/stocks/corporate-actions", params={"symbol": "NVDA"})
    assert res.status_code == 404
    detail = res.json()["detail"]
    # Not the vendor's "Invalid ticker": true about the wrong thing.
    assert "not a ticker" in detail and "/market/corporate-actions" in detail


def test_the_guard_covers_the_related_route_and_deeper_paths(client: TestClient) -> None:
    assert client.get("/market/stocks/coverage/inventory").status_code == 404
    assert client.get("/market/stocks/daily-checklist/related").status_code == 404


def test_a_real_ticker_still_reaches_the_vendor(monkeypatch, client: TestClient) -> None:
    class _Poly:
        async def fetch_ticker_details(self, symbol: str) -> dict[str, Any]:
            return {"ok": True, "symbol": symbol}

    app = create_app()
    from bifrost_market_data.api.deps import get_polygon_client

    app.dependency_overrides[get_polygon_client] = lambda: _Poly()
    res = TestClient(app).get("/market/stocks/NVDA")
    assert res.status_code == 200 and res.json()["symbol"] == "NVDA"


# ── P4: doctor cache ──────────────────────────────────────────────────────


def test_the_first_call_answers_at_once_and_computes_behind_it(monkeypatch, client: TestClient) -> None:
    doc.DOCTOR_CACHE.clear()
    calls: list[bool] = []
    monkeypatch.setattr(
        doc, "_doctor_payload", lambda probes: calls.append(probes) or {"ok": True, "findings": []}
    )
    res = client.get("/market/doctor", params={"probes": "false"})
    body = res.json()
    assert res.status_code == 200
    # Nothing cached yet: the empty shape plus computing, not a held-open request.
    assert body["computing"] is True and body["age_sec"] is None
    assert body["findings"] == []


def test_a_second_call_reads_the_cached_report_without_recomputing(monkeypatch, client: TestClient) -> None:
    doc.DOCTOR_CACHE.clear()
    calls: list[bool] = []
    monkeypatch.setattr(
        doc,
        "_doctor_payload",
        lambda probes: calls.append(probes) or {"ok": True, "findings": [{"id": "x"}]},
    )
    client.get("/market/doctor", params={"probes": "true", "refresh": "true"})
    assert len(calls) == 1
    body = client.get("/market/doctor", params={"probes": "true"}).json()
    assert len(calls) == 1, "a cached report must not be recomputed"
    assert body["findings"] == [{"id": "x"}]
    assert body["age_sec"] is not None and body["computing"] is False


def test_refresh_recomputes_and_probes_key_the_cache_apart(monkeypatch, client: TestClient) -> None:
    doc.DOCTOR_CACHE.clear()
    calls: list[bool] = []

    def _payload(probes: bool) -> dict[str, Any]:
        calls.append(probes)
        return {"ok": True, "findings": [], "probes": probes}

    monkeypatch.setattr(doc, "_doctor_payload", _payload)
    client.get("/market/doctor", params={"probes": "true", "refresh": "true"})
    client.get("/market/doctor", params={"probes": "true", "refresh": "true"})
    assert calls == [True, True]
    # A report that skipped the probes is not an answer to the question that
    # asked for them, so it gets its own entry.
    body = client.get("/market/doctor", params={"probes": "false", "refresh": "true"}).json()
    assert calls == [True, True, False] and body["probes"] is False


def test_the_report_says_when_it_was_produced(monkeypatch) -> None:
    monkeypatch.setattr(doc, "_db", lambda: pytest.fail("must not connect"))
    monkeypatch.setattr(doc, "run_doctor", lambda *a, **k: {"ok": True, "findings": []})
    monkeypatch.setattr(doc, "_db", lambda: _NullConn())
    monkeypatch.setattr(doc, "probe_worker_health", lambda: {"ok": True})
    payload = doc._doctor_payload(False)
    assert payload["generated_at"] and payload["probes"] is False


class _NullConn:
    def close(self) -> None:
        pass
