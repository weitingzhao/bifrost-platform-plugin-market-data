"""HTTP request metrics carry the Trade APIs' names and labels (TD-161).

The API alert rules read ``http_requests_total`` and
``http_request_duration_highr_seconds_bucket`` in every namespace; these tests
pin the series, their labels and the bounded-cardinality rules.
"""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.testclient import TestClient

from bifrost_market_data.api.http_metrics import HttpMetrics, HttpMetricsMiddleware


def _sample(text: str, name: str, **labels: str) -> float | None:
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = re.match(r"^([a-zA-Z_:][\w:]*)(?:\{(.*)\})? (\S+)$", line)
        if not m or m.group(1) != name:
            continue
        got = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(2) or ""))
        if got == labels:
            return float(m.group(3))
    return None


@pytest.fixture()
def client_and_metrics():
    metrics = HttpMetrics()
    app = FastAPI()
    app.add_middleware(HttpMetricsMiddleware, metrics=metrics)

    @app.get("/x/{symbol}")
    def x(symbol: str) -> dict:
        return {"symbol": symbol}

    @app.get("/boom")
    def boom() -> dict:
        raise RuntimeError("boom")

    @app.get("/health")
    def health() -> dict:
        return {"ok": True}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics_route() -> str:
        return metrics.render()

    @app.get("/stream")
    def stream() -> StreamingResponse:
        return StreamingResponse(iter([b"a", b"b"]), media_type="text/plain")

    return TestClient(app, raise_server_exceptions=False), metrics


def test_requests_are_counted_by_route_template_method_and_status_class(client_and_metrics):
    client, metrics = client_and_metrics
    assert client.get("/x/AAPL").status_code == 200
    assert client.get("/x/MSFT").status_code == 200
    assert client.get("/boom").status_code == 500
    text = metrics.render()
    assert (
        _sample(text, "http_requests_total", handler="/x/{symbol}", method="GET", status="2xx") == 2
    )
    assert _sample(text, "http_requests_total", handler="/boom", method="GET", status="5xx") == 1
    # raw paths never become label values
    assert "/x/AAPL" not in text


def test_untemplated_paths_and_metrics_scrapes_are_not_recorded(client_and_metrics):
    client, metrics = client_and_metrics
    assert client.get("/no/such/path").status_code == 404
    client.get("/metrics")
    text = metrics.render()
    assert "/no/such/path" not in text
    assert 'handler="/metrics"' not in text


def test_health_is_counted_but_kept_out_of_latency(client_and_metrics):
    client, metrics = client_and_metrics
    client.get("/health")
    text = metrics.render()
    assert _sample(text, "http_requests_total", handler="/health", method="GET", status="2xx") == 1
    assert (
        _sample(text, "http_request_duration_seconds_count", handler="/health", method="GET")
        is None
    )
    assert _sample(text, "http_request_duration_highr_seconds_count") == 0


def test_latency_histograms_have_the_trade_buckets(client_and_metrics):
    client, metrics = client_and_metrics
    client.get("/x/A")
    client.get("/stream")
    text = metrics.render()
    for le in ("0.1", "0.5", "1.0", "+Inf"):
        assert (
            _sample(
                text,
                "http_request_duration_seconds_bucket",
                handler="/x/{symbol}",
                method="GET",
                le=le,
            )
            == 1
        )
    # the fine histogram reaches past the latency rule's 2 s threshold
    assert _sample(text, "http_request_duration_highr_seconds_bucket", le="2.0") == 2
    assert _sample(text, "http_request_duration_highr_seconds_bucket", le="60.0") == 2
    assert _sample(text, "http_request_duration_highr_seconds_count") == 2


def test_unknown_methods_fold_into_other():
    metrics = HttpMetrics()
    metrics.observe("/x", "PROPFIND", 405, 0.01)
    assert (
        _sample(metrics.render(), "http_requests_total", handler="/x", method="OTHER", status="4xx")
        == 1
    )


def test_the_plugin_app_records_and_exports_http_metrics(monkeypatch):
    from bifrost_market_data.api import metrics as metrics_module
    from bifrost_market_data.api.app import create_app

    assert any(m.cls is HttpMetricsMiddleware for m in create_app().user_middleware)
    monkeypatch.setattr(metrics_module.DOCTOR_CACHE, "read", lambda *a, **k: {})
    body = metrics_module.metrics().body.decode()
    assert "bifrost_market_data_plugin_info" in body
    assert "# TYPE http_requests_total counter" in body
    assert "http_request_duration_highr_seconds_count" in body
