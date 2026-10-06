"""HTTP request metrics with the Trade APIs' names and labels (TD-161).

The alert rules ``BifrostAPIHighErrorRate`` / ``BifrostAPIHighLatency``
(bifrost-trade-infra ``k8s/monitoring/bifrost-alerting-rules.yaml``) read
``http_requests_total`` and ``http_request_duration_seconds_bucket``. The
Trade APIs get them from prometheus-fastapi-instrumentator (bifrost-trade-core
``observability/prometheus.py``); this service declares no Prometheus client library
and already writes its ``/metrics`` text by hand, so the same series are kept
here in a small ASGI middleware instead of a new dependency. The rules stay one
expression across every namespace.

Series (same as the instrumentator with ``should_group_status_codes`` and
``should_ignore_untemplated``):

- ``http_requests_total{handler, method, status}`` — status is the class (``2xx``).
- ``http_request_duration_seconds{handler, method}`` — buckets 0.1 / 0.5 / 1.
- ``http_request_duration_highr_seconds`` — no labels, buckets up to 60 s (the
  coarse histogram tops out at 1 s, so a p99 read from it never exceeds 1 s).

Cardinality stays bounded: ``handler`` is the route template (``/x/{symbol}``),
never the raw path; requests that match no route are not recorded; unknown
methods are folded into ``OTHER``. ``/metrics`` is not recorded at all; health
probes are counted but kept out of both latency histograms.

Latency is time to the response headers, so a streaming response is not
measured by how long the client keeps reading it. Single process (uvicorn
without workers), so in-process counters are the pod's counters.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

#: Never recorded (the instrumentator's ``excluded_handlers``).
UNRECORDED_HANDLERS = frozenset({"/metrics"})
#: Counted, but kept out of the latency histograms.
UNTIMED_HANDLERS = frozenset({"/metrics", "/health"})

DURATION_BUCKETS = (0.1, 0.5, 1.0)
HIGHR_BUCKETS = (
    0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0,
    3.5, 4.0, 4.5, 5.0, 7.5, 10.0, 30.0, 60.0,
)  # fmt: skip
_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})


def _escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _labels(pairs: tuple[tuple[str, str], ...]) -> str:
    return ",".join(f'{k}="{_escape(v)}"' for k, v in pairs)


class _Histogram:
    __slots__ = ("bounds", "counts", "n", "total")

    def __init__(self, bounds: tuple[float, ...]) -> None:
        self.bounds = bounds
        self.counts = [0] * len(bounds)
        self.total = 0.0
        self.n = 0

    def observe(self, seconds: float) -> None:
        for i, bound in enumerate(self.bounds):
            if seconds <= bound:
                self.counts[i] += 1
                break
        self.total += seconds
        self.n += 1

    def render(self, name: str, pairs: tuple[tuple[str, str], ...]) -> list[str]:
        lines = []
        running = 0
        for bound, count in zip(self.bounds, self.counts):
            running += count
            le = _labels(pairs + (("le", str(float(bound))),))
            lines.append(f"{name}_bucket{{{le}}} {running}")
        inf = _labels(pairs + (("le", "+Inf"),))
        lines.append(f"{name}_bucket{{{inf}}} {self.n}")
        base = f"{{{_labels(pairs)}}}" if pairs else ""
        lines.append(f"{name}_sum{base} {self.total}")
        lines.append(f"{name}_count{base} {self.n}")
        return lines


class HttpMetrics:
    """In-process request counters and latency histograms."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._requests: dict[tuple[str, str, str], int] = {}
            self._duration: dict[tuple[str, str], _Histogram] = {}
            self._highr = _Histogram(HIGHR_BUCKETS)

    def observe(self, handler: str, method: str, status_code: int, seconds: float) -> None:
        if handler in UNRECORDED_HANDLERS:
            return
        method = method if method in _METHODS else "OTHER"
        status = f"{status_code // 100}xx"
        with self._lock:
            key = (handler, method, status)
            self._requests[key] = self._requests.get(key, 0) + 1
            if handler in UNTIMED_HANDLERS:
                return
            hist = self._duration.get((handler, method))
            if hist is None:
                hist = self._duration[(handler, method)] = _Histogram(DURATION_BUCKETS)
            hist.observe(seconds)
            self._highr.observe(seconds)

    def render(self) -> str:
        out = [
            "# HELP http_requests_total Total number of requests by method, status and handler.",
            "# TYPE http_requests_total counter",
        ]
        with self._lock:
            for (handler, method, status), n in sorted(self._requests.items()):
                pairs = (("handler", handler), ("method", method), ("status", status))
                out.append(f"http_requests_total{{{_labels(pairs)}}} {n}")
            out.append(
                "# HELP http_request_duration_seconds Latency with only few buckets by handler."
            )
            out.append("# TYPE http_request_duration_seconds histogram")
            for (handler, method), hist in sorted(self._duration.items()):
                pairs = (("handler", handler), ("method", method))
                out.extend(hist.render("http_request_duration_seconds", pairs))
            out.append(
                "# HELP http_request_duration_highr_seconds Latency with many buckets but no "
                "API specific labels."
            )
            out.append("# TYPE http_request_duration_highr_seconds histogram")
            out.extend(self._highr.render("http_request_duration_highr_seconds", ()))
        return "\n".join(out) + "\n"


HTTP_METRICS = HttpMetrics()


def _route_template(scope: Scope) -> str | None:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else None


class HttpMetricsMiddleware:
    """Pure ASGI middleware (no response buffering, safe for streaming)."""

    def __init__(self, app: ASGIApp, metrics: HttpMetrics = HTTP_METRICS) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        start = time.perf_counter()
        started = False

        def record(status_code: int) -> None:
            handler = _route_template(scope)
            if handler is not None:
                self.metrics.observe(
                    handler, scope.get("method", ""), status_code, time.perf_counter() - start
                )

        async def send_and_record(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start" and not started:
                started = True
                record(int(message["status"]))
            await send(message)

        try:
            await self.app(scope, receive, send_and_record)
        except Exception:
            if not started:
                record(500)
            raise


__all__ = ["HTTP_METRICS", "HttpMetrics", "HttpMetricsMiddleware"]
