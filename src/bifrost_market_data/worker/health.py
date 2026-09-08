"""Lightweight HTTP /health endpoint (stdlib asyncio, no extra deps)."""

from __future__ import annotations

import asyncio
import threading
import time
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class HealthState:
    """Shared mutable health snapshot updated by the worker loop."""

    pool: str = "stocks"
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_claim_at: datetime | None = None
    jobs_done: int = 0
    jobs_failed: int = 0
    status: str = "ok"
    # Monotonic stamp of the worker loop's last pass. The handlers do their
    # database work synchronously, so a heavy batch blocks the loop; reporting
    # the lag is honest where a bare "ok" would not be.
    last_tick: float = field(default_factory=time.monotonic)

    def tick(self) -> None:
        self.last_tick = time.monotonic()

    def record_claim(self, when: datetime | None = None) -> None:
        self.last_claim_at = when or datetime.now(timezone.utc)

    def record_done(self) -> None:
        self.jobs_done += 1

    def record_failed(self) -> None:
        self.jobs_failed += 1

    def snapshot(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        uptime = max(0.0, (now - self.started_at).total_seconds())
        return {
            "status": self.status,
            "pool": self.pool,
            "last_claim_at": self.last_claim_at.isoformat().replace("+00:00", "Z")
            if self.last_claim_at
            else None,
            "jobs_done": self.jobs_done,
            "jobs_failed": self.jobs_failed,
            "uptime_sec": int(uptime),
            "loop_lag_sec": round(max(0.0, time.monotonic() - self.last_tick), 1),
        }


def _http_response(status: int, body: bytes, content_type: str = "application/json") -> bytes:
    reason = {200: "OK", 404: "Not Found", 405: "Method Not Allowed"}.get(status, "OK")
    headers = (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    return headers.encode("ascii") + body


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    state: HealthState,
) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        # Drain remaining headers
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line in (b"\r\n", b"\n", b""):
                break

        text = request_line.decode("latin-1", errors="replace").strip()
        parts = text.split()
        method = parts[0].upper() if parts else "GET"
        path = parts[1] if len(parts) > 1 else "/"

        if method != "GET":
            writer.write(_http_response(405, b'{"error":"method not allowed"}'))
        elif path.split("?", 1)[0] != "/health":
            writer.write(_http_response(404, b'{"error":"not found"}'))
        else:
            body = json.dumps(state.snapshot()).encode("utf-8")
            writer.write(_http_response(200, body))
        await writer.drain()
    except Exception as e:
        logger.debug("health client error: %s", e)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


def start_health_server_thread(
    state: HealthState,
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
) -> threading.Thread:
    """Serve /health from its own thread and event loop.

    The worker loop blocks on synchronous upserts — a 12,000-row grouped-daily
    batch holds it for seconds — and a health endpoint sharing that loop simply
    stops answering, which reads as a dead pod. Health is an observability
    surface: it must answer even while the worker is busy, and say how far
    behind the loop is.
    """

    def _serve() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            server = loop.run_until_complete(start_health_server(state, host=host, port=port))
            loop.run_until_complete(server.serve_forever())
        except Exception:  # noqa: BLE001 — the worker must outlive its probe
            logger.exception("health server thread stopped")
        finally:
            loop.close()

    thread = threading.Thread(target=_serve, name="health-server", daemon=True)
    thread.start()
    return thread


async def start_health_server(
    state: HealthState,
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
) -> asyncio.Server:
    """Start the /health HTTP server; caller owns the returned Server lifecycle."""

    async def _client_cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _handle_client(reader, writer, state)

    server = await asyncio.start_server(_client_cb, host=host, port=port)
    sockets = server.sockets or []
    bound = sockets[0].getsockname() if sockets else (host, port)
    logger.info("health server listening on %s:%s", bound[0], bound[1])
    return server
