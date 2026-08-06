"""Health endpoint with optional PostgreSQL connectivity probe."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

router = APIRouter(tags=["health"])

_DB_PROBE_TIMEOUT_SEC = 2.0


def _probe_db() -> str:
    """Return 'ok' or 'unreachable' after a short SELECT 1 probe."""
    try:
        import psycopg

        from bifrost_market_data.config import load_config, postgres_connect_kwargs

        kw = postgres_connect_kwargs(load_config())
        kw = {**kw, "connect_timeout": int(_DB_PROBE_TIMEOUT_SEC)}
        with psycopg.connect(**kw) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return "ok"
    except Exception:
        return "unreachable"


@router.get("/health")
def health() -> dict[str, Any]:
    """Process health plus DB probe.

    Always HTTP 200 so liveness stays up; readiness consumers should check
    ``status`` / ``db`` in the body (``degraded`` when DB is unreachable).
    """
    db = _probe_db()
    status = "ok" if db == "ok" else "degraded"
    return {
        "status": status,
        "service": "market-data-api",
        "db": db,
    }
