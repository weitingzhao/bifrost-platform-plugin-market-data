"""Shared FastAPI dependencies for Polygon pass-through and DB-read routes."""

from __future__ import annotations

import hmac
import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from fastapi import HTTPException, Request

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError, PolygonRateLimitError

logger = logging.getLogger(__name__)

_client: PolygonClient | None = None

_startup_ok = True
_startup_error: str | None = None


def run_startup_schema_guard() -> None:
    """Best-effort legacy schema guard — does not block process start."""
    global _startup_ok, _startup_error
    try:
        from bifrost_market_data.db.schema_guard import assert_no_legacy_schemas

        conn = connect_db(timeout=5)
        try:
            assert_no_legacy_schemas(conn)
            _startup_ok = True
            _startup_error = None
        finally:
            conn.close()
    except Exception as exc:
        _startup_ok = False
        _startup_error = str(exc)
        logger.error("startup schema guard failed: %s", exc)


def startup_ok() -> bool:
    return _startup_ok


def startup_error() -> str | None:
    return _startup_error


def resolve_polygon_api_key() -> str:
    """Resolve Polygon API key from config or environment."""
    cfg = load_config()
    poly = dict(cfg.get("polygon") or {})
    key = (
        str(poly.get("api_key") or "").strip()
        or os.environ.get("POLYGON_API_KEY", "").strip()
        or os.environ.get("MASSIVE_API_KEY", "").strip()
    )
    if not key:
        raise HTTPException(status_code=503, detail="Polygon API key not configured")
    return key


async def get_polygon_client() -> PolygonClient:
    """FastAPI dependency returning a shared ``PolygonClient`` instance."""
    global _client
    key = resolve_polygon_api_key()
    cfg = load_config()
    poly = dict(cfg.get("polygon") or {})
    tier = str(poly.get("tier") or "developer")
    rest_base = str(poly.get("rest_base") or "https://api.polygon.io")
    if _client is None or _client.api_key != key:
        if _client is not None:
            await _client.aclose()
        _client = PolygonClient(key, tier=tier, rest_base=rest_base)
    return _client


def polygon_error_to_http(exc: PolygonAPIError) -> HTTPException:
    """Map ``PolygonAPIError`` to an HTTP response."""
    status = exc.status_code or 502
    if isinstance(exc, PolygonRateLimitError):
        status = 429
    return HTTPException(status_code=status, detail=exc.message)


def connect_db(*, timeout: int = 10) -> Any:
    """Open a psycopg connection using plugin config."""
    import psycopg

    return psycopg.connect(**postgres_connect_kwargs(load_config()), connect_timeout=timeout)


def require_db() -> Any:
    """Connect or raise HTTP 503."""
    try:
        return connect_db()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


def _relation_exists(conn: Any, schema: str, table: str) -> bool:
    """Exact ``schema.table`` presence check (no aliasing)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                LIMIT 1
                """,
                (schema, table),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def resolve_market_schema(conn: Any, schema: str, table: str) -> str | None:
    """Resolve logical schema for Golden Source tables.

    Wave relocate: persisted Polygon tables live under ``raw_market.*``. Call sites
    still pass ``market`` for historical reasons — treat it as an alias.
    """
    if _relation_exists(conn, schema, table):
        return schema
    if schema == "market" and _relation_exists(conn, "raw_market", table):
        return "raw_market"
    return None


def table_exists(conn: Any, schema: str, table: str) -> bool:
    """Return True when ``schema.table`` is present (``market`` → ``raw_market`` alias)."""
    return resolve_market_schema(conn, schema, table) is not None


def safe_count(conn: Any, qualified_table: str) -> int | None:
    """``COUNT(*)`` on a table; return None when missing or on error."""
    schema, _, name = qualified_table.partition(".")
    if not schema or not name:
        return None
    resolved = resolve_market_schema(conn, schema, name)
    if not resolved:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*)::bigint FROM {resolved}.{name}")
            row = cur.fetchone()
        if row is None:
            return 0
        return int(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))
    except Exception:
        return None


def normalize_symbol(value: str | None) -> str:
    return str(value or "").strip().upper()


def as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def iso_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def row_dict(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    if isinstance(row, Mapping):
        out = {k: row[k] for k in columns if k in row}
    else:
        out = {columns[i]: row[i] for i in range(min(len(columns), len(row)))}
    for key in ("bar_date", "trade_date", "expiry", "ex_date", "record_date", "payment_date", "session_date"):
        if key in out and out[key] is not None:
            d = as_date(out[key])
            if d is not None:
                out[key] = d.isoformat()
    for key in ("snapshot_ts", "fetched_at", "updated_at", "last_run_at", "computed_at"):
        if key in out and out[key] is not None:
            out[key] = iso_value(out[key])
    return out


def view_exists(conn: Any, schema: str, view: str) -> bool:
    """Return True when ``schema.view`` is present in ``information_schema.views``."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.views
                WHERE table_schema = %s AND table_name = %s
                LIMIT 1
                """,
                (schema, view),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def polygon_key_configured() -> bool:
    """Return True when a Polygon API key is present (bool only; no secret)."""
    cfg = load_config()
    poly = dict(cfg.get("polygon") or {})
    key = str(poly.get("api_key") or "").strip()
    if key:
        return True
    return bool(
        str(os.environ.get("POLYGON_API_KEY") or "").strip()
        or str(os.environ.get("MASSIVE_API_KEY") or "").strip()
    )


WRITE_TOKEN_HEADER = "X-Market-Data-Write-Token"


def write_token_expected() -> str:
    """Operator token for Plugin write routes (unarmed when empty)."""
    return (
        os.environ.get("MARKET_DATA_WRITE_TOKEN", "").strip()
        or os.environ.get("PLUGIN_OPERATOR_TOKEN", "").strip()
        or os.environ.get("PLATFORM_OPERATOR_TOKEN", "").strip()
    )


def presented_write_token(request: Request) -> str:
    """Token from Console proxy header (preferred) or Authorization Bearer.

    platform-api on a Mac uses the Kubernetes API service proxy, which replaces
    Authorization with the kube token. The proxy therefore also sends
    ``X-Market-Data-Write-Token``. Trade writers still use Bearer.
    """
    extra = (request.headers.get(WRITE_TOKEN_HEADER) or "").strip()
    if extra:
        return extra
    header = request.headers.get("Authorization") or ""
    if header.startswith("Bearer "):
        return header[7:].strip()
    return ""


def require_write_token(request: Request) -> None:
    """FastAPI dependency: POST/DELETE ingest requires operator token when armed.

    When no token is configured, writes stay open (NetworkPolicy-only). Cluster
    arms ``MARKET_DATA_WRITE_TOKEN`` together with Trade writer env so IB bars
    keep working.
    """
    expected = write_token_expected()
    if not expected:
        return
    got = presented_write_token(request)
    if not got:
        raise HTTPException(status_code=401, detail="operator token required")
    if len(got) != len(expected) or not hmac.compare_digest(got, expected):
        raise HTTPException(status_code=401, detail="operator token required")
