"""Shared FastAPI dependencies for Polygon pass-through and DB-read routes."""

from __future__ import annotations

import hmac
import logging
import os
import threading
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from fastapi import HTTPException, Request

from bifrost_market_data.config import load_config, postgres_connect_kwargs
from bifrost_market_data.db.schema_guard import LegacySchemaError, assert_no_legacy_schemas
from bifrost_market_data.polygon.client import PolygonClient
from bifrost_market_data.polygon.errors import PolygonAPIError, PolygonRateLimitError

logger = logging.getLogger(__name__)

_client: PolygonClient | None = None

_startup_ok = True
_startup_error: str | None = None
#: The guard has not managed to ask the database yet — as opposed to having
#: asked and found a legacy schema. Only this state is worth asking again.
_guard_unverified = False
_guard_lock = threading.Lock()


def run_startup_schema_guard(*, timeout: int = 5) -> None:
    """Best-effort legacy schema guard — does not block process start.

    Finding a legacy schema is a verdict and stays. Not reaching the database
    is no verdict at all: measured on the 0.41.2 rollout (2026-09-26), the
    guard ran 100 ms after process start, the server closed its connection —
    most likely the egress NetworkPolicy not yet programmed for the new pod —
    and every database call after it worked. The guard ran once, so /health
    said ``degraded`` for the whole life of a healthy pod.
    """
    global _startup_ok, _startup_error, _guard_unverified
    try:
        conn = connect_db(timeout=timeout)
        try:
            assert_no_legacy_schemas(conn)
        finally:
            conn.close()
    except LegacySchemaError as exc:
        _startup_ok, _startup_error, _guard_unverified = False, str(exc), False
        logger.error("startup schema guard failed: %s", exc)
    except Exception as exc:  # noqa: BLE001 — could not check; /health asks again
        _startup_ok, _startup_error, _guard_unverified = False, str(exc), True
        logger.error("startup schema guard could not run: %s", exc)
    else:
        if _guard_unverified:
            logger.info("startup schema guard passed on re-check")
        _startup_ok, _startup_error, _guard_unverified = True, None, False


def recheck_schema_guard() -> bool:
    """Re-run, in the background, a guard that never reached the database.

    Returns True when a re-run was started. Behind the response rather than in
    it: the readiness probe allows /health 3 seconds and its own database probe
    already spends up to 2. The next probe reads the result.
    """
    if not _guard_unverified or not _guard_lock.acquire(blocking=False):
        return False

    def run() -> None:
        try:
            run_startup_schema_guard(timeout=2)
        finally:
            _guard_lock.release()

    threading.Thread(target=run, name="schema-guard-recheck", daemon=True).start()
    return True


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


def connect_db(*, timeout: int = 10, statement_timeout: str = "60s") -> Any:
    """Open a psycopg connection using plugin config.

    The ``bifrost`` role defaults to ``statement_timeout=2s`` (writer safety).
    Coverage / inventory / quality reads regularly exceed that on large
    ``raw_market.*`` tables, so API connections raise the session limit — as do
    the workers, whose handlers write batches the role's 2s cannot finish.

    The session limit is applied by ``postgres_connect_kwargs`` — one
    implementation, shared with the workers.
    """
    import psycopg

    kw = postgres_connect_kwargs(load_config(), statement_timeout=statement_timeout or "60s")
    return psycopg.connect(**kw, connect_timeout=timeout)


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


def estimated_rows(conn: Any, qualified_table: str) -> int | None:
    """Planner row estimate for a table, summed across its partitions.

    For the tables where ``COUNT(*)`` is not affordable. Measured 2026-09-10:
    counting ``raw_market.option_daily`` exceeded a 180s budget, while this
    answered 37,317,672 in under a millisecond — the retired sepa-stats panel
    had been timing out on exactly that count and reporting the failure as a
    null the console painted red.

    An estimate, and callers must say so. ``reltuples`` is whatever the last
    ANALYZE saw; on a table taking 70,000 rows a night it drifts between runs.
    """
    schema, _, name = qualified_table.partition(".")
    if not schema or not name:
        return None
    resolved = resolve_market_schema(conn, schema, name)
    if not resolved:
        return None
    try:
        with conn.cursor() as cur:
            # Partitioned parents hold no rows of their own, so sum the leaves.
            # LIKE on relname rather than pg_inherits: the partition naming is
            # the plugin's own (`option_daily_2026_09`), and a stray table that
            # merely starts with the name would have to be one we created.
            cur.execute(
                """
                SELECT COALESCE(SUM(c.reltuples), 0)::bigint
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND (c.relname = %s OR c.relname LIKE %s)
                  AND c.relkind = 'r'
                """,
                (resolved, name, name + r"\_%"),
            )
            row = cur.fetchone()
        if row is None:
            return None
        value = row[0] if not isinstance(row, Mapping) else next(iter(row.values()))
        return max(0, int(value or 0))
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return None


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


def normalize_symbols(values: Sequence[str] | None) -> list[str]:
    """``normalize_symbol`` over a list: blanks dropped, duplicates removed, order kept.

    Symbol columns are compared bare so their indexes can be probed; that is only
    the same answer as ``UPPER(TRIM(col))`` when the input is normalised too.
    """
    return list(dict.fromkeys(s for s in map(normalize_symbol, values or ()) if s))


def reject_unknown_params(request: Request, aliases: Mapping[str, str]) -> None:
    """422 naming the right parameter when a caller uses one this route does not have.

    FastAPI drops query parameters it does not declare, so ``?expiration=…`` on a
    route that filters by ``expiry`` used to return the unfiltered answer and look
    like the filter had matched everything. Silence is the worst of the options.
    """
    for wrong, right in aliases.items():
        if wrong in request.query_params:
            raise HTTPException(
                status_code=422,
                detail=f"unknown parameter {wrong!r} for this route; use {right!r}",
            )


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
    for key in (
        "bar_date",
        "trade_date",
        "expiry",
        "ex_date",
        "record_date",
        "payment_date",
        "session_date",
    ):
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
