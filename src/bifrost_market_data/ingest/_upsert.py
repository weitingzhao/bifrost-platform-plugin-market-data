"""Shared upsert helpers and Polygon → row transforms for ingest handlers."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from bifrost_market_data.worker.claim import JobRow

# Polygon option ticker: O:AAPL250620C00150000
_OPTION_TICKER_RE = re.compile(
    r"^O:(?P<underlying>[A-Z.\-]+)"
    r"(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})"
    r"(?P<right>[CP])"
    r"(?P<strike>\d{8})$",
    re.IGNORECASE,
)

_NY = ZoneInfo("America/New_York")

# Logical ingest names (market.*) map to Golden Source physical schemas.
# Wave 7: market_analytics / features_daily retired — Research owns features.* writes.
_LOGICAL_TO_PHYSICAL_SCHEMA = {
    "market": "raw_market",
}

_FORBIDDEN_LOGICAL_SCHEMAS = frozenset({"market_analytics", "features_daily"})


def physical_table_name(qualified: str) -> str:
    """Map logical ``market.*`` ingest targets to physical Golden Source tables."""
    if "." not in qualified:
        raise ValueError(f"expected qualified table name, got: {qualified!r}")
    schema, name = qualified.split(".", 1)
    if schema in _FORBIDDEN_LOGICAL_SCHEMAS:
        raise ValueError(
            f"logical schema {schema!r} retired (Wave 7); "
            "use bifrost_research features.* for analytics writes"
        )
    physical_schema = _LOGICAL_TO_PHYSICAL_SCHEMA.get(schema, schema)
    return f"{physical_schema}.{name}"


class _Cursor(Protocol):
    def execute(self, query: str, params: Any = None) -> Any: ...

    def executemany(self, query: str, params_seq: Sequence[Any]) -> Any: ...

    def __enter__(self) -> _Cursor: ...

    def __exit__(self, *args: object) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


HandlerFn = Callable[[JobRow, Any, Any], Awaitable[Mapping[str, Any] | None]]
Handler = Callable[[JobRow], Awaitable[Mapping[str, Any] | None]]


def daily_snapshot_anchor(now: datetime | None = None) -> datetime:
    """Stable NY-session daily timestamp (16:00 America/New_York) for idempotent snapshots.

    Re-running the same job on the same NY calendar day yields the same ``snapshot_ts``.
    """
    base = now if now is not None else datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    ny = base.astimezone(_NY)
    d = ny.date()
    return datetime(d.year, d.month, d.day, 16, 0, tzinfo=_NY)


def epoch_ms_to_datetime(t: int | float) -> datetime:
    """Polygon bar ``t`` (epoch milliseconds) → UTC datetime."""
    return datetime.fromtimestamp(float(t) / 1000.0, tz=timezone.utc)


def epoch_ms_to_date(t: int | float) -> date:
    """Polygon bar ``t`` (epoch milliseconds) → UTC calendar date."""
    return epoch_ms_to_datetime(t).date()


def epoch_ns_to_datetime(t: int | float) -> datetime:
    """Polygon SIP timestamp (epoch nanoseconds) → UTC datetime."""
    return datetime.fromtimestamp(float(t) / 1_000_000_000.0, tz=timezone.utc)


def parse_option_right(value: Any) -> str:
    """Normalize contract_type / right to ``C`` or ``P``."""
    s = str(value or "").strip().upper()
    if s in ("C", "CALL"):
        return "C"
    if s in ("P", "PUT"):
        return "P"
    raise ValueError(f"invalid option_right: {value!r}")


def parse_option_ticker(option_ticker: str) -> dict[str, Any]:
    """Parse Polygon native option key ``O:AAPL250620C00150000``.

    Returns dict with ``option_ticker``, ``underlying``, ``expiry`` (date),
    ``strike`` (float), ``option_right`` (``C``|``P``).
    """
    raw = str(option_ticker or "").strip().upper()
    m = _OPTION_TICKER_RE.match(raw)
    if not m:
        raise ValueError(f"invalid option_ticker: {option_ticker!r}")
    yy = int(m.group("yy"))
    year = 2000 + yy if yy < 70 else 1900 + yy
    expiry = date(year, int(m.group("mm")), int(m.group("dd")))
    strike = int(m.group("strike")) / 1000.0
    return {
        "option_ticker": raw,
        "underlying": m.group("underlying").upper(),
        "expiry": expiry,
        "strike": strike,
        "option_right": m.group("right").upper(),
    }


def period_label(multiplier: int, timespan: str) -> str:
    """Build stock/option minute ``period`` column value, e.g. ``1 minute``."""
    return f"{int(multiplier)} {str(timespan).strip().lower()}"


def parse_date(value: Any) -> date | None:
    """Parse ``YYYY-MM-DD`` / date / datetime into ``date``; None if empty."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()[:10]
    return date.fromisoformat(s)


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def batch_upsert(
    conn: _Connection,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    conflict_keys: Sequence[str],
    update_cols: Sequence[str] | None = None,
    set_fetched_at: bool = True,
    auto_commit: bool = True,
) -> int:
    """Insert rows with ``ON CONFLICT DO UPDATE``. Returns number of input rows.

    ``table`` must be a qualified name like ``market.stock_daily`` (no user input).
    Values that are ``dict``/``list`` are JSON-serialized for jsonb columns.
    Pass ``auto_commit=False`` to batch multiple upserts in one transaction
    (caller must ``conn.commit()``).
    """
    if not rows:
        return 0
    cols = list(columns)
    conflict = list(conflict_keys)
    if not cols or not conflict:
        raise ValueError("columns and conflict_keys are required")

    physical_table = physical_table_name(table)
    updates = list(update_cols) if update_cols is not None else [c for c in cols if c not in conflict]
    set_parts = [f"{c} = EXCLUDED.{c}" for c in updates]
    if set_fetched_at and "fetched_at" not in updates and "fetched_at" not in conflict:
        # Only add if the table is expected to have fetched_at (callers control via flag)
        if "fetched_at" in cols or set_fetched_at:
            set_parts.append("fetched_at = now()")

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    conflict_list = ", ".join(conflict)
    if set_parts:
        sql = (
            f"INSERT INTO {physical_table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_list}) DO UPDATE SET {', '.join(set_parts)}"
        )
    else:
        sql = (
            f"INSERT INTO {physical_table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_list}) DO NOTHING"
        )

    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        if len(row) != len(cols):
            raise ValueError(f"row length {len(row)} != columns {len(cols)}")
        prepared.append(tuple(_prepare_value(v) for v in row))

    try:
        with conn.cursor() as cur:
            cur.executemany(sql, prepared)
        if auto_commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(prepared)


def _prepare_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def make_handler(
    fn: HandlerFn,
    *,
    client: Any,
    connect: Callable[[], Any],
) -> Handler:
    """Wrap ``async def fn(job, client, conn)`` into a loop-compatible Handler.

    Opens a dedicated PG connection for the handler write path, separate from
    the claim/mark connection owned by the worker loop.
    """

    async def _handler(job: JobRow) -> Mapping[str, Any] | None:
        conn = await asyncio.to_thread(connect)
        try:
            result = await fn(job, client, conn)
            return result
        finally:
            try:
                await asyncio.to_thread(conn.close)
            except Exception:
                pass

    return _handler
