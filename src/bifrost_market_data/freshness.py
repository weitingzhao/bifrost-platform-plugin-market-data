"""Best-effort UPSERT into ops_jobs.ingest_freshness after successful ingest jobs."""

from __future__ import annotations

from typing import Any, Mapping

# Jobs that write the same logical table map to one freshness dimension.
_DIMENSION_ALIASES: dict[str, str] = {
    "stock_daily_grouped": "stock_daily",
    # Full-market pulls fill the same tables as the per-symbol kinds.
    "splits_market": "splits",
    "dividends_market": "dividends",
    "ratios_market": "ratios",
    "short_volume_market": "short_volume",
    "short_interest_market": "short_interest",
    # Both filing kinds write the same three tables; one dimension evidences them.
    "sec_filings_market": "sec_filings",
    "sec_filings_symbol": "sec_filings",
    # The planner writes no rows itself; its jobs fill option_daily.
    "option_backfill_plan": "option_daily",
}


def dimension_for_kind(kind: str) -> str:
    """Map job kind → freshness dimension (PK of ops_jobs.ingest_freshness)."""
    key = str(kind or "").strip()
    return _DIMENSION_ALIASES.get(key, key)


#: Prefix of the freshness rows a policed slot's own jobs write. A dimension
#: row says when anything last touched the table; several slots share one, so
#: ``ticker_sync`` was bumped every night by ticker-details' 200 detail jobs
#: (one row each) while the reference walk it was meant to evidence could have
#: stopped. A ``slot:<id>`` row is bumped only by that slot's jobs, and only when
#: they delivered rows.
SLOT_FRESHNESS_PREFIX = "slot:"


def slot_freshness_key(slot: str) -> str:
    return f"{SLOT_FRESHNESS_PREFIX}{slot}"


def _policed_slot_of_option_contract(payload: Mapping[str, Any]) -> str | None:
    # option-refresh walks the live catalogue: explicit expired=False and no
    # date bounds. option-contract-expired walks quarters of the expired one.
    if payload.get("expired") is not False:
        return None
    if any(payload.get(k) for k in ("expiration_date", "expiration_date_gte", "expiration_date_lte")):
        return None
    return "option-refresh"


#: Every kind ``policed_slot_for_job`` can name a slot for.
POLICED_SLOT_KINDS: tuple[str, ...] = (
    "calendar",
    "ticker_sync",
    "option_contract",
    "dividends_market",
    "splits_market",
    "financials",
)


def policed_slot_for_job(kind: str, payload: Mapping[str, Any] | None) -> str | None:
    """The shape-named slot whose own work this job is, or None.

    Every slot this can name is in ``SHAPE_NAMED_SLOTS``; the doctor polices all
    of them but ticker-details (``doctor.POLICED_SLOTS``).

    Read from the job's shape, the way the slot enqueues it
    (``scheduler.daily.enqueue_slot``); ``tests/test_slot_freshness.py`` runs
    every slot through the real enqueue and holds this function to it, so a
    sibling slot that shares a kind (ticker-details, option-contract-expired,
    corporate-backfill) can never evidence a policed one. An Owner-run backfill
    of the same shape (``scheduler.backfill``) does count: it is the same fetch.
    """
    k = str(kind or "").strip()
    p = payload or {}
    if k == "calendar":
        return "calendar"
    if k == "ticker_sync":
        # The whole-market list walk. Its delisted lookups ride the same slot but
        # are a handful of single names; the walk is what the slot is for.
        if p.get("mode") == "universe":
            return "reference"
        # The per-symbol detail rotation. Without its own name, reference's
        # 21:30 walk bumped ``ticker_sync`` after the 03:30 fire, and a stopped
        # ticker-details read on plan in the Console (TD-175).
        if p.get("mode") == "detail":
            return "ticker-details"
        return None
    if k == "option_contract":
        return _policed_slot_of_option_contract(p)
    if k in ("dividends_market", "splits_market"):
        return "corporate"
    if k == "financials":
        return "fundamentals-rotate"
    return None


#: Every slot ``policed_slot_for_job`` can name: the slots whose own jobs can be
#: told apart from a sibling's by shape. The Console's schedule adherence credits
#: these only with their own jobs and their ``slot:<id>`` row (TD-167, TD-175).
#: The doctor polices all of them but ticker-details, which has no staleness
#: contract: naming it here changes what the Console credits, not what the doctor
#: warns about (``tests/test_slot_freshness.py`` holds that difference).
SHAPE_NAMED_SLOTS: frozenset[str] = frozenset(
    {
        "calendar",
        "reference",
        "option-refresh",
        "corporate",
        "fundamentals-rotate",
        "ticker-details",
    }
)

#: The payload fields ``policed_slot_for_job`` reads, as columns over
#: ``ops_jobs.job_ingest``. A reader groups by these and hands each group to
#: ``payload_from_shape`` — one place, so the SQL cannot drift from the function.
JOB_SHAPE_COLUMNS_SQL = """payload->>'mode' AS mode,
       payload->>'expired' AS expired,
       COALESCE(payload->>'expiration_date', payload->>'expiration_date_gte',
                payload->>'expiration_date_lte') IS NOT NULL AS dated"""


def payload_from_shape(mode: Any, expired: Any, dated: Any) -> dict[str, Any]:
    """The payload ``policed_slot_for_job`` needs, rebuilt from ``JOB_SHAPE_COLUMNS_SQL``."""
    payload: dict[str, Any] = {}
    if mode is not None:
        payload["mode"] = mode
    if expired is not None:
        payload["expired"] = {"true": True, "false": False}.get(str(expired), expired)
    if dated:
        payload["expiration_date_gte"] = "dated"
    return payload


def rows_written_from_result(result: Mapping[str, Any] | None) -> int:
    """Extract rows_written from a handler result dict (default 0)."""
    if not result:
        return 0
    raw = result.get("rows_written")
    if raw is None:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def extra_freshness_from_result(result: Mapping[str, Any] | None) -> dict[str, int]:
    """Other dimensions a handler filled from the same fetch (``freshness_extra``)."""
    if not result:
        return {}
    raw = result.get("freshness_extra")
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, int] = {}
    for dim, rows in raw.items():
        key = str(dim or "").strip()
        if not key:
            continue
        try:
            out[key] = max(0, int(rows))
        except (TypeError, ValueError):
            out[key] = 0
    return out


def update_freshness(
    conn: Any,
    dimension: str,
    rows_written: int,
    *,
    status: str = "ok",
) -> None:
    """UPSERT ``ops_jobs.ingest_freshness`` after a successful job.

    Caller should catch exceptions — freshness must not fail the job.
    """
    dim = str(dimension or "").strip()
    if not dim:
        raise ValueError("dimension is required")
    rows = max(0, int(rows_written))
    status_s = str(status or "ok").strip() or "ok"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops_jobs.ingest_freshness
                (dimension, last_run_at, rows_written, status, updated_at)
            VALUES (%s, now(), %s, %s, now())
            ON CONFLICT (dimension) DO UPDATE SET
                last_run_at = now(),
                rows_written = EXCLUDED.rows_written,
                status = EXCLUDED.status,
                updated_at = now()
            """,
            (dim, rows, status_s),
        )
    if hasattr(conn, "commit"):
        conn.commit()
