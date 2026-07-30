"""Historical backfill enqueue helpers (used by scripts/backfill.py)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from bifrost_market_data.scheduler.enqueue import insert_job

# Kinds that require a date range and are chunked by day/range.
# stock_daily / stock_minute: symbols = equity tickers
# option_daily / option_minute: symbols = option_tickers (O:...)
_DATE_RANGE_KINDS = frozenset(
    {"stock_daily", "option_daily", "stock_daily_grouped", "stock_minute", "option_minute"}
)

# Kinds that enqueue one job per symbol (no date range).
_PER_SYMBOL_KINDS = frozenset({"financials", "option_snapshot", "option_contract"})

# Single-job kinds (no symbols / dates required).
_SINGLE_JOB_KINDS = frozenset({"ticker_sync"})

SUPPORTED_BACKFILL_KINDS = _DATE_RANGE_KINDS | _PER_SYMBOL_KINDS | _SINGLE_JOB_KINDS


def date_chunks(from_date: date, to_date: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split [from_date, to_date] into inclusive chunks of at most ``chunk_days`` days."""
    if to_date < from_date:
        raise ValueError(f"to_date {to_date} < from_date {from_date}")
    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")
    chunks: list[tuple[date, date]] = []
    cur = from_date
    while cur <= to_date:
        end = min(cur + timedelta(days=chunk_days - 1), to_date)
        chunks.append((cur, end))
        cur = end + timedelta(days=1)
    return chunks


def _record(
    jobs: list[dict[str, Any]],
    *,
    kind: str,
    payload: dict[str, Any],
    job_id: int | None,
) -> tuple[int, int]:
    if job_id is None:
        jobs.append({"kind": kind, "payload": payload, "deduped": True})
        return 0, 1
    jobs.append({"kind": kind, "payload": payload, "id": job_id, "deduped": False})
    return 1, 0


def enqueue_backfill(
    conn: Any,
    *,
    kind: str,
    symbols: list[str] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    chunk_days: int | None = None,
    priority: int = 0,
) -> dict[str, Any]:
    """Enqueue backfill jobs for supported kinds. Returns summary.

    Minute kinds (``stock_minute`` / ``option_minute``) default to ``chunk_days=30``;
    other date-range kinds default to ``365``.
    """
    kind_s = str(kind).strip()
    if kind_s not in SUPPORTED_BACKFILL_KINDS:
        raise ValueError(
            f"unsupported kind for backfill: {kind_s!r} "
            f"(expected one of {sorted(SUPPORTED_BACKFILL_KINDS)})"
        )

    if chunk_days is None:
        chunk_days = 30 if kind_s in ("stock_minute", "option_minute") else 365

    syms = [str(s).strip().upper() for s in (symbols or []) if str(s).strip()]
    enqueued = 0
    deduped = 0
    jobs: list[dict[str, Any]] = []
    chunks: list[tuple[date, date]] = []

    if kind_s in _SINGLE_JOB_KINDS:
        payload: dict[str, Any] = {}
        job_id = insert_job(conn, kind=kind_s, payload=payload, priority=priority)
        e, d = _record(jobs, kind=kind_s, payload=payload, job_id=job_id)
        enqueued += e
        deduped += d
        return {
            "kind": kind_s,
            "symbols": 0,
            "chunks": 0,
            "enqueued": enqueued,
            "deduped": deduped,
            "jobs": jobs,
        }

    if kind_s in _PER_SYMBOL_KINDS:
        if not syms:
            raise ValueError(f"symbols required for kind={kind_s!r}")
        for sym in syms:
            if kind_s == "financials":
                payload = {"symbol": sym}
            elif kind_s == "option_snapshot":
                payload = {"underlying": sym}
            else:  # option_contract
                payload = {"underlying": sym, "expired": False}
            job_id = insert_job(conn, kind=kind_s, payload=payload, priority=priority)
            e, d = _record(jobs, kind=kind_s, payload=payload, job_id=job_id)
            enqueued += e
            deduped += d
        return {
            "kind": kind_s,
            "symbols": len(syms),
            "chunks": 0,
            "enqueued": enqueued,
            "deduped": deduped,
            "jobs": jobs,
        }

    # Date-range kinds
    if from_date is None or to_date is None:
        raise ValueError(f"from_date and to_date required for kind={kind_s!r}")

    if kind_s == "stock_daily_grouped":
        # One job per weekday (grouped daily is single-day API; skip weekends).
        chunks = date_chunks(from_date, to_date, 1)
        weekday_chunks: list[tuple[date, date]] = []
        for start, _end in chunks:
            if start.weekday() >= 5:
                continue  # skip weekends; holidays still pass but Polygon returns empty
            weekday_chunks.append((start, _end))
            day_s = start.isoformat()
            payload = {"from": day_s, "to": day_s, "market": "stocks"}
            job_id = insert_job(conn, kind=kind_s, payload=payload, priority=priority)
            e, d = _record(jobs, kind=kind_s, payload=payload, job_id=job_id)
            enqueued += e
            deduped += d
        return {
            "kind": kind_s,
            "symbols": 0,
            "chunks": len(weekday_chunks),
            "enqueued": enqueued,
            "deduped": deduped,
            "jobs": jobs,
        }

    if not syms:
        raise ValueError(f"symbols required for kind={kind_s!r}")

    chunks = date_chunks(from_date, to_date, chunk_days)
    for sym in syms:
        for start, end in chunks:
            if kind_s == "stock_daily":
                payload = {"symbol": sym, "from": start.isoformat(), "to": end.isoformat()}
            elif kind_s == "stock_minute":
                payload = {
                    "symbol": sym,
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                    "multiplier": 1,
                    "timespan": "minute",
                }
            elif kind_s == "option_minute":
                payload = {
                    "option_ticker": sym,
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                    "multiplier": 1,
                    "timespan": "minute",
                }
            else:
                # option_daily
                payload = {
                    "option_ticker": sym,
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                }
            job_id = insert_job(conn, kind=kind_s, payload=payload, priority=priority)
            e, d = _record(jobs, kind=kind_s, payload=payload, job_id=job_id)
            enqueued += e
            deduped += d

    return {
        "kind": kind_s,
        "symbols": len(syms),
        "chunks": len(chunks),
        "enqueued": enqueued,
        "deduped": deduped,
        "jobs": jobs,
    }
