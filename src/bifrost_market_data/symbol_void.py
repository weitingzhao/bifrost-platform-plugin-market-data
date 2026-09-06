"""Per-symbol vendor voids — names the vendor has no data for.

The fundamentals rotate used to put every symbol without a statement first in
line, every day, including warrants, ETFs and delisted names the vendor will
never answer for (914 of 5,378 on 2026-09-06, all confirmed empty). A void row
is written when a fetch returns nothing and cleared when it returns rows; the
rotate skips voids checked within ``max_age_days`` so they are re-tried once a
month, not once a day.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)


def record_symbol_void(conn: Any, symbol: str, data_type: str, *, note: str | None = None) -> None:
    """Best-effort: the vendor answered with nothing for ``symbol`` / ``data_type``."""
    sym = str(symbol or "").strip().upper()
    if not sym:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops_jobs.symbol_source_void (symbol, data_type, checks, note)
                VALUES (%s, %s, 1, %s)
                ON CONFLICT (symbol, data_type) DO UPDATE SET
                    checks = ops_jobs.symbol_source_void.checks + 1,
                    last_checked = now(),
                    note = COALESCE(EXCLUDED.note, ops_jobs.symbol_source_void.note)
                """,
                (sym, str(data_type), note),
            )
        if hasattr(conn, "commit"):
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — a void note must not fail the job
        logger.warning("symbol void record failed for %s/%s: %s", sym, data_type, exc)
        try:
            conn.rollback()
        except Exception:
            pass


def clear_symbol_void(conn: Any, symbol: str, data_type: str) -> None:
    """The vendor answered after all — forget the void."""
    sym = str(symbol or "").strip().upper()
    if not sym:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ops_jobs.symbol_source_void WHERE symbol = %s AND data_type = %s",
                (sym, str(data_type)),
            )
        if hasattr(conn, "commit"):
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("symbol void clear failed for %s/%s: %s", sym, data_type, exc)
        try:
            conn.rollback()
        except Exception:
            pass


def load_voided_symbols(conn: Any, data_type: str, *, max_age_days: int = 30) -> set[str]:
    """Symbols to skip: voided for ``data_type`` and checked within ``max_age_days``."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol FROM ops_jobs.symbol_source_void
                WHERE data_type = %s
                  AND last_checked >= now() - make_interval(days => %s)
                """,
                (str(data_type), int(max_age_days)),
            )
            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    except Exception as exc:  # noqa: BLE001 — no void table means no skips
        logger.warning("symbol void lookup failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return set()
    out: set[str] = set()
    for row in rows or []:
        sym = row.get("symbol") if isinstance(row, Mapping) else (row[0] if row else None)
        if sym:
            out.add(str(sym).strip().upper())
    return out
