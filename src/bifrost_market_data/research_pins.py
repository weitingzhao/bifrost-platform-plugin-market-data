"""The contracts Research asks this plugin to keep — read-only (R9 C3-P7 / P8).

``research.option_pinned_contract`` lists the option contracts the Owner holds or
closed recently. They are the ones whose history has to survive: a leg closed last
month is the evidence for the post-mortem, and once its contract expires the
catalogue drops it and the 90-session snapshot trim takes the rest.

This plugin only reads that table, never writes it (R19), and an unreadable table
is an empty list with a warning — never a stalled slot and never a trim that
believes nothing needs keeping.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

#: Still pinned as of today. Research sets ``pin_until``; a contract that falls out
#: of it goes back to the ordinary retention window.
PINNED_QUERY = """
SELECT option_ticker, underlying
FROM research.option_pinned_contract
WHERE pin_until >= CURRENT_DATE
ORDER BY option_ticker
""".strip()

#: Every underlying with a pin, expired contracts included — the scope the expired
#: catalogue backfill has to cover for those pins to resolve to a ticker at all.
PINNED_UNDERLYING_QUERY = """
SELECT DISTINCT underlying
FROM research.option_pinned_contract
ORDER BY 1
""".strip()


def _rows(conn: Any, sql: str) -> list[Any]:
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall() or [])
    except Exception as exc:  # noqa: BLE001 — a missing table must not stop the slot
        logger.warning("research.option_pinned_contract unreadable: %s", exc)
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        return []


def _first_two(row: Any) -> tuple[str, str]:
    if isinstance(row, Mapping):
        return str(row.get("option_ticker") or ""), str(row.get("underlying") or "")
    values = list(row)
    return str(values[0] or ""), str(values[1] or "") if len(values) > 1 else ""


def load_pinned_contracts(conn: Any) -> list[tuple[str, str]]:
    """``[(option_ticker, underlying)]`` still pinned today; ``[]`` when unreadable."""
    out: list[tuple[str, str]] = []
    for row in _rows(conn, PINNED_QUERY):
        ticker, underlying = _first_two(row)
        if ticker:
            out.append((ticker.strip().upper(), underlying.strip().upper()))
    return out


def load_pinned_underlyings(conn: Any) -> set[str]:
    """Underlyings with any pin, expired or not; empty set when unreadable."""
    out: set[str] = set()
    for row in _rows(conn, PINNED_UNDERLYING_QUERY):
        value = row.get("underlying") if isinstance(row, Mapping) else list(row)[0]
        if value:
            out.add(str(value).strip().upper())
    return out
