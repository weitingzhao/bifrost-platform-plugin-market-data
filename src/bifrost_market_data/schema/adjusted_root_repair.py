"""Rewrite option bar rows that were filed under an adjusted root.

OCC appends a numeric suffix to an option root after a split, merger or
special dividend, so an adjusted contract reads ``O:BDX1260918C00085000`` and
still belongs to BDX. ``option_contract`` records that — the catalogue lists
342 of BDX's 842 contracts under the adjusted root with ``underlying = 'BDX'``
— but ``option_daily`` and ``option_minute`` took the underlying from
``parse_option_ticker``, which can only report what the ticker spells.

The rows could not exist before 0.21.0, because the parser rejected these
tickers outright and the jobs failed. Once it accepted them, ``BDX1`` appeared
in ``option_daily`` as a symbol of its own: the depth axis counted 581
underlyings where ``option_snapshot``, which has always stored the request's
underlying, counted 570, and a downstream ``WHERE underlying = 'BDX'`` would
miss the adjusted series entirely.

Idempotent by construction: once rewritten, no row matches again.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, Sequence

logger = logging.getLogger(__name__)

#: Tables whose ``underlying`` came from the ticker rather than the catalogue.
#: option_snapshot and option_open_interest are absent on purpose — they store
#: the request's underlying and were never wrong.
BAR_TABLES: tuple[str, ...] = ("option_daily", "option_minute")

#: ``O:`` + root + 6 date + 1 right + 8 strike. The root is whatever is left,
#: so the tail is a fixed 15 characters and the prefix is 2.
_TICKER_TAIL = 15
_TICKER_PREFIX = 2

#: Root/underlying pairs the catalogue disagrees on. A scan of option_contract
#: (734k rows) rather than of the bar tables (tens of millions).
_MISMATCHED_ROOTS = f"""
SELECT DISTINCT
       substring(option_ticker FROM {_TICKER_PREFIX + 1}
                 FOR length(option_ticker) - {_TICKER_TAIL + _TICKER_PREFIX}) AS root,
       underlying
FROM raw_market.option_contract
WHERE length(option_ticker) > {_TICKER_TAIL + _TICKER_PREFIX}
  AND substring(option_ticker FROM {_TICKER_PREFIX + 1}
                FOR length(option_ticker) - {_TICKER_TAIL + _TICKER_PREFIX}) <> underlying
""".strip()


class _Cursor(Protocol):
    rowcount: int

    def execute(self, query: str, params: Any = None) -> Any: ...

    def fetchall(self) -> Sequence[Any]: ...


def _rows(cur: _Cursor) -> list[tuple[Any, ...]]:
    fetched = cur.fetchall() if hasattr(cur, "fetchall") else []
    out: list[tuple[Any, ...]] = []
    for r in fetched or []:
        out.append(tuple(r.values()) if hasattr(r, "values") else tuple(r))
    return out


def mismatched_roots(cur: _Cursor) -> list[tuple[str, str]]:
    """``(root, canonical underlying)`` for every adjusted contract family."""
    cur.execute(_MISMATCHED_ROOTS)
    pairs: list[tuple[str, str]] = []
    for row in _rows(cur):
        if len(row) < 2 or not row[0] or not row[1]:
            continue
        root, und = str(row[0]).strip().upper(), str(row[1]).strip().upper()
        if root and und and root != und:
            pairs.append((root, und))
    return sorted(set(pairs))


def repair_adjusted_underlyings(
    cur: _Cursor,
    *,
    tables: Sequence[str] = BAR_TABLES,
    statement_timeout: str = "120s",
) -> dict[str, int]:
    """Rewrite each mismatched root to the underlying the catalogue names.

    One statement per (root, table) so each rides
    ``(underlying, bar_date)`` instead of scanning, and so a slow family cannot
    hold a transaction open across all of them. The EXISTS is not decoration:
    it rewrites a row only when the catalogue confirms *that* contract belongs
    to *that* underlying, so a root that is genuinely some other instrument's
    symbol is left alone.
    """
    pairs = mismatched_roots(cur)
    if not pairs:
        return {t: 0 for t in tables}

    repaired: dict[str, int] = {t: 0 for t in tables}
    cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
    for table in tables:
        for root, canonical in pairs:
            cur.execute(
                f"""
                UPDATE raw_market.{table} d
                SET underlying = %s
                WHERE d.underlying = %s
                  AND EXISTS (
                        SELECT 1 FROM raw_market.option_contract c
                        WHERE c.option_ticker = d.option_ticker
                          AND c.underlying = %s
                  )
                """,
                (canonical, root, canonical),
            )
            n = int(getattr(cur, "rowcount", 0) or 0)
            if n > 0:
                repaired[table] += n
                logger.info("repaired %s rows in %s: %s -> %s", n, table, root, canonical)
    return repaired


__all__ = [
    "BAR_TABLES",
    "mismatched_roots",
    "repair_adjusted_underlyings",
]
