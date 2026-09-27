"""Move option rows filed under a symbol the vendor has renamed.

The chain handlers stamp the requested underlying, so every session we asked
for ``SATS`` wrote ``O:ECHO…`` contracts under ``SATS``. Because
``option_snapshot`` is keyed ``(option_ticker, snapshot_ts)``, one company's
chain ended up split across two labels, and a reader asking for the live symbol
got whichever half the last job wrote. Measured 2026-09-26: ECHO's 09-25 chain
was 362 rows under ``ECHO`` and 414 under ``SATS``, with every expiry past
2026-12-18 on the dead side, and ``features.option_metric_max_pain_daily`` held
two answers for the 2026-10-16 expiry — strike 99 over 2,592 open interest
against strike 90 over 59,986.

``symbol_rename`` stops new rows arriving that way. This moves the ones already
written, so a recomputation reads one chain instead of two halves.

Rows moved, measured 2026-09-26 (24,238 in total, and none in
``option_minute``):

======================  ====  ===  =====
table                   SATS  ISSC  EQR
======================  ====  ===  =====
option_snapshot         8012  1672   360
option_open_interest    9228  1672   798*
option_daily            1562   149     5
option_contract          188   128     0
======================  ====  ===  =====

\\* 438 of the EQR rows are rooted ``VMRK1`` and 26 of SATS's ``ECHO1``. Those
are OCC adjusted roots, not tickers, so this repair leaves them alone: mapping
an adjusted root onto its catalogue underlying is ``adjusted_root_repair``'s
direction, and that module is the one that knows about the per-contract guard.
It runs after this one for that reason — once the plain roots are single-labelled
its ambiguity check stops seeing ``ECHO`` under two underlyings.

Idempotent by construction: the predicate requires the label to differ from the
root, which the update itself makes false.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol, Sequence

logger = logging.getLogger(__name__)

#: Every table that carries an ``underlying`` beside an ``option_ticker``.
#: ``option_contract`` is included even though its label self-heals — last
#: writer wins on a primary key of ``option_ticker`` — because "eventually, if
#: the rotation reaches it" is not a state the bar ladder can read from.
RENAME_TABLES: tuple[str, ...] = (
    "option_snapshot",
    "option_open_interest",
    "option_daily",
    "option_minute",
    "option_contract",
)

#: ``O:`` + root + 6 date + 1 right + 8 strike.
_TICKER_TAIL = 15
_TICKER_PREFIX = 2

#: Renames the reference table can prove: one registrant (CIK) and one
#: instrument (composite FIGI), the old symbol inactive and the new one active.
#: ``HAVING count(*) = 1`` drops a dead symbol that matches two live ones —
#: ambiguity is a reason to do nothing, not to pick.
_PAIRS_FROM_TICKER = """
SELECT dead.symbol, min(alive.symbol)
FROM raw_market.ticker dead
JOIN raw_market.ticker alive
  ON alive.cik = dead.cik
 AND alive.composite_figi = dead.composite_figi
 AND alive.symbol <> dead.symbol
WHERE NOT dead.active AND alive.active
  AND dead.cik IS NOT NULL AND dead.cik <> ''
  AND dead.composite_figi IS NOT NULL AND dead.composite_figi <> ''
GROUP BY dead.symbol
HAVING count(*) = 1
"""

#: Renames only the catalogue can show, because the old symbol has no reference
#: row to link from: the walk inserts what the vendor lists as active, and SATS
#: was delisted before the table was first written. A root that is a live
#: ticker over a label that is not one is the same fact by another route.
_PAIRS_FROM_CATALOGUE = f"""
WITH r AS (
    SELECT DISTINCT underlying,
           substring(option_ticker FROM {_TICKER_PREFIX + 1}
                     FOR length(option_ticker) - {_TICKER_TAIL + _TICKER_PREFIX}) AS root
    FROM raw_market.option_contract
    WHERE length(option_ticker) > {_TICKER_TAIL + _TICKER_PREFIX}
)
SELECT r.underlying, r.root
FROM r
WHERE r.root <> r.underlying
  AND EXISTS (SELECT 1 FROM raw_market.ticker t WHERE t.symbol = r.root AND t.active)
  AND NOT EXISTS (SELECT 1 FROM raw_market.ticker t WHERE t.symbol = r.underlying AND t.active)
"""

#: One statement per (table, pair). Equality on ``underlying`` so the label
#: index carries it, and the root check so only contracts that actually spell
#: the successor move.
_MOVE = """
UPDATE raw_market.{table}
SET underlying = %s
WHERE underlying = %s
  AND length(option_ticker) > {span}
  AND substring(option_ticker FROM {start} FOR length(option_ticker) - {span}) = %s
"""

#: How long one deploy may spend on this. The whole backlog measured 24,238
#: rows, so the budget is a guard against a surprise rather than a schedule.
DEFAULT_BUDGET_SEC = 30.0


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


def rename_pairs(cur: _Cursor) -> list[tuple[str, str]]:
    """``(dead symbol, live symbol)`` from both routes, deduplicated."""
    pairs: set[tuple[str, str]] = set()
    for sql in (_PAIRS_FROM_TICKER, _PAIRS_FROM_CATALOGUE):
        cur.execute(sql)
        for row in _rows(cur):
            if len(row) < 2 or not row[0] or not row[1]:
                continue
            dead, alive = str(row[0]).strip().upper(), str(row[1]).strip().upper()
            if dead and alive and dead != alive:
                pairs.add((dead, alive))
    return sorted(pairs)


def repair_renamed_labels(
    conn: Any,
    *,
    budget_sec: float = DEFAULT_BUDGET_SEC,
    tables: Sequence[str] = RENAME_TABLES,
) -> dict[str, int]:
    """Move every option row whose label is a symbol its root was renamed from.

    Commits per (table, pair) so a budget that runs out keeps what it moved and
    the next deploy continues. Returns rows moved per table, empty when there
    was nothing to do.
    """
    started = time.monotonic()
    with conn.cursor() as cur:
        pairs = rename_pairs(cur)
    conn.commit()
    if not pairs:
        return {}
    moved: dict[str, int] = {}
    for table in tables:
        for dead, alive in pairs:
            if time.monotonic() - started > budget_sec:
                logger.info(
                    "rename label repair out of budget after %s; %s pair(s) left for the next deploy",
                    moved or "no rows",
                    len(pairs),
                )
                return moved
            sql = _MOVE.format(
                table=table,
                span=_TICKER_TAIL + _TICKER_PREFIX,
                start=_TICKER_PREFIX + 1,
            )
            with conn.cursor() as cur:
                cur.execute(sql, (alive, dead, alive))
                n = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
            if n > 0:
                moved[table] = moved.get(table, 0) + n
                logger.info("moved %s %s rows from %s to %s", n, table, dead, alive)
    return moved


__all__ = ["DEFAULT_BUDGET_SEC", "RENAME_TABLES", "rename_pairs", "repair_renamed_labels"]
