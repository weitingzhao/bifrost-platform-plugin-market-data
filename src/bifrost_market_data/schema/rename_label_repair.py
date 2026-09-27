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

#: A rename hands the price series over within days: the retired symbol prints
#: its last bar and the successor picks up on the next session. Anything further
#: apart shares a CIK and a FIGI for some other reason -- a registrant reusing an
#: identifier after a gap, or vendor error -- and must not move option rows.
#:
#: One weekend plus a holiday. Measured 2026-09-27 over every pair the CIK/FIGI
#: test finds: sixteen abut at one day and four at three, and **none** is further
#: out, so this bounds what the rule can do in future rather than filtering
#: anything today. It was added on the expectation that it would drop pairs like
#: ADIGW/ADIG and BBBY/NXH; it does not, because those are real symbol changes --
#: ADIG carries the same company name as ADIGW and is instrument_type CS, not a
#: warrant. The guard is honest as an invariant and idle as a filter.
_RENAME_MAX_GAP_DAYS = 5

#: Renames the reference table can prove: one registrant (CIK), one instrument
#: (composite FIGI), the old symbol inactive and the new one active, and the two
#: listings abutting in time. ``HAVING count(*) = 1`` drops a dead symbol that
#: still matches two live ones — ambiguity is a reason to do nothing, not to pick.
#: The adjacency test runs before that count, so evidence narrows the ambiguity
#: instead of the count refusing a pair that evidence could have settled.
#:
#: ``next_bar`` is the successor's first bar **after** the retired symbol's last,
#: not its first bar outright, so a successor that has held the symbol before is
#: still judged on the handover and not on its earlier life. No pair on this route
#: needs that today -- the case that does, SATS → ECHO, has no ticker row and so
#: arrives through the catalogue route below.
_PAIRS_FROM_TICKER = f"""
SELECT dead.symbol, min(alive.symbol)
FROM raw_market.ticker dead
JOIN raw_market.ticker alive
  ON alive.cik = dead.cik
 AND alive.composite_figi = dead.composite_figi
 AND alive.symbol <> dead.symbol
CROSS JOIN LATERAL (
  SELECT max(bar_date) AS last_bar
  FROM raw_market.stock_daily WHERE symbol = dead.symbol
) dl
CROSS JOIN LATERAL (
  SELECT min(bar_date) AS next_bar
  FROM raw_market.stock_daily
  WHERE symbol = alive.symbol AND bar_date > dl.last_bar
) an
WHERE NOT dead.active AND alive.active
  AND dead.cik IS NOT NULL AND dead.cik <> ''
  AND dead.composite_figi IS NOT NULL AND dead.composite_figi <> ''
  AND dl.last_bar IS NOT NULL
  AND an.next_bar IS NOT NULL
  AND an.next_bar - dl.last_bar <= {_RENAME_MAX_GAP_DAYS}
GROUP BY dead.symbol
HAVING count(*) = 1
"""

#: Renames only the catalogue can show, because the old symbol has no reference
#: row to link from: the walk inserts what the vendor lists as active, and SATS
#: was delisted before the table was first written. A root that is a live
#: ticker over a label that is not one is the same fact by another route.
#:
#: This route carries the same adjacency test, and it is the one that needs it.
#: With no CIK to link through, a live root over a dead label is all the evidence
#: there is, and a reused ticker looks exactly like a rename -- which is not
#: hypothetical here: ``stock_daily`` holds ECHO bars from 2021-09-09 to
#: 2021-11-22 for Echo Global Logistics, nothing until EchoStar took the symbol on
#: 2026-06-24, the session after SATS's last bar. So this is where the successor's
#: first bar **after** the dead symbol's last matters: read as its first bar
#: outright it is 2021-09-09, a 4.6-year gap, and the one pair this repair exists
#: for would be rejected.
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
  AND EXISTS (
    SELECT 1
    FROM (
      SELECT max(bar_date) AS last_bar
      FROM raw_market.stock_daily WHERE symbol = r.underlying
    ) dl
    CROSS JOIN LATERAL (
      SELECT min(bar_date) AS next_bar
      FROM raw_market.stock_daily
      WHERE symbol = r.root AND bar_date > dl.last_bar
    ) an
    WHERE dl.last_bar IS NOT NULL
      AND an.next_bar IS NOT NULL
      AND an.next_bar - dl.last_bar <= {_RENAME_MAX_GAP_DAYS}
  )
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

#: The other half of a rename: the history the company wrote under its old
#: symbol. OCC renames option contracts when the underlying renames, so ECHO's
#: ``option_daily`` begins at the handover and the two years before it sit under
#: ``O:SATS…`` tickers labelled SATS. Those rows are not mislabelled -- the label
#: agrees with the root, which is why ``_MOVE`` correctly leaves them -- but the
#: same company's series is still torn, on the time axis instead of within a
#: session, and Research reads ``option_daily`` by ``(bar_date, underlying)``.
#: Asking ECHO for a date before the handover returns nothing and says nothing.
#:
#: ⚠️ **This statement is not self-limiting and ``_MOVE`` is.** ``_MOVE`` only
#: touches rows whose ticker spells the *successor*, which is why eighteen of the
#: twenty-one pairs moved nothing: the root check does the work. Here the root
#: check matches the ordinary case, so every row under the dead label qualifies
#: and the only limits are the pair discovery's guards and the date bound below.
#: Measured 2026-09-27 that bounds it to 39,449 rows -- option_daily SATS 32,263,
#: EQR 5,571, ISSC 1,615 -- because the other eighteen dead symbols hold no option
#: rows at all in any table. That is the data's doing, not the statement's, so the
#: adjacency test and ``HAVING count(*) = 1`` are load-bearing here in a way they
#: were not for ``_MOVE``.
#:
#: The date bound is the one structural limit: a row dated after the handover is
#: not pre-rename history, it is a writer still using the old label, which is a
#: different fault and must not be swept up silently. Measured, every one of the
#: 39,449 falls on or before its symbol's last stock bar -- SATS 2026-06-23,
#: EQR and ISSC 2026-08-17 -- so today it costs nothing and it is the guard that
#: keeps this honest tomorrow. A dead symbol with no stock bars yields NULL and
#: therefore moves nothing: no dates is no evidence.
#:
#: ``option_contract`` is absent deliberately. It stamps no date, so there is
#: nothing to bound by, and it holds no such row today.
_HISTORY_DATE_COLUMN: dict[str, str] = {
    "option_daily": "bar_date",
    "option_open_interest": "trade_date",
    "option_snapshot": "date(snapshot_ts AT TIME ZONE 'America/New_York')",
    "option_minute": "date(bar_time AT TIME ZONE 'America/New_York')",
}

_MOVE_HISTORY = """
UPDATE raw_market.{table}
SET underlying = %s
WHERE underlying = %s
  AND length(option_ticker) > {span}
  AND substring(option_ticker FROM {start} FOR length(option_ticker) - {span}) = %s
  AND {date_col} <= (
    SELECT max(bar_date) FROM raw_market.stock_daily WHERE symbol = %s
  )
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
            # The same company's rows from before the handover, still under the old
            # symbol because that is what the contracts were called then. Separate
            # statement and separate commit: it is a wider reach than the one above
            # and a timeout on it must not cost what that one moved.
            date_col = _HISTORY_DATE_COLUMN.get(table)
            if date_col is None:
                continue
            hist_sql = _MOVE_HISTORY.format(
                table=table,
                span=_TICKER_TAIL + _TICKER_PREFIX,
                start=_TICKER_PREFIX + 1,
                date_col=date_col,
            )
            with conn.cursor() as cur:
                cur.execute(hist_sql, (alive, dead, dead, dead))
                h = int(getattr(cur, "rowcount", 0) or 0)
            conn.commit()
            if h > 0:
                moved[table] = moved.get(table, 0) + h
                logger.info(
                    "moved %s %s pre-rename rows from %s to %s (rooted %s)",
                    h,
                    table,
                    dead,
                    alive,
                    dead,
                )
    return moved


__all__ = [
    "DEFAULT_BUDGET_SEC",
    "RENAME_TABLES",
    "_HISTORY_DATE_COLUMN",
    "_RENAME_MAX_GAP_DAYS",
    "rename_pairs",
    "repair_renamed_labels",
]
