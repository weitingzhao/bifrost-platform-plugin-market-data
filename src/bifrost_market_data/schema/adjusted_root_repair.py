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

Two things this file got wrong the first time, both recorded because the second
hid the first. It ran every rewrite inside the migration's one transaction, so a
timeout rolled the whole run back — and the run after that reported "nothing
written", which reads exactly like "nothing left to do". The backlog was
2,964,147 rows the whole time. And the enqueuer that created them,
``option_backfill``, was never passing the catalogue's underlying at all; fixing
the rewrite without fixing that would have been a treadmill.
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


#: Roots the catalogue files under two different underlyings. Measured
#: 2026-09-10: ECHO → {ECHO, SATS} and VMRK → {EQR, VMRK}, both mid-rename. A
#: root like that cannot be rewritten from the pair alone — which contract it
#: is decides the answer, and only the per-contract EXISTS knows.
_AMBIGUOUS_ROOTS = """
WITH r AS (
    SELECT substring(option_ticker FROM 3 FOR length(option_ticker) - 17) AS root,
           underlying
    FROM raw_market.option_contract
    WHERE length(option_ticker) > 17
)
SELECT root FROM r GROUP BY root HAVING count(DISTINCT underlying) > 1
""".strip()

#: Roots that are also a real equity symbol. ECHO, VMRK and IA all are, and a
#: root that names a live instrument is the one case where rewriting on the
#: catalogue's mapping could clobber something real.
_ROOTS_THAT_ARE_SYMBOLS = "SELECT symbol FROM raw_market.ticker WHERE symbol = ANY(%s)"

#: Rows per statement when rewriting without the per-contract guard. SPXW alone
#: is 2.8M rows; one UPDATE for it would not finish inside any statement budget
#: worth setting, so it goes in bites that each commit.
CHUNK_ROWS = 50_000


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


#: How long one deploy may spend chipping at the backlog. The repair rides the
#: schema-migration Job, and a deploy that waits ten minutes on a data fix is a
#: deploy nobody runs.
DEFAULT_BUDGET_SEC = 90.0


def unguarded_pairs(cur: _Cursor, pairs: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Pairs safe to rewrite from the catalogue's mapping, without asking per row.

    Two independent checks, both per pair rather than per contract:

    * the catalogue maps the root to exactly one underlying, and
    * the root is not itself an equity symbol.

    That is what the per-contract EXISTS was buying, and measurement says where
    it is actually needed. Of the five roots holding mis-filed rows on
    2026-09-10, ``SPXW`` (2,799,555 rows — the weekly SPX option root, not an
    adjusted contract at all) and ``BRKB`` (158,123 — OCC drops the dot from
    BRK.B) pass both. ``ECHO``, ``VMRK`` and ``IA`` fail: each is a live symbol,
    and two of them are mid-rename in the catalogue itself. They keep the guard
    and their 6,476 rows stay as they are.

    A first pass looked for roots shaped ``canonical + digit`` — the OCC
    adjusted-contract convention this module was written for. Twenty-six pairs
    match it and they hold **no rows at all**; the backlog is three other
    phenomena wearing the same symptom. Recorded because the rule sounded right
    and covered nothing.
    """
    if not pairs:
        return []
    cur.execute(_AMBIGUOUS_ROOTS)
    ambiguous = {str(r[0]).strip().upper() for r in _rows(cur) if r and r[0]}
    roots = sorted({r for r, _ in pairs})
    cur.execute(_ROOTS_THAT_ARE_SYMBOLS, (roots,))
    real_symbols = {str(r[0]).strip().upper() for r in _rows(cur) if r and r[0]}
    return [
        (root, canonical)
        for root, canonical in pairs
        if root not in ambiguous and root not in real_symbols
    ]


def repair_adjusted_underlyings(
    conn: Any,
    *,
    tables: Sequence[str] = BAR_TABLES,
    statement_timeout: str = "120s",
    budget_sec: float = DEFAULT_BUDGET_SEC,
    now: Any = None,
) -> dict[str, int]:
    """Rewrite each mismatched root to the underlying the catalogue names.

    **Commits per root, and that is the point.** The first version ran all
    ``len(pairs) × len(tables)`` statements inside the migration's single
    transaction, so a timeout on the fortieth rolled back the thirty-nine before
    it. Measured 2026-09-10 that was not an edge case: 2,964,147 option_daily
    rows are still filed under an adjusted root, across 36 root families and two
    dozen monthly partitions, and no single pass rewrites that inside any
    sensible budget while the queue is ingesting. Rolling back on the way out
    meant the repair could never finish — and made it look finished, because the
    next run reported "nothing written".

    Committing each root means a statement that times out costs that one root
    rather than the run.

    **It does not reach the backlog, and measurement said so.** Of those
    2,964,147 rows, 135 have a contract the catalogue still holds; the other
    2,964,019 belong to contracts that expired months ago, and
    ``option_contract`` starts at expiry 2026-08-05. The EXISTS below is what
    makes this repair safe and is also what puts those rows out of reach — it
    has already done everything it can. Whether to relax it for the 35 roots
    that are ``canonical + digit`` (the OCC adjusted convention, which no real
    equity symbol matches) is an Owner call, because it is 2.9M irreversible
    rewrites. The 36th pair, ``BRKB`` → ``BRK.B``, is a punctuation difference
    rather than an adjusted root and would need judging separately.

    ``budget_sec`` bounds the whole thing, because this rides a deploy.

    One statement per (root, table) so each rides ``(underlying, bar_date)``
    instead of scanning. The EXISTS is not decoration: it rewrites a row only
    when the catalogue confirms *that* contract belongs to *that* underlying,
    so a root that is genuinely some other instrument's symbol is left alone.
    """
    import time

    clock = now or time.monotonic
    deadline = clock() + float(budget_sec)

    with conn.cursor() as cur:
        pairs = mismatched_roots(cur)
        if not pairs:
            return {t: 0 for t in tables}
        unguarded = {root for root, _ in unguarded_pairs(cur, pairs)}

    repaired: dict[str, int] = {t: 0 for t in tables}
    for table in tables:
        for root, canonical in pairs:
            if clock() >= deadline:
                logger.info(
                    "adjusted-root repair out of budget after %.0fs; "
                    "the rest carries to the next run",
                    budget_sec,
                )
                return repaired
            try:
                n = _rewrite_root(
                    conn,
                    table,
                    root,
                    canonical,
                    guarded=root not in unguarded,
                    statement_timeout=statement_timeout,
                    clock=clock,
                    deadline=deadline,
                )
            except Exception as exc:  # noqa: BLE001 — one slow family is not a failed deploy
                logger.warning("adjusted-root repair skipped %s.%s: %s", table, root, exc)
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue
            if n > 0:
                repaired[table] += n
                logger.info("repaired %s rows in %s: %s -> %s", n, table, root, canonical)
    return repaired


#: The guarded form: rewrite a row only where the catalogue confirms *that*
#: contract belongs to *that* underlying. Correct, and unable to touch a bar
#: whose contract has expired out of the catalogue — which is 99.995% of them.
_GUARDED = """
UPDATE raw_market.{table} d
SET underlying = %s
WHERE d.underlying = %s
  AND EXISTS (
        SELECT 1 FROM raw_market.option_contract c
        WHERE c.option_ticker = d.option_ticker
          AND c.underlying = %s
  )
"""

#: The unguarded form, for pairs ``unguarded_pairs`` has cleared. Chunked by
#: ctid: SPXW is 2.8M rows and a single statement for it finishes inside no
#: budget worth setting. Each chunk is its own statement and its own commit, so
#: the work survives a timeout and a deploy leaves behind what it managed.
_UNGUARDED_CHUNK = """
UPDATE raw_market.{table} d
SET underlying = %s
WHERE d.ctid = ANY(ARRAY(
    SELECT ctid FROM raw_market.{table}
    WHERE underlying = %s
    LIMIT {chunk}
))
"""


def _rewrite_root(
    conn: Any,
    table: str,
    root: str,
    canonical: str,
    *,
    guarded: bool,
    statement_timeout: str,
    clock: Any,
    deadline: float,
) -> int:
    """Rewrite one root in one table, committing as it goes. Returns rows moved."""
    if guarded:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(_GUARDED.format(table=table), (canonical, root, canonical))
            n = int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
        return n

    moved = 0
    while clock() < deadline:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            cur.execute(
                _UNGUARDED_CHUNK.format(table=table, chunk=int(CHUNK_ROWS)),
                (canonical, root),
            )
            n = int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
        moved += n
        if n == 0:
            break
    return moved


__all__ = [
    "BAR_TABLES",
    "mismatched_roots",
    "repair_adjusted_underlyings",
]
