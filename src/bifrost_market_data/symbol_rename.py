"""Ticker renames — the vendor answers a dead symbol with its successor's chain.

Asking ``/v3/snapshot/options/SATS`` returns ``O:ECHO…`` contracts, and asking
for ``ISSC`` returns ``O:IA…``. Both companies only changed their ticker
(EchoStar in June 2026, Innovative Solutions & Support in August), and the
option root followed the new symbol while our request kept the old one. The
chain handlers stamp the *requested* underlying, which is right for ``SPXW`` →
``SPX`` and for ``BRK.B``, whose root is ``BRKB``, and wrong here: measured
2026-09-26, the 776 contracts of ECHO's 09-25 chain were split 362 under
``ECHO`` and 414 under ``SATS``, and Research wrote two different max pains for
the same 2026-10-16 expiry — 99 on 2,592 open interest against 90 on 59,986.
The near expiries landed under ECHO, every LEAP under SATS.

The vendor gives no help: ``details.underlying_ticker`` is absent from the
snapshot payload on this plan (measured: None for every contract). The only
signal in the response is the contract root, so the root is the evidence and
this module is the confirmation.

Two conditions, both required, and deliberately not the same one twice:

- the root is a symbol ``raw_market.ticker`` still calls **active**; and
- the symbol we asked for is **not**.

That second half is why the check cannot lean on a predecessor link alone:
``SATS`` has no row in ``ticker`` at all — the reference walk only inserts what
the vendor lists as active, and SATS was delisted before the table was first
written — so a CIK match has nothing to match against. Where the old row *does*
exist we demand it: same CIK and same composite FIGI, which says one registrant
and one instrument. AVB fails that on purpose. It was merged into Equity
Residential's registrant as Vivmark, so its CIK changes from 0000915912 to
0000906107 and shareholders got 2.793 VMRK for a share — a conversion no option
chain carries across.

Measured against the whole catalogue on 2026-09-26, the pair of conditions
fires on exactly two root/underlying pairs, SATS → ECHO and ISSC → IA, and on
``EQR`` → ``VMRK`` in the snapshot history. It does not fire on ``SPX``/SPXW,
``BRK.B``/BRKB or ``CMCSA``/CMCS, because none of those roots is a ticker; nor
on ``HONA``/HON or ``Q``/DD, because both of those underlyings are still
active. Adjusted roots such as ``ECHO1`` and ``VMRK1`` are not tickers either,
so they stay where they are: rewriting those is the adjusted-root repair's job,
which maps a root to the catalogue's underlying rather than the other way.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping

logger = logging.getLogger(__name__)

#: ``O:`` + root + 6 date + 1 right + 8 strike, so the tail is a fixed 15 and
#: the prefix 2. Same arithmetic as ``adjusted_root_repair``, spelled here
#: because this module must not depend on a schema-repair module.
_TICKER_TAIL = 15
_TICKER_PREFIX = 2

#: Both rows in one round trip, by primary key. ``NULL`` for a symbol the
#: catalogue has never listed, which is a third answer and not a false.
_PAIR_SQL = """
SELECT d.active, d.cik, d.composite_figi, a.active, a.cik, a.composite_figi
FROM (SELECT %s::text AS dead, %s::text AS alive) q
LEFT JOIN raw_market.ticker d ON d.symbol = q.dead
LEFT JOIN raw_market.ticker a ON a.symbol = q.alive
"""


def option_root(option_ticker: str) -> str | None:
    """The root spelled by a Polygon option key, or None if it is not one."""
    raw = str(option_ticker or "").strip().upper()
    if not raw.startswith("O:") or len(raw) <= _TICKER_TAIL + _TICKER_PREFIX:
        return None
    root = raw[_TICKER_PREFIX : len(raw) - _TICKER_TAIL]
    return root or None


def _first_row(cur: Any) -> tuple[Any, ...] | None:
    rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    for row in rows or []:
        if isinstance(row, Mapping):
            return tuple(row.values())
        try:
            return tuple(row)
        except TypeError:
            return None
    return None


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def is_rename(conn: Any, requested: str, root: str) -> bool:
    """True when ``root`` is the live symbol that ``requested`` was renamed to.

    Never raises and never guesses: an unreadable catalogue answers False, which
    leaves the handler labelling rows exactly as it did before this module.
    """
    dead = str(requested or "").strip().upper()
    alive = str(root or "").strip().upper()
    if not dead or not alive or dead == alive:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(_PAIR_SQL, (dead, alive))
            row = _first_row(cur)
    except Exception as exc:  # noqa: BLE001 — a label question must not fail a job
        logger.warning("rename lookup failed for %s/%s: %s", dead, alive, exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    if row is None or len(row) < 6:
        return False
    dead_active, dead_cik, dead_figi, alive_active, alive_cik, alive_figi = row[:6]
    if alive_active is not True:
        return False
    if dead_active is True:
        # Both listed: two live instruments, so the root belongs to the other
        # one. HONA and Q both read like this, and neither is a rename.
        return False
    if dead_active is None:
        # The catalogue never listed it, so there is no link to demand. It
        # cannot be live either, which is the half of the rule that matters.
        return True
    return bool(
        _clean(dead_cik)
        and _clean(dead_cik) == _clean(alive_cik)
        and _clean(dead_figi)
        and _clean(dead_figi) == _clean(alive_figi)
    )


def resolve_storage(conn: Any, storage: str, option_tickers: Iterable[str], *, max_roots: int = 4) -> str:
    """``storage``, or the live symbol its contracts are actually rooted at.

    Asks about the roots the response carries, not about a list of renames kept
    somewhere: the label is only moved when the contracts in hand spell the
    successor. A chain that mixes ``ECHO`` with the adjusted ``ECHO1`` resolves
    on the plain root and files both under it, which is where an adjusted series
    belongs.
    """
    base = str(storage or "").strip().upper()
    if not base:
        return base
    roots: list[str] = []
    for ticker in option_tickers:
        root = option_root(ticker)
        if root and root != base and root not in roots:
            roots.append(root)
            if len(roots) >= max_roots:
                break
    for root in sorted(roots):
        if is_rename(conn, base, root):
            logger.info("%s was renamed to %s; filing its chain under the new symbol", base, root)
            return root
    return base


__all__ = ["is_rename", "option_root", "resolve_storage"]
