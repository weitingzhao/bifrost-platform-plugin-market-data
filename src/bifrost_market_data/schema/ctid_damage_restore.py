"""Undo the rows 0.31.6 rewrote to an underlying that was not theirs.

The chunked rewrite in ``adjusted_root_repair`` filtered on ``ctid`` alone.
``option_daily`` is partitioned by ``bar_date`` and a ctid is unique only
*within* a partition, so each chunk matched the same physical slot in every
other partition too. Measured 2026-09-10 immediately after the run:

    underlying = 'SPX'    1,619,571 rows, 618,118 with a ticker that is not O:SPX*
    underlying = 'BRK.B'  1,016,768 rows, 866,869 with a ticker that is not O:BRKB*

SPY bars filed under SPX, AMD bars filed under BRK.B — 1,484,987 rows.

Recoverable without a backup, because ``option_ticker`` was never touched and
it carries the root. Every damaged row had come from a backfill job whose
payload had no underlying, so its value before the damage was exactly the
parsed root — which is what this puts back.

Bounded by the ticker prefix, so the rows the rewrite moved *correctly* are
left alone: ``O:SPXW…`` legitimately belongs to SPX and starts with ``O:SPX``.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: ``underlying`` → the ticker prefixes that legitimately carry it. Anything
#: else under that underlying was put there by the ctid bug.
DAMAGED: dict[str, tuple[str, ...]] = {
    "SPX": ("O:SPX",),          # covers O:SPX… and O:SPXW…
    "BRK.B": ("O:BRKB", "O:BRK.B"),
}

#: Same chunk size as the rewrite that caused it.
CHUNK_ROWS = 50_000

#: How long one deploy may spend putting rows back.
DEFAULT_BUDGET_SEC = 240.0

#: The root, as ``parse_option_ticker`` would have read it: ``O:`` + root +
#: 6 date + 1 right + 8 strike, so the tail is a fixed 15 and the prefix 2.
_ROOT = "substring(option_ticker FROM 3 FOR length(option_ticker) - 17)"


def _restore_sql(table: str, prefixes: tuple[str, ...]) -> str:
    """The outer WHERE repeats every predicate. The ctid list narrows the batch;
    it must never be the only thing selecting rows — that is the bug this file
    exists to undo."""
    not_like = " AND ".join(f"d.option_ticker NOT LIKE '{p}%%'" for p in prefixes)
    inner_not_like = " AND ".join(f"option_ticker NOT LIKE '{p}%%'" for p in prefixes)
    return f"""
        UPDATE raw_market.{table} d
        SET underlying = upper(btrim({_ROOT}))
        WHERE d.underlying = %s
          AND {not_like}
          AND length(d.option_ticker) > 17
          AND d.ctid = ANY(ARRAY(
                SELECT ctid FROM raw_market.{table}
                WHERE underlying = %s
                  AND {inner_not_like}
                  AND length(option_ticker) > 17
                LIMIT {CHUNK_ROWS}
          ))
    """


def restore_ctid_damage(
    conn: Any,
    *,
    table: str = "option_daily",
    budget_sec: float = DEFAULT_BUDGET_SEC,
    now: Any = None,
) -> dict[str, int]:
    """Put each damaged row's underlying back to its ticker's own root.

    Commits per chunk, like the rewrite — so what it puts back stays back even
    if the budget runs out, and the next deploy continues. Idempotent: once a
    row's underlying matches its ticker it no longer matches the predicate.
    """
    import time

    clock = now or time.monotonic
    deadline = clock() + float(budget_sec)
    restored: dict[str, int] = {}
    for underlying, prefixes in DAMAGED.items():
        moved = 0
        sql = _restore_sql(table, prefixes)
        while clock() < deadline:
            try:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '120s'")
                    cur.execute(sql, (underlying, underlying))
                    n = int(getattr(cur, "rowcount", 0) or 0)
                conn.commit()
            except Exception as exc:  # noqa: BLE001 — a restore must not fail a deploy
                logger.warning("ctid damage restore stopped on %s: %s", underlying, exc)
                try:
                    conn.rollback()
                except Exception:
                    pass
                break
            moved += n
            if n == 0:
                break
        if moved:
            restored[underlying] = moved
            logger.info("restored %s rows wrongly filed under %s", moved, underlying)
    return restored


__all__ = ["DAMAGED", "CHUNK_ROWS", "restore_ctid_damage", "_restore_sql"]
