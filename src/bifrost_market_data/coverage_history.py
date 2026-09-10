"""The matrix's memory: what each axis was judged to be, and when it changed.

The fourth axis is measured *on read* and says so in `continuity.py` — a
recorded measure can only see from the day it was switched on, and seeing
backwards is the entire point. This module does the opposite, and the two are
not in conflict once the difference is named:

    Record what the passage of time destroys. Compute what the table still holds.

Continuity asks a question the rows themselves still answer: the sessions of
the last ninety days are in `stock_daily` right now, so a scan today can say
what the middle looked like in July. A *verdict* is not like that. Freshness
divides by "how late is the newest row **now**"; breadth divides by the tier
scope **as it stood** at that moment; depth compares against a rolling window
whose far edge moved. Re-running any of them tomorrow answers tomorrow's
question. Yesterday's verdict is gone unless it was written down.

Run-length encoded, not one row per compute. The page recomputes on a timer and
almost every recompute reproduces the previous verdicts exactly; storing those
would be storing "nothing happened" a few hundred times a week. A row is
written only when the map actually differs, and an unchanged compute bumps
`last_seen_at` — so consecutive rows differ by construction and "when did this
change" is the row's own `first_seen_at`, not a scan for the boundary between
two runs of identical rows.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Iterable, Mapping

from bifrost_market_data.verdicts import diff

logger = logging.getLogger(__name__)

#: Long enough to cover a quarter of drift, which is the span the calibration
#: document reasons over; the table holds one row per *change*, so this is a
#: floor on history rather than a cap on volume.
KEEP_DAYS = 180


def digest(verdicts: Mapping[str, Any]) -> str:
    """A stable fingerprint of the verdict map.

    `sort_keys` because dict order is an implementation detail of whichever
    process built the payload, and two orderings of the same verdicts are not
    a change worth a row.
    """
    blob = json.dumps(verdicts, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _rows(cur: Any) -> list[tuple[Any, ...]]:
    fetched = cur.fetchall() if hasattr(cur, "fetchall") else []
    return [tuple(r.values()) if isinstance(r, Mapping) else tuple(r) for r in fetched or []]


def _loads(v: Any) -> dict[str, Any]:
    if isinstance(v, Mapping):
        return dict(v)
    if isinstance(v, (str, bytes)):
        try:
            parsed = json.loads(v)
        except Exception:  # noqa: BLE001
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _latest_two(cur: Any) -> list[tuple[Any, ...]]:
    cur.execute(
        """
        SELECT coverage_sample_id, first_seen_at, last_seen_at, digest, verdicts
        FROM ops_jobs.coverage_sample
        ORDER BY first_seen_at DESC, coverage_sample_id DESC
        LIMIT 2
        """
    )
    return _rows(cur)


def _carry_forward(
    current: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
    unread: Iterable[str],
) -> dict[str, Any]:
    """Let the last good verdicts stand for a dataset this compute could not read.

    A failed read is not a reading. `short_volume` timed out on the very first
    recorded compute (2026-09-10) and its four axes came back `unknown` — not
    because anything about the data moved, but because one statement hit its
    budget. Recorded as-is, a dataset that times out now and then would write two
    rows per flap and report "1 changed" on a page whose job is to make a real
    regression stand out.

    Only an outright read failure qualifies. `treasury_yield` answers `unknown`
    on depth because it has no symbol column to spread across, which is a stable
    fact about the dataset and is recorded as what it is.
    """
    merged = dict(current)
    for name in unread:
        prior = (previous or {}).get(name)
        if isinstance(prior, Mapping):
            merged[name] = dict(prior)
    return merged


def record(
    conn: Any, verdicts: Mapping[str, Any], *, unread: Iterable[str] = ()
) -> dict[str, Any]:
    """Store this compute's verdicts and report what moved since the last one.

    Returns the shape the payload carries, never raises: a page that cannot
    remember is worse than one that can, but not as bad as one that will not
    render. A failed write answers `recorded: False` and the reader is told so
    rather than shown an empty diff that looks like "nothing changed".
    """
    empty = {
        "recorded": False,
        "changed_at": None,
        "previous_at": None,
        "samples": 0,
        "carried_forward": [],
        "changes": [],
    }
    if not verdicts:
        return empty
    try:
        with conn.cursor() as cur:
            latest = _latest_two(cur)
            head = latest[0] if latest else None
            # Merged against the head *before* digesting, so an unread dataset
            # cannot produce a different fingerprint and therefore a new row.
            verdicts = _carry_forward(
                verdicts, _loads(head[4]) if head is not None else None, unread
            )
            d = digest(verdicts)
            if head is not None and str(head[3]) == d:
                # Same verdicts as the newest row: nothing to add, but the fact
                # that they were seen again is worth keeping — it is the
                # difference between "still true" and "not looked at since".
                cur.execute(
                    "UPDATE ops_jobs.coverage_sample SET last_seen_at = now() "
                    "WHERE coverage_sample_id = %s",
                    (head[0],),
                )
                before = _loads(latest[1][4]) if len(latest) > 1 else None
                changed_at, previous_at = head[1], (latest[1][1] if len(latest) > 1 else None)
            else:
                cur.execute(
                    """
                    INSERT INTO ops_jobs.coverage_sample (digest, verdicts)
                    VALUES (%s, %s::jsonb)
                    RETURNING first_seen_at
                    """,
                    (d, json.dumps(verdicts, sort_keys=True, default=str)),
                )
                inserted = _rows(cur)
                changed_at = inserted[0][0] if inserted else None
                before = _loads(head[4]) if head is not None else None
                previous_at = head[1] if head is not None else None
            cur.execute("SELECT count(*)::bigint FROM ops_jobs.coverage_sample")
            samples = int((_rows(cur) or [(0,)])[0][0] or 0)
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — the record is a nicety; the page is not
        logger.warning("coverage sample record failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return empty

    return {
        "recorded": True,
        # Named rather than silently folded in: the reader should know which
        # rows on this page are last-known rather than just-measured.
        "carried_forward": sorted(set(unread)),
        # When the *current* verdicts first appeared. On the very first sample
        # there is nothing before it, and the console must say "first reading"
        # rather than "no changes" — those are different claims.
        "changed_at": changed_at.isoformat() if hasattr(changed_at, "isoformat") else changed_at,
        "previous_at": (
            previous_at.isoformat() if hasattr(previous_at, "isoformat") else previous_at
        ),
        "samples": samples,
        "changes": diff(before, verdicts) if before is not None else [],
    }


def history(conn: Any, *, limit: int = 30) -> list[dict[str, Any]]:
    """Recent distinct verdict maps, newest first. Empty when unreadable."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT coverage_sample_id, first_seen_at, last_seen_at, verdicts
                FROM ops_jobs.coverage_sample
                ORDER BY first_seen_at DESC, coverage_sample_id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = _rows(cur)
    except Exception as exc:  # noqa: BLE001
        logger.warning("coverage sample read failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r[0]),
                "first_seen_at": r[1].isoformat() if hasattr(r[1], "isoformat") else r[1],
                "last_seen_at": r[2].isoformat() if hasattr(r[2], "isoformat") else r[2],
                "verdicts": _loads(r[3]),
            }
        )
    return out


def trim_samples(conn: Any, *, keep_days: int = KEEP_DAYS) -> int:
    """Drop changes older than the window — but never the newest row.

    Without that guard a quiet quarter would delete the only description of the
    current state, and the next compute would report every dataset as a first
    reading. Retention here bounds the *history*, not the present.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM ops_jobs.coverage_sample
                WHERE last_seen_at < now() - make_interval(days => %s)
                  AND coverage_sample_id <> (
                      SELECT coverage_sample_id FROM ops_jobs.coverage_sample
                      ORDER BY first_seen_at DESC, coverage_sample_id DESC LIMIT 1
                  )
                """,
                (int(keep_days),),
            )
            n = int(getattr(cur, "rowcount", 0) or 0)
        conn.commit()
        return n
    except Exception as exc:  # noqa: BLE001
        logger.warning("coverage sample trim failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return 0


__all__ = ["KEEP_DAYS", "digest", "record", "history", "trim_samples"]
