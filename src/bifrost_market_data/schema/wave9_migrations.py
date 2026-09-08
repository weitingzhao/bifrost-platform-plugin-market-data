"""Wave 9 — option_snapshot keyed by observation time, not last trade time.

``snapshot_ts`` used to be filled from the contract's *last trade* timestamp.
A contract that had not traded for a week was therefore written into last
week's partition, so a session's chain was only ever 33–72% complete, and a
later fetch silently overwrote the older row's greeks, IV and day bars — rows
dated 2026-08-13 were last written on 2026-09-05.

This migration keeps the column name and the primary key shape, so every
consumer query (``date(snapshot_ts AT TIME ZONE 'America/New_York')``) becomes
correct without a code change. The old value moves to ``last_trade_ts``.

Historical rows are re-keyed by the session they were actually observed in,
derived from ``fetched_at``; a weekend catch-up folds back onto the Friday
session it was fetching. Rows that collide keep the most recent fetch.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class _Cursor(Protocol):
    def execute(self, query: str, params: object = None) -> object: ...
    def fetchone(self) -> Any: ...


_NEW = "option_snapshot_w9"

# NY calendar date of the fetch, snapped back off the weekend, at 16:00 NY.
_ANCHOR_SQL = """
    ((CASE extract(isodow FROM (s.fetched_at AT TIME ZONE 'America/New_York')::date)
        WHEN 6 THEN (s.fetched_at AT TIME ZONE 'America/New_York')::date - 1
        WHEN 7 THEN (s.fetched_at AT TIME ZONE 'America/New_York')::date - 2
        ELSE (s.fetched_at AT TIME ZONE 'America/New_York')::date
      END) + time '16:00') AT TIME ZONE 'America/New_York'
"""

_COLS = (
    "option_ticker, underlying, snapshot_ts, last_trade_ts, iv, delta, gamma, theta, vega, "
    "open_interest, day_open, day_high, day_low, day_close, day_previous_close, "
    "day_change_percent, day_volume, day_vwap, fetched_at"
)

_GRANTS = (
    ("bifrost", "ALL"),
    ("data_writer", "ALL"),
    ("analytics_writer", "SELECT"),
    ("analytics_reader", "SELECT"),
    ("brokerage_writer", "SELECT"),
    ("brokerage_reader", "SELECT"),
    ("market_reader", "SELECT"),
)


def option_snapshot_needs_observed_migration(cur: _Cursor) -> bool:
    """True when ``option_snapshot`` still lacks ``last_trade_ts``."""
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'raw_market'
          AND table_name = 'option_snapshot'
          AND column_name = 'last_trade_ts'
        """
    )
    return cur.fetchone() is None


def observed_time_statements() -> tuple[str, ...]:
    """The migration as plain SQL, in order.

    ``raw_market`` is owned by ``postgres``, so this runs on the superuser path
    (``init_schema.py --wave9-sql | psql -U postgres``) rather than as the
    plugin's DB role. The Python entrypoint runs the same list.
    """
    grants = tuple(
        f"""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN
            EXECUTE 'GRANT {priv} ON raw_market.option_snapshot TO {role}';
          END IF;
        END $$
        """
        for role, priv in _GRANTS
    )
    return (
        f"DROP TABLE IF EXISTS raw_market.{_NEW} CASCADE",
        f"""
        CREATE TABLE raw_market.{_NEW} (
            option_ticker       text        NOT NULL,
            underlying          text        NOT NULL,
            snapshot_ts         timestamptz NOT NULL,
            last_trade_ts       timestamptz,
            iv                  double precision,
            delta               double precision,
            gamma               double precision,
            theta               double precision,
            vega                double precision,
            open_interest       integer,
            day_open            double precision,
            day_high            double precision,
            day_low             double precision,
            day_close           double precision,
            day_previous_close  double precision,
            day_change_percent  double precision,
            day_volume          bigint,
            day_vwap            double precision,
            fetched_at          timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (option_ticker, snapshot_ts)
        ) PARTITION BY RANGE (snapshot_ts)
        """,
        f"CREATE TABLE raw_market.{_NEW}_default PARTITION OF raw_market.{_NEW} DEFAULT",
        f"SELECT ops_jobs.ensure_month_partitions('raw_market', '{_NEW}', 12, 3)",
        f"""
        INSERT INTO raw_market.{_NEW} ({_COLS})
        SELECT DISTINCT ON (option_ticker, anchor)
            option_ticker, underlying, anchor, snapshot_ts, iv, delta, gamma, theta, vega,
            open_interest, day_open, day_high, day_low, day_close, day_previous_close,
            day_change_percent, day_volume, day_vwap, fetched_at
        FROM (SELECT s.*, {_ANCHOR_SQL} AS anchor FROM raw_market.option_snapshot s) t
        ORDER BY option_ticker, anchor, fetched_at DESC
        """,
        # CASCADE drops the dependent views; the caller rebuilds them from
        # ddl.OPTION_SNAPSHOT_VIEW_SQL, the one definition both paths use.
        "DROP TABLE raw_market.option_snapshot CASCADE",
        f"ALTER TABLE raw_market.{_NEW} RENAME TO option_snapshot",
        rf"""
        DO $$
        DECLARE part record;
        BEGIN
          FOR part IN
            SELECT c.relname AS name FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = 'raw_market.option_snapshot'::regclass
              AND c.relname LIKE '{_NEW}\_%'
          LOOP
            EXECUTE format(
              'ALTER TABLE raw_market.%I RENAME TO %I',
              part.name,
              'option_snapshot_' || substr(part.name, length('{_NEW}_') + 1)
            );
          END LOOP;
        END $$
        """,
        f"ALTER INDEX raw_market.{_NEW}_pkey RENAME TO option_snapshot_pkey",
        """
        CREATE INDEX IF NOT EXISTS option_snapshot_underlying_ts
        ON raw_market.option_snapshot (underlying, snapshot_ts DESC)
        """,
        *grants,
    )


def migrate_option_snapshot_observed_time(cur: _Cursor) -> dict[str, Any]:
    """Rebuild ``raw_market.option_snapshot`` keyed by observation time.

    Idempotent: a table that already has ``last_trade_ts`` is left alone.
    """
    cur.execute("SELECT to_regclass('raw_market.option_snapshot')")
    row = cur.fetchone()
    if row is None or row[0] is None:
        return {"migrated": False, "reason": "table absent"}
    if not option_snapshot_needs_observed_migration(cur):
        return {"migrated": False, "reason": "already migrated"}

    for stmt in observed_time_statements():
        cur.execute(stmt)
    cur.execute("SELECT count(*) FROM raw_market.option_snapshot")
    row = cur.fetchone()
    moved = int(row[0] if row else 0)
    logger.info("wave9: option_snapshot re-keyed by observation time, rows=%s", moved)
    return {"migrated": True, "rows": moved}
