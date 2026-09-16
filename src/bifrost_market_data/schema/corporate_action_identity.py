"""raw_market.corporate_action keyed by what the vendor says the distribution is.

The table was unique on ``(symbol, action_type, ex_date)``, so every ex-date held
one row and a second distribution on the same day overwrote the first. Measured
2026-09-16 against the 604 names ``corporate-backfill`` covers: the vendor sent
27,108 dividends, the table kept 26,745 — 363 gone across 48 symbols. Read row by
row from ``/stocks/v1/dividends``, the 363 were:

- 154 a second, real distribution: base + variable, regular + special
  (NPK $1.00 + $5.25, FCX $0.075 + $0.075, MSFT 2004-11-15 $0.08 + $3.00 — the
  $3.00 is the one that was lost);
- 40 the same CNQ dividend quoted in CAD and in USD;
- 10 same type, different amount (PGR's annual variable $13.50 beside its
  quarterly $0.10, both labelled ``recurring``);
- 159 vendor duplicates: two ids, every field equal except
  ``historical_adjustment_factor`` (CVX and CMI carry 28 each).

``IDENTITY`` keeps all of the first three and folds exactly the fourth, and every
part earns its place against that data: without ``amount`` the 2003–2005 pairs of
T, NPK and FCX fold; without ``frequency`` (or without ``distribution_type``) a DVN
pair folds; without type and frequency both, FCX's equal base and variable fold.
``currency`` folds nothing today; it is what keeps a CAD figure and a USD figure
apart. Splits leave the four new parts NULL, and ``NULLS NOT DISTINCT``
(PostgreSQL 15+; the cluster runs 17.9) keeps their key what it was.

A vendor that corrects an amount or relabels a type now lands as a new row, so the
dividend handlers delete what a complete fetch no longer lists — see
``ingest/corporate_action.py``. That is also how rows written before this
migration, which carry no type, leave the table.

Runs on the ``--wave8-only`` Job: raw_market is owned by the plugin's role since
0.19.20 (``create_option_contract_staleness_index`` relies on the same).
"""

from __future__ import annotations

from typing import Any, Protocol


class _Cursor(Protocol):
    def execute(self, query: str, params: Any = None) -> Any: ...


#: The columns one row of raw_market.corporate_action stands for, in constraint order.
IDENTITY: tuple[str, ...] = (
    "symbol",
    "action_type",
    "ex_date",
    "distribution_type",
    "frequency",
    "currency",
    "amount",
)

CONSTRAINT_NAME = "corporate_action_identity_key"

_OLD_KEY_DEF = "UNIQUE (symbol, action_type, ex_date)"


def migrate_corporate_action_identity(cur: _Cursor) -> None:
    """Add ``distribution_type`` / ``frequency`` and swap the unique key. Idempotent."""
    cols = ", ".join(IDENTITY)
    # Nullable, no default: metadata-only, no rewrite.
    cur.execute(
        "ALTER TABLE IF EXISTS raw_market.corporate_action "
        "ADD COLUMN IF NOT EXISTS distribution_type text"
    )
    cur.execute(
        "ALTER TABLE IF EXISTS raw_market.corporate_action "
        "ADD COLUMN IF NOT EXISTS frequency integer"
    )
    # The plugin's role runs with statement_timeout=2s; the index build is ~32k
    # rows, but it must wait for workers' row locks to clear. Scoped to this
    # transaction and handed back below.
    cur.execute("SET LOCAL lock_timeout = '15s'")
    cur.execute("SET LOCAL statement_timeout = '120s'")
    cur.execute(
        f"""
        DO $$
        DECLARE
            rel oid := to_regclass('raw_market.corporate_action');
            old record;
        BEGIN
            IF rel IS NULL THEN
                RETURN;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = rel AND conname = '{CONSTRAINT_NAME}'
            ) THEN
                ALTER TABLE raw_market.corporate_action
                    ADD CONSTRAINT {CONSTRAINT_NAME} UNIQUE NULLS NOT DISTINCT ({cols});
            END IF;
            FOR old IN
                SELECT conname FROM pg_constraint
                WHERE conrelid = rel AND contype = 'u'
                  AND pg_get_constraintdef(oid) = '{_OLD_KEY_DEF}'
            LOOP
                EXECUTE format('ALTER TABLE raw_market.corporate_action DROP CONSTRAINT %I', old.conname);
            END LOOP;
        END $$
        """
    )
    cur.execute("SET LOCAL statement_timeout TO DEFAULT")
    cur.execute("SET LOCAL lock_timeout TO DEFAULT")
