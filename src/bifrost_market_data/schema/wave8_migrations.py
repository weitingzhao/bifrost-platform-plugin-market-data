"""Wave 8 Golden Source migrations (idempotent)."""

from __future__ import annotations

from typing import Protocol

FINANCIALS_ENTITY_TABLES: tuple[str, ...] = (
    "income_statement",
    "balance_sheet",
    "cash_flow",
    "ratios",
    "short_interest",
    "short_volume",
)

_REPORT_TYPE_TO_TABLE: dict[str, str] = {
    "income_statement": "income_statement",
    "balance_sheet": "balance_sheet",
    "cash_flow_statement": "cash_flow",
    "ratios": "ratios",
    "short_interest": "short_interest",
    "short_volume": "short_volume",
}


class _Cursor(Protocol):
    def execute(self, query: str, params: object = None) -> object: ...


def _table_relkind(cur: _Cursor, schema: str, name: str) -> str | None:
    cur.execute(
        """
        SELECT c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, name),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0] if not hasattr(row, "keys") else row.get("relkind")


def create_financials_entity_tables(cur: _Cursor) -> None:
    for table in FINANCIALS_ENTITY_TABLES:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS raw_market.{table} (
                symbol         text    NOT NULL,
                period_date    date    NOT NULL,
                period_type    text    NOT NULL DEFAULT '',
                fiscal_year    integer,
                fiscal_quarter integer,
                data           jsonb   NOT NULL,
                fetched_at     timestamptz DEFAULT now(),
                PRIMARY KEY (symbol, period_date, period_type)
            )
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {table}_symbol_period_date
            ON raw_market.{table} (symbol, period_date DESC)
            """
        )


def create_stock_financials_compat_view(cur: _Cursor) -> None:
    cur.execute("DROP VIEW IF EXISTS raw_market.stock_financials CASCADE")
    cur.execute(
        """
        CREATE OR REPLACE VIEW raw_market.stock_financials AS
        SELECT symbol, 'income_statement'::text AS report_type,
               period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
        FROM raw_market.income_statement
        UNION ALL
        SELECT symbol, 'balance_sheet'::text,
               period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
        FROM raw_market.balance_sheet
        UNION ALL
        SELECT symbol, 'cash_flow_statement'::text,
               period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
        FROM raw_market.cash_flow
        UNION ALL
        SELECT symbol, 'ratios'::text,
               period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
        FROM raw_market.ratios
        UNION ALL
        SELECT symbol, 'short_interest'::text,
               period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
        FROM raw_market.short_interest
        UNION ALL
        SELECT symbol, 'short_volume'::text,
               period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
        FROM raw_market.short_volume
        """
    )


def migrate_stock_financials_split(cur: _Cursor) -> None:
    """Backfill split tables from legacy stock_financials table; replace with compat view."""
    create_financials_entity_tables(cur)
    relkind = _table_relkind(cur, "raw_market", "stock_financials")
    if relkind != "r":
        create_stock_financials_compat_view(cur)
        return

    for report_type, table in _REPORT_TYPE_TO_TABLE.items():
        cur.execute(
            f"""
            INSERT INTO raw_market.{table}
                (symbol, period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at)
            SELECT symbol, period_date, period_type, fiscal_year, fiscal_quarter, data, fetched_at
            FROM raw_market.stock_financials
            WHERE report_type = %s
            ON CONFLICT (symbol, period_date, period_type) DO UPDATE SET
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_quarter = EXCLUDED.fiscal_quarter,
                data = EXCLUDED.data,
                fetched_at = EXCLUDED.fetched_at
            """,
            (report_type,),
        )

    cur.execute(
        "DELETE FROM raw_market.stock_financials WHERE report_type = 'comprehensive_income'"
    )

    cur.execute("DROP TABLE IF EXISTS raw_market.stock_financials CASCADE")
    create_stock_financials_compat_view(cur)


def _create_option_open_interest_partitioned(cur: _Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_open_interest (
            option_ticker  text    NOT NULL,
            underlying     text    NOT NULL,
            expiry         date    NOT NULL,
            strike         double precision NOT NULL,
            option_right   char(1) NOT NULL,
            trade_date     date    NOT NULL,
            open_interest  integer NOT NULL,
            fetched_at     timestamptz DEFAULT now(),
            PRIMARY KEY (option_ticker, trade_date)
        ) PARTITION BY RANGE (trade_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_oi_underlying_date
        ON raw_market.option_open_interest (underlying, trade_date DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_oi_underlying_expiry_strike
        ON raw_market.option_open_interest (underlying, expiry, strike)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_oi_underlying_expiry_date
        ON raw_market.option_open_interest (underlying, expiry, trade_date DESC)
        """
    )


def migrate_option_open_interest_partitioned(cur: _Cursor) -> None:
    relkind = _table_relkind(cur, "raw_market", "option_open_interest")
    if relkind == "p":
        return
    if relkind == "r":
        cur.execute(
            "ALTER TABLE raw_market.option_open_interest RENAME TO option_open_interest_legacy"
        )
        _create_option_open_interest_partitioned(cur)
        cur.execute(
            "SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_open_interest', 12, 3)"
        )
        cur.execute(
            """
            INSERT INTO raw_market.option_open_interest
            SELECT * FROM raw_market.option_open_interest_legacy
            """
        )
        cur.execute("DROP TABLE raw_market.option_open_interest_legacy")
        return
    _create_option_open_interest_partitioned(cur)


def retire_data_ops_compat_schema(cur: _Cursor) -> None:
    cur.execute("DROP VIEW IF EXISTS data_ops.ingest_freshness CASCADE")
    cur.execute("DROP SCHEMA IF EXISTS data_ops CASCADE")
