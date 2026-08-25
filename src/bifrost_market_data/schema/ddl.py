"""Idempotent DDL for raw_market.* and ops_jobs.* schemas.

Design principles:
- Single Polygon source (no source column)
- UTC timestamptz or NY calendar date
- option_ticker = Polygon native key
- Partitioned history tables with auto-extend helper

Wave 7: ``features.*`` Feature Store DDL owned by ``bifrost_research`` only.
Plugin ``db-init`` must not create ``features_daily`` / ``market_analytics`` schemas.
"""

from __future__ import annotations

from typing import Any, Protocol

from bifrost_market_data.schema.wave8_migrations import (
    FINANCIALS_ENTITY_TABLES,
    migrate_option_open_interest_partitioned,
    migrate_stock_financials_split,
    retire_data_ops_compat_schema,
)


class _Cursor(Protocol):
    def execute(self, query: str, params: Any = None) -> Any: ...


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...


def apply_wave8_migrations(conn: _Connection) -> None:
    """Wave 8 idempotent migrations only (no full raw_market DDL — safe for bifrost role)."""
    with conn.cursor() as cur:
        migrate_option_open_interest_partitioned(cur)
        migrate_stock_financials_split(cur)
        retire_data_ops_compat_schema(cur)
    conn.commit()


def apply_ddl(conn: _Connection) -> None:
    """Create schemas, tables, indexes, views, and partition helper (idempotent)."""
    with conn.cursor() as cur:
        _create_schemas(cur)
        _create_market_tables(cur)
        _create_data_ops_tables(cur)
        _create_partition_helper(cur)
        migrate_option_open_interest_partitioned(cur)
        migrate_stock_financials_split(cur)
        retire_data_ops_compat_schema(cur)
        _create_views(cur)
        _ensure_partitions(cur)
    conn.commit()


def _create_schemas(cur: _Cursor) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw_market")
    cur.execute("CREATE SCHEMA IF NOT EXISTS ops_jobs")


def _create_market_tables(cur: _Cursor) -> None:
    # --- stock_daily (RANGE by year on bar_date) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.stock_daily (
            symbol       text        NOT NULL,
            bar_date     date        NOT NULL,
            open         double precision,
            high         double precision,
            low          double precision,
            close        double precision,
            volume       bigint,
            vwap         double precision,
            trade_count  bigint,
            fetched_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, bar_date)
        ) PARTITION BY RANGE (bar_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS stock_daily_symbol_date
        ON raw_market.stock_daily (symbol, bar_date DESC)
        """
    )

    # --- stock_minute (RANGE by month on bar_time) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.stock_minute (
            symbol       text        NOT NULL,
            period       text        NOT NULL,
            bar_time     timestamptz NOT NULL,
            open         double precision,
            high         double precision,
            low          double precision,
            close        double precision,
            volume       bigint,
            vwap         double precision,
            trade_count  bigint,
            fetched_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, period, bar_time)
        ) PARTITION BY RANGE (bar_time)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS stock_minute_symbol_period_time
        ON raw_market.stock_minute (symbol, period, bar_time DESC)
        """
    )

    # --- stock_snapshot (non-partitioned daily upsert) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.stock_snapshot (
            symbol         text        NOT NULL,
            session_date   date        NOT NULL,
            open           double precision,
            high           double precision,
            low            double precision,
            close          double precision,
            volume         bigint,
            vwap           double precision,
            prev_close     double precision,
            change         double precision,
            change_pct     double precision,
            fetched_at     timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, session_date)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS stock_snapshot_session_date
        ON raw_market.stock_snapshot (session_date DESC, symbol)
        """
    )

    # --- stock_movers (gainers / losers daily upsert) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.stock_movers (
            direction      text        NOT NULL,
            symbol         text        NOT NULL,
            session_date   date        NOT NULL,
            change_pct     double precision,
            price          double precision,
            volume         bigint,
            fetched_at     timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (direction, symbol, session_date)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS stock_movers_session_direction
        ON raw_market.stock_movers (session_date DESC, direction)
        """
    )

    # --- option_daily ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_daily (
            option_ticker  text        NOT NULL,
            underlying     text        NOT NULL,
            expiry         date        NOT NULL,
            strike         double precision NOT NULL,
            option_right   char(1)     NOT NULL,
            bar_date       date        NOT NULL,
            open           double precision,
            high           double precision,
            low            double precision,
            close          double precision,
            volume         bigint,
            vwap           double precision,
            trade_count    bigint,
            fetched_at     timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (option_ticker, bar_date)
        ) PARTITION BY RANGE (bar_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_daily_underlying_date
        ON raw_market.option_daily (underlying, bar_date DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_daily_underlying_expiry
        ON raw_market.option_daily (underlying, expiry, bar_date DESC)
        """
    )

    # --- option_minute ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_minute (
            option_ticker  text        NOT NULL,
            underlying     text        NOT NULL,
            expiry         date        NOT NULL,
            strike         double precision NOT NULL,
            option_right   char(1)     NOT NULL,
            period         text        NOT NULL,
            bar_time       timestamptz NOT NULL,
            open           double precision,
            high           double precision,
            low            double precision,
            close          double precision,
            volume         bigint,
            vwap           double precision,
            trade_count    bigint,
            fetched_at     timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (option_ticker, period, bar_time)
        ) PARTITION BY RANGE (bar_time)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_minute_underlying_period_time
        ON raw_market.option_minute (underlying, period, bar_time DESC)
        """
    )

    # --- option_trades (daily REST tape; 30d day-partition retention) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_trades (
            option_ticker      text        NOT NULL,
            underlying         text        NOT NULL,
            expiry             date        NOT NULL,
            strike             double precision NOT NULL,
            option_right       char(1)     NOT NULL,
            trade_date         date        NOT NULL,
            sip_ts             timestamptz NOT NULL,
            sequence_number    bigint      NOT NULL,
            price              double precision,
            size               bigint,
            exchange           integer,
            conditions         integer[],
            correction         integer,
            participant_ts     timestamptz,
            fetched_at         timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (option_ticker, trade_date, sip_ts, sequence_number)
        ) PARTITION BY RANGE (trade_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_trades_underlying_date
        ON raw_market.option_trades (underlying, trade_date DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_trades_underlying_sip
        ON raw_market.option_trades (underlying, sip_ts DESC)
        """
    )

    # --- option_contract ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_contract (
            option_ticker       text    PRIMARY KEY,
            underlying          text    NOT NULL,
            expiry              date    NOT NULL,
            strike              double precision NOT NULL,
            option_right        char(1) NOT NULL,
            exercise_style      text,
            shares_per_contract integer DEFAULT 100,
            first_seen_at       timestamptz DEFAULT now(),
            updated_at          timestamptz DEFAULT now()
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_contract_underlying_expiry
        ON raw_market.option_contract (underlying, expiry, strike, option_right)
        """
    )

    # --- option_snapshot ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_snapshot (
            option_ticker       text        NOT NULL,
            underlying          text        NOT NULL,
            snapshot_ts         timestamptz NOT NULL,
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
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_snapshot_underlying_ts
        ON raw_market.option_snapshot (underlying, snapshot_ts DESC)
        """
    )

    # --- option_expiration ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_expiration (
            underlying   text NOT NULL,
            expiry       date NOT NULL,
            updated_at   timestamptz DEFAULT now(),
            PRIMARY KEY (underlying, expiry)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_expiration_underlying_updated
        ON raw_market.option_expiration (underlying, updated_at DESC)
        """
    )

    # option_open_interest — Wave 8: PARTITION BY RANGE (trade_date); see wave8_migrations.

    # --- ticker (merged tickers + ticker_overview) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.ticker (
            symbol            text    PRIMARY KEY,
            name              text,
            market            text,
            locale            text,
            primary_exchange  text,
            instrument_type   text,
            active            boolean DEFAULT true,
            currency          text,
            cik               text,
            composite_figi    text,
            sic_code          text,
            sector            text    DEFAULT '',
            industry          text    DEFAULT '',
            market_cap        double precision,
            list_date         date,
            homepage_url      text,
            total_employees   integer,
            description       text,
            updated_at        timestamptz DEFAULT now()
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_active
        ON raw_market.ticker (active) WHERE active IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_instrument_type
        ON raw_market.ticker (instrument_type)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_primary_exchange
        ON raw_market.ticker (primary_exchange)
        """
    )

    # stock_financials — Wave 8: split entity tables + compat view (wave8_migrations).

    # --- corporate_action ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.corporate_action (
            id           bigserial PRIMARY KEY,
            symbol       text    NOT NULL,
            action_type  text    NOT NULL,
            ex_date      date,
            record_date  date,
            payment_date date,
            ratio_from   double precision,
            ratio_to     double precision,
            amount       double precision,
            currency     text,
            description  text,
            fetched_at   timestamptz DEFAULT now(),
            UNIQUE (symbol, action_type, ex_date)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS corporate_action_symbol_ex
        ON raw_market.corporate_action (symbol, ex_date DESC)
        """
    )

    # --- us_market_holiday (vendor calendar; replaces public.reference_us_holidays) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.us_market_holiday (
            exchange     text        NOT NULL DEFAULT 'NYSE',
            holiday_date date        NOT NULL,
            name         text,
            status       text,
            open_time    timestamptz,
            close_time   timestamptz,
            fetched_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (exchange, holiday_date)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS us_market_holiday_date
        ON raw_market.us_market_holiday (holiday_date DESC)
        """
    )

    # --- ticker_related (Polygon related-companies; replaces public.ticker_related_tickers) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.ticker_related (
            from_symbol  text        NOT NULL,
            to_symbol    text        NOT NULL,
            rank         integer     NOT NULL DEFAULT 0,
            fetched_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (from_symbol, to_symbol)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_related_from
        ON raw_market.ticker_related (from_symbol)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_related_to
        ON raw_market.ticker_related (to_symbol)
        """
    )

    # --- ticker_type (Polygon ticker types dictionary; replaces public.ticker_types) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.ticker_type (
            code         text        NOT NULL,
            description  text,
            asset_class  text        NOT NULL DEFAULT '',
            locale       text        NOT NULL DEFAULT '',
            fetched_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (code, asset_class, locale)
        )
        """
    )


def _create_data_ops_tables(cur: _Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.job_ingest (
            id             bigserial   PRIMARY KEY,
            kind           text        NOT NULL,
            payload        jsonb       NOT NULL DEFAULT '{}'::jsonb,
            payload_hash   text,
            priority       smallint    NOT NULL DEFAULT 0,
            status         text        NOT NULL DEFAULT 'pending',
            result         jsonb,
            attempts       smallint    NOT NULL DEFAULT 0,
            max_attempts   smallint    NOT NULL DEFAULT 3,
            created_at     timestamptz DEFAULT now(),
            updated_at     timestamptz DEFAULT now(),
            started_at     timestamptz,
            finished_at    timestamptz
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS job_ingest_dedup
        ON ops_jobs.job_ingest (kind, payload_hash)
        WHERE status IN ('pending', 'running') AND payload_hash IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_status_priority_created
        ON ops_jobs.job_ingest (status, priority DESC, created_at)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.ingest_freshness (
            dimension    text    PRIMARY KEY,
            last_run_at  timestamptz,
            rows_written integer DEFAULT 0,
            status       text    DEFAULT 'unknown',
            updated_at   timestamptz DEFAULT now()
        )
        """
    )

    # Operator ack: vendor cannot provide this fundamentals data_type (migrated from
    # Trade public.preference_data_gap_ack — Golden Source owns data completeness).
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.data_source_void (
            data_type        text        PRIMARY KEY,
            is_void          boolean     NOT NULL DEFAULT false,
            acked_gap_count  integer,
            note             text,
            updated_at       timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    cur.execute(
        """
        COMMENT ON TABLE ops_jobs.data_source_void IS
          'Vendor cannot provide this fundamentals data_type; operator ack. '
          'Sourced from Trade preference_data_gap_ack (2026-08).'
        """
    )

    # Retired: flat is_trading calendar → derive from raw_market.us_market_holiday.
    cur.execute("DROP TABLE IF EXISTS ops_jobs.us_trading_calendar CASCADE")


def _create_views(cur: _Cursor) -> None:
    # CREATE OR REPLACE cannot rename/reorder columns; drop first for idempotent apply.
    cur.execute("DROP VIEW IF EXISTS raw_market.v_option_snapshot_with_stock")
    cur.execute("DROP VIEW IF EXISTS raw_market.v_option_chain_latest")
    cur.execute("DROP VIEW IF EXISTS raw_market.v_us_equity_universe")
    cur.execute(
        """
        CREATE OR REPLACE VIEW raw_market.v_us_equity_universe AS
        SELECT
            symbol,
            name,
            market,
            locale,
            primary_exchange,
            instrument_type,
            active,
            sector,
            industry,
            list_date,
            market_cap
        FROM raw_market.ticker
        WHERE COALESCE(active, false) = true
          AND lower(COALESCE(locale, '')) = 'us'
          AND lower(COALESCE(market, '')) = 'stocks'
          AND lower(COALESCE(instrument_type, '')) = 'cs'
        """
    )
    # Convenience view: latest snapshot row per option_ticker (may be heavy; optional for consumers)
    cur.execute(
        """
        CREATE OR REPLACE VIEW raw_market.v_option_chain_latest AS
        SELECT DISTINCT ON (s.option_ticker)
            s.option_ticker,
            s.underlying,
            s.snapshot_ts,
            s.iv,
            s.delta,
            s.gamma,
            s.theta,
            s.vega,
            s.open_interest,
            s.day_open,
            s.day_high,
            s.day_low,
            s.day_close,
            s.day_previous_close,
            s.day_change_percent,
            s.day_volume,
            s.day_vwap,
            s.fetched_at
        FROM raw_market.option_snapshot s
        ORDER BY s.option_ticker, s.snapshot_ts DESC
        """
    )
    # Bridge for Trade consumers replacing public.option_snapshots_with_underlying_day
    cur.execute(
        """
        CREATE OR REPLACE VIEW raw_market.v_option_snapshot_with_stock AS
        SELECT
            os.option_ticker,
            os.underlying,
            os.snapshot_ts,
            os.iv,
            os.delta,
            os.gamma,
            os.theta,
            os.vega,
            os.open_interest,
            os.day_open,
            os.day_high,
            os.day_low,
            os.day_close,
            os.day_previous_close,
            os.day_change_percent,
            os.day_volume,
            os.day_vwap,
            os.fetched_at,
            sd.close AS underlying_price,
            sd.bar_date AS underlying_bar_date
        FROM raw_market.option_snapshot os
        LEFT JOIN raw_market.stock_daily sd
            ON sd.symbol = os.underlying
           AND sd.bar_date = date(os.snapshot_ts AT TIME ZONE 'America/New_York')
        """
    )


def _create_partition_helper(cur: _Cursor) -> None:
    """Install PL/pgSQL helpers that create missing RANGE partitions."""
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION ops_jobs.ensure_year_partitions(
            p_schema text,
            p_table text,
            p_years_back integer DEFAULT 5,
            p_years_forward integer DEFAULT 2
        ) RETURNS void
        LANGUAGE plpgsql
        AS $$
        DECLARE
          y_start integer;
          y_end integer;
          y integer;
          part_name text;
          from_d date;
          to_d date;
          parent regclass;
        BEGIN
          parent := to_regclass(format('%I.%I', p_schema, p_table));
          IF parent IS NULL THEN
            RETURN;
          END IF;
          y_start := extract(year from CURRENT_DATE)::integer - p_years_back;
          y_end := extract(year from CURRENT_DATE)::integer + p_years_forward;
          FOR y IN y_start..y_end LOOP
            part_name := p_table || '_y' || y::text;
            from_d := make_date(y, 1, 1);
            to_d := make_date(y + 1, 1, 1);
            IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
              EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                p_schema, part_name, p_schema, p_table, from_d, to_d
              );
            END IF;
          END LOOP;
          IF to_regclass(format('%I.%I', p_schema, p_table || '_default')) IS NULL THEN
            EXECUTE format(
              'CREATE TABLE %I.%I PARTITION OF %I.%I DEFAULT',
              p_schema, p_table || '_default', p_schema, p_table
            );
          END IF;
        END;
        $$
        """
    )
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION ops_jobs.ensure_month_partitions(
            p_schema text,
            p_table text,
            p_months_back integer DEFAULT 12,
            p_months_forward integer DEFAULT 4
        ) RETURNS void
        LANGUAGE plpgsql
        AS $$
        DECLARE
          m_start date;
          m_end date;
          cur_m date;
          part_name text;
          parent regclass;
        BEGIN
          parent := to_regclass(format('%I.%I', p_schema, p_table));
          IF parent IS NULL THEN
            RETURN;
          END IF;
          m_start := (date_trunc('month', CURRENT_DATE) - (p_months_back || ' months')::interval)::date;
          m_end := (date_trunc('month', CURRENT_DATE) + ((p_months_forward + 1) || ' months')::interval)::date;
          cur_m := m_start;
          WHILE cur_m < m_end LOOP
            part_name := p_table || '_y' || to_char(cur_m, 'YYYY') || 'm' || to_char(cur_m, 'MM');
            IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
              EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                p_schema, part_name, p_schema, p_table,
                cur_m, (cur_m + interval '1 month')::date
              );
            END IF;
            cur_m := (cur_m + interval '1 month')::date;
          END LOOP;
          IF to_regclass(format('%I.%I', p_schema, p_table || '_default')) IS NULL THEN
            EXECUTE format(
              'CREATE TABLE %I.%I PARTITION OF %I.%I DEFAULT',
              p_schema, p_table || '_default', p_schema, p_table
            );
          END IF;
        END;
        $$
        """
    )
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION ops_jobs.ensure_day_partitions(
            p_schema text,
            p_table text,
            p_days_back integer DEFAULT 35,
            p_days_forward integer DEFAULT 2
        ) RETURNS void
        LANGUAGE plpgsql
        AS $$
        DECLARE
          d_start date;
          d_end date;
          cur_d date;
          part_name text;
          parent regclass;
        BEGIN
          parent := to_regclass(format('%I.%I', p_schema, p_table));
          IF parent IS NULL THEN
            RETURN;
          END IF;
          d_start := CURRENT_DATE - p_days_back;
          d_end := CURRENT_DATE + p_days_forward + 1;
          cur_d := d_start;
          WHILE cur_d < d_end LOOP
            part_name := p_table || '_d' || to_char(cur_d, 'YYYYMMDD');
            IF to_regclass(format('%I.%I', p_schema, part_name)) IS NULL THEN
              EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                p_schema, part_name, p_schema, p_table,
                cur_d, (cur_d + interval '1 day')::date
              );
            END IF;
            cur_d := (cur_d + interval '1 day')::date;
          END LOOP;
          IF to_regclass(format('%I.%I', p_schema, p_table || '_default')) IS NULL THEN
            EXECUTE format(
              'CREATE TABLE %I.%I PARTITION OF %I.%I DEFAULT',
              p_schema, p_table || '_default', p_schema, p_table
            );
          END IF;
        END;
        $$
        """
    )
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION ops_jobs.drop_day_partitions_older_than(
            p_schema text,
            p_table text,
            p_keep_days integer DEFAULT 30
        ) RETURNS integer
        LANGUAGE plpgsql
        AS $$
        DECLARE
          cutoff date;
          r record;
          dropped integer := 0;
          part_day date;
          suffix text;
        BEGIN
          cutoff := CURRENT_DATE - p_keep_days;
          FOR r IN
            SELECT c.relname AS part_name
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace pn ON pn.oid = p.relnamespace
            WHERE pn.nspname = p_schema
              AND p.relname = p_table
              AND n.nspname = p_schema
              AND c.relname LIKE (p_table || '_d%')
              AND c.relname <> (p_table || '_default')
          LOOP
            suffix := substring(r.part_name from length(p_table) + 3);
            BEGIN
              part_day := to_date(suffix, 'YYYYMMDD');
            EXCEPTION WHEN others THEN
              CONTINUE;
            END;
            IF part_day < cutoff THEN
              EXECUTE format('DROP TABLE IF EXISTS %I.%I', p_schema, r.part_name);
              dropped := dropped + 1;
            END IF;
          END LOOP;
          RETURN dropped;
        END;
        $$
        """
    )
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION ops_jobs.drop_month_partitions_older_than(
            p_schema text,
            p_table text,
            p_keep_days integer DEFAULT 90
        ) RETURNS integer
        LANGUAGE plpgsql
        AS $$
        DECLARE
          cutoff date;
          r record;
          dropped integer := 0;
          part_month date;
          y text;
          m text;
        BEGIN
          cutoff := date_trunc('month', CURRENT_DATE - p_keep_days)::date;
          FOR r IN
            SELECT c.relname AS part_name
            FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_class p ON p.oid = i.inhparent
            JOIN pg_namespace pn ON pn.oid = p.relnamespace
            WHERE pn.nspname = p_schema
              AND p.relname = p_table
              AND n.nspname = p_schema
              AND c.relname ~ (p_table || '_y[0-9]{4}m[0-9]{2}$')
          LOOP
            y := substring(r.part_name from '_y([0-9]{4})m');
            m := substring(r.part_name from 'm([0-9]{2})$');
            IF y IS NULL OR m IS NULL THEN
              CONTINUE;
            END IF;
            part_month := make_date(y::integer, m::integer, 1);
            IF part_month < cutoff THEN
              EXECUTE format('DROP TABLE IF EXISTS %I.%I', p_schema, r.part_name);
              dropped := dropped + 1;
            END IF;
          END LOOP;
          RETURN dropped;
        END;
        $$
        """
    )


def _ensure_partitions(cur: _Cursor) -> None:
    # Rolling window: keep recent history + at most ~12 months forward.
    # Schema names must match physical schemas (raw_market only; features.* = Research).
    cur.execute("SELECT ops_jobs.ensure_year_partitions('raw_market', 'stock_daily', 1, 1)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'stock_minute', 12, 3)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_daily', 12, 3)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_minute', 12, 3)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_snapshot', 3, 3)")
    cur.execute(
        "SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_open_interest', 12, 3)"
    )
    # Tape: day partitions + ~35d window; trim drops partitions older than 30d.
    cur.execute("SELECT ops_jobs.ensure_day_partitions('raw_market', 'option_trades', 35, 2)")


# Expected table names for tests / docs
MARKET_TABLES: tuple[str, ...] = (
    "stock_daily",
    "stock_minute",
    "stock_snapshot",
    "stock_movers",
    "option_daily",
    "option_minute",
    "option_trades",
    "option_contract",
    "option_snapshot",
    "option_expiration",
    "option_open_interest",
    "ticker",
    *FINANCIALS_ENTITY_TABLES,
    "corporate_action",
    "us_market_holiday",
    "ticker_related",
    "ticker_type",
)

# Retired Wave 7 — analytics tables live in features.* (bifrost-research).
MARKET_ANALYTICS_TABLES: tuple[str, ...] = ()

DATA_OPS_TABLES: tuple[str, ...] = (
    "job_ingest",
    "ingest_freshness",
    "data_source_void",
)

MARKET_VIEWS: tuple[str, ...] = (
    "v_us_equity_universe",
    "v_option_chain_latest",
    "v_option_snapshot_with_stock",
    "stock_financials",
)
