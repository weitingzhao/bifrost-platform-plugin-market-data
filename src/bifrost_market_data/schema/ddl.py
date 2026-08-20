"""Idempotent DDL for market.*, market_analytics.*, and data_ops.* schemas.

Design principles:
- Single Polygon source (no source column)
- UTC timestamptz or NY calendar date
- option_ticker = Polygon native key
- Partitioned history tables with auto-extend helper
- market_analytics holds derived daily analytics (max pain, ATM IV, PCR, IV percentile)
"""

from __future__ import annotations

from typing import Any, Protocol


class _Cursor(Protocol):
    def execute(self, query: str, params: Any = None) -> Any: ...


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...


def apply_ddl(conn: _Connection) -> None:
    """Create schemas, tables, indexes, views, and partition helper (idempotent)."""
    with conn.cursor() as cur:
        _create_schemas(cur)
        _create_market_tables(cur)
        _create_market_analytics_tables(cur)
        _create_data_ops_tables(cur)
        _create_views(cur)
        _create_partition_helper(cur)
        _ensure_partitions(cur)
    conn.commit()


def _create_schemas(cur: _Cursor) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS market")
    cur.execute("CREATE SCHEMA IF NOT EXISTS market_analytics")
    cur.execute("CREATE SCHEMA IF NOT EXISTS data_ops")


def _create_market_tables(cur: _Cursor) -> None:
    # --- stock_daily (RANGE by year on bar_date) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.stock_daily (
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
        ON market.stock_daily (symbol, bar_date DESC)
        """
    )

    # --- stock_minute (RANGE by month on bar_time) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.stock_minute (
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
        ON market.stock_minute (symbol, period, bar_time DESC)
        """
    )

    # --- stock_snapshot (non-partitioned daily upsert) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.stock_snapshot (
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
        ON market.stock_snapshot (session_date DESC, symbol)
        """
    )

    # --- stock_movers (gainers / losers daily upsert) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.stock_movers (
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
        ON market.stock_movers (session_date DESC, direction)
        """
    )

    # --- option_daily ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.option_daily (
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
        ON market.option_daily (underlying, bar_date DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_daily_underlying_expiry
        ON market.option_daily (underlying, expiry, bar_date DESC)
        """
    )

    # --- option_minute ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.option_minute (
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
        ON market.option_minute (underlying, period, bar_time DESC)
        """
    )

    # --- option_contract ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.option_contract (
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
        ON market.option_contract (underlying, expiry, strike, option_right)
        """
    )

    # --- option_snapshot ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.option_snapshot (
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
        ON market.option_snapshot (underlying, snapshot_ts DESC)
        """
    )

    # --- option_expiration ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.option_expiration (
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
        ON market.option_expiration (underlying, updated_at DESC)
        """
    )

    # --- option_open_interest ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.option_open_interest (
            option_ticker  text    NOT NULL,
            underlying     text    NOT NULL,
            expiry         date    NOT NULL,
            strike         double precision NOT NULL,
            option_right   char(1) NOT NULL,
            trade_date     date    NOT NULL,
            open_interest  integer NOT NULL,
            fetched_at     timestamptz DEFAULT now(),
            PRIMARY KEY (option_ticker, trade_date)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_oi_underlying_date
        ON market.option_open_interest (underlying, trade_date DESC)
        """
    )

    # --- ticker (merged tickers + ticker_overview) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.ticker (
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
        ON market.ticker (active) WHERE active IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_instrument_type
        ON market.ticker (instrument_type)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS ticker_primary_exchange
        ON market.ticker (primary_exchange)
        """
    )

    # --- stock_financials (jsonb unified fundamentals) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.stock_financials (
            symbol         text    NOT NULL,
            report_type    text    NOT NULL,
            period_date    date    NOT NULL,
            period_type    text    NOT NULL DEFAULT '',
            fiscal_year    integer,
            fiscal_quarter integer,
            data           jsonb   NOT NULL,
            fetched_at     timestamptz DEFAULT now(),
            PRIMARY KEY (symbol, report_type, period_date, period_type)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS stock_financials_symbol_type
        ON market.stock_financials (symbol, report_type, period_date DESC)
        """
    )

    # --- corporate_action ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.corporate_action (
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
        ON market.corporate_action (symbol, ex_date DESC)
        """
    )

    # --- us_market_holiday (vendor calendar; replaces public.reference_us_holidays) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market.us_market_holiday (
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
        ON market.us_market_holiday (holiday_date DESC)
        """
    )


def _create_market_analytics_tables(cur: _Cursor) -> None:
    # --- max_pain_daily (RANGE by month on trade_date) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_analytics.max_pain_daily (
            symbol                 text        NOT NULL,
            trade_date             date        NOT NULL,
            expiry                 date        NOT NULL,
            max_pain_strike        double precision,
            total_oi               integer,
            total_pain_at_strike   double precision,
            computed_at            timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, trade_date, expiry)
        ) PARTITION BY RANGE (trade_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS max_pain_daily_symbol_date
        ON market_analytics.max_pain_daily (symbol, trade_date DESC)
        """
    )

    # --- atm_iv_daily ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_analytics.atm_iv_daily (
            symbol             text        NOT NULL,
            trade_date         date        NOT NULL,
            expiry             date        NOT NULL,
            atm_strike         double precision,
            atm_iv             double precision,
            underlying_price   double precision,
            iv_source          text,
            computed_at        timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, trade_date, expiry)
        ) PARTITION BY RANGE (trade_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS atm_iv_daily_symbol_date
        ON market_analytics.atm_iv_daily (symbol, trade_date DESC)
        """
    )

    # --- pcr_daily ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_analytics.pcr_daily (
            symbol              text        NOT NULL,
            trade_date          date        NOT NULL,
            pcr_oi              double precision,
            pcr_volume          double precision,
            total_put_oi        integer,
            total_call_oi       integer,
            total_put_volume    bigint,
            total_call_volume   bigint,
            computed_at         timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, trade_date)
        ) PARTITION BY RANGE (trade_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS pcr_daily_symbol_date
        ON market_analytics.pcr_daily (symbol, trade_date DESC)
        """
    )

    # --- iv_percentile_daily ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_analytics.iv_percentile_daily (
            symbol               text        NOT NULL,
            trade_date           date        NOT NULL,
            iv_current           double precision,
            iv_percentile_1y     double precision,
            iv_rank_1y           double precision,
            lookback_days        integer,
            computed_at          timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (symbol, trade_date)
        ) PARTITION BY RANGE (trade_date)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS iv_percentile_daily_symbol_date
        ON market_analytics.iv_percentile_daily (symbol, trade_date DESC)
        """
    )


def _create_data_ops_tables(cur: _Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS data_ops.job_ingest (
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
        ON data_ops.job_ingest (kind, payload_hash)
        WHERE status IN ('pending', 'running') AND payload_hash IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_status_priority_created
        ON data_ops.job_ingest (status, priority DESC, created_at)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS data_ops.ingest_freshness (
            dimension    text    PRIMARY KEY,
            last_run_at  timestamptz,
            rows_written integer DEFAULT 0,
            status       text    DEFAULT 'unknown',
            updated_at   timestamptz DEFAULT now()
        )
        """
    )

    # Retired: flat is_trading calendar → derive from market.us_market_holiday.
    cur.execute("DROP TABLE IF EXISTS data_ops.us_trading_calendar CASCADE")


def _create_views(cur: _Cursor) -> None:
    # CREATE OR REPLACE cannot rename/reorder columns; drop first for idempotent apply.
    cur.execute("DROP VIEW IF EXISTS market.v_option_snapshot_with_stock")
    cur.execute("DROP VIEW IF EXISTS market.v_option_chain_latest")
    cur.execute("DROP VIEW IF EXISTS market.v_us_equity_universe")
    cur.execute(
        """
        CREATE OR REPLACE VIEW market.v_us_equity_universe AS
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
        FROM market.ticker
        WHERE COALESCE(active, false) = true
          AND lower(COALESCE(locale, '')) = 'us'
          AND lower(COALESCE(market, '')) = 'stocks'
          AND lower(COALESCE(instrument_type, '')) = 'cs'
        """
    )
    # Convenience view: latest snapshot row per option_ticker (may be heavy; optional for consumers)
    cur.execute(
        """
        CREATE OR REPLACE VIEW market.v_option_chain_latest AS
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
        FROM market.option_snapshot s
        ORDER BY s.option_ticker, s.snapshot_ts DESC
        """
    )
    # Bridge for Trade consumers replacing public.option_snapshots_with_underlying_day
    cur.execute(
        """
        CREATE OR REPLACE VIEW market.v_option_snapshot_with_stock AS
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
        FROM market.option_snapshot os
        LEFT JOIN market.stock_daily sd
            ON sd.symbol = os.underlying
           AND sd.bar_date = date(os.snapshot_ts AT TIME ZONE 'America/New_York')
        """
    )


def _create_partition_helper(cur: _Cursor) -> None:
    """Install PL/pgSQL helpers that create missing RANGE partitions."""
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION data_ops.ensure_year_partitions(
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
        CREATE OR REPLACE FUNCTION data_ops.ensure_month_partitions(
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


def _ensure_partitions(cur: _Cursor) -> None:
    cur.execute("SELECT data_ops.ensure_year_partitions('market', 'stock_daily', 5, 2)")
    cur.execute("SELECT data_ops.ensure_month_partitions('market', 'stock_minute', 12, 4)")
    cur.execute("SELECT data_ops.ensure_month_partitions('market', 'option_daily', 12, 4)")
    cur.execute("SELECT data_ops.ensure_month_partitions('market', 'option_minute', 12, 4)")
    cur.execute("SELECT data_ops.ensure_month_partitions('market', 'option_snapshot', 12, 4)")
    cur.execute(
        "SELECT data_ops.ensure_month_partitions('market_analytics', 'max_pain_daily', 12, 4)"
    )
    cur.execute(
        "SELECT data_ops.ensure_month_partitions('market_analytics', 'atm_iv_daily', 12, 4)"
    )
    cur.execute(
        "SELECT data_ops.ensure_month_partitions('market_analytics', 'pcr_daily', 12, 4)"
    )
    cur.execute(
        "SELECT data_ops.ensure_month_partitions('market_analytics', 'iv_percentile_daily', 12, 4)"
    )


# Expected table names for tests / docs
MARKET_TABLES: tuple[str, ...] = (
    "stock_daily",
    "stock_minute",
    "stock_snapshot",
    "stock_movers",
    "option_daily",
    "option_minute",
    "option_contract",
    "option_snapshot",
    "option_expiration",
    "option_open_interest",
    "ticker",
    "stock_financials",
    "corporate_action",
    "us_market_holiday",
)

MARKET_ANALYTICS_TABLES: tuple[str, ...] = (
    "max_pain_daily",
    "atm_iv_daily",
    "pcr_daily",
    "iv_percentile_daily",
)

DATA_OPS_TABLES: tuple[str, ...] = (
    "job_ingest",
    "ingest_freshness",
)

MARKET_VIEWS: tuple[str, ...] = (
    "v_us_equity_universe",
    "v_option_chain_latest",
    "v_option_snapshot_with_stock",
)
