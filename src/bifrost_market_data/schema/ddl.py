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
    add_financials_filing_date,
    create_option_contract_staleness_index,
    migrate_option_open_interest_partitioned,
    migrate_stock_financials_split,
    retire_data_ops_compat_schema,
)
from bifrost_market_data.schema.adjusted_root_repair import repair_adjusted_underlyings
from bifrost_market_data.schema.ctid_damage_restore import restore_ctid_damage
from bifrost_market_data.schema.wave9_migrations import migrate_option_snapshot_observed_time


class _Cursor(Protocol):
    def execute(self, query: str, params: Any = None) -> Any: ...


class _Connection(Protocol):
    def cursor(self) -> Any: ...

    def commit(self) -> None: ...


def apply_wave8_migrations(conn: _Connection) -> None:
    """Wave 8 idempotent migrations only (no full raw_market DDL — safe for bifrost role).

    add_financials_filing_date is deliberately NOT here: raw_market tables are
    owned by ``postgres``, so its ALTER needs ownership and would make this
    job-run path fail on every invocation. It lives in apply_ddl, which the
    job manifest already documents as the superuser path.
    """
    with conn.cursor() as cur:
        migrate_option_open_interest_partitioned(cur)
        migrate_stock_financials_split(cur)
        retire_data_ops_compat_schema(cur)
        create_option_contract_staleness_index(cur)
        # In ops_jobs, which the plugin's role owns — so a new table of its own
        # can be created on this path rather than waiting for a superuser run.
        create_coverage_sample(cur)
    # The schema is what the deploy needs, so it lands first and on its own. The
    # data repair below commits per root and may run out of budget; committing
    # here means it cannot take the schema down with it.
    conn.commit()

    # A data repair rather than a schema change, riding the same path because it
    # needs raw_market write access. Given the connection, not a cursor: it
    # commits each root as it goes, so a slow family costs that family and not
    # the run, and what it does not finish carries to the next deploy. It must
    # never fail the migration — measured 2026-09-10, a timeout in here is what
    # made a successful deploy print "DDL failed" as its last line.
    # First, undo what 0.31.6's chunked rewrite put in the wrong place. It runs
    # before the repair so a single deploy cannot damage and then re-damage.
    try:
        restored = restore_ctid_damage(conn)
    except Exception as exc:  # noqa: BLE001
        print(f"ctid damage restore skipped: {exc}")
    else:
        if restored:
            print(f"ctid damage restored: {restored}")

    try:
        repaired = repair_adjusted_underlyings(conn)
    except Exception as exc:  # noqa: BLE001 — a data fix must not fail a schema deploy
        print(f"adjusted-root repair skipped: {exc}")
    else:
        if any(repaired.values()):
            print(f"adjusted roots repaired: {repaired}")


def apply_wave9_migrations(conn: _Connection) -> dict[str, Any]:
    """Wave 9 only: re-key option_snapshot by observation time and rebuild its views.

    Requires ownership of ``raw_market.option_snapshot`` (it swaps the table),
    so this runs as the superuser path, not as the plugin's DB role.
    """
    with conn.cursor() as cur:
        result = migrate_option_snapshot_observed_time(cur)
        for stmt in OPTION_SNAPSHOT_VIEW_SQL:
            cur.execute(stmt)
    conn.commit()
    return result


def apply_ddl(conn: _Connection) -> None:
    """Create schemas, tables, indexes, views, and partition helper (idempotent)."""
    with conn.cursor() as cur:
        _create_schemas(cur)
        _create_market_tables(cur)
        _create_data_ops_tables(cur)
        _create_partition_helper(cur)
        migrate_option_open_interest_partitioned(cur)
        migrate_stock_financials_split(cur)
        add_financials_filing_date(cur)
        retire_data_ops_compat_schema(cur)
        migrate_option_snapshot_observed_time(cur)
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
    # "Who has waited longest for a re-enumeration" — one index probe per
    # underlying instead of a scan. option-refresh used to rotate on a hash of
    # the target date, which is the same for all four of its six-hourly runs, so
    # three of them re-fetched the batch the first had just done and the 575-name
    # universe took ~48 days to come round.
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_contract_underlying_updated
        ON raw_market.option_contract (underlying, updated_at DESC)
        """
    )

    # --- option_snapshot ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.option_snapshot (
            option_ticker       text        NOT NULL,
            underlying          text        NOT NULL,
            -- Observation time: 16:00 NY of the session for EOD chains, the
            -- actual observation instant for intraday. NOT the last trade.
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
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS option_snapshot_underlying_ts
        ON raw_market.option_snapshot (underlying, snapshot_ts DESC)
        """
    )

    # --- treasury_yield (Massive /fed/v1/treasury-yields; free with any plan) ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_market.treasury_yield (
            yield_date     date NOT NULL PRIMARY KEY,
            yield_1_month  double precision,
            yield_3_month  double precision,
            yield_1_year   double precision,
            yield_2_year   double precision,
            yield_5_year   double precision,
            yield_10_year  double precision,
            yield_30_year  double precision,
            fetched_at     timestamptz NOT NULL DEFAULT now()
        )
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


def create_coverage_sample(cur: _Cursor) -> None:
    """The coverage matrix's memory. Idempotent, and on the *migration* path.

    Called from ``_create_data_ops_tables`` for a fresh install and from
    ``apply_wave8_migrations`` for a deploy, because those are two different
    paths and only the second one runs in the cluster: the schema Job is
    ``init_schema.py --wave8-only``, so a table added to ``apply_ddl`` alone is
    created nowhere. Measured 2026-09-10 — this table was declared, printed in
    the Job's own summary line, and did not exist, and the API answered
    "relation does not exist" on every write.

    ``ops_jobs`` is the plugin's own schema, so the plugin's role can create
    here; that is why this may sit on the un-privileged path at all.

    Run-length encoded: one row per *change* of the verdict map, not one per
    compute. The page recomputes on a timer and almost every recompute
    reproduces the previous verdicts exactly.

    Recorded forward while continuity is computed on read, and the difference is
    the point: continuity asks a question the rows themselves still answer, a
    verdict asks one only the moment could answer. Freshness divides by how late
    the newest row is *now*; breadth divides by the tier scope as it stood.
    Re-running either tomorrow answers tomorrow's question.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.coverage_sample (
            coverage_sample_id bigserial   PRIMARY KEY,
            first_seen_at      timestamptz NOT NULL DEFAULT now(),
            last_seen_at       timestamptz NOT NULL DEFAULT now(),
            digest             text        NOT NULL,
            verdicts           jsonb       NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS coverage_sample_first_seen
        ON ops_jobs.coverage_sample (first_seen_at DESC, coverage_sample_id DESC)
        """
    )
    cur.execute(
        """
        COMMENT ON TABLE ops_jobs.coverage_sample IS
          'Four-axis verdicts per dataset over time, one row per change. '
          'A verdict cannot be computed backwards, so it is written forward; '
          'continuity, which can, is not recorded here.'
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
    # The claim filters by kind, and the index above does not carry it. Once the
    # pending set is millions of option rows, a pool whose kinds have nothing
    # waiting walks the whole set to prove it — measured >30s for the stocks
    # pool against 3.3M pending, so those workers could never claim at all.
    # Partial on `pending`: the claim only ever reads that status, and the index
    # stays small as finished rows accumulate.
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_pending_kind_priority
        ON ops_jobs.job_ingest (kind, priority DESC, created_at)
        WHERE status = 'pending'
        """
    )
    # The queue dashboard's throughput tile counts what settled in the last
    # minutes. Without this it scanned the whole table and was cancelled, so the
    # tile showed 0 jobs/min while the workers were finishing ~700 a minute.
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_finished_at
        ON ops_jobs.job_ingest (finished_at DESC)
        WHERE status IN ('done', 'failed')
        """
    )
    # The swimlane asks each kind when it last finished and first arrived.
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_kind_created
        ON ops_jobs.job_ingest (kind, created_at)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_kind_finished
        ON ops_jobs.job_ingest (kind, finished_at DESC)
        WHERE finished_at IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_open_status_kind
        ON ops_jobs.job_ingest (status, kind)
        WHERE status IN ('pending', 'running')
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS job_ingest_pending_created
        ON ops_jobs.job_ingest (created_at)
        WHERE status = 'pending'
        """
    )

    # Queue history. job_ingest is a work queue, not a record: the trim caps
    # finished rows at a few tens of thousands, which at 600 jobs a minute is
    # about an hour, and ingest_freshness is keyed by dimension and overwritten.
    # So nothing anywhere held a series, and every question about the queue had
    # to be answered by whatever a probe happened to catch. One row per kind per
    # sample; pending and running are null for rows reconstructed after the
    # fact, because a past queue depth cannot be recovered from finished jobs.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.queue_sample (
            sample_ts             timestamptz NOT NULL,
            kind                  text        NOT NULL,
            pending               bigint,
            running               bigint,
            created_delta         bigint      NOT NULL DEFAULT 0,
            done_delta            bigint      NOT NULL DEFAULT 0,
            failed_delta          bigint      NOT NULL DEFAULT 0,
            oldest_pending_age_sec double precision,
            p50_sec               double precision,
            p95_sec               double precision,
            PRIMARY KEY (sample_ts, kind)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS queue_sample_kind_ts
        ON ops_jobs.queue_sample (kind, sample_ts DESC)
        """
    )
    cur.execute(
        """
        COMMENT ON TABLE ops_jobs.queue_sample IS
          'Queue depth and throughput over time, one row per kind per sample. '
          'The only place a history of ops_jobs.job_ingest survives its trim.'
        """
    )

    create_coverage_sample(cur)

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

    # Per-symbol vendor voids (subscription-focus P2): names the vendor has no
    # statements for, so the fundamentals rotate stops re-trying them daily.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.symbol_source_void (
            symbol        text        NOT NULL,
            data_type     text        NOT NULL,
            checks        integer     NOT NULL DEFAULT 1,
            first_seen    timestamptz NOT NULL DEFAULT now(),
            last_checked  timestamptz NOT NULL DEFAULT now(),
            note          text,
            PRIMARY KEY (symbol, data_type)
        )
        """
    )
    cur.execute(
        """
        COMMENT ON TABLE ops_jobs.symbol_source_void IS
          'Vendor returned nothing for (symbol, data_type); rotate skips it for a month.'
        """
    )

    # Last good watchlist union (subscription-focus P2): the scheduler falls
    # back to it when platform-api is unreachable instead of to an empty list.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_jobs.watchlist_cache (
            symbol      text        PRIMARY KEY,
            source      text        NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )

    # Retired: flat is_trading calendar → derive from raw_market.us_market_holiday.
    cur.execute("DROP TABLE IF EXISTS ops_jobs.us_trading_calendar CASCADE")


# View statements that depend on raw_market.option_snapshot. Kept as constants
# so the Wave 9 migration, which must drop them to swap the table, rebuilds
# exactly what apply_ddl would create — one definition, two callers.
OPTION_SNAPSHOT_VIEW_SQL: tuple[str, ...] = (
    "DROP VIEW IF EXISTS raw_market.v_option_snapshot_with_stock",
    "DROP VIEW IF EXISTS raw_market.v_option_chain_latest",
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
        """,
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
        """,
)


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
    cur.execute(OPTION_SNAPSHOT_VIEW_SQL[2])
    # Bridge for Trade consumers replacing public.option_snapshots_with_underlying_day
    cur.execute(OPTION_SNAPSHOT_VIEW_SQL[3])


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
    cur.execute("SELECT ops_jobs.ensure_year_partitions('raw_market', 'stock_daily', 5, 1)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'stock_minute', 12, 3)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_daily', 12, 3)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_minute', 12, 3)")
    cur.execute("SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_snapshot', 3, 3)")
    cur.execute(
        "SELECT ops_jobs.ensure_month_partitions('raw_market', 'option_open_interest', 12, 3)"
    )
    # option_trades is retired — option trades are not in Options Starter, the
    # slot went in 0.10.3 and the table holds zero rows. Rotating day partitions
    # for a table nothing writes to buys nothing. Restore with the slot.


def ensure_partitions(conn: _Connection) -> None:
    """Extend every partitioned table's forward window.

    Called by ``apply_ddl`` and, nightly, by the trim slot. It used to run only
    inside the DDL, which nothing re-runs on a schedule, so the forward window
    never advanced: on 2026-09-09 four tables had partitions to 2026-12-01 and
    83 days left before inserts would have had nowhere to land. One list, two
    callers — a second copy in the scheduler would be a list that drifts.
    """
    with conn.cursor() as cur:
        # Creating a partition on a large parent takes a lock and builds the
        # indexes; the role's 2s default cancels it. Defence in depth — the
        # callers raise it too, but this must work whoever opens the connection.
        cur.execute("SET LOCAL statement_timeout = '120s'")
        _ensure_partitions(cur)
    conn.commit()


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
    "treasury_yield",
    "ticker_related",
    "ticker_type",
)

# Retired Wave 7 — analytics tables live in features.* (bifrost-research).
MARKET_ANALYTICS_TABLES: tuple[str, ...] = ()

DATA_OPS_TABLES: tuple[str, ...] = (
    "job_ingest",
    "queue_sample",
    "coverage_sample",
    "ingest_freshness",
    "data_source_void",
    "symbol_source_void",
    "watchlist_cache",
)

MARKET_VIEWS: tuple[str, ...] = (
    "v_us_equity_universe",
    "v_option_chain_latest",
    "v_option_snapshot_with_stock",
    "stock_financials",
)


#: The schemas the plugin owns (spine D13). features.* belongs to
#: bifrost-research and dw_stock.* to its dbt models; ownership never reaches
#: them.
PLUGIN_SCHEMAS: tuple[str, ...] = ("raw_market", "ops_jobs")

#: The role the plugin connects as, and therefore the role that must own the
#: plugin's objects. Postgres checks ownership, not grants, for DDL.
PLUGIN_ROLE = "bifrost"


def ownership_statements(role: str = PLUGIN_ROLE) -> tuple[str, ...]:
    """SQL making ``role`` the owner of everything in the plugin's schemas.

    Ownership is part of the schema, not an operational afterthought: an object
    the plugin does not own is one it cannot drop, re-create, or partition. The
    162 relations in ``raw_market`` are owned by ``postgres`` only because the
    first migration happened to run as that role, and the consequence is dated —
    ``ensure_month_partitions`` builds three months ahead, so from 2026-10 it
    fails, and inserts for 2027-01 have nowhere to land.

    Returned rather than executed because applying it needs a role that owns the
    objects, which the plugin's own role by definition does not. Same shape as
    ``--wave9-sql``: the tool generates it, a privileged session runs it.

    Idempotent — an object already owned by ``role`` is skipped.
    """
    if not role.isidentifier():
        raise ValueError(f"role must be a bare identifier, got {role!r}")
    schemas = ", ".join(f"'{s}'" for s in PLUGIN_SCHEMAS)
    return (
        f"""
        DO $ownership$
        DECLARE
          target_role CONSTANT text := '{role}';
          obj     record;
          changed int := 0;
          skipped int := 0;
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_role) THEN
            RAISE EXCEPTION 'role % does not exist; apply create_roles.sql first', target_role;
          END IF;

          FOR obj IN
            SELECT n.nspname AS schema_name, c.relname AS object_name, c.relkind,
                   pg_get_userbyid(c.relowner) AS current_owner
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ({schemas}) AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
            ORDER BY n.nspname, c.relname
          LOOP
            IF obj.current_owner = target_role THEN
              skipped := skipped + 1;
              CONTINUE;
            END IF;
            -- ALTER TABLE covers ordinary tables, partitions and partitioned
            -- tables; sequences and views need their own verbs.
            IF obj.relkind = 'S' THEN
              EXECUTE format('ALTER SEQUENCE %I.%I OWNER TO %I',
                             obj.schema_name, obj.object_name, target_role);
            ELSIF obj.relkind = 'v' THEN
              EXECUTE format('ALTER VIEW %I.%I OWNER TO %I',
                             obj.schema_name, obj.object_name, target_role);
            ELSIF obj.relkind = 'm' THEN
              EXECUTE format('ALTER MATERIALIZED VIEW %I.%I OWNER TO %I',
                             obj.schema_name, obj.object_name, target_role);
            ELSE
              EXECUTE format('ALTER TABLE %I.%I OWNER TO %I',
                             obj.schema_name, obj.object_name, target_role);
            END IF;
            changed := changed + 1;
          END LOOP;
          RAISE NOTICE 'relations: % reassigned, % already owned by %',
                       changed, skipped, target_role;

          changed := 0;
          skipped := 0;
          FOR obj IN
            SELECT p.oid::regprocedure AS signature,
                   pg_get_userbyid(p.proowner) AS current_owner
            FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname IN ({schemas})
            ORDER BY 1
          LOOP
            IF obj.current_owner = target_role THEN
              skipped := skipped + 1;
              CONTINUE;
            END IF;
            EXECUTE format('ALTER FUNCTION %s OWNER TO %I', obj.signature, target_role);
            changed := changed + 1;
          END LOOP;
          RAISE NOTICE 'functions: % reassigned, % already owned by %',
                       changed, skipped, target_role;

          FOR obj IN
            SELECT n.nspname AS schema_name, pg_get_userbyid(n.nspowner) AS current_owner
            FROM pg_namespace n WHERE n.nspname IN ({schemas})
          LOOP
            IF obj.current_owner <> target_role THEN
              EXECUTE format('ALTER SCHEMA %I OWNER TO %I', obj.schema_name, target_role);
              RAISE NOTICE 'schema % reassigned to %', obj.schema_name, target_role;
            END IF;
          END LOOP;
        END
        $ownership$
        """.strip(),
        # Objects the new owner creates from here must stay reachable by the
        # consumers. Ownership does not change existing grants; this covers the
        # ones made after it.
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA raw_market "
        "GRANT SELECT ON TABLES TO market_reader",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA raw_market "
        "GRANT ALL ON TABLES TO data_writer",
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA ops_jobs "
        "GRANT ALL ON TABLES TO data_writer",
    )


def apply_ownership(conn: _Connection, role: str = PLUGIN_ROLE) -> None:
    """Run ``ownership_statements``. Needs a role that owns the objects."""
    with conn.cursor() as cur:
        for stmt in ownership_statements(role):
            cur.execute(stmt)
    conn.commit()
