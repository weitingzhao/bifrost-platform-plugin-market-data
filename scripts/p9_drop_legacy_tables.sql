-- P9: Drop legacy public.* market tables superseded by market.* / data_ops.*.
-- Prerequisites:
--   1. Trade API / core / worker no longer query these tables (S1–S4).
--   2. IB OHLC writes target market.stock_daily / market.stock_minute (S2).
--   3. Run against bifrost_dev first; verify; then bifrost_prod.
--
-- Mapping (see docs/SCHEMA.md):
--   stock_day              → market.stock_daily
--   stock_min              → market.stock_minute
--   option_day             → market.option_daily
--   option_min             → market.option_minute
--   option_contracts       → market.option_contract
--   option_snapshots       → market.option_snapshot
--   option_expiration_cache→ market.option_expiration
--   option_open_interest_daily → market.option_open_interest
--   tickers + ticker_overview → market.ticker
--   massive_corporate_action → market.corporate_action
--   job_massive_backfill   → data_ops.job_ingest
--   stock_* fundamentals   → market.stock_financials
--
-- NOT dropped (Trade / Research owned):
--   stock_readiness_daily, cache_stock_snapshot, report_option_*,
--   option_trades, ticker_types,
--   job_bars_backfill, job_sepa_phase4, watchlist
--
-- Dropped in holiday migration wave:
--   reference_us_holidays → market.us_market_holiday (Golden Source)
--
-- Dropped in related-tickers migration wave:
--   ticker_related_tickers → market.ticker_related (Golden Source / FDW)

BEGIN;

DROP TABLE IF EXISTS public.stock_day CASCADE;
DROP TABLE IF EXISTS public.stock_min CASCADE;
DROP TABLE IF EXISTS public.option_day CASCADE;
DROP TABLE IF EXISTS public.option_min CASCADE;
DROP TABLE IF EXISTS public.option_contracts CASCADE;
DROP TABLE IF EXISTS public.option_snapshots CASCADE;
DROP TABLE IF EXISTS public.option_expiration_cache CASCADE;
DROP TABLE IF EXISTS public.option_open_interest_daily CASCADE;
DROP TABLE IF EXISTS public.tickers CASCADE;
DROP TABLE IF EXISTS public.ticker_overview CASCADE;
DROP TABLE IF EXISTS public.massive_corporate_action CASCADE;
DROP TABLE IF EXISTS public.job_massive_backfill CASCADE;
DROP TABLE IF EXISTS public.reference_us_holidays CASCADE;
DROP TABLE IF EXISTS public.ticker_related_tickers CASCADE;
DROP TABLE IF EXISTS public.stock_related_tickers CASCADE;

-- Legacy flat fundamentals (names vary by historical DDL; IF EXISTS is safe)
DROP TABLE IF EXISTS public.stock_financial_balance_sheet CASCADE;
DROP TABLE IF EXISTS public.stock_financial_income_statement CASCADE;
DROP TABLE IF EXISTS public.stock_financial_cash_flow CASCADE;
DROP TABLE IF EXISTS public.stock_financial_comprehensive_income CASCADE;
DROP TABLE IF EXISTS public.stock_financial_ratios CASCADE;
DROP TABLE IF EXISTS public.stock_financial_overview CASCADE;
DROP TABLE IF EXISTS public.stock_income_statements CASCADE;
DROP TABLE IF EXISTS public.stock_balance_sheets CASCADE;
DROP TABLE IF EXISTS public.stock_cash_flows CASCADE;
DROP TABLE IF EXISTS public.stock_ratios CASCADE;
DROP TABLE IF EXISTS public.stock_short_interest CASCADE;
DROP TABLE IF EXISTS public.stock_short_volume CASCADE;

DROP SEQUENCE IF EXISTS public.option_snapshots_option_snapshots_id_seq CASCADE;

COMMIT;

-- Recreate Trade-facing SEPA/universe views on market.* (CASCADE may have dropped them).
-- Prefer also running bifrost-trade-core DDL ensure (same view SQL).
DO $$
BEGIN
  IF to_regclass('market.ticker') IS NOT NULL THEN
    EXECUTE $sql$
    CREATE OR REPLACE VIEW public.v_us_equity_universe AS
    SELECT
        NULL::bigint AS tickers_id,
        upper(trim(t.symbol)) AS symbol,
        t.name, t.market, t.locale, t.primary_exchange, t.instrument_type, t.active,
        NULL::timestamptz AS delisted_utc,
        t.list_date, t.sector, t.industry
    FROM market.ticker t
    WHERE COALESCE(t.active, false) = true
      AND lower(COALESCE(t.locale, '')) = 'us'
      AND lower(COALESCE(t.market, '')) = 'stocks'
      AND lower(COALESCE(t.instrument_type, '')) = 'cs'
    $sql$;
    EXECUTE 'CREATE OR REPLACE VIEW public.v_sepa_us_equity_universe AS SELECT * FROM public.v_us_equity_universe';
  END IF;
  IF to_regclass('market.stock_daily') IS NOT NULL THEN
    EXECUTE $sql$
    CREATE OR REPLACE VIEW public.v_sepa_symbol_price_readiness AS
    WITH params AS (
        SELECT 'polygon'::text AS price_source,
               (CURRENT_DATE - integer '420') AS window_start,
               CURRENT_DATE AS as_of_date,
               240::integer AS min_bar_rows,
               7::integer AS max_stale_calendar_days
    )
    SELECT p.as_of_date, upper(trim(sd.symbol)) AS symbol, p.price_source,
           count(*)::integer AS bar_rows,
           min(sd.bar_date)::date AS first_bar_date,
           max(sd.bar_date)::date AS last_bar_date,
           count(*) FILTER (WHERE sd.close IS NULL)::integer AS null_close_rows,
           count(*) FILTER (WHERE sd.volume IS NULL)::integer AS null_volume_rows,
           (count(*) >= p.min_bar_rows
            AND max(sd.bar_date) >= (p.as_of_date - (p.max_stale_calendar_days || ' days')::interval)::date
            AND count(*) FILTER (WHERE sd.close IS NULL) = 0
            AND count(*) FILTER (WHERE sd.volume IS NULL) = 0) AS price_ready
    FROM params p
    JOIN market.stock_daily sd
      ON sd.bar_date >= p.window_start AND sd.bar_date <= p.as_of_date
    GROUP BY p.as_of_date, p.price_source, p.min_bar_rows, p.max_stale_calendar_days,
             p.window_start, upper(trim(sd.symbol))
    $sql$;
  END IF;
END $$;

-- After DROP: enqueue ticker_sync if market.ticker is empty:
--   INSERT INTO data_ops.job_ingest (kind, payload, status, priority, payload_hash)
--   VALUES ('ticker_sync', '{"mode":"universe"}'::jsonb, 'pending', 50, 'post-p9-ticker-sync');

-- Post-check (expect 0 for market-data legacy names; stock_readiness_* may remain):
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'public'
--   AND table_name IN (
--     'stock_day','stock_min','option_day','option_min','option_contracts',
--     'option_snapshots','option_expiration_cache','option_open_interest_daily',
--     'tickers','ticker_overview','massive_corporate_action','job_massive_backfill',
--     'reference_us_holidays'
--   );
