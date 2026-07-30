-- Migrate public.ticker_related_tickers from from_tickers_id → from_symbol.
-- MUST run BEFORE p9_drop_legacy_tables.sql (needs public.tickers).
-- Idempotent: no-op if from_symbol already present and from_tickers_id gone.

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'ticker_related_tickers'
      AND column_name = 'from_tickers_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'ticker_related_tickers'
      AND column_name = 'from_symbol'
  ) THEN
    ALTER TABLE public.ticker_related_tickers
      ADD COLUMN from_symbol text;

    -- Legacy public.tickers uses column "ticker" (not "symbol").
    UPDATE public.ticker_related_tickers r
    SET from_symbol = upper(trim(t.ticker))
    FROM public.tickers t
    WHERE r.from_tickers_id = t.tickers_id;

    DELETE FROM public.ticker_related_tickers
    WHERE from_symbol IS NULL OR trim(from_symbol) = '';

    ALTER TABLE public.ticker_related_tickers
      ALTER COLUMN from_symbol SET NOT NULL;

    ALTER TABLE public.ticker_related_tickers
      DROP COLUMN from_tickers_id;

    RAISE NOTICE 'Migrated ticker_related_tickers to from_symbol';
  ELSIF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'ticker_related_tickers'
      AND column_name = 'from_symbol'
  ) THEN
    RAISE NOTICE 'ticker_related_tickers already has from_symbol — skip';
  ELSE
    RAISE NOTICE 'ticker_related_tickers missing or unexpected schema — skip';
  END IF;
END $$;

COMMIT;
