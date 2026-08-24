-- PG roles for Market Data Subcontractor (idempotent-ish).
-- Run as a superuser / database owner against bifrost_golden_source.
-- Replace CHANGE_ME passwords before applying in any shared environment.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'data_writer') THEN
    CREATE ROLE data_writer WITH LOGIN PASSWORD 'CHANGE_ME_data_writer';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'market_reader') THEN
    CREATE ROLE market_reader WITH LOGIN PASSWORD 'CHANGE_ME_market_reader';
  END IF;
END
$$;

-- Schemas must already exist (make db-init / scripts/init_schema.py).
GRANT USAGE, CREATE ON SCHEMA raw_market TO data_writer;
GRANT USAGE, CREATE ON SCHEMA ops_jobs TO data_writer;
GRANT ALL ON ALL TABLES IN SCHEMA raw_market TO data_writer;
GRANT ALL ON ALL TABLES IN SCHEMA ops_jobs TO data_writer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA raw_market TO data_writer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ops_jobs TO data_writer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ops_jobs TO data_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw_market
  GRANT ALL ON TABLES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops_jobs
  GRANT ALL ON TABLES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_market
  GRANT ALL ON SEQUENCES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops_jobs
  GRANT ALL ON SEQUENCES TO data_writer;

GRANT USAGE ON SCHEMA raw_market TO market_reader;
-- Wave 7: features.* owned by bifrost-research; Plugin API reads analytics from features.
GRANT USAGE ON SCHEMA features TO market_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_market TO market_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA features TO market_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_market
  GRANT SELECT ON TABLES TO market_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA features
  GRANT SELECT ON TABLES TO market_reader;

-- P9 lockdown: data_writer must not write Trade / public business tables.
-- Revoke blanket public privileges, then re-grant SELECT-only on watchlist
-- (scheduler loads symbols from Trade public.watchlist).
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM data_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM data_writer;
REVOKE CREATE ON SCHEMA public FROM data_writer;

DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'watchlist'
  ) THEN
    EXECUTE 'GRANT SELECT ON public.watchlist TO data_writer';
    RAISE NOTICE 'Granted SELECT on public.watchlist to data_writer (P9 lockdown)';
  ELSE
    RAISE NOTICE 'public.watchlist not found — skipping GRANT (apply Trade DDL first)';
  END IF;
END $$;

-- Optional: allow readers to see job status (not write)
GRANT USAGE ON SCHEMA ops_jobs TO market_reader;
GRANT SELECT ON ops_jobs.job_ingest TO market_reader;
GRANT SELECT ON ops_jobs.ingest_freshness TO market_reader;
