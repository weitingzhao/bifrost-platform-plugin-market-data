-- PG roles for Market Data Subcontractor (idempotent-ish).
-- Run as a superuser / database owner against bifrost_dev or bifrost_prod.
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
GRANT USAGE, CREATE ON SCHEMA market TO data_writer;
GRANT USAGE, CREATE ON SCHEMA data_ops TO data_writer;
GRANT ALL ON ALL TABLES IN SCHEMA market TO data_writer;
GRANT ALL ON ALL TABLES IN SCHEMA data_ops TO data_writer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA market TO data_writer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA data_ops TO data_writer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA data_ops TO data_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA market
  GRANT ALL ON TABLES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_ops
  GRANT ALL ON TABLES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA market
  GRANT ALL ON SEQUENCES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_ops
  GRANT ALL ON SEQUENCES TO data_writer;

GRANT USAGE ON SCHEMA market TO market_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA market TO market_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA market
  GRANT SELECT ON TABLES TO market_reader;

-- Optional: allow readers to see job status (not write)
GRANT USAGE ON SCHEMA data_ops TO market_reader;
GRANT SELECT ON data_ops.job_ingest TO market_reader;
GRANT SELECT ON data_ops.ingest_freshness TO market_reader;
GRANT SELECT ON data_ops.us_trading_calendar TO market_reader;
