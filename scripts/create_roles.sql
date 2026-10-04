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
GRANT ALL ON ALL SEQUENCES IN SCHEMA raw_market TO data_writer;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ops_jobs TO data_writer;
-- ops_jobs is shared with the Flex Query plugin (D6, 2026-10-04: owners split per
-- plugin). data_writer gets the market tables (schema/ddl.py DATA_OPS_TABLES)
-- and their sequences, never job_flex_ingest / flex_*.
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['job_ingest', 'queue_sample', 'coverage_sample', 'ingest_freshness',
                           'data_source_void', 'symbol_source_void', 'watchlist_cache']
  LOOP
    IF to_regclass(format('ops_jobs.%I', t)) IS NOT NULL THEN
      EXECUTE format('GRANT ALL ON ops_jobs.%I TO data_writer', t);
    END IF;
  END LOOP;
  FOR t IN
    SELECT format('%I.%I', sn.nspname, s.relname)
    FROM pg_depend d
    JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
    JOIN pg_namespace sn ON sn.oid = s.relnamespace
    JOIN pg_class tb ON tb.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = tb.relnamespace
    WHERE d.deptype IN ('a', 'i') AND tn.nspname = 'ops_jobs'
      AND tb.relname IN ('job_ingest', 'queue_sample', 'coverage_sample', 'ingest_freshness',
                         'data_source_void', 'symbol_source_void', 'watchlist_cache')
  LOOP
    EXECUTE format('GRANT ALL ON SEQUENCE %s TO data_writer', t);
  END LOOP;
END
$$;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw_market
  GRANT ALL ON TABLES TO data_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_market
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
-- Revoke blanket public privileges. The scheduler reads the watchlist from the
-- Platform union (2026-10-03), never from a table, so nothing is re-granted.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM data_writer;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM data_writer;
REVOKE CREATE ON SCHEMA public FROM data_writer;


-- Optional: allow readers to see job status (not write)
GRANT USAGE ON SCHEMA ops_jobs TO market_reader;
GRANT SELECT ON ops_jobs.job_ingest TO market_reader;
GRANT SELECT ON ops_jobs.ingest_freshness TO market_reader;
