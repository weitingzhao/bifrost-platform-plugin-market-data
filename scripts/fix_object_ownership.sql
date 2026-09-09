-- Give the plugin's role ownership of the plugin's own schemas.
--
-- Run as a superuser / database owner against bifrost_golden_source:
--     psql "$POSTGRES_ADMIN_URL" -f scripts/fix_object_ownership.sql
-- or  make apply-ownership   (with elevated POSTGRES_* in the environment)
--
-- WHY
-- ---
-- raw_market.* and ops_jobs.* are the Market Data Plugin's schemas (spine D13),
-- but 23 tables and 134 partitions in raw_market are owned by `postgres` — the
-- role the first schema migration happened to run as. Ownership, not grants, is
-- what Postgres checks for DDL, so the plugin's own role cannot:
--
--   * drop a partition past its retention window
--       "must be owner of table option_snapshot_y2025m09"
--   * create the next month's partition
--       "must be owner of table option_snapshot"
--   * re-apply its own DDL (CREATE OR REPLACE on an object it does not own)
--
-- The first is cosmetic — retention deletes the rows instead. The second is
-- dated: partitions run to 2026-12, `ensure_month_partitions` builds three
-- months ahead, so from 2026-10 it starts failing, and inserts for 2027-01 have
-- nowhere to land. This script is how that is avoided.
--
-- WHAT IT DOES NOT TOUCH
-- ----------------------
-- Only raw_market and ops_jobs. features.* belongs to bifrost-research and
-- dw_stock.* to its dbt models; nothing here reaches them.
--
-- Idempotent: an object already owned by the target role is skipped, so a
-- second run reports zero changes.

\set ON_ERROR_STOP on

DO $$
DECLARE
  target_role CONSTANT text := 'bifrost';
  obj         record;
  changed     int := 0;
  skipped     int := 0;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = target_role) THEN
    RAISE EXCEPTION 'role % does not exist; run scripts/create_roles.sql first', target_role;
  END IF;

  -- Tables, partitions, views and sequences.
  FOR obj IN
    SELECT n.nspname AS schema_name,
           c.relname AS object_name,
           c.relkind,
           pg_get_userbyid(c.relowner) AS current_owner
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('raw_market', 'ops_jobs')
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
    ORDER BY n.nspname, c.relname
  LOOP
    IF obj.current_owner = target_role THEN
      skipped := skipped + 1;
      CONTINUE;
    END IF;
    -- ALTER TABLE covers ordinary tables, partitions and partitioned tables;
    -- views and sequences need their own verbs.
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

  RAISE NOTICE 'relations: % reassigned to %, % already owned', changed, target_role, skipped;

  -- The partition helpers and every other function the plugin re-creates from
  -- its own DDL. Without these, CREATE OR REPLACE FUNCTION fails as the plugin.
  changed := 0;
  skipped := 0;
  FOR obj IN
    SELECT n.nspname AS schema_name,
           p.oid::regprocedure AS signature,
           pg_get_userbyid(p.proowner) AS current_owner
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname IN ('raw_market', 'ops_jobs')
    ORDER BY 1, 2
  LOOP
    IF obj.current_owner = target_role THEN
      skipped := skipped + 1;
      CONTINUE;
    END IF;
    EXECUTE format('ALTER FUNCTION %s OWNER TO %I', obj.signature, target_role);
    changed := changed + 1;
  END LOOP;

  RAISE NOTICE 'functions: % reassigned to %, % already owned', changed, target_role, skipped;

  -- Schemas themselves, so the role can create new objects in them.
  FOR obj IN
    SELECT n.nspname AS schema_name, pg_get_userbyid(n.nspowner) AS current_owner
    FROM pg_namespace n
    WHERE n.nspname IN ('raw_market', 'ops_jobs')
  LOOP
    IF obj.current_owner <> target_role THEN
      EXECUTE format('ALTER SCHEMA %I OWNER TO %I', obj.schema_name, target_role);
      RAISE NOTICE 'schema % reassigned to %', obj.schema_name, target_role;
    END IF;
  END LOOP;
END
$$;

-- The grants the consumers rely on are independent of ownership and unchanged,
-- but re-assert them so a future object created by the new owner is reachable.
ALTER DEFAULT PRIVILEGES FOR ROLE bifrost IN SCHEMA raw_market
  GRANT SELECT ON TABLES TO market_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE bifrost IN SCHEMA raw_market
  GRANT ALL ON TABLES TO data_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE bifrost IN SCHEMA ops_jobs
  GRANT ALL ON TABLES TO data_writer;

-- What the plugin could not do before, and can now.
SELECT n.nspname AS schema_name,
       pg_get_userbyid(c.relowner) AS owner,
       count(*) AS relations
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('raw_market', 'ops_jobs')
  AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
GROUP BY 1, 2
ORDER BY 1, 2;
