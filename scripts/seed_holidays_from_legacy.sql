-- Seed market.us_market_holiday from legacy public.reference_us_holidays.
--
-- Prerequisites:
--   1. market.us_market_holiday exists on bifrost_golden_source (apply Plugin DDL).
--   2. Source rows live on a Trade per-env DB (bifrost_dev / bifrost_prod).
--
-- Same-cluster / same-server (dblink):
--   Replace host/port/dbname/user/password below, then:
--     psql -d bifrost_golden_source -f scripts/seed_holidays_from_legacy.sql
--
-- Cross-host fallback:
--   On Trade DB:
--     \copy (SELECT exchange, holiday_date, COALESCE(name, label) AS name,
--                   COALESCE(status, 'closed') AS status, open_time, close_time,
--                   COALESCE(updated_at, now()) AS fetched_at
--            FROM public.reference_us_holidays) TO '/tmp/holidays.csv' CSV HEADER
--   On Golden Source:
--     \copy market.us_market_holiday (exchange, holiday_date, name, status,
--           open_time, close_time, fetched_at) FROM '/tmp/holidays.csv' CSV HEADER

CREATE EXTENSION IF NOT EXISTS dblink;

-- Adjust connection string to the Trade DB that still holds reference_us_holidays.
-- Example: host=192.168.10.73 port=30432 dbname=bifrost_dev user=bifrost password=***
DO $$
DECLARE
  src_conn text := current_setting('bifrost.seed_holidays_src_conn', true);
BEGIN
  IF src_conn IS NULL OR btrim(src_conn) = '' THEN
    RAISE EXCEPTION
      'Set bifrost.seed_holidays_src_conn before running, e.g. '
      'SET bifrost.seed_holidays_src_conn = '
      '''host=192.168.10.73 port=30432 dbname=bifrost_dev user=bifrost password=***''';
  END IF;

  INSERT INTO market.us_market_holiday (
      exchange, holiday_date, name, status, open_time, close_time, fetched_at
  )
  SELECT
      exchange,
      holiday_date,
      name,
      status,
      open_time,
      close_time,
      fetched_at
  FROM dblink(
      src_conn,
      $q$
        SELECT
            exchange,
            holiday_date,
            COALESCE(name, label) AS name,
            COALESCE(status, 'closed') AS status,
            open_time,
            close_time,
            COALESCE(updated_at, now()) AS fetched_at
        FROM public.reference_us_holidays
      $q$
  ) AS t(
      exchange text,
      holiday_date date,
      name text,
      status text,
      open_time timestamptz,
      close_time timestamptz,
      fetched_at timestamptz
  )
  ON CONFLICT (exchange, holiday_date) DO UPDATE SET
      name = EXCLUDED.name,
      status = EXCLUDED.status,
      open_time = EXCLUDED.open_time,
      close_time = EXCLUDED.close_time,
      fetched_at = EXCLUDED.fetched_at;
END $$;
