-- P7 (market-data-expand): Drop legacy public report_option_* tables.
-- Replaced by Plugin market_analytics.* (max_pain_daily / atm_iv_daily).
-- Prerequisites:
--   1. Trade core ddl.py no longer CREATE TABLE these (bifrost-core >= 0.5.3).
--   2. Trade worker/API no longer write or read these tables.
--   3. Run against bifrost_dev first; verify; then bifrost_prod if needed.
--
-- Manual:
--   PGPASSWORD=... psql -h 192.168.10.73 -p 30432 -U bifrost -d bifrost_dev \
--     -f scripts/p7_drop_legacy_tables.sql

BEGIN;

DROP TABLE IF EXISTS public.report_option_max_pain_daily CASCADE;
DROP TABLE IF EXISTS public.report_option_atm_iv_daily CASCADE;

COMMIT;
