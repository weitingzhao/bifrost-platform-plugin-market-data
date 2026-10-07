-- TD-107: (period_date, symbol) on the six financials entity tables.
--
-- Live Golden Source (read 2026-10-07, replica): only short_volume_period_date_symbol
-- exists. The other five tables have pkey + filing_date. symbol_period_date was
-- declared in code and is not created; the primary key already leads with symbol.
-- Do not run this file inside a transaction (no psql -1). CREATE INDEX
-- CONCURRENTLY takes ShareUpdateExclusiveLock and allows reads and writes.
-- short_volume is about 5 GB; the build can take minutes. IF NOT EXISTS makes
-- the short_volume statement a no-op.
--
-- Rollback: scripts/ddl/2026-10-07-td107-period-date-symbol-rollback.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS income_statement_period_date_symbol
    ON raw_market.income_statement (period_date, symbol);

CREATE INDEX CONCURRENTLY IF NOT EXISTS balance_sheet_period_date_symbol
    ON raw_market.balance_sheet (period_date, symbol);

CREATE INDEX CONCURRENTLY IF NOT EXISTS cash_flow_period_date_symbol
    ON raw_market.cash_flow (period_date, symbol);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ratios_period_date_symbol
    ON raw_market.ratios (period_date, symbol);

CREATE INDEX CONCURRENTLY IF NOT EXISTS short_interest_period_date_symbol
    ON raw_market.short_interest (period_date, symbol);

CREATE INDEX CONCURRENTLY IF NOT EXISTS short_volume_period_date_symbol
    ON raw_market.short_volume (period_date, symbol);
