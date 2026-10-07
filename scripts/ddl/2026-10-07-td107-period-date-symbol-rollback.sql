-- TD-107 rollback. Not inside a transaction (no psql -1).
-- Leaves short_volume without the index the four-axis read needs; only run
-- this to undo a build that failed invalid (pg_index.indisvalid = false)
-- or because the Owner rejected the change before anything read the new indexes.

DROP INDEX CONCURRENTLY IF EXISTS raw_market.income_statement_period_date_symbol;
DROP INDEX CONCURRENTLY IF EXISTS raw_market.balance_sheet_period_date_symbol;
DROP INDEX CONCURRENTLY IF EXISTS raw_market.cash_flow_period_date_symbol;
DROP INDEX CONCURRENTLY IF EXISTS raw_market.ratios_period_date_symbol;
DROP INDEX CONCURRENTLY IF EXISTS raw_market.short_interest_period_date_symbol;
DROP INDEX CONCURRENTLY IF EXISTS raw_market.short_volume_period_date_symbol;
