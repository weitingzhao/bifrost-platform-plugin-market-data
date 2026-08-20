-- Retire Plugin-internal flat trading calendar.
-- Trading days are derived from market.us_market_holiday (weekday − NYSE closed).
-- Safe to run after market-data image >= 0.5.0 is rolled out (readers no longer query this table).

DROP TABLE IF EXISTS data_ops.us_trading_calendar CASCADE;
