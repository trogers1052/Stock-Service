DROP INDEX IF EXISTS idx_monitored_stocks_enabled;
DROP INDEX IF EXISTS idx_monitored_stocks_stock_id;
DROP TABLE IF EXISTS monitored_stocks;

DROP INDEX IF EXISTS idx_stocks_sector;
DROP INDEX IF EXISTS idx_stocks_last_updated;
DROP INDEX IF EXISTS idx_stocks_symbol;
DROP TABLE IF EXISTS stocks;