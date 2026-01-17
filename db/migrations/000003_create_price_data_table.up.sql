CREATE TABLE IF NOT EXISTS price_data_daily (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(18, 4) NOT NULL,
    high DECIMAL(18, 4) NOT NULL,
    low DECIMAL(18, 4) NOT NULL,
    close DECIMAL(18, 4) NOT NULL,
    volume BIGINT NOT NULL,
    vwap DECIMAL(18, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

CREATE INDEX idx_price_data_symbol ON price_data_daily(symbol);
CREATE INDEX idx_price_data_date ON price_data_daily(date);
CREATE INDEX idx_price_data_symbol_date ON price_data_daily(symbol, date DESC);