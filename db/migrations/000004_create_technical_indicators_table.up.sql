CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    indicator_type VARCHAR(50) NOT NULL,
    value DECIMAL(18, 4) NOT NULL,
    timeframe VARCHAR(10) DEFAULT 'daily',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date, indicator_type, timeframe)
);

CREATE INDEX idx_indicators_symbol ON technical_indicators(symbol);
CREATE INDEX idx_indicators_date ON technical_indicators(date);
CREATE INDEX idx_indicators_type ON technical_indicators(indicator_type);
CREATE INDEX idx_indicators_symbol_date_type ON technical_indicators(symbol, date DESC, indicator_type);

-- Common indicator types:
-- RSI_14, MACD, MACD_SIGNAL, MACD_HIST, SMA_20, SMA_50, SMA_200
-- EMA_12, EMA_26, BB_UPPER, BB_MIDDLE, BB_LOWER, ATR_14
-- STOCH_K, STOCH_D, ADX, OBV