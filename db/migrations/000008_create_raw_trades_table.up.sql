-- Raw trades table: stores every individual execution from brokers
CREATE TABLE IF NOT EXISTS raw_trades (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'manual',
    symbol VARCHAR(10) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity DECIMAL(18, 8) NOT NULL,
    price DECIMAL(18, 4) NOT NULL,
    total_cost DECIMAL(18, 4) NOT NULL,
    fees DECIMAL(18, 4) DEFAULT 0,
    executed_at TIMESTAMP NOT NULL,

    -- Links to aggregated tables (set during aggregation)
    position_id INTEGER REFERENCES positions(id) ON DELETE SET NULL,
    trade_history_id INTEGER REFERENCES trades_history(id) ON DELETE SET NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint for deduplication (same order from same source)
    UNIQUE(order_id, source)
);

-- Indexes for common queries
CREATE INDEX idx_raw_trades_symbol ON raw_trades(symbol);
CREATE INDEX idx_raw_trades_executed_at ON raw_trades(executed_at);
CREATE INDEX idx_raw_trades_source ON raw_trades(source);
CREATE INDEX idx_raw_trades_side ON raw_trades(side);
CREATE INDEX idx_raw_trades_position_id ON raw_trades(position_id);
CREATE INDEX idx_raw_trades_trade_history_id ON raw_trades(trade_history_id);
