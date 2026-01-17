CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    entry_price DECIMAL(18, 4) NOT NULL,
    entry_date TIMESTAMP NOT NULL,
    current_price DECIMAL(18, 4),
    unrealized_pnl_pct DECIMAL(10, 4),
    days_held INTEGER,
    entry_rsi DECIMAL(5, 2),
    entry_reason TEXT,
    sector VARCHAR(50),
    industry VARCHAR(100),
    position_size_pct DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol)
);

CREATE INDEX idx_positions_symbol ON positions(symbol);
CREATE INDEX idx_positions_entry_date ON positions(entry_date);