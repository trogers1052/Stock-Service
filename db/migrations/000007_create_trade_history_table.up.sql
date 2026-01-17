CREATE TABLE IF NOT EXISTS trades_history (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    trade_type VARCHAR(4) NOT NULL CHECK (trade_type IN ('BUY', 'SELL')),
    quantity DECIMAL(18, 8) NOT NULL,
    price DECIMAL(18, 4) NOT NULL,
    total_cost DECIMAL(18, 4) NOT NULL,
    fee DECIMAL(18, 4) DEFAULT 0,
    entry_date TIMESTAMP,
    exit_date TIMESTAMP,
    holding_period_hours INTEGER,
    entry_rsi DECIMAL(5, 2),
    exit_rsi DECIMAL(5, 2),
    realized_pnl DECIMAL(18, 4),
    realized_pnl_pct DECIMAL(10, 4),
    max_drawdown_pct DECIMAL(10, 4),
    entry_reason TEXT,
    exit_reason TEXT,
    emotional_state INTEGER CHECK (emotional_state BETWEEN 1 AND 10),
    conviction_level INTEGER CHECK (conviction_level BETWEEN 1 AND 10),
    market_conditions TEXT,
    what_went_right TEXT,
    what_went_wrong TEXT,
    trade_grade CHAR(1) CHECK (trade_grade IN ('A', 'B', 'C', 'D', 'F')),
    strategy_tag VARCHAR(50),
    notes TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trades_symbol ON trades(symbol);
CREATE INDEX idx_trades_executed_at ON trades(executed_at);
CREATE INDEX idx_trades_trade_type ON trades(trade_type);
CREATE INDEX idx_trades_strategy_tag ON trades(strategy_tag);