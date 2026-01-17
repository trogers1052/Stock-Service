-- Watchlist with buy zones and targets
CREATE TABLE IF NOT EXISTS monitored_stocks (
    symbol VARCHAR(10) PRIMARY KEY REFERENCES stocks(symbol) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT true,
    priority INTEGER DEFAULT 1, -- 1=high, 2=medium, 3=low
    
    -- Your buy zones and targets
    buy_zone_low DECIMAL(18,4),
    buy_zone_high DECIMAL(18,4),
    target_price DECIMAL(18,4),
    stop_loss_price DECIMAL(18,4),
    
    -- Alert settings
    alert_on_buy_zone BOOLEAN DEFAULT true,
    alert_on_rsi_oversold BOOLEAN DEFAULT false,
    rsi_oversold_threshold DECIMAL(5,2) DEFAULT 30,
    
    notes TEXT,
    reason TEXT, -- Why watching this stock?
    
    added_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_monitored_stocks_enabled ON monitored_stocks(enabled) WHERE enabled = true;
CREATE INDEX idx_monitored_stocks_priority ON monitored_stocks(priority, enabled);