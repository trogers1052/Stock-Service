-- Core stock information
CREATE TABLE stocks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    exchange VARCHAR(50),
    sector VARCHAR(100),
    industry VARCHAR(100),
    
    -- Current price data
    current_price DECIMAL(10,2),
    previous_close DECIMAL(10,2),
    change_amount DECIMAL(10,2),
    change_percent DECIMAL(8,4),
    
    -- Day range
    day_high DECIMAL(10,2),
    day_low DECIMAL(10,2),
    
    -- Volume
    volume BIGINT,
    average_volume BIGINT,
    
    -- 52-week range
    week_52_high DECIMAL(10,2),
    week_52_low DECIMAL(10,2),
    
    -- Market data
    market_cap BIGINT,
    shares_outstanding BIGINT,
    
    -- Timestamps
    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for fast lookups
CREATE INDEX idx_stocks_symbol ON stocks(symbol);
CREATE INDEX idx_stocks_last_updated ON stocks(last_updated);
CREATE INDEX idx_stocks_sector ON stocks(sector);