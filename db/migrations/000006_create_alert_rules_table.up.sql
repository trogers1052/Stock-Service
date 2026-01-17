CREATE TABLE IF NOT EXISTS alert_rules (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL REFERENCES stocks(symbol) ON DELETE CASCADE,
    rule_type VARCHAR(50) NOT NULL,
    -- Types: 'PRICE_TARGET', 'RSI_OVERSOLD', 'RSI_OVERBOUGHT', 
    --        'SUPPORT_BOUNCE', 'RESISTANCE_BREAK', 'VOLUME_SPIKE'
    
    condition_value DECIMAL(18,4), -- The threshold value
    comparison VARCHAR(10) NOT NULL, -- 'ABOVE', 'BELOW', 'EQUALS'
    
    enabled BOOLEAN DEFAULT true,
    triggered_count INTEGER DEFAULT 0,
    last_triggered_at TIMESTAMP,
    
    -- Cooldown to prevent spam (minutes)
    cooldown_minutes INTEGER DEFAULT 60,
    
    notification_channel VARCHAR(20) DEFAULT 'telegram',
    -- Options: 'telegram', 'pushover', 'sms', 'email'
    
    message_template TEXT,
    priority VARCHAR(10) DEFAULT 'normal', -- 'low', 'normal', 'high', 'critical'
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_alert_rules_symbol ON alert_rules(symbol);
CREATE INDEX idx_alert_rules_enabled ON alert_rules(enabled) WHERE enabled = true;
CREATE INDEX idx_alert_rules_type ON alert_rules(rule_type);

-- Alert history (track all fired alerts)
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    alert_rule_id INTEGER REFERENCES alert_rules(id) ON DELETE CASCADE,
    symbol VARCHAR(10) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    triggered_value DECIMAL(18,4),
    message TEXT,
    notification_sent BOOLEAN DEFAULT false,
    notification_channel VARCHAR(20),
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_alert_history_symbol ON alert_history(symbol);
CREATE INDEX idx_alert_history_triggered_at ON alert_history(triggered_at DESC);
CREATE INDEX idx_alert_history_rule_id ON alert_history(alert_rule_id);