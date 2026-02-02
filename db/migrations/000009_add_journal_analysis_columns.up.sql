-- Add analysis columns to journal_positions for trade compliance tracking
-- These columns are populated by the reporting-service

-- Rule compliance score (0.0 - 1.0)
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS rule_compliance_score DECIMAL(10, 4);

-- Entry signal details
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS entry_signal_confidence DECIMAL(10, 4);

ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS entry_signal_type VARCHAR(20);

-- Position sizing deviation from recommended (can be negative or positive)
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS position_size_deviation DECIMAL(10, 4);

-- How the position was exited
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS exit_type VARCHAR(50);

-- Risk metrics captured at entry time (VaR, ATR, etc.)
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS risk_metrics_at_entry JSONB;

-- Analysis notes and warnings
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS analysis_notes TEXT;

-- When this position was analyzed
ALTER TABLE journal_positions
ADD COLUMN IF NOT EXISTS analyzed_at TIMESTAMP WITH TIME ZONE;

-- Indexes for filtering/reporting
CREATE INDEX IF NOT EXISTS idx_journal_positions_compliance_score
ON journal_positions(rule_compliance_score);

CREATE INDEX IF NOT EXISTS idx_journal_positions_exit_type
ON journal_positions(exit_type);

CREATE INDEX IF NOT EXISTS idx_journal_positions_analyzed_at
ON journal_positions(analyzed_at);

CREATE INDEX IF NOT EXISTS idx_journal_positions_entry_signal_type
ON journal_positions(entry_signal_type);

-- Comment on columns for documentation
COMMENT ON COLUMN journal_positions.rule_compliance_score IS 'Overall rule compliance score 0.0-1.0 calculated by reporting-service';
COMMENT ON COLUMN journal_positions.entry_signal_confidence IS 'Confidence level of buy signal at entry time';
COMMENT ON COLUMN journal_positions.entry_signal_type IS 'Type of signal at entry: BUY, SELL, WATCH, or NULL if no signal';
COMMENT ON COLUMN journal_positions.position_size_deviation IS 'Deviation from recommended position size (negative = smaller, positive = larger)';
COMMENT ON COLUMN journal_positions.exit_type IS 'How position was exited: profit_target, stop_loss, trailing_stop, time_based, manual, unknown';
COMMENT ON COLUMN journal_positions.risk_metrics_at_entry IS 'JSON object with risk metrics at entry (atr, var, rsi, etc.)';
COMMENT ON COLUMN journal_positions.analysis_notes IS 'Notes and warnings from trade analysis';
COMMENT ON COLUMN journal_positions.analyzed_at IS 'Timestamp when this position was analyzed by reporting-service';
