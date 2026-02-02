-- Remove analysis columns from journal_positions

DROP INDEX IF EXISTS idx_journal_positions_compliance_score;
DROP INDEX IF EXISTS idx_journal_positions_exit_type;
DROP INDEX IF EXISTS idx_journal_positions_analyzed_at;
DROP INDEX IF EXISTS idx_journal_positions_entry_signal_type;

ALTER TABLE journal_positions DROP COLUMN IF EXISTS rule_compliance_score;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS entry_signal_confidence;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS entry_signal_type;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS position_size_deviation;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS exit_type;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS risk_metrics_at_entry;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS analysis_notes;
ALTER TABLE journal_positions DROP COLUMN IF EXISTS analyzed_at;
