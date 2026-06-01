-- The CreateSignalFeedback upsert relies on ON CONFLICT (symbol, signal, feedback_timestamp).
-- That requires a matching unique constraint, which was missing from the original schema.
-- De-duplicate any existing rows before adding the constraint.
DELETE FROM signal_feedback a
USING signal_feedback b
WHERE a.id < b.id
  AND a.symbol = b.symbol
  AND a.signal = b.signal
  AND a.feedback_timestamp = b.feedback_timestamp;

ALTER TABLE signal_feedback
    ADD CONSTRAINT signal_feedback_symbol_signal_ts_key
    UNIQUE (symbol, signal, feedback_timestamp);
