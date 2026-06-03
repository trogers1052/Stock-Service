package models

import "time"

// SignalFeedback represents a user's feedback on a trading signal.
type SignalFeedback struct {
	ID                 int        `json:"id"`
	Symbol             string     `json:"symbol"`
	Signal             string     `json:"signal"`
	Action             string     `json:"action"`
	Confidence         float64    `json:"confidence,omitempty"`
	RulesTriggered     []string   `json:"rules_triggered,omitempty"`
	RegimeID           string     `json:"regime_id,omitempty"`
	DecisionConfidence float64    `json:"decision_confidence,omitempty"`
	EntryPrice         float64    `json:"entry_price,omitempty"`
	StopPrice          float64    `json:"stop_price,omitempty"`
	Target1            float64    `json:"target_1,omitempty"`
	Target2            float64    `json:"target_2,omitempty"`
	ValidUntil         *time.Time `json:"valid_until,omitempty"`
	Outcome            string     `json:"outcome,omitempty"`
	OutcomeAt          *time.Time `json:"outcome_at,omitempty"`
	FeedbackTimestamp  time.Time  `json:"feedback_timestamp"`
	CreatedAt          time.Time  `json:"created_at"`
}

// Signal outcome constants
const (
	OutcomeTarget1Hit = "TARGET_1_HIT"
	OutcomeTarget2Hit = "TARGET_2_HIT"
	OutcomeStoppedOut = "STOPPED_OUT"
	OutcomeExpired    = "EXPIRED"
)

// FeedbackSummary holds aggregate counts of feedback actions.
type FeedbackSummary struct {
	Total   int `json:"total"`
	Traded  int `json:"traded"`
	Skipped int `json:"skipped"`
}

// RuleOutcomeQuality holds per-rule, per-regime signal quality metrics
// based on actual price outcomes (did the signal hit its target or stop?).
type RuleOutcomeQuality struct {
	RuleName     string  `json:"rule_name"`
	RegimeID     string  `json:"regime_id"`
	SignalCount  int     `json:"signal_count"`
	WinCount     int     `json:"win_count"`
	LossCount    int     `json:"loss_count"`
	ExpiredCount int     `json:"expired_count"`
	WinRate      float64 `json:"win_rate"`
	Multiplier   float64 `json:"multiplier"`
}

// RuleAccuracy holds per-rule, per-regime accuracy metrics computed from signal_feedback.
type RuleAccuracy struct {
	RuleName     string  `json:"rule_name"`
	RegimeID     string  `json:"regime_id"`
	SignalCount  int     `json:"signal_count"`
	TradedCount  int     `json:"traded_count"`
	SkippedCount int     `json:"skipped_count"`
	TradeRate    float64 `json:"trade_rate"`
	Multiplier   float64 `json:"multiplier"`
}
