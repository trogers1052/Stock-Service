package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Trade type constants
const (
	TradeTypeBuy  = "BUY"
	TradeTypeSell = "SELL"
)

// Trade grade constants
const (
	TradeGradeA = "A"
	TradeGradeB = "B"
	TradeGradeC = "C"
	TradeGradeD = "D"
	TradeGradeF = "F"
)

// TradeHistory represents a completed trade with journal entries
type TradeHistory struct {
	ID                 int              `json:"id"`
	Symbol             string           `json:"symbol"`
	TradeType          string           `json:"trade_type"`
	Quantity           decimal.Decimal  `json:"quantity"`
	Price              decimal.Decimal  `json:"price"`
	TotalCost          decimal.Decimal  `json:"total_cost"`
	Fee                decimal.Decimal  `json:"fee"`
	EntryDate          *time.Time       `json:"entry_date,omitempty"`
	ExitDate           *time.Time       `json:"exit_date,omitempty"`
	HoldingPeriodHours *int             `json:"holding_period_hours,omitempty"`
	EntryRSI           decimal.Decimal  `json:"entry_rsi,omitempty"`
	ExitRSI            decimal.Decimal  `json:"exit_rsi,omitempty"`
	RealizedPnl        decimal.Decimal  `json:"realized_pnl,omitempty"`
	RealizedPnlPct     decimal.Decimal  `json:"realized_pnl_pct,omitempty"`
	MaxDrawdownPct     decimal.Decimal  `json:"max_drawdown_pct,omitempty"`
	EntryReason        string           `json:"entry_reason,omitempty"`
	ExitReason         string           `json:"exit_reason,omitempty"`
	EmotionalState     *int             `json:"emotional_state,omitempty"`
	ConvictionLevel    *int             `json:"conviction_level,omitempty"`
	MarketConditions   string           `json:"market_conditions,omitempty"`
	WhatWentRight      string           `json:"what_went_right,omitempty"`
	WhatWentWrong      string           `json:"what_went_wrong,omitempty"`
	TradeGrade         string           `json:"trade_grade,omitempty"`
	StrategyTag        string           `json:"strategy_tag,omitempty"`
	Notes              string           `json:"notes,omitempty"`
	ExecutedAt         time.Time        `json:"executed_at"`
	CreatedAt          time.Time        `json:"created_at"`
}
