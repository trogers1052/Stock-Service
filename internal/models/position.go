package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Position represents a current stock holding
type Position struct {
	ID               int             `json:"id"`
	Symbol           string          `json:"symbol"`
	Quantity         decimal.Decimal `json:"quantity"`
	EntryPrice       decimal.Decimal `json:"entry_price"`
	EntryDate        time.Time       `json:"entry_date"`
	CurrentPrice     decimal.Decimal `json:"current_price,omitempty"`
	UnrealizedPnlPct decimal.Decimal `json:"unrealized_pnl_pct,omitempty"`
	DaysHeld         int             `json:"days_held,omitempty"`
	EntryRSI         decimal.Decimal `json:"entry_rsi,omitempty"`
	EntryReason      string          `json:"entry_reason,omitempty"`
	Sector           string          `json:"sector,omitempty"`
	Industry         string          `json:"industry,omitempty"`
	PositionSizePct  decimal.Decimal `json:"position_size_pct,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// PositionsEvent represents a Kafka message with position snapshot from Robinhood
type PositionsEvent struct {
	EventType string             `json:"event_type"`
	Source    string             `json:"source"`
	Timestamp string             `json:"timestamp"`
	Data      PositionsEventData `json:"data"`
}

// PositionsEventData contains the positions and account balance
type PositionsEventData struct {
	Positions   []PositionData `json:"positions"`
	BuyingPower string         `json:"buying_power"`
	Cash        string         `json:"cash"`
	TotalEquity string         `json:"total_equity"`
}

// PositionData represents a single position from Robinhood
type PositionData struct {
	Symbol          string `json:"symbol"`
	Quantity        string `json:"quantity"`
	AverageBuyPrice string `json:"average_buy_price"`
	Equity          string `json:"equity"`
	PercentChange   string `json:"percent_change"`
	EquityChange    string `json:"equity_change"`
	UpdatedAt       string `json:"updated_at"`
}
