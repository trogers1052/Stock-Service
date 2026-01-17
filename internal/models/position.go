package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Position represents a current stock holding
type Position struct {
	ID              int             `json:"id"`
	Symbol          string          `json:"symbol"`
	Quantity        decimal.Decimal `json:"quantity"`
	EntryPrice      decimal.Decimal `json:"entry_price"`
	EntryDate       time.Time       `json:"entry_date"`
	CurrentPrice    decimal.Decimal `json:"current_price,omitempty"`
	UnrealizedPnlPct decimal.Decimal `json:"unrealized_pnl_pct,omitempty"`
	DaysHeld        int             `json:"days_held,omitempty"`
	EntryRSI        decimal.Decimal `json:"entry_rsi,omitempty"`
	EntryReason     string          `json:"entry_reason,omitempty"`
	Sector          string          `json:"sector,omitempty"`
	Industry        string          `json:"industry,omitempty"`
	PositionSizePct decimal.Decimal `json:"position_size_pct,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}
