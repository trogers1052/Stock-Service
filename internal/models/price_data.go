package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// PriceDataDaily represents daily OHLCV price data for a stock
type PriceDataDaily struct {
	ID        int             `json:"id"`
	Symbol    string          `json:"symbol"`
	Date      time.Time       `json:"date"`
	Open      decimal.Decimal `json:"open"`
	High      decimal.Decimal `json:"high"`
	Low       decimal.Decimal `json:"low"`
	Close     decimal.Decimal `json:"close"`
	Volume    int64           `json:"volume"`
	VWAP      decimal.Decimal `json:"vwap,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}
