package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Common indicator type constants
const (
	IndicatorRSI14      = "RSI_14"
	IndicatorMACD       = "MACD"
	IndicatorMACDSignal = "MACD_SIGNAL"
	IndicatorMACDHist   = "MACD_HIST"
	IndicatorSMA20      = "SMA_20"
	IndicatorSMA50      = "SMA_50"
	IndicatorSMA200     = "SMA_200"
	IndicatorEMA12      = "EMA_12"
	IndicatorEMA26      = "EMA_26"
	IndicatorBBUpper    = "BB_UPPER"
	IndicatorBBMiddle   = "BB_MIDDLE"
	IndicatorBBLower    = "BB_LOWER"
	IndicatorATR14      = "ATR_14"
	IndicatorStochK     = "STOCH_K"
	IndicatorStochD     = "STOCH_D"
	IndicatorADX        = "ADX"
	IndicatorOBV        = "OBV"
)

// TechnicalIndicator represents a calculated technical indicator value
type TechnicalIndicator struct {
	ID            int             `json:"id"`
	Symbol        string          `json:"symbol"`
	Date          time.Time       `json:"date"`
	IndicatorType string          `json:"indicator_type"`
	Value         decimal.Decimal `json:"value"`
	Timeframe     string          `json:"timeframe"`
	CreatedAt     time.Time       `json:"created_at"`
}
