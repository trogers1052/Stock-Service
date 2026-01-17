package models

import "time"

// StockEvent represents a Kafka event for stock changes
type StockEvent struct {
	EventType string    `json:"event_type"`
	Stock     *Stock    `json:"stock,omitempty"`
	Symbol    string    `json:"symbol"`
	Timestamp time.Time `json:"timestamp"`
}

// Stock represents core stock information
type Stock struct {
	ID                string    `json:"id"`
	Symbol            string    `json:"symbol"`
	Name              string    `json:"name"`
	Exchange          string    `json:"exchange,omitempty"`
	Sector            string    `json:"sector,omitempty"`
	Industry          string    `json:"industry,omitempty"`
	CurrentPrice      float64   `json:"current_price"`
	PreviousClose     float64   `json:"previous_close"`
	ChangeAmount      float64   `json:"change_amount"`
	ChangePercent     float64   `json:"change_percent"`
	DayHigh           float64   `json:"day_high"`
	DayLow            float64   `json:"day_low"`
	Volume            int64     `json:"volume"`
	AverageVolume     int64     `json:"average_volume,omitempty"`
	Week52High        float64   `json:"week_52_high,omitempty"`
	Week52Low         float64   `json:"week_52_low,omitempty"`
	MarketCap         int64     `json:"market_cap,omitempty"`
	SharesOutstanding int64     `json:"shares_outstanding,omitempty"`
	LastUpdated       time.Time `json:"last_updated"`
	CreatedAt         time.Time `json:"created_at"`
}

// MonitoredStock represents a stock in our watchlist with buy zones and targets
type MonitoredStock struct {
	Symbol              string          `json:"symbol"`
	Enabled             bool            `json:"enabled"`
	Priority            int             `json:"priority"` // 1=high, 2=medium, 3=low
	BuyZoneLow          *float64        `json:"buy_zone_low,omitempty"`
	BuyZoneHigh         *float64        `json:"buy_zone_high,omitempty"`
	TargetPrice         *float64        `json:"target_price,omitempty"`
	StopLossPrice       *float64        `json:"stop_loss_price,omitempty"`
	AlertOnBuyZone      bool            `json:"alert_on_buy_zone"`
	AlertOnRSIOversold  bool            `json:"alert_on_rsi_oversold"`
	RSIOversoldThreshold *float64       `json:"rsi_oversold_threshold,omitempty"`
	Notes               string          `json:"notes,omitempty"`
	Reason              string          `json:"reason,omitempty"`
	AddedAt             time.Time       `json:"added_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
}
