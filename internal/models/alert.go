package models

import (
	"time"

	"github.com/shopspring/decimal"
)

// Alert rule type constants
const (
	RuleTypePriceTarget      = "PRICE_TARGET"
	RuleTypeRSIOversold      = "RSI_OVERSOLD"
	RuleTypeRSIOverbought    = "RSI_OVERBOUGHT"
	RuleTypeSupportBounce    = "SUPPORT_BOUNCE"
	RuleTypeResistanceBreak  = "RESISTANCE_BREAK"
	RuleTypeVolumeSpike      = "VOLUME_SPIKE"
)

// Comparison constants
const (
	ComparisonAbove  = "ABOVE"
	ComparisonBelow  = "BELOW"
	ComparisonEquals = "EQUALS"
)

// Notification channel constants
const (
	ChannelTelegram = "telegram"
	ChannelPushover = "pushover"
	ChannelSMS      = "sms"
	ChannelEmail    = "email"
)

// Priority constants
const (
	PriorityLow      = "low"
	PriorityNormal   = "normal"
	PriorityHigh     = "high"
	PriorityCritical = "critical"
)

// AlertRule represents a configurable alert condition
type AlertRule struct {
	ID                  int              `json:"id"`
	Symbol              string           `json:"symbol"`
	RuleType            string           `json:"rule_type"`
	ConditionValue      decimal.Decimal  `json:"condition_value,omitempty"`
	Comparison          string           `json:"comparison"`
	Enabled             bool             `json:"enabled"`
	TriggeredCount      int              `json:"triggered_count"`
	LastTriggeredAt     *time.Time       `json:"last_triggered_at,omitempty"`
	CooldownMinutes     int              `json:"cooldown_minutes"`
	NotificationChannel string           `json:"notification_channel"`
	MessageTemplate     string           `json:"message_template,omitempty"`
	Priority            string           `json:"priority"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
}

// AlertHistory represents a triggered alert record
type AlertHistory struct {
	ID                  int             `json:"id"`
	AlertRuleID         int             `json:"alert_rule_id,omitempty"`
	Symbol              string          `json:"symbol"`
	RuleType            string          `json:"rule_type"`
	TriggeredValue      decimal.Decimal `json:"triggered_value,omitempty"`
	Message             string          `json:"message,omitempty"`
	NotificationSent    bool            `json:"notification_sent"`
	NotificationChannel string          `json:"notification_channel,omitempty"`
	TriggeredAt         time.Time       `json:"triggered_at"`
}
