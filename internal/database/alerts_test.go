package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestAlertsRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	// Helper to create a stock for foreign key references
	createTestStock := func(t *testing.T, symbol string) {
		stock := &models.Stock{
			Symbol:      symbol,
			Name:        symbol + " Inc.",
			LastUpdated: time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)
	}

	t.Run("CreateAlertRule creates new rule", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "AAPL")

		rule := &models.AlertRule{
			Symbol:              "AAPL",
			RuleType:            models.RuleTypePriceTarget,
			ConditionValue:      decimal.NewFromFloat(200.00),
			Comparison:          models.ComparisonAbove,
			Enabled:             true,
			CooldownMinutes:     60,
			NotificationChannel: models.ChannelTelegram,
			MessageTemplate:     "AAPL hit target price!",
			Priority:            models.PriorityHigh,
		}

		err := testDB.CreateAlertRule(rule)
		require.NoError(t, err)
		assert.NotZero(t, rule.ID)
		assert.False(t, rule.CreatedAt.IsZero())
	})

	t.Run("GetAlertRuleByID retrieves rule", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "GOOGL")

		rule := &models.AlertRule{
			Symbol:              "GOOGL",
			RuleType:            models.RuleTypeRSIOversold,
			ConditionValue:      decimal.NewFromFloat(30.00),
			Comparison:          models.ComparisonBelow,
			Enabled:             true,
			CooldownMinutes:     30,
			NotificationChannel: models.ChannelTelegram,
			Priority:            models.PriorityNormal,
		}
		err := testDB.CreateAlertRule(rule)
		require.NoError(t, err)

		retrieved, err := testDB.GetAlertRuleByID(rule.ID)
		require.NoError(t, err)
		assert.Equal(t, "GOOGL", retrieved.Symbol)
		assert.Equal(t, models.RuleTypeRSIOversold, retrieved.RuleType)
		assert.True(t, decimal.NewFromFloat(30.00).Equal(retrieved.ConditionValue))
	})

	t.Run("GetAlertRulesBySymbol retrieves rules for symbol", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "MSFT")
		createTestStock(t, "OTHER")

		rules := []*models.AlertRule{
			{Symbol: "MSFT", RuleType: models.RuleTypePriceTarget, ConditionValue: decimal.NewFromFloat(400.00), Comparison: models.ComparisonAbove, Enabled: true, CooldownMinutes: 60, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
			{Symbol: "MSFT", RuleType: models.RuleTypeRSIOversold, ConditionValue: decimal.NewFromFloat(30.00), Comparison: models.ComparisonBelow, Enabled: true, CooldownMinutes: 30, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityHigh},
			{Symbol: "OTHER", RuleType: models.RuleTypePriceTarget, ConditionValue: decimal.NewFromFloat(100.00), Comparison: models.ComparisonAbove, Enabled: true, CooldownMinutes: 60, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
		}

		for _, r := range rules {
			err := testDB.CreateAlertRule(r)
			require.NoError(t, err)
		}

		msftRules, err := testDB.GetAlertRulesBySymbol("MSFT")
		require.NoError(t, err)
		assert.Len(t, msftRules, 2)
	})

	t.Run("GetEnabledAlertRules retrieves only enabled rules", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "NVDA")

		rules := []*models.AlertRule{
			{Symbol: "NVDA", RuleType: models.RuleTypePriceTarget, ConditionValue: decimal.NewFromFloat(500.00), Comparison: models.ComparisonAbove, Enabled: true, CooldownMinutes: 60, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
			{Symbol: "NVDA", RuleType: models.RuleTypeRSIOversold, ConditionValue: decimal.NewFromFloat(30.00), Comparison: models.ComparisonBelow, Enabled: false, CooldownMinutes: 30, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
			{Symbol: "NVDA", RuleType: models.RuleTypeVolumeSpike, ConditionValue: decimal.NewFromFloat(2.0), Comparison: models.ComparisonAbove, Enabled: true, CooldownMinutes: 120, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
		}

		for _, r := range rules {
			err := testDB.CreateAlertRule(r)
			require.NoError(t, err)
		}

		enabled, err := testDB.GetEnabledAlertRules()
		require.NoError(t, err)
		assert.Len(t, enabled, 2)
	})

	t.Run("GetEnabledAlertRulesBySymbol retrieves enabled rules for symbol", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "TSLA")

		rules := []*models.AlertRule{
			{Symbol: "TSLA", RuleType: models.RuleTypePriceTarget, ConditionValue: decimal.NewFromFloat(300.00), Comparison: models.ComparisonAbove, Enabled: true, CooldownMinutes: 60, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
			{Symbol: "TSLA", RuleType: models.RuleTypeRSIOversold, ConditionValue: decimal.NewFromFloat(30.00), Comparison: models.ComparisonBelow, Enabled: false, CooldownMinutes: 30, NotificationChannel: models.ChannelTelegram, Priority: models.PriorityNormal},
		}

		for _, r := range rules {
			err := testDB.CreateAlertRule(r)
			require.NoError(t, err)
		}

		enabled, err := testDB.GetEnabledAlertRulesBySymbol("TSLA")
		require.NoError(t, err)
		assert.Len(t, enabled, 1)
		assert.Equal(t, models.RuleTypePriceTarget, enabled[0].RuleType)
	})

	t.Run("UpdateAlertRule updates existing rule", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "AMD")

		rule := &models.AlertRule{
			Symbol:              "AMD",
			RuleType:            models.RuleTypePriceTarget,
			ConditionValue:      decimal.NewFromFloat(150.00),
			Comparison:          models.ComparisonAbove,
			Enabled:             true,
			CooldownMinutes:     60,
			NotificationChannel: models.ChannelTelegram,
			Priority:            models.PriorityNormal,
		}
		err := testDB.CreateAlertRule(rule)
		require.NoError(t, err)

		// Update
		rule.ConditionValue = decimal.NewFromFloat(160.00)
		rule.Priority = models.PriorityHigh
		err = testDB.UpdateAlertRule(rule)
		require.NoError(t, err)

		retrieved, err := testDB.GetAlertRuleByID(rule.ID)
		require.NoError(t, err)
		assert.True(t, decimal.NewFromFloat(160.00).Equal(retrieved.ConditionValue))
		assert.Equal(t, models.PriorityHigh, retrieved.Priority)
	})

	t.Run("MarkAlertTriggered updates triggered count and timestamp", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "META")

		rule := &models.AlertRule{
			Symbol:              "META",
			RuleType:            models.RuleTypePriceTarget,
			ConditionValue:      decimal.NewFromFloat(350.00),
			Comparison:          models.ComparisonAbove,
			Enabled:             true,
			CooldownMinutes:     60,
			NotificationChannel: models.ChannelTelegram,
			Priority:            models.PriorityNormal,
		}
		err := testDB.CreateAlertRule(rule)
		require.NoError(t, err)

		// Trigger the alert
		err = testDB.MarkAlertTriggered(rule.ID)
		require.NoError(t, err)

		retrieved, err := testDB.GetAlertRuleByID(rule.ID)
		require.NoError(t, err)
		assert.Equal(t, 1, retrieved.TriggeredCount)
		assert.NotNil(t, retrieved.LastTriggeredAt)

		// Trigger again
		err = testDB.MarkAlertTriggered(rule.ID)
		require.NoError(t, err)

		retrieved, err = testDB.GetAlertRuleByID(rule.ID)
		require.NoError(t, err)
		assert.Equal(t, 2, retrieved.TriggeredCount)
	})

	t.Run("DeleteAlertRule removes rule", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "INTC")

		rule := &models.AlertRule{
			Symbol:              "INTC",
			RuleType:            models.RuleTypePriceTarget,
			ConditionValue:      decimal.NewFromFloat(50.00),
			Comparison:          models.ComparisonAbove,
			Enabled:             true,
			CooldownMinutes:     60,
			NotificationChannel: models.ChannelTelegram,
			Priority:            models.PriorityNormal,
		}
		err := testDB.CreateAlertRule(rule)
		require.NoError(t, err)

		err = testDB.DeleteAlertRule(rule.ID)
		require.NoError(t, err)

		_, err = testDB.GetAlertRuleByID(rule.ID)
		require.Error(t, err)
	})

	// Alert History Tests
	t.Run("CreateAlertHistory creates history record", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "QCOM")

		rule := &models.AlertRule{
			Symbol:              "QCOM",
			RuleType:            models.RuleTypePriceTarget,
			ConditionValue:      decimal.NewFromFloat(180.00),
			Comparison:          models.ComparisonAbove,
			Enabled:             true,
			CooldownMinutes:     60,
			NotificationChannel: models.ChannelTelegram,
			Priority:            models.PriorityNormal,
		}
		err := testDB.CreateAlertRule(rule)
		require.NoError(t, err)

		history := &models.AlertHistory{
			AlertRuleID:         rule.ID,
			Symbol:              "QCOM",
			RuleType:            models.RuleTypePriceTarget,
			TriggeredValue:      decimal.NewFromFloat(185.00),
			Message:             "QCOM crossed $180 target",
			NotificationSent:    true,
			NotificationChannel: models.ChannelTelegram,
		}

		err = testDB.CreateAlertHistory(history)
		require.NoError(t, err)
		assert.NotZero(t, history.ID)
	})

	t.Run("GetAlertHistoryByID retrieves history", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "AVGO")

		history := &models.AlertHistory{
			Symbol:              "AVGO",
			RuleType:            models.RuleTypeRSIOversold,
			TriggeredValue:      decimal.NewFromFloat(28.5),
			Message:             "AVGO RSI oversold",
			NotificationSent:    false,
			NotificationChannel: models.ChannelTelegram,
		}
		err := testDB.CreateAlertHistory(history)
		require.NoError(t, err)

		retrieved, err := testDB.GetAlertHistoryByID(history.ID)
		require.NoError(t, err)
		assert.Equal(t, "AVGO", retrieved.Symbol)
		assert.Equal(t, models.RuleTypeRSIOversold, retrieved.RuleType)
	})

	t.Run("GetAlertHistoryBySymbol retrieves history for symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create multiple history records
		for i := 0; i < 5; i++ {
			history := &models.AlertHistory{
				Symbol:           "HIST_TEST",
				RuleType:         models.RuleTypePriceTarget,
				TriggeredValue:   decimal.NewFromFloat(100.00 + float64(i)),
				NotificationSent: true,
			}
			err := testDB.CreateAlertHistory(history)
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetAlertHistoryBySymbol("HIST_TEST", 3)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
	})

	t.Run("GetRecentAlertHistory retrieves recent history", func(t *testing.T) {
		testDB.TruncateAll(t)

		symbols := []string{"SYM1", "SYM2", "SYM3"}
		for _, sym := range symbols {
			history := &models.AlertHistory{
				Symbol:           sym,
				RuleType:         models.RuleTypePriceTarget,
				TriggeredValue:   decimal.NewFromFloat(100.00),
				NotificationSent: true,
			}
			err := testDB.CreateAlertHistory(history)
			require.NoError(t, err)
		}

		recent, err := testDB.GetRecentAlertHistory(10)
		require.NoError(t, err)
		assert.Len(t, recent, 3)
	})

	t.Run("MarkNotificationSent updates notification status", func(t *testing.T) {
		testDB.TruncateAll(t)

		history := &models.AlertHistory{
			Symbol:           "NOTIFY",
			RuleType:         models.RuleTypePriceTarget,
			TriggeredValue:   decimal.NewFromFloat(100.00),
			NotificationSent: false,
		}
		err := testDB.CreateAlertHistory(history)
		require.NoError(t, err)

		err = testDB.MarkNotificationSent(history.ID)
		require.NoError(t, err)

		retrieved, err := testDB.GetAlertHistoryByID(history.ID)
		require.NoError(t, err)
		assert.True(t, retrieved.NotificationSent)
	})

	t.Run("DeleteAlertHistory removes history", func(t *testing.T) {
		testDB.TruncateAll(t)

		history := &models.AlertHistory{
			Symbol:           "DEL_HIST",
			RuleType:         models.RuleTypePriceTarget,
			TriggeredValue:   decimal.NewFromFloat(100.00),
			NotificationSent: true,
		}
		err := testDB.CreateAlertHistory(history)
		require.NoError(t, err)

		err = testDB.DeleteAlertHistory(history.ID)
		require.NoError(t, err)

		_, err = testDB.GetAlertHistoryByID(history.ID)
		require.Error(t, err)
	})

	t.Run("DeleteAlertHistoryOlderThan removes old history", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create history (triggered_at is set automatically to NOW())
		for i := 0; i < 5; i++ {
			history := &models.AlertHistory{
				Symbol:           "OLDHIST",
				RuleType:         models.RuleTypePriceTarget,
				TriggeredValue:   decimal.NewFromFloat(100.00),
				NotificationSent: true,
			}
			err := testDB.CreateAlertHistory(history)
			require.NoError(t, err)
		}

		// Delete records older than tomorrow (should delete all)
		tomorrow := time.Now().Add(24 * time.Hour)
		deleted, err := testDB.DeleteAlertHistoryOlderThan(tomorrow)
		require.NoError(t, err)
		assert.Equal(t, int64(5), deleted)

		remaining, err := testDB.GetAlertHistoryBySymbol("OLDHIST", 100)
		require.NoError(t, err)
		assert.Len(t, remaining, 0)
	})
}
