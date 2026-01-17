package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestTradesRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("CreateTradeHistory creates new trade", func(t *testing.T) {
		testDB.TruncateAll(t)

		entryDate := time.Now().Add(-7 * 24 * time.Hour)
		exitDate := time.Now()
		holdingPeriod := 168
		emotionalState := 7
		convictionLevel := 8

		trade := &models.TradeHistory{
			Symbol:             "AAPL",
			TradeType:          models.TradeTypeSell,
			Quantity:           decimal.NewFromFloat(100),
			Price:              decimal.NewFromFloat(180.00),
			TotalCost:          decimal.NewFromFloat(18000.00),
			Fee:                decimal.NewFromFloat(5.00),
			EntryDate:          &entryDate,
			ExitDate:           &exitDate,
			HoldingPeriodHours: &holdingPeriod,
			EntryRSI:           decimal.NewFromFloat(32.5),
			ExitRSI:            decimal.NewFromFloat(68.0),
			RealizedPnl:        decimal.NewFromFloat(2500.00),
			RealizedPnlPct:     decimal.NewFromFloat(16.13),
			MaxDrawdownPct:     decimal.NewFromFloat(3.5),
			EntryReason:        "RSI oversold bounce",
			ExitReason:         "Target hit",
			EmotionalState:     &emotionalState,
			ConvictionLevel:    &convictionLevel,
			MarketConditions:   "Bull market, low VIX",
			WhatWentRight:      "Held through volatility",
			WhatWentWrong:      "Could have held longer",
			TradeGrade:         models.TradeGradeA,
			StrategyTag:        "RSI_BOUNCE",
			Notes:              "Clean trade execution",
		}

		err := testDB.CreateTradeHistory(trade)
		require.NoError(t, err)
		assert.NotZero(t, trade.ID)
		assert.False(t, trade.CreatedAt.IsZero())
	})

	t.Run("GetTradeHistoryByID retrieves trade", func(t *testing.T) {
		testDB.TruncateAll(t)

		trade := &models.TradeHistory{
			Symbol:      "GOOGL",
			TradeType:   models.TradeTypeBuy,
			Quantity:    decimal.NewFromFloat(50),
			Price:       decimal.NewFromFloat(140.00),
			TotalCost:   decimal.NewFromFloat(7000.00),
			StrategyTag: "MOMENTUM",
			TradeGrade:  models.TradeGradeB,
		}
		err := testDB.CreateTradeHistory(trade)
		require.NoError(t, err)

		retrieved, err := testDB.GetTradeHistoryByID(trade.ID)
		require.NoError(t, err)
		assert.Equal(t, "GOOGL", retrieved.Symbol)
		assert.Equal(t, models.TradeTypeBuy, retrieved.TradeType)
		assert.True(t, decimal.NewFromFloat(50).Equal(retrieved.Quantity))
		assert.Equal(t, "MOMENTUM", retrieved.StrategyTag)
	})

	t.Run("GetTradeHistoryByID returns error for non-existent trade", func(t *testing.T) {
		testDB.TruncateAll(t)

		_, err := testDB.GetTradeHistoryByID(99999)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("GetTradeHistoryBySymbol retrieves trades for symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create trades for multiple symbols
		trades := []*models.TradeHistory{
			{Symbol: "MSFT", TradeType: models.TradeTypeBuy, Quantity: decimal.NewFromFloat(25), Price: decimal.NewFromFloat(370.00), TotalCost: decimal.NewFromFloat(9250.00), TradeGrade: models.TradeGradeB},
			{Symbol: "MSFT", TradeType: models.TradeTypeSell, Quantity: decimal.NewFromFloat(25), Price: decimal.NewFromFloat(385.00), TotalCost: decimal.NewFromFloat(9625.00), TradeGrade: models.TradeGradeA},
			{Symbol: "OTHER", TradeType: models.TradeTypeBuy, Quantity: decimal.NewFromFloat(10), Price: decimal.NewFromFloat(100.00), TotalCost: decimal.NewFromFloat(1000.00), TradeGrade: models.TradeGradeC},
		}

		for _, tr := range trades {
			err := testDB.CreateTradeHistory(tr)
			require.NoError(t, err)
		}

		msftTrades, err := testDB.GetTradeHistoryBySymbol("MSFT", 10)
		require.NoError(t, err)
		assert.Len(t, msftTrades, 2)
	})

	t.Run("GetAllTradeHistory retrieves all trades with limit", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create 5 trades
		for i := 0; i < 5; i++ {
			trade := &models.TradeHistory{
				Symbol:     "TRADE",
				TradeType:  models.TradeTypeBuy,
				Quantity:   decimal.NewFromFloat(10),
				Price:      decimal.NewFromFloat(100.00 + float64(i)),
				TotalCost:  decimal.NewFromFloat(1000.00 + float64(i)*10),
				TradeGrade: models.TradeGradeB,
			}
			err := testDB.CreateTradeHistory(trade)
			require.NoError(t, err)
		}

		// Get with limit
		retrieved, err := testDB.GetAllTradeHistory(3)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
	})

	t.Run("GetTradeHistoryByDateRange retrieves trades in range", func(t *testing.T) {
		testDB.TruncateAll(t)

		now := time.Now()

		// Create trades at different times
		for i := 0; i < 10; i++ {
			executedAt := now.Add(time.Duration(-i*24) * time.Hour)
			trade := &models.TradeHistory{
				Symbol:     "RANGE",
				TradeType:  models.TradeTypeBuy,
				Quantity:   decimal.NewFromFloat(10),
				Price:      decimal.NewFromFloat(100.00),
				TotalCost:  decimal.NewFromFloat(1000.00),
				ExecutedAt: executedAt,
				TradeGrade: models.TradeGradeC,
			}
			err := testDB.CreateTradeHistory(trade)
			require.NoError(t, err)
		}

		// Get trades from last 5 days
		startDate := now.Add(-5 * 24 * time.Hour)
		endDate := now.Add(24 * time.Hour)

		retrieved, err := testDB.GetTradeHistoryByDateRange(startDate, endDate)
		require.NoError(t, err)
		assert.Len(t, retrieved, 6) // Today + 5 days back
	})

	t.Run("GetTradeHistoryByStrategy retrieves trades with strategy", func(t *testing.T) {
		testDB.TruncateAll(t)

		trades := []*models.TradeHistory{
			{Symbol: "STR1", TradeType: models.TradeTypeBuy, Quantity: decimal.NewFromFloat(10), Price: decimal.NewFromFloat(100.00), TotalCost: decimal.NewFromFloat(1000.00), StrategyTag: "RSI_BOUNCE", TradeGrade: models.TradeGradeA},
			{Symbol: "STR2", TradeType: models.TradeTypeBuy, Quantity: decimal.NewFromFloat(20), Price: decimal.NewFromFloat(200.00), TotalCost: decimal.NewFromFloat(4000.00), StrategyTag: "RSI_BOUNCE", TradeGrade: models.TradeGradeB},
			{Symbol: "STR3", TradeType: models.TradeTypeBuy, Quantity: decimal.NewFromFloat(30), Price: decimal.NewFromFloat(300.00), TotalCost: decimal.NewFromFloat(9000.00), StrategyTag: "MOMENTUM", TradeGrade: models.TradeGradeC},
		}

		for _, tr := range trades {
			err := testDB.CreateTradeHistory(tr)
			require.NoError(t, err)
		}

		rsiBounce, err := testDB.GetTradeHistoryByStrategy("RSI_BOUNCE", 10)
		require.NoError(t, err)
		assert.Len(t, rsiBounce, 2)

		momentum, err := testDB.GetTradeHistoryByStrategy("MOMENTUM", 10)
		require.NoError(t, err)
		assert.Len(t, momentum, 1)
	})

	t.Run("UpdateTradeHistory updates existing trade", func(t *testing.T) {
		testDB.TruncateAll(t)

		trade := &models.TradeHistory{
			Symbol:     "UPDATE",
			TradeType:  models.TradeTypeBuy,
			Quantity:   decimal.NewFromFloat(50),
			Price:      decimal.NewFromFloat(150.00),
			TotalCost:  decimal.NewFromFloat(7500.00),
			TradeGrade: models.TradeGradeC,
		}
		err := testDB.CreateTradeHistory(trade)
		require.NoError(t, err)

		// Update
		trade.Notes = "Updated notes"
		trade.TradeGrade = models.TradeGradeB
		convLevel := 9
		trade.ConvictionLevel = &convLevel

		err = testDB.UpdateTradeHistory(trade)
		require.NoError(t, err)

		retrieved, err := testDB.GetTradeHistoryByID(trade.ID)
		require.NoError(t, err)
		assert.Equal(t, "Updated notes", retrieved.Notes)
		assert.Equal(t, models.TradeGradeB, retrieved.TradeGrade)
		assert.Equal(t, 9, *retrieved.ConvictionLevel)
	})

	t.Run("DeleteTradeHistory removes trade", func(t *testing.T) {
		testDB.TruncateAll(t)

		trade := &models.TradeHistory{
			Symbol:     "DELETE",
			TradeType:  models.TradeTypeBuy,
			Quantity:   decimal.NewFromFloat(10),
			Price:      decimal.NewFromFloat(100.00),
			TotalCost:  decimal.NewFromFloat(1000.00),
			TradeGrade: models.TradeGradeD,
		}
		err := testDB.CreateTradeHistory(trade)
		require.NoError(t, err)

		err = testDB.DeleteTradeHistory(trade.ID)
		require.NoError(t, err)

		_, err = testDB.GetTradeHistoryByID(trade.ID)
		require.Error(t, err)
	})

	t.Run("GetTradeStats calculates statistics", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create winning and losing trades
		trades := []*models.TradeHistory{
			{Symbol: "WIN1", TradeType: models.TradeTypeSell, Quantity: decimal.NewFromFloat(100), Price: decimal.NewFromFloat(110.00), TotalCost: decimal.NewFromFloat(11000.00), RealizedPnl: decimal.NewFromFloat(1000.00), RealizedPnlPct: decimal.NewFromFloat(10.0), TradeGrade: models.TradeGradeA},
			{Symbol: "WIN2", TradeType: models.TradeTypeSell, Quantity: decimal.NewFromFloat(100), Price: decimal.NewFromFloat(120.00), TotalCost: decimal.NewFromFloat(12000.00), RealizedPnl: decimal.NewFromFloat(2000.00), RealizedPnlPct: decimal.NewFromFloat(20.0), TradeGrade: models.TradeGradeA},
			{Symbol: "LOSS1", TradeType: models.TradeTypeSell, Quantity: decimal.NewFromFloat(100), Price: decimal.NewFromFloat(90.00), TotalCost: decimal.NewFromFloat(9000.00), RealizedPnl: decimal.NewFromFloat(-500.00), RealizedPnlPct: decimal.NewFromFloat(-5.0), TradeGrade: models.TradeGradeD},
			{Symbol: "LOSS2", TradeType: models.TradeTypeSell, Quantity: decimal.NewFromFloat(100), Price: decimal.NewFromFloat(85.00), TotalCost: decimal.NewFromFloat(8500.00), RealizedPnl: decimal.NewFromFloat(-1000.00), RealizedPnlPct: decimal.NewFromFloat(-10.0), TradeGrade: models.TradeGradeF},
		}

		for _, tr := range trades {
			err := testDB.CreateTradeHistory(tr)
			require.NoError(t, err)
		}

		stats, err := testDB.GetTradeStats()
		require.NoError(t, err)

		assert.Equal(t, 4, stats.TotalTrades)
		assert.Equal(t, 2, stats.WinningTrades)
		assert.Equal(t, 2, stats.LosingTrades)
		assert.True(t, decimal.NewFromFloat(50.0).Equal(stats.WinRate)) // 50% win rate
		assert.True(t, decimal.NewFromFloat(1500.00).Equal(stats.TotalPnl)) // 1000 + 2000 - 500 - 1000 = 1500
	})

	t.Run("GetTradeStats with no trades", func(t *testing.T) {
		testDB.TruncateAll(t)

		stats, err := testDB.GetTradeStats()
		require.NoError(t, err)

		assert.Equal(t, 0, stats.TotalTrades)
		assert.True(t, stats.WinRate.IsZero())
		assert.True(t, stats.TotalPnl.IsZero())
	})

	t.Run("trade grade constraints", func(t *testing.T) {
		testDB.TruncateAll(t)

		validGrades := []string{models.TradeGradeA, models.TradeGradeB, models.TradeGradeC, models.TradeGradeD, models.TradeGradeF}

		for _, grade := range validGrades {
			trade := &models.TradeHistory{
				Symbol:     "GRADE",
				TradeType:  models.TradeTypeBuy,
				Quantity:   decimal.NewFromFloat(10),
				Price:      decimal.NewFromFloat(100.00),
				TotalCost:  decimal.NewFromFloat(1000.00),
				TradeGrade: grade,
			}
			err := testDB.CreateTradeHistory(trade)
			require.NoError(t, err, "grade %s should be valid", grade)
			testDB.TruncateAll(t)
		}
	})

	t.Run("emotional state and conviction level constraints", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Valid values (1-10)
		emotionalState := 5
		convictionLevel := 8

		trade := &models.TradeHistory{
			Symbol:          "EMOTION",
			TradeType:       models.TradeTypeBuy,
			Quantity:        decimal.NewFromFloat(10),
			Price:           decimal.NewFromFloat(100.00),
			TotalCost:       decimal.NewFromFloat(1000.00),
			EmotionalState:  &emotionalState,
			ConvictionLevel: &convictionLevel,
			TradeGrade:      models.TradeGradeB,
		}
		err := testDB.CreateTradeHistory(trade)
		require.NoError(t, err)

		retrieved, err := testDB.GetTradeHistoryByID(trade.ID)
		require.NoError(t, err)
		assert.Equal(t, 5, *retrieved.EmotionalState)
		assert.Equal(t, 8, *retrieved.ConvictionLevel)
	})
}
