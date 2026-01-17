package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestTechnicalIndicatorsRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("CreateTechnicalIndicator creates new indicator", func(t *testing.T) {
		testDB.TruncateAll(t)

		indicator := &models.TechnicalIndicator{
			Symbol:        "AAPL",
			Date:          time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			IndicatorType: models.IndicatorRSI14,
			Value:         decimal.NewFromFloat(45.5),
			Timeframe:     "daily",
		}

		err := testDB.CreateTechnicalIndicator(indicator)
		require.NoError(t, err)
		assert.NotZero(t, indicator.ID)
	})

	t.Run("CreateTechnicalIndicator defaults timeframe to daily", func(t *testing.T) {
		testDB.TruncateAll(t)

		indicator := &models.TechnicalIndicator{
			Symbol:        "AAPL",
			Date:          time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC),
			IndicatorType: models.IndicatorMACD,
			Value:         decimal.NewFromFloat(2.35),
			// Timeframe not set
		}

		err := testDB.CreateTechnicalIndicator(indicator)
		require.NoError(t, err)
		assert.Equal(t, "daily", indicator.Timeframe)
	})

	t.Run("CreateTechnicalIndicator upserts on conflict", func(t *testing.T) {
		testDB.TruncateAll(t)

		date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		indicator1 := &models.TechnicalIndicator{
			Symbol:        "GOOGL",
			Date:          date,
			IndicatorType: models.IndicatorRSI14,
			Value:         decimal.NewFromFloat(50.0),
			Timeframe:     "daily",
		}
		err := testDB.CreateTechnicalIndicator(indicator1)
		require.NoError(t, err)

		// Update with same key
		indicator2 := &models.TechnicalIndicator{
			Symbol:        "GOOGL",
			Date:          date,
			IndicatorType: models.IndicatorRSI14,
			Value:         decimal.NewFromFloat(55.0),
			Timeframe:     "daily",
		}
		err = testDB.CreateTechnicalIndicator(indicator2)
		require.NoError(t, err)

		// Verify updated value
		retrieved, err := testDB.GetIndicator("GOOGL", date, models.IndicatorRSI14, "daily")
		require.NoError(t, err)
		assert.True(t, decimal.NewFromFloat(55.0).Equal(retrieved.Value))
	})

	t.Run("CreateTechnicalIndicatorBatch inserts multiple indicators", func(t *testing.T) {
		testDB.TruncateAll(t)

		date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		indicators := []*models.TechnicalIndicator{
			{Symbol: "AAPL", Date: date, IndicatorType: models.IndicatorRSI14, Value: decimal.NewFromFloat(45.0)},
			{Symbol: "AAPL", Date: date, IndicatorType: models.IndicatorMACD, Value: decimal.NewFromFloat(2.5)},
			{Symbol: "AAPL", Date: date, IndicatorType: models.IndicatorSMA20, Value: decimal.NewFromFloat(175.00)},
			{Symbol: "AAPL", Date: date, IndicatorType: models.IndicatorSMA50, Value: decimal.NewFromFloat(170.00)},
		}

		err := testDB.CreateTechnicalIndicatorBatch(indicators)
		require.NoError(t, err)

		// Verify all inserted
		retrieved, err := testDB.GetIndicatorsBySymbol("AAPL", date)
		require.NoError(t, err)
		assert.Len(t, retrieved, 4)
	})

	t.Run("GetTechnicalIndicatorByID retrieves indicator", func(t *testing.T) {
		testDB.TruncateAll(t)

		indicator := &models.TechnicalIndicator{
			Symbol:        "MSFT",
			Date:          time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			IndicatorType: models.IndicatorRSI14,
			Value:         decimal.NewFromFloat(62.5),
		}
		err := testDB.CreateTechnicalIndicator(indicator)
		require.NoError(t, err)

		retrieved, err := testDB.GetTechnicalIndicatorByID(indicator.ID)
		require.NoError(t, err)
		assert.Equal(t, "MSFT", retrieved.Symbol)
		assert.Equal(t, models.IndicatorRSI14, retrieved.IndicatorType)
		assert.True(t, decimal.NewFromFloat(62.5).Equal(retrieved.Value))
	})

	t.Run("GetIndicator retrieves specific indicator", func(t *testing.T) {
		testDB.TruncateAll(t)

		date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		indicators := []*models.TechnicalIndicator{
			{Symbol: "NVDA", Date: date, IndicatorType: models.IndicatorRSI14, Value: decimal.NewFromFloat(70.0)},
			{Symbol: "NVDA", Date: date, IndicatorType: models.IndicatorMACD, Value: decimal.NewFromFloat(5.0)},
		}

		for _, ind := range indicators {
			err := testDB.CreateTechnicalIndicator(ind)
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetIndicator("NVDA", date, models.IndicatorMACD, "daily")
		require.NoError(t, err)
		assert.Equal(t, models.IndicatorMACD, retrieved.IndicatorType)
		assert.True(t, decimal.NewFromFloat(5.0).Equal(retrieved.Value))
	})

	t.Run("GetIndicatorsBySymbol retrieves all indicators for symbol on date", func(t *testing.T) {
		testDB.TruncateAll(t)

		date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		indicators := []*models.TechnicalIndicator{
			{Symbol: "TSLA", Date: date, IndicatorType: models.IndicatorRSI14, Value: decimal.NewFromFloat(55.0)},
			{Symbol: "TSLA", Date: date, IndicatorType: models.IndicatorMACD, Value: decimal.NewFromFloat(3.0)},
			{Symbol: "TSLA", Date: date, IndicatorType: models.IndicatorSMA20, Value: decimal.NewFromFloat(250.00)},
			{Symbol: "OTHER", Date: date, IndicatorType: models.IndicatorRSI14, Value: decimal.NewFromFloat(40.0)}, // Different symbol
		}

		for _, ind := range indicators {
			err := testDB.CreateTechnicalIndicator(ind)
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetIndicatorsBySymbol("TSLA", date)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
	})

	t.Run("GetIndicatorHistory retrieves historical values", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Insert RSI values for multiple days
		for i := 0; i < 10; i++ {
			indicator := &models.TechnicalIndicator{
				Symbol:        "AMD",
				Date:          time.Date(2024, 1, 10+i, 0, 0, 0, 0, time.UTC),
				IndicatorType: models.IndicatorRSI14,
				Value:         decimal.NewFromFloat(30.0 + float64(i)*5),
			}
			err := testDB.CreateTechnicalIndicator(indicator)
			require.NoError(t, err)
		}

		// Get last 5 RSI values
		history, err := testDB.GetIndicatorHistory("AMD", models.IndicatorRSI14, 5)
		require.NoError(t, err)
		assert.Len(t, history, 5)

		// Should be ordered by date DESC - check the most recent has the highest value
		// The most recent (Jan 19) should have value 75 (30 + 9*5)
		assert.True(t, decimal.NewFromFloat(75.0).Equal(history[0].Value), "first value should be 75, got %s", history[0].Value)
		// The oldest in result (Jan 15) should have value 55 (30 + 5*5)
		assert.True(t, decimal.NewFromFloat(55.0).Equal(history[4].Value), "last value should be 55, got %s", history[4].Value)
	})

	t.Run("GetLatestIndicators retrieves most recent of each type", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Insert multiple days of indicators
		for i := 0; i < 5; i++ {
			date := time.Date(2024, 1, 15+i, 0, 0, 0, 0, time.UTC)
			indicators := []*models.TechnicalIndicator{
				{Symbol: "META", Date: date, IndicatorType: models.IndicatorRSI14, Value: decimal.NewFromFloat(40.0 + float64(i))},
				{Symbol: "META", Date: date, IndicatorType: models.IndicatorMACD, Value: decimal.NewFromFloat(1.0 + float64(i)*0.5)},
			}
			for _, ind := range indicators {
				err := testDB.CreateTechnicalIndicator(ind)
				require.NoError(t, err)
			}
		}

		latest, err := testDB.GetLatestIndicators("META")
		require.NoError(t, err)
		assert.Len(t, latest, 2) // RSI and MACD

		// Find RSI - should be the latest value
		for _, ind := range latest {
			if ind.IndicatorType == models.IndicatorRSI14 {
				assert.True(t, decimal.NewFromFloat(44.0).Equal(ind.Value)) // 40 + 4 = 44
			}
		}
	})

	t.Run("GetLatestRSI retrieves most recent RSI", func(t *testing.T) {
		testDB.TruncateAll(t)

		for i := 0; i < 5; i++ {
			indicator := &models.TechnicalIndicator{
				Symbol:        "INTC",
				Date:          time.Date(2024, 1, 15+i, 0, 0, 0, 0, time.UTC),
				IndicatorType: models.IndicatorRSI14,
				Value:         decimal.NewFromFloat(35.0 + float64(i)*3),
			}
			err := testDB.CreateTechnicalIndicator(indicator)
			require.NoError(t, err)
		}

		rsi, err := testDB.GetLatestRSI("INTC")
		require.NoError(t, err)
		assert.True(t, decimal.NewFromFloat(47.0).Equal(rsi)) // 35 + 4*3 = 47
	})

	t.Run("GetLatestRSI returns error for no data", func(t *testing.T) {
		testDB.TruncateAll(t)

		_, err := testDB.GetLatestRSI("NONEXISTENT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no RSI data found")
	})

	t.Run("DeleteTechnicalIndicator removes indicator", func(t *testing.T) {
		testDB.TruncateAll(t)

		indicator := &models.TechnicalIndicator{
			Symbol:        "QCOM",
			Date:          time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			IndicatorType: models.IndicatorRSI14,
			Value:         decimal.NewFromFloat(50.0),
		}
		err := testDB.CreateTechnicalIndicator(indicator)
		require.NoError(t, err)

		err = testDB.DeleteTechnicalIndicator(indicator.ID)
		require.NoError(t, err)

		_, err = testDB.GetTechnicalIndicatorByID(indicator.ID)
		require.Error(t, err)
	})

	t.Run("DeleteIndicatorsBySymbol removes all for symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		for _, symbol := range []string{"DELETE", "KEEP"} {
			err := testDB.CreateTechnicalIndicator(&models.TechnicalIndicator{
				Symbol: symbol, Date: date, IndicatorType: models.IndicatorRSI14, Value: decimal.NewFromFloat(50.0),
			})
			require.NoError(t, err)
		}

		err := testDB.DeleteIndicatorsBySymbol("DELETE")
		require.NoError(t, err)

		deleted, err := testDB.GetIndicatorsBySymbol("DELETE", date)
		require.NoError(t, err)
		assert.Len(t, deleted, 0)

		kept, err := testDB.GetIndicatorsBySymbol("KEEP", date)
		require.NoError(t, err)
		assert.Len(t, kept, 1)
	})

	t.Run("DeleteIndicatorsOlderThan removes old indicators", func(t *testing.T) {
		testDB.TruncateAll(t)

		for i := 0; i < 10; i++ {
			err := testDB.CreateTechnicalIndicator(&models.TechnicalIndicator{
				Symbol:        "OLD_IND",
				Date:          time.Date(2024, 1, 10+i, 0, 0, 0, 0, time.UTC),
				IndicatorType: models.IndicatorRSI14,
				Value:         decimal.NewFromFloat(50.0),
			})
			require.NoError(t, err)
		}

		cutoff := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		deleted, err := testDB.DeleteIndicatorsOlderThan(cutoff)
		require.NoError(t, err)
		assert.Equal(t, int64(5), deleted)

		history, err := testDB.GetIndicatorHistory("OLD_IND", models.IndicatorRSI14, 100)
		require.NoError(t, err)
		assert.Len(t, history, 5)
	})
}
