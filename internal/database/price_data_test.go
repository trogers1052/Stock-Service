package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestPriceDataRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("CreatePriceData creates new record", func(t *testing.T) {
		testDB.TruncateAll(t)

		priceData := &models.PriceDataDaily{
			Symbol: "AAPL",
			Date:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			Open:   decimal.NewFromFloat(175.00),
			High:   decimal.NewFromFloat(178.50),
			Low:    decimal.NewFromFloat(174.00),
			Close:  decimal.NewFromFloat(177.25),
			Volume: 55000000,
			VWAP:   decimal.NewFromFloat(176.50),
		}

		err := testDB.CreatePriceData(priceData)
		require.NoError(t, err)
		assert.NotZero(t, priceData.ID)
	})

	t.Run("CreatePriceData upserts on conflict", func(t *testing.T) {
		testDB.TruncateAll(t)

		date := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		priceData1 := &models.PriceDataDaily{
			Symbol: "AAPL",
			Date:   date,
			Open:   decimal.NewFromFloat(175.00),
			High:   decimal.NewFromFloat(178.50),
			Low:    decimal.NewFromFloat(174.00),
			Close:  decimal.NewFromFloat(177.25),
			Volume: 55000000,
		}
		err := testDB.CreatePriceData(priceData1)
		require.NoError(t, err)

		// Insert with same symbol and date but different values
		priceData2 := &models.PriceDataDaily{
			Symbol: "AAPL",
			Date:   date,
			Open:   decimal.NewFromFloat(176.00),
			High:   decimal.NewFromFloat(180.00),
			Low:    decimal.NewFromFloat(175.00),
			Close:  decimal.NewFromFloat(179.00),
			Volume: 60000000,
		}
		err = testDB.CreatePriceData(priceData2)
		require.NoError(t, err)

		// Should have been updated, not inserted
		retrieved, err := testDB.GetPriceDataBySymbolAndDate("AAPL", date)
		require.NoError(t, err)
		assert.True(t, decimal.NewFromFloat(179.00).Equal(retrieved.Close))
		assert.Equal(t, int64(60000000), retrieved.Volume)
	})

	t.Run("CreatePriceDataBatch inserts multiple records", func(t *testing.T) {
		testDB.TruncateAll(t)

		prices := []*models.PriceDataDaily{
			{Symbol: "AAPL", Date: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), Open: decimal.NewFromFloat(175.00), High: decimal.NewFromFloat(178.00), Low: decimal.NewFromFloat(174.00), Close: decimal.NewFromFloat(177.00), Volume: 50000000},
			{Symbol: "AAPL", Date: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC), Open: decimal.NewFromFloat(177.00), High: decimal.NewFromFloat(180.00), Low: decimal.NewFromFloat(176.00), Close: decimal.NewFromFloat(179.00), Volume: 55000000},
			{Symbol: "AAPL", Date: time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC), Open: decimal.NewFromFloat(179.00), High: decimal.NewFromFloat(182.00), Low: decimal.NewFromFloat(178.00), Close: decimal.NewFromFloat(181.00), Volume: 60000000},
		}

		err := testDB.CreatePriceDataBatch(prices)
		require.NoError(t, err)

		// Verify all were inserted
		retrieved, err := testDB.GetPriceDataBySymbol("AAPL", 10)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
	})

	t.Run("GetPriceDataByID retrieves record", func(t *testing.T) {
		testDB.TruncateAll(t)

		priceData := &models.PriceDataDaily{
			Symbol: "GOOGL",
			Date:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			Open:   decimal.NewFromFloat(140.00),
			High:   decimal.NewFromFloat(142.00),
			Low:    decimal.NewFromFloat(139.00),
			Close:  decimal.NewFromFloat(141.50),
			Volume: 25000000,
		}
		err := testDB.CreatePriceData(priceData)
		require.NoError(t, err)

		retrieved, err := testDB.GetPriceDataByID(priceData.ID)
		require.NoError(t, err)
		assert.Equal(t, "GOOGL", retrieved.Symbol)
		assert.True(t, decimal.NewFromFloat(141.50).Equal(retrieved.Close))
	})

	t.Run("GetPriceDataBySymbol retrieves with limit", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Insert 5 days of data
		for i := 0; i < 5; i++ {
			priceData := &models.PriceDataDaily{
				Symbol: "MSFT",
				Date:   time.Date(2024, 1, 15+i, 0, 0, 0, 0, time.UTC),
				Open:   decimal.NewFromFloat(370.00 + float64(i)),
				High:   decimal.NewFromFloat(375.00 + float64(i)),
				Low:    decimal.NewFromFloat(368.00 + float64(i)),
				Close:  decimal.NewFromFloat(373.00 + float64(i)),
				Volume: 30000000,
			}
			err := testDB.CreatePriceData(priceData)
			require.NoError(t, err)
		}

		// Get with limit of 3
		retrieved, err := testDB.GetPriceDataBySymbol("MSFT", 3)
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)

		// Should be ordered by date DESC
		assert.Equal(t, 2024, retrieved[0].Date.Year())
		assert.Equal(t, time.January, retrieved[0].Date.Month())
		assert.Equal(t, 19, retrieved[0].Date.Day())
	})

	t.Run("GetPriceDataRange retrieves data in date range", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Insert 10 days of data
		for i := 0; i < 10; i++ {
			priceData := &models.PriceDataDaily{
				Symbol: "NVDA",
				Date:   time.Date(2024, 1, 10+i, 0, 0, 0, 0, time.UTC),
				Open:   decimal.NewFromFloat(450.00 + float64(i)),
				High:   decimal.NewFromFloat(455.00 + float64(i)),
				Low:    decimal.NewFromFloat(448.00 + float64(i)),
				Close:  decimal.NewFromFloat(452.00 + float64(i)),
				Volume: 40000000,
			}
			err := testDB.CreatePriceData(priceData)
			require.NoError(t, err)
		}

		startDate := time.Date(2024, 1, 12, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC)

		retrieved, err := testDB.GetPriceDataRange("NVDA", startDate, endDate)
		require.NoError(t, err)
		assert.Len(t, retrieved, 5) // Jan 12, 13, 14, 15, 16

		// Should be ordered by date ASC
		assert.Equal(t, startDate.Year(), retrieved[0].Date.Year())
		assert.Equal(t, startDate.Month(), retrieved[0].Date.Month())
		assert.Equal(t, startDate.Day(), retrieved[0].Date.Day())
	})

	t.Run("GetLatestPriceData retrieves most recent", func(t *testing.T) {
		testDB.TruncateAll(t)

		prices := []*models.PriceDataDaily{
			{Symbol: "TSLA", Date: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), Open: decimal.NewFromFloat(240.00), High: decimal.NewFromFloat(245.00), Low: decimal.NewFromFloat(238.00), Close: decimal.NewFromFloat(243.00), Volume: 100000000},
			{Symbol: "TSLA", Date: time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC), Open: decimal.NewFromFloat(243.00), High: decimal.NewFromFloat(250.00), Low: decimal.NewFromFloat(242.00), Close: decimal.NewFromFloat(248.00), Volume: 110000000},
			{Symbol: "TSLA", Date: time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC), Open: decimal.NewFromFloat(248.00), High: decimal.NewFromFloat(255.00), Low: decimal.NewFromFloat(247.00), Close: decimal.NewFromFloat(253.00), Volume: 120000000},
		}

		for _, p := range prices {
			err := testDB.CreatePriceData(p)
			require.NoError(t, err)
		}

		latest, err := testDB.GetLatestPriceData("TSLA")
		require.NoError(t, err)
		assert.Equal(t, 2024, latest.Date.Year())
		assert.Equal(t, time.January, latest.Date.Month())
		assert.Equal(t, 17, latest.Date.Day())
		assert.True(t, decimal.NewFromFloat(253.00).Equal(latest.Close))
	})

	t.Run("GetLatestPriceData returns error for non-existent symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		_, err := testDB.GetLatestPriceData("NONEXISTENT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no price data found")
	})

	t.Run("DeletePriceData removes record", func(t *testing.T) {
		testDB.TruncateAll(t)

		priceData := &models.PriceDataDaily{
			Symbol: "AMD",
			Date:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			Open:   decimal.NewFromFloat(120.00),
			High:   decimal.NewFromFloat(125.00),
			Low:    decimal.NewFromFloat(118.00),
			Close:  decimal.NewFromFloat(123.00),
			Volume: 50000000,
		}
		err := testDB.CreatePriceData(priceData)
		require.NoError(t, err)

		err = testDB.DeletePriceData(priceData.ID)
		require.NoError(t, err)

		_, err = testDB.GetPriceDataByID(priceData.ID)
		require.Error(t, err)
	})

	t.Run("DeletePriceDataBySymbol removes all records for symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create data for two symbols
		for i := 0; i < 3; i++ {
			err := testDB.CreatePriceData(&models.PriceDataDaily{
				Symbol: "DELETE_ME", Date: time.Date(2024, 1, 15+i, 0, 0, 0, 0, time.UTC),
				Open: decimal.NewFromFloat(100.00), High: decimal.NewFromFloat(105.00),
				Low: decimal.NewFromFloat(98.00), Close: decimal.NewFromFloat(103.00), Volume: 1000000,
			})
			require.NoError(t, err)

			err = testDB.CreatePriceData(&models.PriceDataDaily{
				Symbol: "KEEP_ME", Date: time.Date(2024, 1, 15+i, 0, 0, 0, 0, time.UTC),
				Open: decimal.NewFromFloat(200.00), High: decimal.NewFromFloat(205.00),
				Low: decimal.NewFromFloat(198.00), Close: decimal.NewFromFloat(203.00), Volume: 2000000,
			})
			require.NoError(t, err)
		}

		err := testDB.DeletePriceDataBySymbol("DELETE_ME")
		require.NoError(t, err)

		deleted, err := testDB.GetPriceDataBySymbol("DELETE_ME", 10)
		require.NoError(t, err)
		assert.Len(t, deleted, 0)

		kept, err := testDB.GetPriceDataBySymbol("KEEP_ME", 10)
		require.NoError(t, err)
		assert.Len(t, kept, 3)
	})

	t.Run("DeletePriceDataOlderThan removes old records", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create data spanning multiple days
		for i := 0; i < 10; i++ {
			err := testDB.CreatePriceData(&models.PriceDataDaily{
				Symbol: "OLD_DATA", Date: time.Date(2024, 1, 10+i, 0, 0, 0, 0, time.UTC),
				Open: decimal.NewFromFloat(100.00), High: decimal.NewFromFloat(105.00),
				Low: decimal.NewFromFloat(98.00), Close: decimal.NewFromFloat(103.00), Volume: 1000000,
			})
			require.NoError(t, err)
		}

		cutoffDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		deleted, err := testDB.DeletePriceDataOlderThan(cutoffDate)
		require.NoError(t, err)
		assert.Equal(t, int64(5), deleted) // Jan 10, 11, 12, 13, 14

		remaining, err := testDB.GetPriceDataBySymbol("OLD_DATA", 100)
		require.NoError(t, err)
		assert.Len(t, remaining, 5) // Jan 15, 16, 17, 18, 19
	})
}
