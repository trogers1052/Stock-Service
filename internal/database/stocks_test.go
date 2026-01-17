package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestStocksRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("SaveStock creates new stock", func(t *testing.T) {
		testDB.TruncateAll(t)

		stock := &models.Stock{
			Symbol:        "AAPL",
			Name:          "Apple Inc.",
			Exchange:      "NASDAQ",
			Sector:        "Technology",
			Industry:      "Consumer Electronics",
			CurrentPrice:  175.50,
			PreviousClose: 174.00,
			ChangeAmount:  1.50,
			ChangePercent: 0.86,
			DayHigh:       176.00,
			DayLow:        173.50,
			Volume:        50000000,
			AverageVolume: 55000000,
			Week52High:    199.62,
			Week52Low:     124.17,
			MarketCap:     2800000000000,
			LastUpdated:   time.Now(),
		}

		err := testDB.SaveStock(stock)
		require.NoError(t, err)
		assert.NotEmpty(t, stock.ID)
	})

	t.Run("SaveStock updates existing stock", func(t *testing.T) {
		testDB.TruncateAll(t)

		stock := &models.Stock{
			Symbol:       "AAPL",
			Name:         "Apple Inc.",
			CurrentPrice: 175.50,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)
		originalID := stock.ID

		// Update the stock
		stock.CurrentPrice = 180.00
		stock.LastUpdated = time.Now()
		err = testDB.SaveStock(stock)
		require.NoError(t, err)

		// ID should remain the same (upsert)
		assert.Equal(t, originalID, stock.ID)

		// Verify the update
		retrieved, err := testDB.GetStock("AAPL")
		require.NoError(t, err)
		assert.Equal(t, 180.00, retrieved.CurrentPrice)
	})

	t.Run("GetStock retrieves by symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		stock := &models.Stock{
			Symbol:       "GOOGL",
			Name:         "Alphabet Inc.",
			Exchange:     "NASDAQ",
			Sector:       "Technology",
			CurrentPrice: 140.00,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)

		retrieved, err := testDB.GetStock("GOOGL")
		require.NoError(t, err)
		assert.Equal(t, "GOOGL", retrieved.Symbol)
		assert.Equal(t, "Alphabet Inc.", retrieved.Name)
		assert.Equal(t, "NASDAQ", retrieved.Exchange)
		assert.Equal(t, 140.00, retrieved.CurrentPrice)
	})

	t.Run("GetStock returns error for non-existent symbol", func(t *testing.T) {
		testDB.TruncateAll(t)

		_, err := testDB.GetStock("NONEXISTENT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("GetStockByID retrieves by UUID", func(t *testing.T) {
		testDB.TruncateAll(t)

		stock := &models.Stock{
			Symbol:       "MSFT",
			Name:         "Microsoft Corporation",
			CurrentPrice: 380.00,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)

		retrieved, err := testDB.GetStockByID(stock.ID)
		require.NoError(t, err)
		assert.Equal(t, "MSFT", retrieved.Symbol)
		assert.Equal(t, stock.ID, retrieved.ID)
	})

	t.Run("GetAllStocks retrieves all stocks", func(t *testing.T) {
		testDB.TruncateAll(t)

		stocks := []*models.Stock{
			{Symbol: "AAPL", Name: "Apple Inc.", CurrentPrice: 175.00, LastUpdated: time.Now()},
			{Symbol: "GOOGL", Name: "Alphabet Inc.", CurrentPrice: 140.00, LastUpdated: time.Now()},
			{Symbol: "MSFT", Name: "Microsoft", CurrentPrice: 380.00, LastUpdated: time.Now()},
		}

		for _, s := range stocks {
			err := testDB.SaveStock(s)
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetAllStocks()
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)

		// Should be ordered by symbol
		assert.Equal(t, "AAPL", retrieved[0].Symbol)
		assert.Equal(t, "GOOGL", retrieved[1].Symbol)
		assert.Equal(t, "MSFT", retrieved[2].Symbol)
	})

	t.Run("GetStocksBySector retrieves stocks in sector", func(t *testing.T) {
		testDB.TruncateAll(t)

		stocks := []*models.Stock{
			{Symbol: "AAPL", Name: "Apple Inc.", Sector: "Technology", CurrentPrice: 175.00, LastUpdated: time.Now()},
			{Symbol: "GOOGL", Name: "Alphabet Inc.", Sector: "Technology", CurrentPrice: 140.00, LastUpdated: time.Now()},
			{Symbol: "JPM", Name: "JPMorgan Chase", Sector: "Financial", CurrentPrice: 180.00, LastUpdated: time.Now()},
		}

		for _, s := range stocks {
			err := testDB.SaveStock(s)
			require.NoError(t, err)
		}

		techStocks, err := testDB.GetStocksBySector("Technology")
		require.NoError(t, err)
		assert.Len(t, techStocks, 2)

		finStocks, err := testDB.GetStocksBySector("Financial")
		require.NoError(t, err)
		assert.Len(t, finStocks, 1)
		assert.Equal(t, "JPM", finStocks[0].Symbol)
	})

	t.Run("DeleteStock removes stock", func(t *testing.T) {
		testDB.TruncateAll(t)

		stock := &models.Stock{
			Symbol:       "TSLA",
			Name:         "Tesla Inc.",
			CurrentPrice: 250.00,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)

		err = testDB.DeleteStock("TSLA")
		require.NoError(t, err)

		_, err = testDB.GetStock("TSLA")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("DeleteStock returns error for non-existent stock", func(t *testing.T) {
		testDB.TruncateAll(t)

		err := testDB.DeleteStock("NONEXISTENT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("DeleteStockByID removes stock", func(t *testing.T) {
		testDB.TruncateAll(t)

		stock := &models.Stock{
			Symbol:       "NVDA",
			Name:         "NVIDIA Corporation",
			CurrentPrice: 450.00,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)

		err = testDB.DeleteStockByID(stock.ID)
		require.NoError(t, err)

		_, err = testDB.GetStockByID(stock.ID)
		require.Error(t, err)
	})
}
