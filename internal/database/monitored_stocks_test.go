package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestMonitoredStocksRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	// Helper to create a stock for foreign key references
	createTestStock := func(t *testing.T, symbol string) {
		stock := &models.Stock{
			Symbol:       symbol,
			Name:         symbol + " Inc.",
			CurrentPrice: 100.00,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)
	}

	t.Run("CreateMonitoredStock creates new monitored stock", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "AAPL")

		buyZoneLow := 170.00
		buyZoneHigh := 175.00
		targetPrice := 200.00
		stopLossPrice := 165.00
		rsiThreshold := 30.0

		monitored := &models.MonitoredStock{
			Symbol:               "AAPL",
			Enabled:              true,
			Priority:             1,
			BuyZoneLow:           &buyZoneLow,
			BuyZoneHigh:          &buyZoneHigh,
			TargetPrice:          &targetPrice,
			StopLossPrice:        &stopLossPrice,
			AlertOnBuyZone:       true,
			AlertOnRSIOversold:   true,
			RSIOversoldThreshold: &rsiThreshold,
			Notes:                "Watch for earnings",
			Reason:               "Strong technicals",
		}

		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)
		assert.False(t, monitored.AddedAt.IsZero())
	})

	t.Run("CreateMonitoredStock defaults priority to 1", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "GOOGL")

		monitored := &models.MonitoredStock{
			Symbol:  "GOOGL",
			Enabled: true,
			// Priority not set
		}

		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("GOOGL")
		require.NoError(t, err)
		assert.Equal(t, 1, retrieved.Priority)
	})

	t.Run("CreateMonitoredStock upserts on conflict", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "MSFT")

		// Create initial
		monitored1 := &models.MonitoredStock{
			Symbol:   "MSFT",
			Enabled:  true,
			Priority: 1,
			Notes:    "Initial notes",
		}
		err := testDB.CreateMonitoredStock(monitored1)
		require.NoError(t, err)

		// Upsert with updated values
		newTarget := 400.00
		monitored2 := &models.MonitoredStock{
			Symbol:      "MSFT",
			Enabled:     true,
			Priority:    2,
			TargetPrice: &newTarget,
			Notes:       "Updated notes",
		}
		err = testDB.CreateMonitoredStock(monitored2)
		require.NoError(t, err)

		// Verify updated
		retrieved, err := testDB.GetMonitoredStockBySymbol("MSFT")
		require.NoError(t, err)
		assert.Equal(t, 2, retrieved.Priority)
		assert.Equal(t, "Updated notes", retrieved.Notes)
		assert.Equal(t, 400.00, *retrieved.TargetPrice)
	})

	t.Run("GetMonitoredStockBySymbol retrieves stock", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "NVDA")

		buyZone := 450.00
		monitored := &models.MonitoredStock{
			Symbol:       "NVDA",
			Enabled:      true,
			Priority:     1,
			BuyZoneLow:   &buyZone,
			AlertOnBuyZone: true,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("NVDA")
		require.NoError(t, err)
		assert.Equal(t, "NVDA", retrieved.Symbol)
		assert.True(t, retrieved.Enabled)
		assert.True(t, retrieved.AlertOnBuyZone)
		assert.Equal(t, 450.00, *retrieved.BuyZoneLow)
	})

	t.Run("GetMonitoredStockBySymbol returns error for non-existent", func(t *testing.T) {
		testDB.TruncateAll(t)

		_, err := testDB.GetMonitoredStockBySymbol("NONEXISTENT")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("GetAllMonitoredStocks retrieves all stocks ordered by priority", func(t *testing.T) {
		testDB.TruncateAll(t)

		symbols := []string{"HIGH", "MED", "LOW"}
		for _, sym := range symbols {
			createTestStock(t, sym)
		}

		monitoredStocks := []*models.MonitoredStock{
			{Symbol: "LOW", Enabled: true, Priority: 3},
			{Symbol: "HIGH", Enabled: true, Priority: 1},
			{Symbol: "MED", Enabled: true, Priority: 2},
		}

		for _, m := range monitoredStocks {
			err := testDB.CreateMonitoredStock(m)
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetAllMonitoredStocks()
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)

		// Should be ordered by priority ASC
		assert.Equal(t, "HIGH", retrieved[0].Symbol)
		assert.Equal(t, "MED", retrieved[1].Symbol)
		assert.Equal(t, "LOW", retrieved[2].Symbol)
	})

	t.Run("GetEnabledMonitoredStocks retrieves only enabled", func(t *testing.T) {
		testDB.TruncateAll(t)

		symbols := []string{"EN1", "DIS1", "EN2"}
		for _, sym := range symbols {
			createTestStock(t, sym)
		}

		monitoredStocks := []*models.MonitoredStock{
			{Symbol: "EN1", Enabled: true, Priority: 1},
			{Symbol: "DIS1", Enabled: false, Priority: 1},
			{Symbol: "EN2", Enabled: true, Priority: 2},
		}

		for _, m := range monitoredStocks {
			err := testDB.CreateMonitoredStock(m)
			require.NoError(t, err)
		}

		enabled, err := testDB.GetEnabledMonitoredStocks()
		require.NoError(t, err)
		assert.Len(t, enabled, 2)
	})

	t.Run("GetMonitoredStocksByPriority retrieves by priority", func(t *testing.T) {
		testDB.TruncateAll(t)

		symbols := []string{"P1A", "P1B", "P2A", "P3A"}
		for _, sym := range symbols {
			createTestStock(t, sym)
		}

		monitoredStocks := []*models.MonitoredStock{
			{Symbol: "P1A", Enabled: true, Priority: 1},
			{Symbol: "P1B", Enabled: true, Priority: 1},
			{Symbol: "P2A", Enabled: true, Priority: 2},
			{Symbol: "P3A", Enabled: true, Priority: 3},
		}

		for _, m := range monitoredStocks {
			err := testDB.CreateMonitoredStock(m)
			require.NoError(t, err)
		}

		priority1, err := testDB.GetMonitoredStocksByPriority(1)
		require.NoError(t, err)
		assert.Len(t, priority1, 2)

		priority2, err := testDB.GetMonitoredStocksByPriority(2)
		require.NoError(t, err)
		assert.Len(t, priority2, 1)
	})

	t.Run("GetMonitoredSymbols returns just symbols", func(t *testing.T) {
		testDB.TruncateAll(t)

		symbols := []string{"SYM1", "SYM2", "SYM3"}
		for _, sym := range symbols {
			createTestStock(t, sym)
			err := testDB.CreateMonitoredStock(&models.MonitoredStock{Symbol: sym, Enabled: true, Priority: 1})
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetMonitoredSymbols()
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)
		assert.Contains(t, retrieved, "SYM1")
		assert.Contains(t, retrieved, "SYM2")
		assert.Contains(t, retrieved, "SYM3")
	})

	t.Run("UpdateMonitoredStock updates stock", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "UPDATE")

		monitored := &models.MonitoredStock{
			Symbol:   "UPDATE",
			Enabled:  true,
			Priority: 1,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		// Update
		newTarget := 200.00
		monitored.Priority = 2
		monitored.TargetPrice = &newTarget
		monitored.Notes = "Updated"

		err = testDB.UpdateMonitoredStock(monitored)
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("UPDATE")
		require.NoError(t, err)
		assert.Equal(t, 2, retrieved.Priority)
		assert.Equal(t, 200.00, *retrieved.TargetPrice)
		assert.Equal(t, "Updated", retrieved.Notes)
	})

	t.Run("EnableMonitoredStock enables stock", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "ENABLE")

		monitored := &models.MonitoredStock{
			Symbol:   "ENABLE",
			Enabled:  false,
			Priority: 1,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		err = testDB.EnableMonitoredStock("ENABLE")
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("ENABLE")
		require.NoError(t, err)
		assert.True(t, retrieved.Enabled)
	})

	t.Run("DisableMonitoredStock disables stock", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "DISABLE")

		monitored := &models.MonitoredStock{
			Symbol:   "DISABLE",
			Enabled:  true,
			Priority: 1,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		err = testDB.DisableMonitoredStock("DISABLE")
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("DISABLE")
		require.NoError(t, err)
		assert.False(t, retrieved.Enabled)
	})

	t.Run("SetBuyZone updates buy zone", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "BUYZONE")

		monitored := &models.MonitoredStock{
			Symbol:   "BUYZONE",
			Enabled:  true,
			Priority: 1,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		err = testDB.SetBuyZone("BUYZONE", 95.00, 100.00)
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("BUYZONE")
		require.NoError(t, err)
		assert.Equal(t, 95.00, *retrieved.BuyZoneLow)
		assert.Equal(t, 100.00, *retrieved.BuyZoneHigh)
	})

	t.Run("SetTargetAndStopLoss updates target and stop", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "TARGET")

		monitored := &models.MonitoredStock{
			Symbol:   "TARGET",
			Enabled:  true,
			Priority: 1,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		err = testDB.SetTargetAndStopLoss("TARGET", 150.00, 85.00)
		require.NoError(t, err)

		retrieved, err := testDB.GetMonitoredStockBySymbol("TARGET")
		require.NoError(t, err)
		assert.Equal(t, 150.00, *retrieved.TargetPrice)
		assert.Equal(t, 85.00, *retrieved.StopLossPrice)
	})

	t.Run("DeleteMonitoredStock removes stock", func(t *testing.T) {
		testDB.TruncateAll(t)
		createTestStock(t, "DELETE")

		monitored := &models.MonitoredStock{
			Symbol:   "DELETE",
			Enabled:  true,
			Priority: 1,
		}
		err := testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		err = testDB.DeleteMonitoredStock("DELETE")
		require.NoError(t, err)

		_, err = testDB.GetMonitoredStockBySymbol("DELETE")
		require.Error(t, err)
	})

	t.Run("GetStocksInBuyZone returns stocks in buy zone", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create stocks with specific current prices
		for _, data := range []struct {
			symbol       string
			currentPrice float64
		}{
			{"INZONE1", 100.00}, // In buy zone (95-105)
			{"INZONE2", 102.00}, // In buy zone
			{"BELOW", 90.00},    // Below buy zone
			{"ABOVE", 110.00},   // Above buy zone
		} {
			stock := &models.Stock{
				Symbol:       data.symbol,
				Name:         data.symbol + " Inc.",
				CurrentPrice: data.currentPrice,
				LastUpdated:  time.Now(),
			}
			err := testDB.SaveStock(stock)
			require.NoError(t, err)

			buyLow := 95.00
			buyHigh := 105.00
			monitored := &models.MonitoredStock{
				Symbol:      data.symbol,
				Enabled:     true,
				Priority:    1,
				BuyZoneLow:  &buyLow,
				BuyZoneHigh: &buyHigh,
			}
			err = testDB.CreateMonitoredStock(monitored)
			require.NoError(t, err)
		}

		inZone, err := testDB.GetStocksInBuyZone()
		require.NoError(t, err)
		assert.Len(t, inZone, 2)

		symbols := make([]string, len(inZone))
		for i, s := range inZone {
			symbols[i] = s.Symbol
		}
		assert.Contains(t, symbols, "INZONE1")
		assert.Contains(t, symbols, "INZONE2")
	})

	t.Run("GetStocksInBuyZone excludes disabled stocks", func(t *testing.T) {
		testDB.TruncateAll(t)

		// Create a stock in buy zone but disabled
		stock := &models.Stock{
			Symbol:       "DISABLED",
			Name:         "Disabled Inc.",
			CurrentPrice: 100.00,
			LastUpdated:  time.Now(),
		}
		err := testDB.SaveStock(stock)
		require.NoError(t, err)

		buyLow := 95.00
		buyHigh := 105.00
		monitored := &models.MonitoredStock{
			Symbol:      "DISABLED",
			Enabled:     false, // Disabled
			Priority:    1,
			BuyZoneLow:  &buyLow,
			BuyZoneHigh: &buyHigh,
		}
		err = testDB.CreateMonitoredStock(monitored)
		require.NoError(t, err)

		inZone, err := testDB.GetStocksInBuyZone()
		require.NoError(t, err)
		assert.Len(t, inZone, 0)
	})
}
