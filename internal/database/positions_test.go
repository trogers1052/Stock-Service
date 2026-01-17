package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestPositionsRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("CreatePosition creates new position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			Symbol:          "AAPL",
			Quantity:        decimal.NewFromFloat(100),
			EntryPrice:      decimal.NewFromFloat(150.00),
			EntryDate:       time.Now().Add(-7 * 24 * time.Hour),
			CurrentPrice:    decimal.NewFromFloat(175.00),
			UnrealizedPnlPct: decimal.NewFromFloat(16.67),
			DaysHeld:        7,
			EntryRSI:        decimal.NewFromFloat(32.5),
			EntryReason:     "RSI oversold bounce",
			Sector:          "Technology",
			Industry:        "Consumer Electronics",
			PositionSizePct: decimal.NewFromFloat(10.0),
		}

		err := testDB.CreatePosition(position)
		require.NoError(t, err)
		assert.NotZero(t, position.ID)
		assert.False(t, position.CreatedAt.IsZero())
		assert.False(t, position.UpdatedAt.IsZero())
	})

	t.Run("GetPositionByID retrieves position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			Symbol:     "GOOGL",
			Quantity:   decimal.NewFromFloat(50),
			EntryPrice: decimal.NewFromFloat(130.00),
			EntryDate:  time.Now(),
		}
		err := testDB.CreatePosition(position)
		require.NoError(t, err)

		retrieved, err := testDB.GetPositionByID(position.ID)
		require.NoError(t, err)
		assert.Equal(t, "GOOGL", retrieved.Symbol)
		assert.True(t, decimal.NewFromFloat(50).Equal(retrieved.Quantity))
		assert.True(t, decimal.NewFromFloat(130.00).Equal(retrieved.EntryPrice))
	})

	t.Run("GetPositionByID returns error for non-existent ID", func(t *testing.T) {
		testDB.TruncateAll(t)

		_, err := testDB.GetPositionByID(99999)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("GetPositionBySymbol retrieves position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			Symbol:     "MSFT",
			Quantity:   decimal.NewFromFloat(25),
			EntryPrice: decimal.NewFromFloat(370.00),
			EntryDate:  time.Now(),
		}
		err := testDB.CreatePosition(position)
		require.NoError(t, err)

		retrieved, err := testDB.GetPositionBySymbol("MSFT")
		require.NoError(t, err)
		assert.Equal(t, "MSFT", retrieved.Symbol)
		assert.Equal(t, position.ID, retrieved.ID)
	})

	t.Run("GetAllPositions retrieves all positions ordered by entry date", func(t *testing.T) {
		testDB.TruncateAll(t)

		now := time.Now()
		positions := []*models.Position{
			{Symbol: "AAPL", Quantity: decimal.NewFromFloat(100), EntryPrice: decimal.NewFromFloat(150.00), EntryDate: now.Add(-3 * 24 * time.Hour)},
			{Symbol: "GOOGL", Quantity: decimal.NewFromFloat(50), EntryPrice: decimal.NewFromFloat(130.00), EntryDate: now.Add(-1 * 24 * time.Hour)},
			{Symbol: "MSFT", Quantity: decimal.NewFromFloat(25), EntryPrice: decimal.NewFromFloat(370.00), EntryDate: now.Add(-5 * 24 * time.Hour)},
		}

		for _, p := range positions {
			err := testDB.CreatePosition(p)
			require.NoError(t, err)
		}

		retrieved, err := testDB.GetAllPositions()
		require.NoError(t, err)
		assert.Len(t, retrieved, 3)

		// Should be ordered by entry_date DESC (most recent first)
		assert.Equal(t, "GOOGL", retrieved[0].Symbol)
		assert.Equal(t, "AAPL", retrieved[1].Symbol)
		assert.Equal(t, "MSFT", retrieved[2].Symbol)
	})

	t.Run("UpdatePosition updates existing position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			Symbol:       "NVDA",
			Quantity:     decimal.NewFromFloat(30),
			EntryPrice:   decimal.NewFromFloat(400.00),
			EntryDate:    time.Now(),
			CurrentPrice: decimal.NewFromFloat(420.00),
		}
		err := testDB.CreatePosition(position)
		require.NoError(t, err)

		// Update position
		position.CurrentPrice = decimal.NewFromFloat(450.00)
		position.UnrealizedPnlPct = decimal.NewFromFloat(12.5)
		position.DaysHeld = 5

		err = testDB.UpdatePosition(position)
		require.NoError(t, err)

		// Verify update
		retrieved, err := testDB.GetPositionByID(position.ID)
		require.NoError(t, err)
		assert.True(t, decimal.NewFromFloat(450.00).Equal(retrieved.CurrentPrice))
		assert.True(t, decimal.NewFromFloat(12.5).Equal(retrieved.UnrealizedPnlPct))
		assert.Equal(t, 5, retrieved.DaysHeld)
	})

	t.Run("UpdatePosition returns error for non-existent position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			ID:         99999,
			Symbol:     "FAKE",
			Quantity:   decimal.NewFromFloat(10),
			EntryPrice: decimal.NewFromFloat(100.00),
			EntryDate:  time.Now(),
		}

		err := testDB.UpdatePosition(position)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("DeletePosition removes position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			Symbol:     "TSLA",
			Quantity:   decimal.NewFromFloat(20),
			EntryPrice: decimal.NewFromFloat(240.00),
			EntryDate:  time.Now(),
		}
		err := testDB.CreatePosition(position)
		require.NoError(t, err)

		err = testDB.DeletePosition(position.ID)
		require.NoError(t, err)

		_, err = testDB.GetPositionByID(position.ID)
		require.Error(t, err)
	})

	t.Run("DeletePositionBySymbol removes position", func(t *testing.T) {
		testDB.TruncateAll(t)

		position := &models.Position{
			Symbol:     "AMD",
			Quantity:   decimal.NewFromFloat(75),
			EntryPrice: decimal.NewFromFloat(120.00),
			EntryDate:  time.Now(),
		}
		err := testDB.CreatePosition(position)
		require.NoError(t, err)

		err = testDB.DeletePositionBySymbol("AMD")
		require.NoError(t, err)

		_, err = testDB.GetPositionBySymbol("AMD")
		require.Error(t, err)
	})

	t.Run("CreatePosition enforces unique symbol constraint", func(t *testing.T) {
		testDB.TruncateAll(t)

		position1 := &models.Position{
			Symbol:     "UNIQUE",
			Quantity:   decimal.NewFromFloat(10),
			EntryPrice: decimal.NewFromFloat(100.00),
			EntryDate:  time.Now(),
		}
		err := testDB.CreatePosition(position1)
		require.NoError(t, err)

		position2 := &models.Position{
			Symbol:     "UNIQUE",
			Quantity:   decimal.NewFromFloat(20),
			EntryPrice: decimal.NewFromFloat(110.00),
			EntryDate:  time.Now(),
		}
		err = testDB.CreatePosition(position2)
		require.Error(t, err) // Should fail due to unique constraint
	})
}
