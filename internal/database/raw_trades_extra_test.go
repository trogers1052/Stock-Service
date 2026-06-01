package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func truncateRawTrades(t *testing.T, tdb *TestDB) {
	t.Helper()
	_, err := tdb.GetRawConn().Exec("TRUNCATE TABLE raw_trades RESTART IDENTITY CASCADE")
	require.NoError(t, err)
}

func newRawTrade(orderID, symbol, side string) *models.RawTrade {
	return &models.RawTrade{
		OrderID:    orderID,
		Source:     "robinhood",
		Symbol:     symbol,
		Side:       side,
		Quantity:   decimal.NewFromInt(10),
		Price:      decimal.NewFromFloat(100.50),
		TotalCost:  decimal.NewFromFloat(1005.00),
		Fees:       decimal.NewFromFloat(0.05),
		ExecutedAt: time.Now(),
	}
}

func TestRawTradesRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("CreateRawTrade and GetRawTradeByID", func(t *testing.T) {
		truncateRawTrades(t, testDB)

		rt := newRawTrade("ord-1", "AAPL", models.TradeTypeBuy)
		require.NoError(t, testDB.CreateRawTrade(rt))
		assert.NotZero(t, rt.ID)
		assert.False(t, rt.CreatedAt.IsZero())

		got, err := testDB.GetRawTradeByID(rt.ID)
		require.NoError(t, err)
		assert.Equal(t, "ord-1", got.OrderID)
		assert.Equal(t, "AAPL", got.Symbol)
		assert.Equal(t, models.TradeTypeBuy, got.Side)
		assert.True(t, got.Fees.Equal(decimal.NewFromFloat(0.05)))
		assert.Nil(t, got.PositionID)
		assert.Nil(t, got.TradeHistoryID)
	})

	t.Run("GetRawTradeByID returns error for missing", func(t *testing.T) {
		truncateRawTrades(t, testDB)

		_, err := testDB.GetRawTradeByID(999999)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("RawTradeExistsByOrderID", func(t *testing.T) {
		truncateRawTrades(t, testDB)

		rt := newRawTrade("ord-exist", "MSFT", models.TradeTypeBuy)
		require.NoError(t, testDB.CreateRawTrade(rt))

		exists, err := testDB.RawTradeExistsByOrderID("ord-exist", "robinhood")
		require.NoError(t, err)
		assert.True(t, exists)

		notExist, err := testDB.RawTradeExistsByOrderID("ord-exist", "manual")
		require.NoError(t, err)
		assert.False(t, notExist)

		missing, err := testDB.RawTradeExistsByOrderID("nope", "robinhood")
		require.NoError(t, err)
		assert.False(t, missing)
	})

	t.Run("GetRawTradesBySymbol", func(t *testing.T) {
		truncateRawTrades(t, testDB)

		a := newRawTrade("o1", "TSLA", models.TradeTypeBuy)
		a.ExecutedAt = time.Now().Add(-2 * time.Hour)
		b := newRawTrade("o2", "TSLA", models.TradeTypeSell)
		b.ExecutedAt = time.Now()
		c := newRawTrade("o3", "AMZN", models.TradeTypeBuy)
		require.NoError(t, testDB.CreateRawTrade(a))
		require.NoError(t, testDB.CreateRawTrade(b))
		require.NoError(t, testDB.CreateRawTrade(c))

		tsla, err := testDB.GetRawTradesBySymbol("TSLA", 10)
		require.NoError(t, err)
		require.Len(t, tsla, 2)
		// ordered executed_at DESC => o2 first
		assert.Equal(t, "o2", tsla[0].OrderID)

		limited, err := testDB.GetRawTradesBySymbol("TSLA", 1)
		require.NoError(t, err)
		assert.Len(t, limited, 1)
	})

	t.Run("position linking: update, get, unlinked, link", func(t *testing.T) {
		truncateRawTrades(t, testDB)
		testDB.TruncateAll(t)

		// Create a position to satisfy the FK.
		pos := &models.Position{
			Symbol:     "NVDA",
			Quantity:   decimal.NewFromInt(5),
			EntryPrice: decimal.NewFromFloat(400),
			EntryDate:  time.Now(),
		}
		require.NoError(t, testDB.CreatePosition(pos))

		// Two unlinked trades.
		t1 := newRawTrade("p1", "NVDA", models.TradeTypeBuy)
		t1.ExecutedAt = time.Now().Add(-time.Hour)
		t2 := newRawTrade("p2", "NVDA", models.TradeTypeSell)
		t2.ExecutedAt = time.Now()
		require.NoError(t, testDB.CreateRawTrade(t1))
		require.NoError(t, testDB.CreateRawTrade(t2))

		unlinked, err := testDB.GetUnlinkedRawTradesBySymbol("NVDA")
		require.NoError(t, err)
		require.Len(t, unlinked, 2)
		assert.Equal(t, "p1", unlinked[0].OrderID) // ASC

		// Link one to the position.
		require.NoError(t, testDB.UpdateRawTradePositionID(t1.ID, pos.ID))

		linked, err := testDB.GetRawTradesByPositionID(pos.ID)
		require.NoError(t, err)
		require.Len(t, linked, 1)
		require.NotNil(t, linked[0].PositionID)
		assert.Equal(t, pos.ID, *linked[0].PositionID)

		// Now only one remains unlinked.
		stillUnlinked, err := testDB.GetUnlinkedRawTradesBySymbol("NVDA")
		require.NoError(t, err)
		assert.Len(t, stillUnlinked, 1)

		// Updating a missing trade errors.
		err = testDB.UpdateRawTradePositionID(999999, pos.ID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("trade history linking", func(t *testing.T) {
		truncateRawTrades(t, testDB)
		testDB.TruncateAll(t)

		pos := &models.Position{
			Symbol: "META", Quantity: decimal.NewFromInt(3),
			EntryPrice: decimal.NewFromFloat(300), EntryDate: time.Now(),
		}
		require.NoError(t, testDB.CreatePosition(pos))

		// Create a trade history row directly for the FK.
		var historyID int
		err := testDB.GetRawConn().QueryRow(
			`INSERT INTO trades_history (symbol, trade_type, quantity, price, total_cost, executed_at)
			 VALUES ('META', 'SELL', 3, 320, 960, NOW()) RETURNING id`,
		).Scan(&historyID)
		require.NoError(t, err)

		rt := newRawTrade("h1", "META", models.TradeTypeSell)
		require.NoError(t, testDB.CreateRawTrade(rt))
		require.NoError(t, testDB.UpdateRawTradePositionID(rt.ID, pos.ID))

		// UpdateRawTradeHistoryID directly.
		require.NoError(t, testDB.UpdateRawTradeHistoryID(rt.ID, historyID))
		got, err := testDB.GetRawTradeByID(rt.ID)
		require.NoError(t, err)
		require.NotNil(t, got.TradeHistoryID)
		assert.Equal(t, historyID, *got.TradeHistoryID)

		// Missing trade => error.
		err = testDB.UpdateRawTradeHistoryID(999999, historyID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		// LinkRawTradesToTradeHistory by position (no error even if applied).
		require.NoError(t, testDB.LinkRawTradesToTradeHistory(pos.ID, historyID))
	})
}
