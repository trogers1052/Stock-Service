package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStocksRepositoryUpsertHelpers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	// queryStockField reads a single text column directly, tolerating NULLs
	// (UpsertStockBasic leaves exchange/sector NULL, which GetStock cannot scan).
	queryStockField := func(t *testing.T, symbol, column string) string {
		t.Helper()
		var v *string
		err := testDB.GetRawConn().QueryRow(
			"SELECT "+column+" FROM stocks WHERE symbol = $1", symbol,
		).Scan(&v)
		require.NoError(t, err)
		if v == nil {
			return ""
		}
		return *v
	}

	t.Run("UpsertStockBasic inserts then preserves name", func(t *testing.T) {
		testDB.TruncateAll(t)

		require.NoError(t, testDB.UpsertStockBasic("AAPL", "Apple Inc."))

		exists, err := testDB.StockExists("AAPL")
		require.NoError(t, err)
		assert.True(t, exists)

		assert.Equal(t, "Apple Inc.", queryStockField(t, "AAPL", "name"))

		// Re-upsert with a different name: existing non-empty name is preserved.
		require.NoError(t, testDB.UpsertStockBasic("AAPL", "Something Else"))
		assert.Equal(t, "Apple Inc.", queryStockField(t, "AAPL", "name"))
	})

	t.Run("UpsertStockWithSector inserts and updates sector", func(t *testing.T) {
		testDB.TruncateAll(t)

		require.NoError(t, testDB.UpsertStockWithSector("MSFT", "Microsoft", "Technology", "Software"))

		assert.Equal(t, "Technology", queryStockField(t, "MSFT", "sector"))
		assert.Equal(t, "Software", queryStockField(t, "MSFT", "industry"))

		// Upsert again with empty sector keeps existing sector via COALESCE/NULLIF.
		require.NoError(t, testDB.UpsertStockWithSector("MSFT", "Microsoft", "", ""))
		assert.Equal(t, "Technology", queryStockField(t, "MSFT", "sector"))
	})

	t.Run("StockExists false for missing", func(t *testing.T) {
		testDB.TruncateAll(t)

		exists, err := testDB.StockExists("NOPE")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("GetSectorMap returns symbol->sector", func(t *testing.T) {
		testDB.TruncateAll(t)

		require.NoError(t, testDB.UpsertStockWithSector("AAPL", "Apple", "Technology", "Hardware"))
		require.NoError(t, testDB.UpsertStockWithSector("JPM", "JPMorgan", "Financial", "Banks"))
		// Stock with no sector should be excluded.
		require.NoError(t, testDB.UpsertStockBasic("NOSEC", "No Sector Co"))

		m, err := testDB.GetSectorMap()
		require.NoError(t, err)
		assert.Equal(t, "Technology", m["AAPL"])
		assert.Equal(t, "Financial", m["JPM"])
		_, hasNoSec := m["NOSEC"]
		assert.False(t, hasNoSec)
	})
}
