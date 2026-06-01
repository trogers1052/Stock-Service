package database

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestDeleteAllPositions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)
	testDB.TruncateAll(t)

	for _, sym := range []string{"AAPL", "MSFT"} {
		require.NoError(t, testDB.CreatePosition(&models.Position{
			Symbol:     sym,
			Quantity:   decimal.NewFromInt(1),
			EntryPrice: decimal.NewFromFloat(100),
			EntryDate:  time.Now(),
		}))
	}

	all, err := testDB.GetAllPositions()
	require.NoError(t, err)
	require.Len(t, all, 2)

	require.NoError(t, testDB.DeleteAllPositions())

	all, err = testDB.GetAllPositions()
	require.NoError(t, err)
	assert.Empty(t, all)
}

func TestDeleteAlertRulesBySymbol(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)
	testDB.TruncateAll(t)

	// Insert an alert rule directly (the stocks row is needed if FK exists).
	require.NoError(t, testDB.SaveStock(&models.Stock{
		Symbol: "AAPL", Name: "Apple", LastUpdated: time.Now(),
	}))
	_, err := testDB.GetRawConn().Exec(
		`INSERT INTO alert_rules (symbol, rule_type, comparison, enabled)
		 VALUES ('AAPL', 'RSI_OVERSOLD', 'BELOW', true)`,
	)
	require.NoError(t, err)

	// Deleting by symbol should not error.
	require.NoError(t, testDB.DeleteAlertRulesBySymbol("AAPL"))

	var count int
	require.NoError(t, testDB.GetRawConn().
		QueryRow(`SELECT COUNT(*) FROM alert_rules WHERE symbol = 'AAPL'`).Scan(&count))
	assert.Equal(t, 0, count)

	// Deleting a symbol with no rules is also a no-op (no error).
	require.NoError(t, testDB.DeleteAlertRulesBySymbol("NONE"))
}
