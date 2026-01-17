package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("all tables exist", func(t *testing.T) {
		expectedTables := []string{
			"stocks",
			"monitored_stocks",
			"positions",
			"price_data_daily",
			"technical_indicators",
			"alert_rules",
			"alert_history",
			"trades_history",
		}

		for _, tableName := range expectedTables {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.tables
					WHERE table_schema = 'public'
					AND table_name = $1
				)
			`, tableName).Scan(&exists)

			require.NoError(t, err, "failed to check table existence for %s", tableName)
			assert.True(t, exists, "table %s should exist", tableName)
		}
	})

	t.Run("stocks table has correct columns", func(t *testing.T) {
		expectedColumns := map[string]string{
			"id":                "uuid",
			"symbol":            "character varying",
			"name":              "character varying",
			"exchange":          "character varying",
			"sector":            "character varying",
			"industry":          "character varying",
			"current_price":     "numeric",
			"previous_close":    "numeric",
			"change_amount":     "numeric",
			"change_percent":    "numeric",
			"day_high":          "numeric",
			"day_low":           "numeric",
			"volume":            "bigint",
			"average_volume":    "bigint",
			"week_52_high":      "numeric",
			"week_52_low":       "numeric",
			"market_cap":        "bigint",
			"shares_outstanding": "bigint",
			"last_updated":      "timestamp without time zone",
			"created_at":        "timestamp without time zone",
		}

		for colName, expectedType := range expectedColumns {
			var actualType string
			err := testDB.GetRawConn().QueryRow(`
				SELECT data_type
				FROM information_schema.columns
				WHERE table_name = 'stocks' AND column_name = $1
			`, colName).Scan(&actualType)

			require.NoError(t, err, "column %s should exist in stocks table", colName)
			assert.Equal(t, expectedType, actualType, "column %s should have type %s", colName, expectedType)
		}
	})

	t.Run("positions table has correct columns", func(t *testing.T) {
		expectedColumns := []string{
			"id", "symbol", "quantity", "entry_price", "entry_date",
			"current_price", "unrealized_pnl_pct", "days_held", "entry_rsi",
			"entry_reason", "sector", "industry", "position_size_pct",
			"created_at", "updated_at",
		}

		for _, colName := range expectedColumns {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.columns
					WHERE table_name = 'positions' AND column_name = $1
				)
			`, colName).Scan(&exists)

			require.NoError(t, err)
			assert.True(t, exists, "column %s should exist in positions table", colName)
		}
	})

	t.Run("price_data_daily table has correct columns", func(t *testing.T) {
		expectedColumns := []string{
			"id", "symbol", "date", "open", "high", "low", "close",
			"volume", "vwap", "created_at",
		}

		for _, colName := range expectedColumns {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.columns
					WHERE table_name = 'price_data_daily' AND column_name = $1
				)
			`, colName).Scan(&exists)

			require.NoError(t, err)
			assert.True(t, exists, "column %s should exist in price_data_daily table", colName)
		}
	})

	t.Run("technical_indicators table has correct columns", func(t *testing.T) {
		expectedColumns := []string{
			"id", "symbol", "date", "indicator_type", "value",
			"timeframe", "created_at",
		}

		for _, colName := range expectedColumns {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.columns
					WHERE table_name = 'technical_indicators' AND column_name = $1
				)
			`, colName).Scan(&exists)

			require.NoError(t, err)
			assert.True(t, exists, "column %s should exist in technical_indicators table", colName)
		}
	})

	t.Run("alert_rules table has correct columns", func(t *testing.T) {
		expectedColumns := []string{
			"id", "symbol", "rule_type", "condition_value", "comparison",
			"enabled", "triggered_count", "last_triggered_at", "cooldown_minutes",
			"notification_channel", "message_template", "priority",
			"created_at", "updated_at",
		}

		for _, colName := range expectedColumns {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.columns
					WHERE table_name = 'alert_rules' AND column_name = $1
				)
			`, colName).Scan(&exists)

			require.NoError(t, err)
			assert.True(t, exists, "column %s should exist in alert_rules table", colName)
		}
	})

	t.Run("trades_history table has correct columns", func(t *testing.T) {
		expectedColumns := []string{
			"id", "symbol", "trade_type", "quantity", "price", "total_cost",
			"fee", "entry_date", "exit_date", "holding_period_hours",
			"entry_rsi", "exit_rsi", "realized_pnl", "realized_pnl_pct",
			"max_drawdown_pct", "entry_reason", "exit_reason",
			"emotional_state", "conviction_level", "market_conditions",
			"what_went_right", "what_went_wrong", "trade_grade",
			"strategy_tag", "notes", "executed_at", "created_at",
		}

		for _, colName := range expectedColumns {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.columns
					WHERE table_name = 'trades_history' AND column_name = $1
				)
			`, colName).Scan(&exists)

			require.NoError(t, err)
			assert.True(t, exists, "column %s should exist in trades_history table", colName)
		}
	})

	t.Run("indexes exist", func(t *testing.T) {
		expectedIndexes := []struct {
			table string
			index string
		}{
			{"stocks", "idx_stocks_symbol"},
			{"stocks", "idx_stocks_last_updated"},
			{"stocks", "idx_stocks_sector"},
			{"positions", "idx_positions_symbol"},
			{"positions", "idx_positions_entry_date"},
			{"price_data_daily", "idx_price_data_symbol"},
			{"price_data_daily", "idx_price_data_date"},
			{"technical_indicators", "idx_indicators_symbol"},
			{"technical_indicators", "idx_indicators_date"},
			{"alert_rules", "idx_alert_rules_symbol"},
			{"alert_rules", "idx_alert_rules_type"},
		}

		for _, idx := range expectedIndexes {
			var exists bool
			err := testDB.GetRawConn().QueryRow(`
				SELECT EXISTS (
					SELECT FROM pg_indexes
					WHERE tablename = $1 AND indexname = $2
				)
			`, idx.table, idx.index).Scan(&exists)

			require.NoError(t, err)
			assert.True(t, exists, "index %s should exist on table %s", idx.index, idx.table)
		}
	})

	t.Run("unique constraints exist", func(t *testing.T) {
		// Check stocks.symbol unique
		var symbolUnique bool
		err := testDB.GetRawConn().QueryRow(`
			SELECT EXISTS (
				SELECT FROM pg_constraint c
				JOIN pg_class t ON c.conrelid = t.oid
				WHERE t.relname = 'stocks'
				AND c.contype = 'u'
				AND c.conname LIKE '%symbol%'
			)
		`).Scan(&symbolUnique)
		require.NoError(t, err)
		assert.True(t, symbolUnique, "stocks.symbol should have unique constraint")

		// Check positions.symbol unique
		var posSymbolUnique bool
		err = testDB.GetRawConn().QueryRow(`
			SELECT EXISTS (
				SELECT FROM pg_constraint c
				JOIN pg_class t ON c.conrelid = t.oid
				WHERE t.relname = 'positions'
				AND c.contype = 'u'
				AND c.conname LIKE '%symbol%'
			)
		`).Scan(&posSymbolUnique)
		require.NoError(t, err)
		assert.True(t, posSymbolUnique, "positions.symbol should have unique constraint")

		// Check price_data_daily (symbol, date) unique
		var priceUnique bool
		err = testDB.GetRawConn().QueryRow(`
			SELECT EXISTS (
				SELECT FROM pg_constraint c
				JOIN pg_class t ON c.conrelid = t.oid
				WHERE t.relname = 'price_data_daily'
				AND c.contype = 'u'
			)
		`).Scan(&priceUnique)
		require.NoError(t, err)
		assert.True(t, priceUnique, "price_data_daily should have unique constraint on (symbol, date)")
	})

	t.Run("foreign keys exist", func(t *testing.T) {
		// Check monitored_stocks references stocks
		var monitoredFK bool
		err := testDB.GetRawConn().QueryRow(`
			SELECT EXISTS (
				SELECT FROM pg_constraint c
				JOIN pg_class t ON c.conrelid = t.oid
				WHERE t.relname = 'monitored_stocks'
				AND c.contype = 'f'
			)
		`).Scan(&monitoredFK)
		require.NoError(t, err)
		assert.True(t, monitoredFK, "monitored_stocks should have foreign key to stocks")

		// Check alert_rules references stocks
		var alertFK bool
		err = testDB.GetRawConn().QueryRow(`
			SELECT EXISTS (
				SELECT FROM pg_constraint c
				JOIN pg_class t ON c.conrelid = t.oid
				WHERE t.relname = 'alert_rules'
				AND c.contype = 'f'
			)
		`).Scan(&alertFK)
		require.NoError(t, err)
		assert.True(t, alertFK, "alert_rules should have foreign key to stocks")
	})
}
