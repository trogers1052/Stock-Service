package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func truncateBacktestTiers(t *testing.T, tdb *TestDB) {
	t.Helper()
	_, err := tdb.GetRawConn().Exec("TRUNCATE TABLE backtest_tiers CASCADE")
	require.NoError(t, err)
}

// ensureStock satisfies the backtest_tiers.symbol FK to stocks(symbol).
func ensureStock(t *testing.T, tdb *TestDB, symbol string) {
	t.Helper()
	require.NoError(t, tdb.UpsertStockBasic(symbol, symbol))
}

func floatPtr(f float64) *float64 { return &f }

func TestBacktestTiersRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("UpsertBacktestTier inserts and updates", func(t *testing.T) {
		truncateBacktestTiers(t, testDB)
		ensureStock(t, testDB, "AAPL")

		tier := &models.BacktestTier{
			Symbol:                 "AAPL",
			Tier:                   "A",
			CompositeScore:         88.5,
			GatesPassed:            4,
			GatesTotal:             4,
			RegimePass:             true,
			AllowedRegimes:         []string{"BULL", "NEUTRAL"},
			Sharpe:                 floatPtr(1.2),
			TotalReturn:            floatPtr(45.0),
			WinRate:                floatPtr(0.6),
			ProfitFactor:           floatPtr(2.1),
			MaxDrawdown:            floatPtr(0.15),
			TradeCount:             30,
			ConfidenceMultiplier:   1.1,
			PositionSizeMultiplier: 1.0,
			Blacklisted:            false,
			RankingDate:            time.Now(),
			Notes:                  "strong",
		}
		require.NoError(t, testDB.UpsertBacktestTier(tier))
		assert.False(t, tier.CreatedAt.IsZero())
		assert.False(t, tier.UpdatedAt.IsZero())

		got, err := testDB.GetBacktestTier("AAPL")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "A", got.Tier)
		assert.Equal(t, 88.5, got.CompositeScore)
		assert.Equal(t, []string{"BULL", "NEUTRAL"}, got.AllowedRegimes)
		require.NotNil(t, got.Sharpe)
		assert.Equal(t, 1.2, *got.Sharpe)
		assert.Equal(t, "strong", got.Notes)

		// Update via conflict.
		tier.Tier = "B"
		tier.CompositeScore = 70.0
		require.NoError(t, testDB.UpsertBacktestTier(tier))

		got2, err := testDB.GetBacktestTier("AAPL")
		require.NoError(t, err)
		require.NotNil(t, got2)
		assert.Equal(t, "B", got2.Tier)
		assert.Equal(t, 70.0, got2.CompositeScore)
	})

	t.Run("UpsertBacktestTier with nil optional metrics", func(t *testing.T) {
		truncateBacktestTiers(t, testDB)
		ensureStock(t, testDB, "MINI")

		tier := &models.BacktestTier{
			Symbol:                 "MINI",
			Tier:                   "C",
			CompositeScore:         50.0,
			RegimePass:             false,
			ConfidenceMultiplier:   1.0,
			PositionSizeMultiplier: 1.0,
			RankingDate:            time.Now(),
		}
		require.NoError(t, testDB.UpsertBacktestTier(tier))

		got, err := testDB.GetBacktestTier("MINI")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Nil(t, got.Sharpe)
		assert.Nil(t, got.TotalReturn)
		assert.Empty(t, got.Notes)
	})

	t.Run("GetBacktestTier returns nil for missing", func(t *testing.T) {
		truncateBacktestTiers(t, testDB)

		got, err := testDB.GetBacktestTier("NOPE")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("GetAllBacktestTiers orders by composite score desc", func(t *testing.T) {
		truncateBacktestTiers(t, testDB)

		for _, spec := range []struct {
			sym   string
			score float64
		}{
			{"LOW", 40.0},
			{"HIGH", 95.0},
			{"MID", 70.0},
		} {
			ensureStock(t, testDB, spec.sym)
			require.NoError(t, testDB.UpsertBacktestTier(&models.BacktestTier{
				Symbol: spec.sym, Tier: "B", CompositeScore: spec.score,
				ConfidenceMultiplier: 1.0, PositionSizeMultiplier: 1.0, RankingDate: time.Now(),
			}))
		}

		all, err := testDB.GetAllBacktestTiers()
		require.NoError(t, err)
		require.Len(t, all, 3)
		assert.Equal(t, "HIGH", all[0].Symbol)
		assert.Equal(t, "MID", all[1].Symbol)
		assert.Equal(t, "LOW", all[2].Symbol)
	})

	t.Run("GetBacktestTiersByTier filters by grade", func(t *testing.T) {
		truncateBacktestTiers(t, testDB)
		ensureStock(t, testDB, "A1")
		ensureStock(t, testDB, "A2")
		ensureStock(t, testDB, "B1")

		require.NoError(t, testDB.UpsertBacktestTier(&models.BacktestTier{
			Symbol: "A1", Tier: "A", CompositeScore: 90,
			ConfidenceMultiplier: 1.0, PositionSizeMultiplier: 1.0, RankingDate: time.Now(),
		}))
		require.NoError(t, testDB.UpsertBacktestTier(&models.BacktestTier{
			Symbol: "A2", Tier: "A", CompositeScore: 85,
			ConfidenceMultiplier: 1.0, PositionSizeMultiplier: 1.0, RankingDate: time.Now(),
		}))
		require.NoError(t, testDB.UpsertBacktestTier(&models.BacktestTier{
			Symbol: "B1", Tier: "B", CompositeScore: 60,
			ConfidenceMultiplier: 1.0, PositionSizeMultiplier: 1.0, RankingDate: time.Now(),
		}))

		aTiers, err := testDB.GetBacktestTiersByTier("A")
		require.NoError(t, err)
		require.Len(t, aTiers, 2)
		assert.Equal(t, "A1", aTiers[0].Symbol) // higher score first

		bTiers, err := testDB.GetBacktestTiersByTier("B")
		require.NoError(t, err)
		require.Len(t, bTiers, 1)

		none, err := testDB.GetBacktestTiersByTier("F")
		require.NoError(t, err)
		assert.Empty(t, none)
	})
}
