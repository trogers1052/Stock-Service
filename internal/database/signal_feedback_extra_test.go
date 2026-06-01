package database

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// truncateSignalFeedback clears the signal_feedback table for test isolation.
func truncateSignalFeedback(t *testing.T, tdb *TestDB) {
	t.Helper()
	_, err := tdb.GetRawConn().Exec("TRUNCATE TABLE signal_feedback RESTART IDENTITY CASCADE")
	require.NoError(t, err)
}

func TestSignalFeedbackRepository(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testDB := SetupTestDB(t)
	defer testDB.Cleanup(t)

	t.Run("CreateSignalFeedback inserts minimal row", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		fb := &models.SignalFeedback{
			Symbol:            "AAPL",
			Signal:            "BUY",
			Action:            "traded",
			FeedbackTimestamp: time.Now(),
		}
		err := testDB.CreateSignalFeedback(fb)
		require.NoError(t, err)
		assert.NotZero(t, fb.ID)
		assert.False(t, fb.CreatedAt.IsZero())
	})

	t.Run("CreateSignalFeedback inserts fully-populated row", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		valid := time.Now().Add(48 * time.Hour)
		fb := &models.SignalFeedback{
			Symbol:             "MSFT",
			Signal:             "BUY",
			Action:             "traded",
			Confidence:         0.82,
			RulesTriggered:     []string{"golden_cross", "volume_surge"},
			RegimeID:           "BULL",
			DecisionConfidence: 0.75,
			EntryPrice:         100.0,
			StopPrice:          95.0,
			Target1:            110.0,
			Target2:            120.0,
			ValidUntil:         &valid,
			FeedbackTimestamp:  time.Now(),
		}
		err := testDB.CreateSignalFeedback(fb)
		require.NoError(t, err)
		assert.NotZero(t, fb.ID)
	})

	t.Run("CreateSignalFeedback upserts on conflict", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		ts := time.Now().Truncate(time.Microsecond)
		fb := &models.SignalFeedback{
			Symbol:            "GOOGL",
			Signal:            "BUY",
			Action:            "skipped",
			Confidence:        0.5,
			FeedbackTimestamp: ts,
		}
		require.NoError(t, testDB.CreateSignalFeedback(fb))

		// Same (symbol, signal, feedback_timestamp) => update action.
		fb2 := &models.SignalFeedback{
			Symbol:            "GOOGL",
			Signal:            "BUY",
			Action:            "traded",
			Confidence:        0.9,
			FeedbackTimestamp: ts,
		}
		require.NoError(t, testDB.CreateSignalFeedback(fb2))

		entries, err := testDB.GetSignalFeedback(10, nil, "GOOGL")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "traded", entries[0].Action)
		assert.Equal(t, 0.9, entries[0].Confidence)
	})

	t.Run("GetSignalFeedback applies filters", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		old := time.Now().Add(-30 * 24 * time.Hour)
		recent := time.Now()
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "AAPL", Signal: "BUY", Action: "traded", FeedbackTimestamp: old,
		}))
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "AAPL", Signal: "SELL", Action: "skipped", FeedbackTimestamp: recent,
		}))
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "TSLA", Signal: "BUY", Action: "traded", FeedbackTimestamp: recent,
		}))

		// No filters returns all 3.
		all, err := testDB.GetSignalFeedback(100, nil, "")
		require.NoError(t, err)
		assert.Len(t, all, 3)

		// Symbol filter.
		aapl, err := testDB.GetSignalFeedback(100, nil, "AAPL")
		require.NoError(t, err)
		assert.Len(t, aapl, 2)

		// Since-date filter excludes the old one.
		since := time.Now().Add(-7 * 24 * time.Hour)
		recentOnly, err := testDB.GetSignalFeedback(100, &since, "")
		require.NoError(t, err)
		assert.Len(t, recentOnly, 2)

		// Limit.
		limited, err := testDB.GetSignalFeedback(1, nil, "")
		require.NoError(t, err)
		assert.Len(t, limited, 1)
	})

	t.Run("UpdateFeedbackAction updates and errors on missing", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		fb := &models.SignalFeedback{
			Symbol: "NVDA", Signal: "BUY", Action: "skipped", FeedbackTimestamp: time.Now(),
		}
		require.NoError(t, testDB.CreateSignalFeedback(fb))

		require.NoError(t, testDB.UpdateFeedbackAction(fb.ID, "traded"))
		entries, err := testDB.GetSignalFeedback(10, nil, "NVDA")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "traded", entries[0].Action)

		err = testDB.UpdateFeedbackAction(999999, "traded")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("GetFeedbackSummary aggregates by action", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "A", Signal: "BUY", Action: "traded", FeedbackTimestamp: time.Now(),
		}))
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "B", Signal: "BUY", Action: "traded", FeedbackTimestamp: time.Now(),
		}))
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "C", Signal: "BUY", Action: "skipped", FeedbackTimestamp: time.Now(),
		}))

		summary, err := testDB.GetFeedbackSummary()
		require.NoError(t, err)
		assert.Equal(t, 3, summary.Total)
		assert.Equal(t, 2, summary.Traded)
		assert.Equal(t, 1, summary.Skipped)
	})

	t.Run("GetRuleAccuracy computes per-rule metrics", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		ts := time.Now()
		// 3 entries for golden_cross in BULL: 2 traded, 1 skipped.
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "A", Signal: "BUY", Action: "traded", RegimeID: "BULL",
			RulesTriggered: []string{"golden_cross"}, FeedbackTimestamp: ts,
		}))
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "B", Signal: "BUY", Action: "traded", RegimeID: "BULL",
			RulesTriggered: []string{"golden_cross"}, FeedbackTimestamp: ts,
		}))
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "C", Signal: "BUY", Action: "skipped", RegimeID: "BULL",
			RulesTriggered: []string{"golden_cross"}, FeedbackTimestamp: ts,
		}))

		acc, err := testDB.GetRuleAccuracy(90, 3)
		require.NoError(t, err)
		require.Len(t, acc, 1)
		assert.Equal(t, "golden_cross", acc[0].RuleName)
		assert.Equal(t, "BULL", acc[0].RegimeID)
		assert.Equal(t, 3, acc[0].SignalCount)
		assert.Equal(t, 2, acc[0].TradedCount)
		assert.Equal(t, 1, acc[0].SkippedCount)
		assert.InDelta(t, 2.0/3.0, acc[0].TradeRate, 0.001)
		// Multiplier = clamp(0.5, 1.5, 0.5+tradeRate) = ~1.167
		assert.InDelta(t, 0.5+2.0/3.0, acc[0].Multiplier, 0.001)

		// minSignals filter excludes (count < 5).
		none, err := testDB.GetRuleAccuracy(90, 5)
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	t.Run("GetUnresolvedSignals returns rows with prices and no outcome", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		// Has prices, no outcome => unresolved.
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "A", Signal: "BUY", Action: "traded",
			EntryPrice: 100, StopPrice: 95, FeedbackTimestamp: time.Now(),
		}))
		// No prices => excluded.
		require.NoError(t, testDB.CreateSignalFeedback(&models.SignalFeedback{
			Symbol: "B", Signal: "BUY", Action: "traded", FeedbackTimestamp: time.Now(),
		}))

		unresolved, err := testDB.GetUnresolvedSignals(100)
		require.NoError(t, err)
		require.Len(t, unresolved, 1)
		assert.Equal(t, "A", unresolved[0].Symbol)
		assert.Equal(t, 100.0, unresolved[0].EntryPrice)
	})

	t.Run("UpdateSignalOutcome sets outcome and errors on missing", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		fb := &models.SignalFeedback{
			Symbol: "A", Signal: "BUY", Action: "traded",
			EntryPrice: 100, StopPrice: 95, FeedbackTimestamp: time.Now(),
		}
		require.NoError(t, testDB.CreateSignalFeedback(fb))

		require.NoError(t, testDB.UpdateSignalOutcome(fb.ID, models.OutcomeTarget1Hit))

		entries, err := testDB.GetSignalFeedback(10, nil, "A")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, models.OutcomeTarget1Hit, entries[0].Outcome)
		assert.NotNil(t, entries[0].OutcomeAt)

		// Now resolved => not unresolved anymore.
		unresolved, err := testDB.GetUnresolvedSignals(100)
		require.NoError(t, err)
		assert.Empty(t, unresolved)

		err = testDB.UpdateSignalOutcome(999999, models.OutcomeStoppedOut)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("GetRuleOutcomeQuality computes win rate", func(t *testing.T) {
		truncateSignalFeedback(t, testDB)

		ts := time.Now()
		mk := func(sym, outcome string) {
			fb := &models.SignalFeedback{
				Symbol: sym, Signal: "BUY", Action: "traded", RegimeID: "BULL",
				RulesTriggered: []string{"breakout"}, EntryPrice: 10, StopPrice: 9,
				FeedbackTimestamp: ts,
			}
			require.NoError(t, testDB.CreateSignalFeedback(fb))
			require.NoError(t, testDB.UpdateSignalOutcome(fb.ID, outcome))
		}
		mk("A", models.OutcomeTarget1Hit)
		mk("B", models.OutcomeTarget2Hit)
		mk("C", models.OutcomeStoppedOut)
		mk("D", models.OutcomeExpired)

		quality, err := testDB.GetRuleOutcomeQuality(90, 1)
		require.NoError(t, err)
		require.Len(t, quality, 1)
		q := quality[0]
		assert.Equal(t, "breakout", q.RuleName)
		assert.Equal(t, 4, q.SignalCount)
		assert.Equal(t, 2, q.WinCount)
		assert.Equal(t, 1, q.LossCount)
		assert.Equal(t, 1, q.ExpiredCount)
		// win rate = 2 / (2+1) = 0.667
		assert.InDelta(t, 2.0/3.0, q.WinRate, 0.001)
		assert.InDelta(t, 1.0, q.Multiplier, 0.01) // 0.667*1.5 ~ 1.0

		// minSignals filter.
		none, err := testDB.GetRuleOutcomeQuality(90, 10)
		require.NoError(t, err)
		assert.Empty(t, none)
	})
}
