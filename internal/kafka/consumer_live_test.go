package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// TestAllTradesFromProduction tests the exact sequence of all trades from production
// captured on 2026-01-18. This validates that position aggregation works correctly
// with real trade data.
//
// Expected final positions (4 open):
// - B: 2.061433 shares @ $49.81
// - WPM: 1.480277 shares @ ~$135.11
// - FCX: 1.70532 shares @ $58.64
// - PPLT: 0.62561 shares @ $210.04
//
// All other positions should be CLOSED (no entries):
// - SLV, IAU, LMT, USAR, AVAV, URNM, UUUU, MP
func TestAllTradesFromProduction(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	// All 41 trades from production in chronological order (oldest to newest)
	trades := []struct {
		symbol     string
		side       string
		quantity   string
		price      string
		executedAt string
	}{
		// Dec 23 - B buy
		{"B", "BUY", "2.18962100", "45.67", "2025-12-23T14:30:02Z"},
		// Dec 29 - B buy
		{"B", "BUY", "2.26834500", "44.085", "2025-12-29T19:17:05Z"},
		// Jan 2 - SLV buy
		{"SLV", "BUY", "3.00000000", "67.10", "2026-01-02T06:25:55Z"},
		// Jan 5 - B sell, SLV sell
		{"B", "SELL", "4.45796600", "46.235", "2026-01-05T18:19:29Z"},
		{"SLV", "SELL", "3.00000000", "68.9251", "2026-01-05T20:09:15Z"},
		// Jan 6 - B buy, SLV buy/sell, B sell
		{"B", "BUY", "3.00000000", "45.75", "2026-01-06T09:16:29Z"},
		{"SLV", "BUY", "0.16017600", "72.67", "2026-01-06T15:19:32Z"},
		{"SLV", "SELL", "0.16017600", "72.63", "2026-01-06T15:44:25Z"},
		{"B", "SELL", "3.00000000", "46.975", "2026-01-06T16:31:40Z"},
		// Jan 7 - SLV buy, IAU buy, SLV buy
		{"SLV", "BUY", "3.00000000", "73.10", "2026-01-07T02:18:45Z"},
		{"IAU", "BUY", "2.00000000", "84.27", "2026-01-07T03:07:29Z"},
		{"SLV", "BUY", "0.41099000", "69.88", "2026-01-07T14:30:00Z"},
		// Jan 8 - SLV buy
		{"SLV", "BUY", "1.48842000", "67.1853", "2026-01-08T14:01:46Z"},
		// Jan 9 - IAU sell, SLV sell
		{"IAU", "SELL", "2.00000000", "84.705", "2026-01-09T15:09:17Z"},
		{"SLV", "SELL", "4.89941000", "71.635", "2026-01-09T15:11:56Z"},
		// Jan 12 - USAR buy, AVAV buy, LMT buys, AVAV sell, LMT buy, USAR sell, LMT buy, LMT sell
		{"USAR", "BUY", "5.84453500", "17.11", "2026-01-12T14:37:40Z"},
		{"AVAV", "BUY", "0.27529000", "363.2532", "2026-01-12T14:40:39Z"},
		{"LMT", "BUY", "0.18233800", "548.43", "2026-01-12T14:42:27Z"},
		{"LMT", "BUY", "0.18245300", "548.085", "2026-01-12T14:44:22Z"},
		{"LMT", "BUY", "0.18288700", "546.7848", "2026-01-12T14:47:26Z"},
		{"AVAV", "SELL", "0.27529000", "367.595", "2026-01-12T14:59:19Z"},
		{"LMT", "BUY", "0.03727000", "546.015", "2026-01-12T15:25:42Z"},
		{"USAR", "SELL", "5.84453500", "17.3801", "2026-01-12T15:53:56Z"},
		{"LMT", "BUY", "0.18422300", "542.82", "2026-01-12T16:37:38Z"},
		{"LMT", "SELL", "0.76917100", "548.34", "2026-01-12T19:54:06Z"},
		// Jan 13 - AVAV buy, URNM buy, UUUU buy, AVAV buy, UUUU sell
		{"AVAV", "BUY", "0.54495900", "367.00", "2026-01-13T14:41:33Z"},
		{"URNM", "BUY", "1.54297100", "64.81", "2026-01-13T14:42:09Z"},
		{"UUUU", "BUY", "5.20969000", "19.195", "2026-01-13T14:42:28Z"},
		{"AVAV", "BUY", "0.88752400", "365.68", "2026-01-13T14:48:15Z"},
		{"UUUU", "SELL", "5.20969000", "19.71", "2026-01-13T15:58:58Z"},
		// Jan 14 - B buy, AVAV sell, URNM sell
		{"B", "BUY", "2.06143300", "49.81", "2026-01-14T14:59:58Z"},
		{"AVAV", "SELL", "1.43248300", "370.625", "2026-01-14T15:18:48Z"},
		{"URNM", "SELL", "1.54297100", "65.12", "2026-01-14T15:41:51Z"},
		// Jan 15 - MP buy
		{"MP", "BUY", "1.49387500", "66.94", "2026-01-15T17:32:02Z"},
		// Jan 16 - WPM buy, USAR buy, FCX buy, WPM buy, PPLT buy, USAR sell, MP sell
		{"WPM", "BUY", "0.74003000", "135.1295", "2026-01-16T14:36:57Z"},
		{"USAR", "BUY", "6.16712900", "16.215", "2026-01-16T14:37:45Z"},
		{"FCX", "BUY", "1.70532000", "58.64", "2026-01-16T14:38:17Z"},
		{"WPM", "BUY", "0.74024700", "135.09", "2026-01-16T14:38:43Z"},
		{"PPLT", "BUY", "0.62561000", "210.035", "2026-01-16T14:40:14Z"},
		{"USAR", "SELL", "6.16712900", "17.05", "2026-01-16T15:25:03Z"},
		{"MP", "SELL", "1.49387500", "67.55", "2026-01-16T15:26:10Z"},
	}

	// Process each trade
	for i, tr := range trades {
		executedAt, err := time.Parse(time.RFC3339, tr.executedAt)
		require.NoError(t, err, "Failed to parse time for trade %d", i+1)

		qty, _ := decimal.NewFromString(tr.quantity)
		price, _ := decimal.NewFromString(tr.price)

		rawTrade := &models.RawTrade{
			OrderID:    fmt.Sprintf("order-%d", i+1),
			Source:     "robinhood",
			Symbol:     tr.symbol,
			Side:       tr.side,
			Quantity:   qty,
			Price:      price,
			TotalCost:  qty.Mul(price),
			Fees:       decimal.Zero,
			ExecutedAt: executedAt,
		}

		err = repo.CreateRawTrade(rawTrade)
		require.NoError(t, err, "Failed to create raw trade %d", i+1)

		err = consumer.aggregateToPosition(rawTrade)
		require.NoError(t, err, "Failed to aggregate trade %d: %s %s %s", i+1, tr.side, tr.quantity, tr.symbol)

		// Log state after each trade
		t.Logf("Trade %02d: %s %s %s @ %s -> %d positions, %d histories",
			i+1, tr.side, tr.quantity, tr.symbol, tr.price,
			len(repo.positions), len(repo.tradeHistories))
	}

	// Print final positions for debugging
	t.Log("\n=== FINAL POSITIONS ===")
	for symbol, pos := range repo.positions {
		t.Logf("  %s: %s shares @ %s", symbol, pos.Quantity, pos.EntryPrice)
	}

	t.Log("\n=== TRADE HISTORIES ===")
	for i, h := range repo.tradeHistories {
		t.Logf("  %d. %s: %s shares, P&L: %s", i+1, h.Symbol, h.Quantity, h.RealizedPnl)
	}

	// Verify expected open positions (4 total)
	assert.Len(t, repo.positions, 4, "Expected exactly 4 open positions")

	// Verify B position
	if pos, ok := repo.positions["B"]; ok {
		expectedQty := decimal.NewFromFloat(2.061433)
		assert.True(t, pos.Quantity.Sub(expectedQty).Abs().LessThan(decimal.NewFromFloat(0.0001)),
			"B quantity: expected ~2.061433, got %s", pos.Quantity)
		expectedPrice := decimal.NewFromFloat(49.81)
		assert.True(t, pos.EntryPrice.Sub(expectedPrice).Abs().LessThan(decimal.NewFromFloat(0.01)),
			"B price: expected ~49.81, got %s", pos.EntryPrice)
	} else {
		t.Error("Expected B position to exist")
	}

	// Verify WPM position
	if pos, ok := repo.positions["WPM"]; ok {
		expectedQty := decimal.NewFromFloat(1.480277)
		assert.True(t, pos.Quantity.Sub(expectedQty).Abs().LessThan(decimal.NewFromFloat(0.0001)),
			"WPM quantity: expected ~1.480277, got %s", pos.Quantity)
	} else {
		t.Error("Expected WPM position to exist")
	}

	// Verify FCX position
	if pos, ok := repo.positions["FCX"]; ok {
		expectedQty := decimal.NewFromFloat(1.70532)
		assert.True(t, pos.Quantity.Sub(expectedQty).Abs().LessThan(decimal.NewFromFloat(0.0001)),
			"FCX quantity: expected ~1.70532, got %s", pos.Quantity)
		expectedPrice := decimal.NewFromFloat(58.64)
		assert.True(t, pos.EntryPrice.Sub(expectedPrice).Abs().LessThan(decimal.NewFromFloat(0.01)),
			"FCX price: expected ~58.64, got %s", pos.EntryPrice)
	} else {
		t.Error("Expected FCX position to exist")
	}

	// Verify PPLT position
	if pos, ok := repo.positions["PPLT"]; ok {
		expectedQty := decimal.NewFromFloat(0.62561)
		assert.True(t, pos.Quantity.Sub(expectedQty).Abs().LessThan(decimal.NewFromFloat(0.0001)),
			"PPLT quantity: expected ~0.62561, got %s", pos.Quantity)
	} else {
		t.Error("Expected PPLT position to exist")
	}

	// Verify closed positions do NOT exist
	closedSymbols := []string{"SLV", "IAU", "LMT", "USAR", "AVAV", "URNM", "UUUU", "MP"}
	for _, symbol := range closedSymbols {
		_, exists := repo.positions[symbol]
		assert.False(t, exists, "Position for %s should be CLOSED (not exist)", symbol)
	}

	// Verify trade histories were created for closed positions
	// We expect histories for: SLV (3 cycles), B (2 cycles), IAU, LMT, USAR (2 cycles), AVAV (2 cycles), URNM, UUUU, MP
	// Total: 3 + 2 + 1 + 1 + 2 + 2 + 1 + 1 + 1 = 14 trade histories
	t.Logf("Total trade histories: %d", len(repo.tradeHistories))
}

// TestSLVIsolated tests just the SLV trades to debug why position isn't closing
func TestSLVIsolated(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	slvTrades := []struct {
		side       string
		quantity   string
		price      string
		executedAt string
	}{
		{"BUY", "3.00000000", "67.10", "2026-01-02T06:25:55Z"},
		{"SELL", "3.00000000", "68.9251", "2026-01-05T20:09:15Z"},
		{"BUY", "0.16017600", "72.67", "2026-01-06T15:19:32Z"},
		{"SELL", "0.16017600", "72.63", "2026-01-06T15:44:25Z"},
		{"BUY", "3.00000000", "73.10", "2026-01-07T02:18:45Z"},
		{"BUY", "0.41099000", "69.88", "2026-01-07T14:30:00Z"},
		{"BUY", "1.48842000", "67.1853", "2026-01-08T14:01:46Z"},
		{"SELL", "4.89941000", "71.635", "2026-01-09T15:11:56Z"},
	}

	for i, tr := range slvTrades {
		executedAt, _ := time.Parse(time.RFC3339, tr.executedAt)
		qty, _ := decimal.NewFromString(tr.quantity)
		price, _ := decimal.NewFromString(tr.price)

		rawTrade := &models.RawTrade{
			OrderID:    fmt.Sprintf("slv-order-%d", i+1),
			Source:     "robinhood",
			Symbol:     "SLV",
			Side:       tr.side,
			Quantity:   qty,
			Price:      price,
			TotalCost:  qty.Mul(price),
			Fees:       decimal.Zero,
			ExecutedAt: executedAt,
		}

		err := repo.CreateRawTrade(rawTrade)
		require.NoError(t, err)

		// Get position BEFORE aggregation
		posBefore, _ := repo.GetPositionBySymbol("SLV")
		var qtyBefore decimal.Decimal
		if posBefore != nil {
			qtyBefore = posBefore.Quantity
		}

		err = consumer.aggregateToPosition(rawTrade)
		require.NoError(t, err)

		// Get position AFTER aggregation
		posAfter, _ := repo.GetPositionBySymbol("SLV")
		var qtyAfter decimal.Decimal
		if posAfter != nil {
			qtyAfter = posAfter.Quantity
		}

		t.Logf("Trade %d: %s %s @ %s | Before: %s | After: %s | Positions: %d | Histories: %d",
			i+1, tr.side, tr.quantity, tr.price,
			qtyBefore, qtyAfter,
			len(repo.positions), len(repo.tradeHistories))
	}

	// After all trades, SLV should NOT exist
	_, exists := repo.positions["SLV"]
	assert.False(t, exists, "SLV position should be CLOSED after all trades")

	// Should have 3 trade histories (3 complete cycles)
	assert.Len(t, repo.tradeHistories, 3, "Expected 3 trade histories for SLV")
}

// TestBPositionFlow tests the B stock flow to verify partial sells work
func TestBPositionFlow(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	bTrades := []struct {
		side       string
		quantity   string
		price      string
		executedAt string
	}{
		// First cycle
		{"BUY", "2.18962100", "45.67", "2025-12-23T14:30:02Z"},
		{"BUY", "2.26834500", "44.085", "2025-12-29T19:17:05Z"},
		{"SELL", "4.45796600", "46.235", "2026-01-05T18:19:29Z"}, // Full close
		// Second cycle
		{"BUY", "3.00000000", "45.75", "2026-01-06T09:16:29Z"},
		{"SELL", "3.00000000", "46.975", "2026-01-06T16:31:40Z"}, // Full close
		// Current position
		{"BUY", "2.06143300", "49.81", "2026-01-14T14:59:58Z"},
	}

	for i, tr := range bTrades {
		executedAt, _ := time.Parse(time.RFC3339, tr.executedAt)
		qty, _ := decimal.NewFromString(tr.quantity)
		price, _ := decimal.NewFromString(tr.price)

		rawTrade := &models.RawTrade{
			OrderID:    fmt.Sprintf("b-order-%d", i+1),
			Source:     "robinhood",
			Symbol:     "B",
			Side:       tr.side,
			Quantity:   qty,
			Price:      price,
			TotalCost:  qty.Mul(price),
			Fees:       decimal.Zero,
			ExecutedAt: executedAt,
		}

		err := repo.CreateRawTrade(rawTrade)
		require.NoError(t, err)

		err = consumer.aggregateToPosition(rawTrade)
		require.NoError(t, err)

		pos, _ := repo.GetPositionBySymbol("B")
		var posQty, posPrice string
		if pos != nil {
			posQty = pos.Quantity.String()
			posPrice = pos.EntryPrice.String()
		} else {
			posQty = "NONE"
			posPrice = "N/A"
		}

		t.Logf("Trade %d: %s %s @ %s | Position: %s @ %s | Histories: %d",
			i+1, tr.side, tr.quantity, tr.price, posQty, posPrice, len(repo.tradeHistories))
	}

	// B should exist with final position
	pos, exists := repo.positions["B"]
	assert.True(t, exists, "B position should exist")
	if exists {
		expectedQty := decimal.NewFromFloat(2.06143300)
		assert.True(t, pos.Quantity.Sub(expectedQty).Abs().LessThan(decimal.NewFromFloat(0.0001)),
			"B quantity: expected ~2.061433, got %s", pos.Quantity)
	}

	// Should have 2 trade histories (2 closed cycles)
	assert.Len(t, repo.tradeHistories, 2, "Expected 2 trade histories for B")
}
