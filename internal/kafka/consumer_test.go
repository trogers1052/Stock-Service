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

// MockRepository implements the Repository interface for testing
type MockRepository struct {
	rawTrades      map[string]*models.RawTrade // key: orderID+source
	positions      map[string]*models.Position // key: symbol
	tradeHistories []*models.TradeHistory
	nextRawTradeID int
	nextPositionID int
	nextHistoryID  int

	// Track method calls for verification
	CreatePositionCalls   int
	UpdatePositionCalls   int
	DeletePositionCalls   int
	CreateTradeHistoryCalls int
}

func NewMockRepository() *MockRepository {
	return &MockRepository{
		rawTrades:      make(map[string]*models.RawTrade),
		positions:      make(map[string]*models.Position),
		tradeHistories: []*models.TradeHistory{},
		nextRawTradeID: 1,
		nextPositionID: 1,
		nextHistoryID:  1,
	}
}

func (m *MockRepository) CreateRawTrade(t *models.RawTrade) error {
	t.ID = m.nextRawTradeID
	m.nextRawTradeID++
	key := t.OrderID + ":" + t.Source
	m.rawTrades[key] = t
	return nil
}

func (m *MockRepository) RawTradeExistsByOrderID(orderID, source string) (bool, error) {
	key := orderID + ":" + source
	_, exists := m.rawTrades[key]
	return exists, nil
}

func (m *MockRepository) UpdateRawTradePositionID(tradeID int, positionID int) error {
	for _, rt := range m.rawTrades {
		if rt.ID == tradeID {
			rt.PositionID = &positionID
			break
		}
	}
	return nil
}

func (m *MockRepository) GetRawTradesByPositionID(positionID int) ([]*models.RawTrade, error) {
	var trades []*models.RawTrade
	for _, rt := range m.rawTrades {
		if rt.PositionID != nil && *rt.PositionID == positionID {
			trades = append(trades, rt)
		}
	}
	return trades, nil
}

func (m *MockRepository) LinkRawTradesToTradeHistory(positionID, historyID int) error {
	for _, rt := range m.rawTrades {
		if rt.PositionID != nil && *rt.PositionID == positionID {
			rt.TradeHistoryID = &historyID
		}
	}
	return nil
}

func (m *MockRepository) GetPositionBySymbol(symbol string) (*models.Position, error) {
	pos, exists := m.positions[symbol]
	if !exists {
		return nil, fmt.Errorf("position not found for symbol: %s", symbol)
	}
	return pos, nil
}

func (m *MockRepository) CreatePosition(p *models.Position) error {
	m.CreatePositionCalls++
	p.ID = m.nextPositionID
	m.nextPositionID++
	m.positions[p.Symbol] = p
	return nil
}

func (m *MockRepository) UpdatePosition(p *models.Position) error {
	m.UpdatePositionCalls++
	m.positions[p.Symbol] = p
	return nil
}

func (m *MockRepository) DeletePosition(id int) error {
	m.DeletePositionCalls++
	for symbol, pos := range m.positions {
		if pos.ID == id {
			delete(m.positions, symbol)
			break
		}
	}
	return nil
}

func (m *MockRepository) CreateTradeHistory(t *models.TradeHistory) error {
	m.CreateTradeHistoryCalls++
	t.ID = m.nextHistoryID
	m.nextHistoryID++
	m.tradeHistories = append(m.tradeHistories, t)
	return nil
}

// Helper function to create a RawTrade for testing
func createTestRawTrade(orderID, symbol, side string, qty, price float64, executedAt time.Time) *models.RawTrade {
	return &models.RawTrade{
		OrderID:    orderID,
		Source:     "robinhood",
		Symbol:     symbol,
		Side:       side,
		Quantity:   decimal.NewFromFloat(qty),
		Price:      decimal.NewFromFloat(price),
		TotalCost:  decimal.NewFromFloat(qty * price),
		Fees:       decimal.Zero,
		ExecutedAt: executedAt,
	}
}

// TestSLVTradeSequence tests the exact sequence of SLV trades from the real data
// This is the critical test that validates position aggregation works correctly
func TestSLVTradeSequence(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	// Base time for the test
	baseTime := time.Date(2026, 1, 2, 6, 0, 0, 0, time.UTC)

	// The exact SLV trade sequence from production logs (oldest to newest):
	trades := []*models.RawTrade{
		// Trade 1: BUY 3 @ $67.10 (Jan 2)
		createTestRawTrade("order-1", "SLV", models.TradeTypeBuy, 3.0, 67.10, baseTime),
		// Trade 2: SELL 3 @ $68.93 (Jan 5) - should close position
		createTestRawTrade("order-2", "SLV", models.TradeTypeSell, 3.0, 68.93, baseTime.Add(72*time.Hour)),
		// Trade 3: BUY 0.16 @ $72.67 (Jan 6) - new position
		createTestRawTrade("order-3", "SLV", models.TradeTypeBuy, 0.16017600, 72.67, baseTime.Add(96*time.Hour)),
		// Trade 4: SELL 0.16 @ $72.63 (Jan 6) - should close position
		createTestRawTrade("order-4", "SLV", models.TradeTypeSell, 0.16017600, 72.63, baseTime.Add(97*time.Hour)),
		// Trade 5: BUY 3 @ $73.10 (Jan 7) - new position
		createTestRawTrade("order-5", "SLV", models.TradeTypeBuy, 3.0, 73.10, baseTime.Add(120*time.Hour)),
		// Trade 6: BUY 0.41 @ $69.88 (Jan 7) - add to position
		createTestRawTrade("order-6", "SLV", models.TradeTypeBuy, 0.41099000, 69.88, baseTime.Add(132*time.Hour)),
		// Trade 7: BUY 1.49 @ $67.19 (Jan 8) - add to position
		createTestRawTrade("order-7", "SLV", models.TradeTypeBuy, 1.48842000, 67.19, baseTime.Add(156*time.Hour)),
		// Trade 8: SELL 4.90 @ $71.64 (Jan 9) - should close position
		createTestRawTrade("order-8", "SLV", models.TradeTypeSell, 4.89941000, 71.64, baseTime.Add(180*time.Hour)),
	}

	// Process each trade in order
	for i, trade := range trades {
		// First save the raw trade
		err := repo.CreateRawTrade(trade)
		require.NoError(t, err, "Failed to create raw trade %d", i+1)

		// Then aggregate to position
		err = consumer.aggregateToPosition(trade)
		require.NoError(t, err, "Failed to aggregate trade %d", i+1)

		t.Logf("After trade %d (%s %s): positions=%d, histories=%d",
			i+1, trade.Side, trade.Quantity, len(repo.positions), len(repo.tradeHistories))
	}

	// Verify final state: NO positions should exist (all closed)
	assert.Empty(t, repo.positions, "Expected no positions after all trades, but found %d", len(repo.positions))

	// Verify we created 3 trade history entries (3 complete cycles)
	assert.Len(t, repo.tradeHistories, 3, "Expected 3 trade history entries")

	// Verify each closed trade
	if len(repo.tradeHistories) >= 3 {
		// First closed trade: BUY 3 @ $67.10, SELL 3 @ $68.93
		h1 := repo.tradeHistories[0]
		assert.Equal(t, "SLV", h1.Symbol)
		assert.True(t, h1.RealizedPnl.GreaterThan(decimal.Zero), "First trade should be profitable")

		// Second closed trade: BUY 0.16 @ $72.67, SELL 0.16 @ $72.63
		h2 := repo.tradeHistories[1]
		assert.Equal(t, "SLV", h2.Symbol)

		// Third closed trade: multiple buys, one sell
		h3 := repo.tradeHistories[2]
		assert.Equal(t, "SLV", h3.Symbol)
	}
}

// TestBuyCreatesPosition verifies a BUY creates a new position when none exists
func TestBuyCreatesPosition(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	trade := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 150.00, time.Now())
	err := repo.CreateRawTrade(trade)
	require.NoError(t, err)

	err = consumer.aggregateToPosition(trade)
	require.NoError(t, err)

	// Should have created a position
	assert.Len(t, repo.positions, 1)
	pos := repo.positions["AAPL"]
	require.NotNil(t, pos)
	assert.Equal(t, "AAPL", pos.Symbol)
	assert.True(t, pos.Quantity.Equal(decimal.NewFromFloat(10)))
	assert.True(t, pos.EntryPrice.Equal(decimal.NewFromFloat(150.00)))
}

// TestBuyUpdatesExistingPosition verifies weighted average calculation
func TestBuyUpdatesExistingPosition(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	// First buy: 10 @ $100
	trade1 := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 100.00, time.Now())
	err := repo.CreateRawTrade(trade1)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade1)
	require.NoError(t, err)

	// Second buy: 10 @ $120
	trade2 := createTestRawTrade("order-2", "AAPL", models.TradeTypeBuy, 10, 120.00, time.Now().Add(time.Hour))
	err = repo.CreateRawTrade(trade2)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade2)
	require.NoError(t, err)

	// Should have one position with weighted average
	assert.Len(t, repo.positions, 1)
	pos := repo.positions["AAPL"]
	require.NotNil(t, pos)

	// Weighted avg = (10*100 + 10*120) / 20 = 2200/20 = 110
	assert.True(t, pos.Quantity.Equal(decimal.NewFromFloat(20)))
	assert.True(t, pos.EntryPrice.Equal(decimal.NewFromFloat(110)))
}

// TestSellClosesPosition verifies selling full position closes it
func TestSellClosesPosition(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	// Buy 10 @ $100
	trade1 := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 100.00, time.Now())
	err := repo.CreateRawTrade(trade1)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade1)
	require.NoError(t, err)

	assert.Len(t, repo.positions, 1)

	// Sell 10 @ $110 (full position)
	trade2 := createTestRawTrade("order-2", "AAPL", models.TradeTypeSell, 10, 110.00, time.Now().Add(time.Hour))
	err = repo.CreateRawTrade(trade2)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade2)
	require.NoError(t, err)

	// Position should be closed (deleted)
	assert.Empty(t, repo.positions, "Position should be deleted after full sell")

	// Trade history should be created
	assert.Len(t, repo.tradeHistories, 1)
	assert.Equal(t, 1, repo.DeletePositionCalls)
	assert.Equal(t, 1, repo.CreateTradeHistoryCalls)
}

// TestPartialSellReducesPosition verifies partial sell updates quantity
func TestPartialSellReducesPosition(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	// Buy 10 @ $100
	trade1 := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 100.00, time.Now())
	err := repo.CreateRawTrade(trade1)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade1)
	require.NoError(t, err)

	// Sell 4 @ $110 (partial)
	trade2 := createTestRawTrade("order-2", "AAPL", models.TradeTypeSell, 4, 110.00, time.Now().Add(time.Hour))
	err = repo.CreateRawTrade(trade2)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade2)
	require.NoError(t, err)

	// Position should still exist with reduced quantity
	assert.Len(t, repo.positions, 1)
	pos := repo.positions["AAPL"]
	require.NotNil(t, pos)
	assert.True(t, pos.Quantity.Equal(decimal.NewFromFloat(6)), "Expected 6 shares remaining, got %s", pos.Quantity)

	// No trade history yet (position not closed)
	assert.Empty(t, repo.tradeHistories)
}

// TestSellWithoutPositionIsIgnored verifies selling without a position doesn't crash
func TestSellWithoutPositionIsIgnored(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	// Sell without any position
	trade := createTestRawTrade("order-1", "AAPL", models.TradeTypeSell, 10, 100.00, time.Now())
	err := repo.CreateRawTrade(trade)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade)
	require.NoError(t, err) // Should not error

	// No position should be created
	assert.Empty(t, repo.positions)
	assert.Empty(t, repo.tradeHistories)
}

// TestMultiplePositionCycles verifies opening/closing multiple times works
func TestMultiplePositionCycles(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	baseTime := time.Now()

	// Cycle 1: Buy then sell
	trade1 := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 5, 100.00, baseTime)
	err := repo.CreateRawTrade(trade1)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade1)
	require.NoError(t, err)

	trade2 := createTestRawTrade("order-2", "AAPL", models.TradeTypeSell, 5, 110.00, baseTime.Add(time.Hour))
	err = repo.CreateRawTrade(trade2)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade2)
	require.NoError(t, err)

	assert.Empty(t, repo.positions, "Position should be closed after cycle 1")
	assert.Len(t, repo.tradeHistories, 1, "Should have 1 trade history after cycle 1")

	// Cycle 2: Buy then sell again
	trade3 := createTestRawTrade("order-3", "AAPL", models.TradeTypeBuy, 10, 105.00, baseTime.Add(2*time.Hour))
	err = repo.CreateRawTrade(trade3)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade3)
	require.NoError(t, err)

	assert.Len(t, repo.positions, 1, "New position should exist after buy in cycle 2")

	trade4 := createTestRawTrade("order-4", "AAPL", models.TradeTypeSell, 10, 115.00, baseTime.Add(3*time.Hour))
	err = repo.CreateRawTrade(trade4)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade4)
	require.NoError(t, err)

	assert.Empty(t, repo.positions, "Position should be closed after cycle 2")
	assert.Len(t, repo.tradeHistories, 2, "Should have 2 trade histories after cycle 2")
}

// TestWrongOrderSellBeforeBuy demonstrates what happens when trades arrive out of order
// This test documents the bug that occurs without proper ordering
func TestWrongOrderSellBeforeBuy(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	baseTime := time.Now()

	// WRONG ORDER: Sell arrives before Buy (simulating old bug)
	sellTrade := createTestRawTrade("order-2", "AAPL", models.TradeTypeSell, 5, 110.00, baseTime.Add(time.Hour))
	err := repo.CreateRawTrade(sellTrade)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(sellTrade)
	require.NoError(t, err)

	// Sell without position is ignored
	assert.Empty(t, repo.positions)
	assert.Empty(t, repo.tradeHistories)

	// Now the buy arrives
	buyTrade := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 5, 100.00, baseTime)
	err = repo.CreateRawTrade(buyTrade)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(buyTrade)
	require.NoError(t, err)

	// BUG: Position is created but will never be closed because the sell already happened
	assert.Len(t, repo.positions, 1, "Position exists but should have been closed")
	assert.Empty(t, repo.tradeHistories, "No trade history because sell was ignored")

	// This demonstrates why correct ordering is critical!
}

// TestPreciseQuantityMatch tests that decimal precision doesn't cause issues
func TestPreciseQuantityMatch(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	baseTime := time.Now()

	// Buy exact fractional shares
	trade1 := createTestRawTrade("order-1", "SLV", models.TradeTypeBuy, 0.16017600, 72.67, baseTime)
	err := repo.CreateRawTrade(trade1)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade1)
	require.NoError(t, err)

	assert.Len(t, repo.positions, 1)

	// Sell exact same fractional shares
	trade2 := createTestRawTrade("order-2", "SLV", models.TradeTypeSell, 0.16017600, 72.63, baseTime.Add(time.Hour))
	err = repo.CreateRawTrade(trade2)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(trade2)
	require.NoError(t, err)

	// Position should be fully closed
	assert.Empty(t, repo.positions, "Position should be closed with exact quantity match")
	assert.Len(t, repo.tradeHistories, 1)
}

// TestMultipleBuysThenSingleSell tests accumulating position then closing
func TestMultipleBuysThenSingleSell(t *testing.T) {
	repo := NewMockRepository()
	consumer := &Consumer{repo: repo}

	baseTime := time.Now()

	// Three buys
	buys := []struct {
		qty   float64
		price float64
	}{
		{3.0, 73.10},
		{0.41099, 69.88},
		{1.48842, 67.19},
	}

	totalQty := decimal.Zero
	for i, b := range buys {
		trade := createTestRawTrade(
			fmt.Sprintf("order-%d", i+1),
			"SLV",
			models.TradeTypeBuy,
			b.qty,
			b.price,
			baseTime.Add(time.Duration(i)*time.Hour),
		)
		err := repo.CreateRawTrade(trade)
		require.NoError(t, err)
		err = consumer.aggregateToPosition(trade)
		require.NoError(t, err)
		totalQty = totalQty.Add(decimal.NewFromFloat(b.qty))
	}

	// Verify accumulated position
	assert.Len(t, repo.positions, 1)
	pos := repo.positions["SLV"]
	require.NotNil(t, pos)

	// Total should be 3 + 0.41099 + 1.48842 = 4.89941
	expectedQty := decimal.NewFromFloat(4.89941)
	assert.True(t, pos.Quantity.Sub(expectedQty).Abs().LessThan(decimal.NewFromFloat(0.0001)),
		"Expected quantity ~4.89941, got %s", pos.Quantity)

	// Sell all shares
	sellTrade := createTestRawTrade("order-sell", "SLV", models.TradeTypeSell, 4.89941, 71.64, baseTime.Add(5*time.Hour))
	err := repo.CreateRawTrade(sellTrade)
	require.NoError(t, err)
	err = consumer.aggregateToPosition(sellTrade)
	require.NoError(t, err)

	// Position should be closed
	assert.Empty(t, repo.positions, "Position should be closed after selling all shares")
	assert.Len(t, repo.tradeHistories, 1, "Should have one trade history entry")
}
