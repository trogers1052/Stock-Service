package kafka

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// MockRawTradeRepository implements the RawTradeRepository interface for testing
type MockRawTradeRepository struct {
	rawTrades      map[string]*models.RawTrade // key: orderID+source
	nextRawTradeID int
}

func NewMockRawTradeRepository() *MockRawTradeRepository {
	return &MockRawTradeRepository{
		rawTrades:      make(map[string]*models.RawTrade),
		nextRawTradeID: 1,
	}
}

func (m *MockRawTradeRepository) CreateRawTrade(t *models.RawTrade) error {
	t.ID = m.nextRawTradeID
	m.nextRawTradeID++
	key := t.OrderID + ":" + t.Source
	m.rawTrades[key] = t
	return nil
}

func (m *MockRawTradeRepository) RawTradeExistsByOrderID(orderID, source string) (bool, error) {
	key := orderID + ":" + source
	_, exists := m.rawTrades[key]
	return exists, nil
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

// TestRawTradeCreation verifies raw trades are stored correctly
func TestRawTradeCreation(t *testing.T) {
	repo := NewMockRawTradeRepository()

	trade := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 150.00, time.Now())
	err := repo.CreateRawTrade(trade)
	require.NoError(t, err)

	assert.Len(t, repo.rawTrades, 1)
	assert.Equal(t, 1, trade.ID)
}

// TestDuplicateDetection verifies duplicate trades are detected
func TestDuplicateDetection(t *testing.T) {
	repo := NewMockRawTradeRepository()

	trade := createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 150.00, time.Now())
	err := repo.CreateRawTrade(trade)
	require.NoError(t, err)

	// Check if duplicate exists
	exists, err := repo.RawTradeExistsByOrderID("order-1", "robinhood")
	require.NoError(t, err)
	assert.True(t, exists)

	// Check for non-existent trade
	exists, err = repo.RawTradeExistsByOrderID("order-2", "robinhood")
	require.NoError(t, err)
	assert.False(t, exists)

	// Same order ID but different source should not be a duplicate
	exists, err = repo.RawTradeExistsByOrderID("order-1", "other-source")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestMultipleTradesStored verifies multiple trades are stored correctly
func TestMultipleTradesStored(t *testing.T) {
	repo := NewMockRawTradeRepository()

	trades := []*models.RawTrade{
		createTestRawTrade("order-1", "AAPL", models.TradeTypeBuy, 10, 150.00, time.Now()),
		createTestRawTrade("order-2", "GOOGL", models.TradeTypeBuy, 5, 2800.00, time.Now()),
		createTestRawTrade("order-3", "AAPL", models.TradeTypeSell, 10, 155.00, time.Now()),
	}

	for _, trade := range trades {
		err := repo.CreateRawTrade(trade)
		require.NoError(t, err)
	}

	assert.Len(t, repo.rawTrades, 3)
}

// TestConvertEventToRawTrade verifies event parsing
func TestConvertEventToRawTrade(t *testing.T) {
	repo := NewMockRawTradeRepository()
	consumer := &Consumer{repo: repo}

	executedAt := "2026-01-18T10:30:00Z"
	event := models.TradeEvent{
		EventType: "TRADE_DETECTED",
		Source:    "robinhood",
		Timestamp: "2026-01-18T10:30:00Z",
		Data: models.TradeEventData{
			OrderID:       "test-order-123",
			Symbol:        "AAPL",
			Side:          "buy",
			Quantity:      "10.5",
			AveragePrice:  "150.25",
			TotalNotional: "1577.625",
			Fees:          "0",
			State:         "filled",
			ExecutedAt:    &executedAt,
		},
	}

	rawTrade, err := consumer.convertEventToRawTrade(event)
	require.NoError(t, err)

	assert.Equal(t, "test-order-123", rawTrade.OrderID)
	assert.Equal(t, "robinhood", rawTrade.Source)
	assert.Equal(t, "AAPL", rawTrade.Symbol)
	assert.Equal(t, models.TradeTypeBuy, rawTrade.Side)
	assert.True(t, rawTrade.Quantity.Equal(decimal.NewFromFloat(10.5)))
	assert.True(t, rawTrade.Price.Equal(decimal.NewFromFloat(150.25)))
}

// TestConvertEventToRawTrade_InvalidSide verifies invalid side is rejected
func TestConvertEventToRawTrade_InvalidSide(t *testing.T) {
	repo := NewMockRawTradeRepository()
	consumer := &Consumer{repo: repo}

	event := models.TradeEvent{
		EventType: "TRADE_DETECTED",
		Source:    "robinhood",
		Data: models.TradeEventData{
			OrderID:      "test-order-123",
			Symbol:       "AAPL",
			Side:         "invalid",
			Quantity:     "10",
			AveragePrice: "150",
		},
	}

	_, err := consumer.convertEventToRawTrade(event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid trade side")
}
