package kafka

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
	testkit "github.com/trogers1052/trading-testkit"
)

func TestContract_TradeEvent_Consumer(t *testing.T) {
	raw := testkit.LoadContract("trade_event.json")

	var event models.TradeEvent
	err := json.Unmarshal(raw, &event)
	require.NoError(t, err, "TradeEvent should unmarshal from contract fixture")

	assert.Equal(t, "TRADE_DETECTED", event.EventType)
	assert.Equal(t, "robinhood", event.Source)

	assert.Equal(t, "order-contract-001", event.Data.OrderID)
	assert.Equal(t, "PLTR", event.Data.Symbol)
	assert.Equal(t, "buy", event.Data.Side)
	assert.Equal(t, "25", event.Data.Quantity)
	assert.Equal(t, "42.50", event.Data.AveragePrice)
}

func TestContract_StockEvent_Producer(t *testing.T) {
	raw := testkit.LoadContract("stock_event.json")

	var event models.StockEvent
	err := json.Unmarshal(raw, &event)
	require.NoError(t, err, "StockEvent should unmarshal from contract fixture")

	assert.Equal(t, "STOCK_ADDED", event.EventType)
	assert.Equal(t, "NVDA", event.Symbol)

	require.NotNil(t, event.Stock, "Stock should not be nil")
	assert.Equal(t, "NVIDIA Corporation", event.Stock.Name)
	assert.Equal(t, "NASDAQ", event.Stock.Exchange)
	assert.Equal(t, 875.50, event.Stock.CurrentPrice)
	assert.Equal(t, int64(45000000), event.Stock.Volume)
}

func TestContract_WatchlistEvent_Consumer(t *testing.T) {
	raw := testkit.LoadContract("watchlist_event_updated.json")

	var event WatchlistEvent
	err := json.Unmarshal(raw, &event)
	require.NoError(t, err, "WatchlistEvent should unmarshal from contract fixture")

	assert.Equal(t, "WATCHLIST_UPDATED", event.EventType)
	assert.Len(t, event.Data.AddedSymbols, 2)
	assert.Equal(t, 4, event.Data.TotalCount)
}

func TestContract_PositionsEvent_Consumer(t *testing.T) {
	raw := testkit.LoadContract("positions_event.json")

	var event models.PositionsEvent
	err := json.Unmarshal(raw, &event)
	require.NoError(t, err, "PositionsEvent should unmarshal from contract fixture")

	assert.Equal(t, "POSITIONS_SNAPSHOT", event.EventType)
	require.Len(t, event.Data.Positions, 2)
	assert.Equal(t, "PLTR", event.Data.Positions[0].Symbol)
	assert.Equal(t, "234.50", event.Data.BuyingPower)
}
