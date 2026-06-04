package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
	schemas "github.com/trogers1052/trading-event-schemas"
)

// capturedMessage records the topic/key/value the real producer marshals and
// hands to the shared Kafka producer.
type capturedMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

// capturingPublisher is a publisher that records the bytes the real producer
// marshals and hands to Kafka. It lets the schema-contract test exercise the
// exact production marshaling path (Producer.publish -> json.Marshal) rather
// than re-marshaling a struct by hand in the test.
type capturingPublisher struct {
	captured []capturedMessage
}

func (w *capturingPublisher) Publish(_ context.Context, topic string, key, value []byte) error {
	w.captured = append(w.captured, capturedMessage{Topic: topic, Key: key, Value: value})
	return nil
}

func (w *capturingPublisher) Close() error { return nil }

// TestSchemaContract_StockEvent_Producer drives the real producer marshaling
// path (NewProducer's struct/JSON tags via PublishStockAdded -> publish) and
// asserts the bytes that would hit Kafka validate against the canonical
// stock_event JSON Schema. This catches producer/contract drift at the wire
// level.
func TestSchemaContract_StockEvent_Producer(t *testing.T) {
	cw := &capturingPublisher{}
	p := newTestProducer(cw)

	stock := &models.Stock{
		ID:            "stock-001",
		Symbol:        "NVDA",
		Name:          "NVIDIA Corporation",
		Exchange:      "NASDAQ",
		Sector:        "Technology",
		Industry:      "Semiconductors",
		CurrentPrice:  875.50,
		PreviousClose: 860.00,
		ChangeAmount:  15.50,
		ChangePercent: 1.80,
		DayHigh:       880.00,
		DayLow:        855.00,
		Volume:        45000000,
		AverageVolume: 40000000,
		LastUpdated:   time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC),
		CreatedAt:     time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC),
	}

	require.NoError(t, p.PublishStockAdded(context.Background(), stock),
		"PublishStockAdded should succeed against the capturing writer")
	require.Len(t, cw.captured, 1, "exactly one message should be produced")

	bytes := cw.captured[0].Value
	require.NoError(t, schemas.Validate("stock_event", bytes),
		"produced STOCK_ADDED event must validate against the stock_event schema")

	// Sanity: the key is the symbol and the envelope is flat (no data wrapper).
	assert.Equal(t, "NVDA", string(cw.captured[0].Key))
	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(bytes, &envelope))
	assert.Contains(t, envelope, "event_type")
	assert.Contains(t, envelope, "symbol")
	assert.Contains(t, envelope, "timestamp")
	assert.Contains(t, envelope, "stock")
	assert.NotContains(t, envelope, "data", "stock_event must use the flat envelope")
}

// TestSchemaContract_PositionsEvent_Consumer loads the canonical positions_event
// example, asserts it validates against its schema, and asserts it parses
// cleanly into the consumer's models.PositionsEvent.
func TestSchemaContract_PositionsEvent_Consumer(t *testing.T) {
	raw, err := schemas.Example("positions_event")
	require.NoError(t, err, "positions_event example must be available")

	require.NoError(t, schemas.Validate("positions_event", raw),
		"canonical positions_event example must validate against its own schema")

	var event models.PositionsEvent
	require.NoError(t, json.Unmarshal(raw, &event),
		"positions_event example must parse into models.PositionsEvent")

	assert.Equal(t, "POSITIONS_SNAPSHOT", event.EventType)
	assert.Equal(t, "robinhood-sync", event.Source)
	require.Len(t, event.Data.Positions, 2)
	assert.Equal(t, "PLTR", event.Data.Positions[0].Symbol)
	assert.Equal(t, "25", event.Data.Positions[0].Quantity)
	assert.Equal(t, "42.50", event.Data.Positions[0].AverageBuyPrice)
	assert.Equal(t, "234.50", event.Data.BuyingPower)
	assert.Equal(t, "1899.50", event.Data.TotalEquity)
}

// TestSchemaContract_WatchlistAdded_Consumer loads the canonical
// watchlist_event_added example, validates it, and parses it into the
// consumer's WatchlistEvent (the same struct processMessage unmarshals into
// for WATCHLIST_SYMBOL_ADDED).
func TestSchemaContract_WatchlistAdded_Consumer(t *testing.T) {
	raw, err := schemas.Example("watchlist_event_added")
	require.NoError(t, err, "watchlist_event_added example must be available")

	require.NoError(t, schemas.Validate("watchlist_event_added", raw),
		"canonical watchlist_event_added example must validate against its own schema")

	var event WatchlistEvent
	require.NoError(t, json.Unmarshal(raw, &event),
		"watchlist_event_added example must parse into WatchlistEvent")

	assert.Equal(t, "WATCHLIST_SYMBOL_ADDED", event.EventType)
	assert.Equal(t, "robinhood-sync", event.Source)
	assert.Equal(t, "NVDA", event.Data.Symbol)
	assert.Equal(t, "NVIDIA Corporation", event.Data.Name)
}

// TestSchemaContract_WatchlistUpdated_Consumer loads the canonical
// watchlist_event_updated example, validates it, and parses it into the
// consumer's WatchlistEvent (the struct processMessage unmarshals into for
// WATCHLIST_UPDATED).
func TestSchemaContract_WatchlistUpdated_Consumer(t *testing.T) {
	raw, err := schemas.Example("watchlist_event_updated")
	require.NoError(t, err, "watchlist_event_updated example must be available")

	require.NoError(t, schemas.Validate("watchlist_event_updated", raw),
		"canonical watchlist_event_updated example must validate against its own schema")

	var event WatchlistEvent
	require.NoError(t, json.Unmarshal(raw, &event),
		"watchlist_event_updated example must parse into WatchlistEvent")

	assert.Equal(t, "WATCHLIST_UPDATED", event.EventType)
	assert.Equal(t, "robinhood-sync", event.Source)
	assert.Equal(t, []string{"NVDA", "AMD"}, event.Data.AddedSymbols)
	assert.Equal(t, []string{"INTC"}, event.Data.RemovedSymbols)
	assert.Equal(t, 4, event.Data.TotalCount)
	require.Len(t, event.Data.Stocks, 2)
	assert.Equal(t, "AAPL", event.Data.Stocks[0].Symbol)
	assert.Equal(t, "Apple Inc.", event.Data.Stocks[0].Name)
}
