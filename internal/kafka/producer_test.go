package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/trogers1052/stock-alert-system/internal/models"
)

// mockPublisher is a configurable publisher for unit tests. It records every
// Publish call and can be made to fail with a fixed error.
type mockPublisher struct {
	calls   int
	failErr error
	closed  bool

	lastTopic string
	lastKey   []byte
	lastValue []byte
}

func (m *mockPublisher) Publish(_ context.Context, topic string, key, value []byte) error {
	m.calls++
	m.lastTopic = topic
	m.lastKey = key
	m.lastValue = value
	return m.failErr
}

func (m *mockPublisher) Close() error {
	m.closed = true
	return nil
}

func newTestProducer(p publisher) *Producer {
	return &Producer{producer: p, topic: "test-topic"}
}

func testStock() *models.Stock {
	return &models.Stock{Symbol: "AAPL"}
}

// TestPublish_Success verifies a successful publish sends exactly one message
// to the configured topic, keyed by symbol, carrying the JSON-marshaled event.
func TestPublish_Success(t *testing.T) {
	mp := &mockPublisher{}
	p := newTestProducer(mp)

	if err := p.PublishStockAdded(context.Background(), testStock()); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if mp.calls != 1 {
		t.Fatalf("expected exactly 1 publish, got %d", mp.calls)
	}
	if mp.lastTopic != "test-topic" {
		t.Fatalf("expected topic %q, got %q", "test-topic", mp.lastTopic)
	}
	if string(mp.lastKey) != "AAPL" {
		t.Fatalf("expected key AAPL, got %q", string(mp.lastKey))
	}

	var event models.StockEvent
	if err := json.Unmarshal(mp.lastValue, &event); err != nil {
		t.Fatalf("published value is not valid StockEvent JSON: %v", err)
	}
	if event.EventType != "STOCK_ADDED" {
		t.Fatalf("expected STOCK_ADDED, got %s", event.EventType)
	}
	if event.Symbol != "AAPL" {
		t.Fatalf("expected symbol AAPL, got %s", event.Symbol)
	}
}

// TestPublish_ErrorSurfaces verifies a producer error is wrapped and returned.
func TestPublish_ErrorSurfaces(t *testing.T) {
	wantErr := errors.New("broker unavailable")
	mp := &mockPublisher{failErr: wantErr}
	p := newTestProducer(mp)

	err := p.PublishStockAdded(context.Background(), testStock())
	if err == nil {
		t.Fatal("expected error to surface, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped producer error, got: %v", err)
	}
}

// TestPublishStockRemoved_KeyAndType verifies the removed event uses the symbol
// as the key and the STOCK_REMOVED type.
func TestPublishStockRemoved_KeyAndType(t *testing.T) {
	mp := &mockPublisher{}
	p := newTestProducer(mp)

	if err := p.PublishStockRemoved(context.Background(), "MSFT"); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if string(mp.lastKey) != "MSFT" {
		t.Fatalf("expected key MSFT, got %q", string(mp.lastKey))
	}

	var event models.StockEvent
	if err := json.Unmarshal(mp.lastValue, &event); err != nil {
		t.Fatalf("published value is not valid StockEvent JSON: %v", err)
	}
	if event.EventType != "STOCK_REMOVED" {
		t.Fatalf("expected STOCK_REMOVED, got %s", event.EventType)
	}
}

// TestPublishStockUpdated_Type verifies the updated event type.
func TestPublishStockUpdated_Type(t *testing.T) {
	mp := &mockPublisher{}
	p := newTestProducer(mp)

	if err := p.PublishStockUpdated(context.Background(), testStock()); err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	var event models.StockEvent
	if err := json.Unmarshal(mp.lastValue, &event); err != nil {
		t.Fatalf("published value is not valid StockEvent JSON: %v", err)
	}
	if event.EventType != "STOCK_UPDATED" {
		t.Fatalf("expected STOCK_UPDATED, got %s", event.EventType)
	}
}

// TestProducerClose verifies Close delegates to the underlying publisher.
func TestProducerClose(t *testing.T) {
	mp := &mockPublisher{}
	p := newTestProducer(mp)

	if err := p.Close(); err != nil {
		t.Fatalf("expected Close to succeed, got: %v", err)
	}
	if !mp.closed {
		t.Fatal("expected underlying publisher to be closed")
	}
}
