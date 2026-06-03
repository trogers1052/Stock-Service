package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// tempError implements the Temporary() bool contract used by kafka-go to
// classify transient vs permanent write failures.
type tempError struct {
	msg  string
	temp bool
}

func (e tempError) Error() string   { return e.msg }
func (e tempError) Temporary() bool { return e.temp }

// mockMessageWriter is a configurable messageWriter for unit tests. It fails
// the first failUntil calls with failErr, then succeeds.
type mockMessageWriter struct {
	calls     int
	failUntil int
	failErr   error
	closed    bool
}

func (m *mockMessageWriter) WriteMessages(_ context.Context, _ ...kafka.Message) error {
	m.calls++
	if m.calls <= m.failUntil {
		return m.failErr
	}
	return nil
}

func (m *mockMessageWriter) Close() error {
	m.closed = true
	return nil
}

func newTestProducer(w messageWriter) *Producer {
	return &Producer{writer: w, topic: "test-topic"}
}

func testStock() *models.Stock {
	return &models.Stock{Symbol: "AAPL"}
}

func TestPublish_TransientErrorIsRetriedThenSucceeds(t *testing.T) {
	mw := &mockMessageWriter{
		failUntil: 2, // fail twice, succeed on the third attempt
		failErr:   tempError{msg: "transient broker timeout", temp: true},
	}
	p := newTestProducer(mw)

	if err := p.PublishStockAdded(context.Background(), testStock()); err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}
	if mw.calls != 3 {
		t.Fatalf("expected 3 write attempts (2 retries), got %d", mw.calls)
	}
}

func TestPublish_PermanentErrorSurfacesWithoutRetry(t *testing.T) {
	permErr := tempError{msg: "invalid message: permanent", temp: false}
	mw := &mockMessageWriter{
		failUntil: maxWriteRetries + 5, // would keep failing if retried
		failErr:   permErr,
	}
	p := newTestProducer(mw)

	err := p.PublishStockAdded(context.Background(), testStock())
	if err == nil {
		t.Fatal("expected permanent error to surface, got nil")
	}
	if !errors.Is(err, permErr) {
		t.Fatalf("expected wrapped permanent error, got: %v", err)
	}
	if mw.calls != 1 {
		t.Fatalf("permanent error must not be retried: expected 1 attempt, got %d", mw.calls)
	}
}

func TestPublish_TransientErrorExhaustsRetriesAndSurfaces(t *testing.T) {
	failErr := tempError{msg: "persistent transient failure", temp: true}
	mw := &mockMessageWriter{
		failUntil: maxWriteRetries + 10, // always fails
		failErr:   failErr,
	}
	p := newTestProducer(mw)

	err := p.PublishStockAdded(context.Background(), testStock())
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if !errors.Is(err, failErr) {
		t.Fatalf("expected wrapped transient error, got: %v", err)
	}
	if mw.calls != maxWriteRetries+1 {
		t.Fatalf("expected %d attempts, got %d", maxWriteRetries+1, mw.calls)
	}
}

func TestPublish_ContextCancellationStopsRetry(t *testing.T) {
	mw := &mockMessageWriter{
		failUntil: maxWriteRetries + 10,
		failErr:   tempError{msg: "transient", temp: true},
	}
	p := newTestProducer(mw)

	// Cancel quickly so the backoff wait between retries is interrupted.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := p.PublishStockAdded(ctx, testStock()); err == nil {
		t.Fatal("expected error when context is cancelled during retries")
	}
}
