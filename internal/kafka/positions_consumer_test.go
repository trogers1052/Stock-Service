package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

type mockPositionsRepo struct {
	mu     sync.Mutex
	calls  int
	last   []*models.Position
	called chan struct{}
}

func (m *mockPositionsRepo) ReplaceAllPositions(positions []*models.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls++
	m.last = positions
	if m.called != nil {
		select {
		case m.called <- struct{}{}:
		default:
		}
	}
	return nil
}

func (m *mockPositionsRepo) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *mockPositionsRepo) LastPositions() []*models.Position {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

type mockPositionsReader struct {
	cfg  kafka.ReaderConfig
	msgs chan kafka.Message

	mu         sync.Mutex
	closeCalls int
}

func newMockPositionsReader(topic string, buffer int) *mockPositionsReader {
	return &mockPositionsReader{
		cfg:  kafka.ReaderConfig{Topic: topic},
		msgs: make(chan kafka.Message, buffer),
	}
}

func (r *mockPositionsReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case msg := <-r.msgs:
		return msg, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (r *mockPositionsReader) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()
	return nil
}

func (r *mockPositionsReader) Config() kafka.ReaderConfig {
	return r.cfg
}

func TestPositionsConsumer_processMessage_ignoresNonSnapshotEventTypes(t *testing.T) {
	repo := &mockPositionsRepo{}
	consumer := &PositionsConsumer{repo: repo}

	event := models.PositionsEvent{
		EventType: "SOMETHING_ELSE",
		Source:    "robinhood",
		Timestamp: time.Now().Format(time.RFC3339),
		Data:      models.PositionsEventData{},
	}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	err = consumer.processMessage(kafka.Message{Value: payload})
	require.NoError(t, err)
	assert.Equal(t, 0, repo.Calls())
}

func TestPositionsConsumer_Start_consumesAndProcessesMessages(t *testing.T) {
	repo := &mockPositionsRepo{called: make(chan struct{}, 1)}
	reader := newMockPositionsReader("positions-topic", 1)
	consumer := &PositionsConsumer{reader: reader, repo: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Start(ctx)
	}()

	event := models.PositionsEvent{
		EventType: "POSITIONS_SNAPSHOT",
		Source:    "robinhood",
		Timestamp: time.Now().Format(time.RFC3339),
		Data: models.PositionsEventData{
			BuyingPower: "1000.00",
			Positions: []models.PositionData{
				{
					Symbol:          "AAPL",
					Quantity:        "1",
					AverageBuyPrice: "100",
					Equity:          "110",
					PercentChange:   "10",
				},
			},
		},
	}
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	reader.msgs <- kafka.Message{Value: payload}

	select {
	case <-repo.called:
		// processed
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for positions snapshot to be processed")
	}

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumer to shut down")
	}

	require.Equal(t, 1, repo.Calls())
	positions := repo.LastPositions()
	require.Len(t, positions, 1)

	p := positions[0]
	assert.Equal(t, "AAPL", p.Symbol)
	assert.True(t, p.Quantity.Equal(decimal.NewFromInt(1)))
	assert.True(t, p.EntryPrice.Equal(decimal.RequireFromString("100")))
	assert.True(t, p.CurrentPrice.Equal(decimal.RequireFromString("110")))
	assert.True(t, p.UnrealizedPnlPct.Equal(decimal.RequireFromString("10")))
	assert.False(t, p.EntryDate.IsZero())
}
