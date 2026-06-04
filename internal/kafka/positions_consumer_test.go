package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
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

// fakeConsumerGroup is a commonsConsumerGroup that blocks Run until ctx is
// cancelled, mimicking the shared ConsumerGroup lifecycle for unit tests.
type fakeConsumerGroup struct {
	mu         sync.Mutex
	runCalls   int
	closeCalls int
}

func (f *fakeConsumerGroup) Run(ctx context.Context) error {
	f.mu.Lock()
	f.runCalls++
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (f *fakeConsumerGroup) Close() error {
	f.mu.Lock()
	f.closeCalls++
	f.mu.Unlock()
	return nil
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

	err = consumer.processMessage(payload)
	require.NoError(t, err)
	assert.Equal(t, 0, repo.Calls())
}

// TestPositionsConsumer_handle_consumesAndProcessesMessages drives the handler
// path (the same path the shared ConsumerGroup invokes per message) and asserts
// the snapshot is converted and persisted.
func TestPositionsConsumer_handle_consumesAndProcessesMessages(t *testing.T) {
	repo := &mockPositionsRepo{called: make(chan struct{}, 1)}
	consumer := &PositionsConsumer{repo: repo, topic: "positions-topic"}

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

	err = consumer.handle(context.Background(), &commonskafka.Message{Value: payload})
	require.NoError(t, err)

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

// TestPositionsConsumer_Start_runsAndShutsDown verifies Start delegates to the
// consumer group's Run and returns cleanly when the context is cancelled.
func TestPositionsConsumer_Start_runsAndShutsDown(t *testing.T) {
	fake := &fakeConsumerGroup{}
	consumer := &PositionsConsumer{group: fake, topic: "positions-topic", repo: &mockPositionsRepo{}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumer to shut down")
	}

	fake.mu.Lock()
	assert.Equal(t, 1, fake.runCalls)
	fake.mu.Unlock()
}
