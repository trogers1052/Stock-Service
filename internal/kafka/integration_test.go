package kafka

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"

	"github.com/trogers1052/stock-alert-system/internal/models"
)

// captureRepo records raw trades created by the consumer.
type captureRepo struct {
	mu       sync.Mutex
	created  []*models.RawTrade
	existing map[string]bool
	signal   chan struct{}
}

func newCaptureRepo() *captureRepo {
	return &captureRepo{existing: map[string]bool{}, signal: make(chan struct{}, 8)}
}

func (r *captureRepo) CreateRawTrade(t *models.RawTrade) error {
	r.mu.Lock()
	r.created = append(r.created, t)
	r.existing[t.OrderID+"|"+t.Source] = true
	r.mu.Unlock()
	r.signal <- struct{}{}
	return nil
}

func (r *captureRepo) RawTradeExistsByOrderID(orderID, source string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.existing[orderID+"|"+source], nil
}

func (r *captureRepo) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.created)
}

func setupRedpanda(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v23.3.3")
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	broker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return []string{broker}
}

// createTopic creates a single-partition topic via the kafka-go controller
// connection so the consumer group has something to read.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kgo.Dial("tcp", brokers[0])
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	require.NoError(t, err)

	ctrlConn, err := kgo.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)
	defer func() { _ = ctrlConn.Close() }()

	err = ctrlConn.CreateTopics(kgo.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
}

func TestProducerAndConsumerRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	brokers := setupRedpanda(t)
	const topic = "trading.orders.test"

	createTopic(t, brokers, topic)

	repo := newCaptureRepo()
	consumer, err := NewConsumer(brokers, topic, "stock-service-test", repo)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()

	// Produce a TRADE_DETECTED event onto the topic via the shared producer.
	executedAt := time.Now().UTC().Format(time.RFC3339)
	event := models.TradeEvent{
		EventType: "TRADE_DETECTED",
		Source:    "robinhood",
		Timestamp: executedAt,
		Data: models.TradeEventData{
			OrderID:       "order-123",
			Symbol:        "AAPL",
			Side:          "buy",
			Quantity:      "10",
			AveragePrice:  "100.5",
			TotalNotional: "1005",
			Fees:          "0.01",
			ExecutedAt:    &executedAt,
			CreatedAt:     executedAt,
		},
	}

	prod, err := commonskafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	publishWithRetry(t, func() error {
		return prod.Publish(context.Background(), topic, []byte("AAPL"), mustMarshal(t, event))
	})

	select {
	case <-repo.signal:
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("timed out waiting for consumer to process message")
	}

	assert.Equal(t, 1, repo.count())
	created := repo.created[0]
	assert.Equal(t, "order-123", created.OrderID)
	assert.Equal(t, "AAPL", created.Symbol)
	assert.Equal(t, models.TradeTypeBuy, created.Side)

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not shut down")
	}

	require.NoError(t, consumer.Close())
}

// stockRepoCapture records stock upserts driven by the watchlist consumer.
type stockRepoCapture struct {
	mu      sync.Mutex
	upserts []string
	signal  chan struct{}
}

func (r *stockRepoCapture) UpsertStockBasic(symbol, name string) error {
	r.mu.Lock()
	r.upserts = append(r.upserts, symbol)
	r.mu.Unlock()
	r.signal <- struct{}{}
	return nil
}

func (r *stockRepoCapture) UpsertStockWithSector(symbol, name, sector, industry string) error {
	return r.UpsertStockBasic(symbol, name)
}

func (r *stockRepoCapture) StockExists(symbol string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.upserts {
		if s == symbol {
			return true, nil
		}
	}
	return false, nil
}

func TestWatchlistConsumerRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	brokers := setupRedpanda(t)
	const topic = "trading.watchlist.test"

	createTopic(t, brokers, topic)

	repo := &stockRepoCapture{signal: make(chan struct{}, 4)}
	consumer, err := NewWatchlistConsumer(brokers, topic, "stock-service-test", repo)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()

	event := WatchlistEvent{
		EventType: "WATCHLIST_SYMBOL_ADDED",
		Data:      WatchlistEventData{Symbol: "NVDA", Name: "NVIDIA Corp"},
	}
	prod, err := commonskafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	publishWithRetry(t, func() error {
		return prod.Publish(context.Background(), topic, []byte("NVDA"), mustMarshal(t, event))
	})

	select {
	case <-repo.signal:
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("timed out waiting for watchlist consumer to process message")
	}

	repo.mu.Lock()
	assert.Contains(t, repo.upserts, "NVDA")
	repo.mu.Unlock()

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watchlist consumer did not shut down")
	}
	require.NoError(t, consumer.Close())
}

// TestPositionsConsumerRoundTrip exercises the positions consumer end-to-end
// against a real broker: a snapshot published to the topic is consumed and
// applied to the (capturing) repository.
func TestPositionsConsumerRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	brokers := setupRedpanda(t)
	const topic = "trading.positions.test"

	createTopic(t, brokers, topic)

	repo := &mockPositionsRepo{called: make(chan struct{}, 1)}
	consumer, err := NewPositionsConsumer(brokers, topic, "stock-service-test", repo)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()

	// The positions consumer starts at the NEWEST offset, so give the group a
	// moment to join and claim partitions before publishing, otherwise the
	// message would predate the consumer's starting position.
	prod, err := commonskafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()

	event := models.PositionsEvent{
		EventType: "POSITIONS_SNAPSHOT",
		Source:    "robinhood",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data: models.PositionsEventData{
			BuyingPower: "1000.00",
			Positions: []models.PositionData{
				{Symbol: "AAPL", Quantity: "1", AverageBuyPrice: "100", Equity: "110", PercentChange: "10"},
			},
		},
	}
	payload := mustMarshal(t, event)

	// Publish repeatedly until the consumer (joined at NEWEST) observes one.
	deadline := time.Now().Add(30 * time.Second)
	for {
		require.NoError(t, prod.Publish(context.Background(), topic, []byte("AAPL"), payload))
		select {
		case <-repo.called:
			goto consumed
		case <-time.After(1 * time.Second):
			if time.Now().After(deadline) {
				cancel()
				t.Fatal("timed out waiting for positions consumer to process message")
			}
		}
	}
consumed:

	assert.GreaterOrEqual(t, repo.Calls(), 1)
	positions := repo.LastPositions()
	require.Len(t, positions, 1)
	assert.Equal(t, "AAPL", positions[0].Symbol)

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("positions consumer did not shut down")
	}
	require.NoError(t, consumer.Close())
}

func TestProducerPublishMethods(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	brokers := setupRedpanda(t)
	const topic = "stock-events.test"

	createTopic(t, brokers, topic)

	p, err := NewProducer(brokers, topic)
	require.NoError(t, err)
	require.NotNil(t, p)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stock := &models.Stock{Symbol: "MSFT", Name: "Microsoft", CurrentPrice: 380}

	publishWithRetry(t, func() error { return p.PublishStockAdded(ctx, stock) })
	publishWithRetry(t, func() error { return p.PublishStockUpdated(ctx, stock) })
	publishWithRetry(t, func() error { return p.PublishStockRemoved(ctx, "MSFT") })
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func publishWithRetry(t *testing.T, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := fn(); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("publish failed: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
