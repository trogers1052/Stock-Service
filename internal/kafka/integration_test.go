package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

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

func TestProducerAndConsumerRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	brokers := setupRedpanda(t)
	const topic = "trading.orders.test"

	// Create the topic up front so the consumer group has something to read.
	conn, err := segkafka.DialContext(context.Background(), "tcp", brokers[0])
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(segkafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}))
	_ = conn.Close()

	repo := newCaptureRepo()
	consumer := NewConsumer(brokers, topic, "stock-service-test", repo)
	require.NotNil(t, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()

	// Produce a TRADE_DETECTED event onto the topic. The trade consumer reads
	// raw segmentio messages, so write directly with a writer.
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

	w := &segkafka.Writer{Addr: segkafka.TCP(brokers...), Topic: topic, Balancer: &segkafka.LeastBytes{}}
	defer w.Close()
	payload := mustMarshal(t, event)
	writeWithRetry(t, w, segkafka.Message{Key: []byte("AAPL"), Value: payload})

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

	conn, err := segkafka.DialContext(context.Background(), "tcp", brokers[0])
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(segkafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}))
	_ = conn.Close()

	repo := &stockRepoCapture{signal: make(chan struct{}, 4)}
	consumer := NewWatchlistConsumer(brokers, topic, "stock-service-test", repo)
	require.NotNil(t, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()

	event := WatchlistEvent{
		EventType: "WATCHLIST_SYMBOL_ADDED",
		Data:      WatchlistEventData{Symbol: "NVDA", Name: "NVIDIA Corp"},
	}
	w := &segkafka.Writer{Addr: segkafka.TCP(brokers...), Topic: topic, Balancer: &segkafka.LeastBytes{}}
	defer w.Close()
	writeWithRetry(t, w, segkafka.Message{Key: []byte("NVDA"), Value: mustMarshal(t, event)})

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

func TestProducerPublishMethods(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	brokers := setupRedpanda(t)
	const topic = "stock-events.test"

	conn, err := segkafka.DialContext(context.Background(), "tcp", brokers[0])
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(segkafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}))
	_ = conn.Close()

	p := NewProducer(brokers, topic)
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

// writeWithRetry tolerates leader-election delays right after topic creation.
func writeWithRetry(t *testing.T, w *segkafka.Writer, msg segkafka.Message) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.WriteMessages(ctx, msg)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("failed to write message: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
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
