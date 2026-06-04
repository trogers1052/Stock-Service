package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/trogers1052/stock-alert-system/internal/models"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
)

const (
	// writeTimeout bounds how long a single write to Kafka may block. It is
	// passed to the shared producer as the broker ack + net timeout.
	writeTimeout = 10 * time.Second
)

// publisher is a small interface wrapper around the shared kafka.Producer to
// enable unit testing of the publish path without a broker.
type publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Close() error
}

// Producer handles publishing events to Kafka.
type Producer struct {
	producer publisher
	topic    string
}

// NewProducer creates a new Kafka producer backed by the shared sarama-based
// producer. The shared producer is durable by default: it waits for all
// in-sync replicas to acknowledge each write (RequiredAcks = WaitForAll) and
// retries transient failures with backoff, so no per-call retry loop is needed
// here. It returns an error if the underlying producer cannot be created.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	p, err := commonskafka.NewProducer(
		brokers,
		commonskafka.WithClientID("stock-service"),
		commonskafka.WithProducerTimeout(writeTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &Producer{
		producer: p,
		topic:    topic,
	}, nil
}

// PublishStockAdded publishes a stock added event
func (p *Producer) PublishStockAdded(ctx context.Context, stock *models.Stock) error {
	event := models.StockEvent{
		EventType: "STOCK_ADDED",
		Stock:     stock,
		Symbol:    stock.Symbol,
		Timestamp: time.Now(),
	}
	return p.publish(ctx, stock.Symbol, event)
}

// PublishStockRemoved publishes a stock removed event
func (p *Producer) PublishStockRemoved(ctx context.Context, symbol string) error {
	event := models.StockEvent{
		EventType: "STOCK_REMOVED",
		Symbol:    symbol,
		Timestamp: time.Now(),
	}
	return p.publish(ctx, symbol, event)
}

// PublishStockUpdated publishes a stock updated event
func (p *Producer) PublishStockUpdated(ctx context.Context, stock *models.Stock) error {
	event := models.StockEvent{
		EventType: "STOCK_UPDATED",
		Stock:     stock,
		Symbol:    stock.Symbol,
		Timestamp: time.Now(),
	}
	return p.publish(ctx, stock.Symbol, event)
}

func (p *Producer) publish(ctx context.Context, key string, event models.StockEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	if err := p.producer.Publish(ctx, p.topic, []byte(key), data); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}
	return nil
}

// Close closes the Kafka producer
func (p *Producer) Close() error {
	return p.producer.Close()
}
