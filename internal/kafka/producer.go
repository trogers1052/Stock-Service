package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

const (
	// writeTimeout bounds how long a single write to Kafka may block.
	writeTimeout = 10 * time.Second
	// maxWriteRetries is the number of additional attempts after the first
	// failed write when the error appears transient.
	maxWriteRetries = 3
	// writeRetryBackoff is the base delay between retries (grows linearly).
	writeRetryBackoff = 200 * time.Millisecond
)

// messageWriter is a small interface wrapper around kafka.Writer to enable
// unit testing of retry/durability behavior.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer handles publishing events to Kafka
type Producer struct {
	writer messageWriter
	topic  string
}

// NewProducer creates a new Kafka producer configured for durability:
// it waits for all in-sync replicas to acknowledge each write (RequiredAcks
// = RequireAll) and bounds write latency with a WriteTimeout. Transient
// write failures are retried with backoff in publish.
func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		// Durability: wait for all in-sync replicas to acknowledge.
		RequiredAcks: kafka.RequireAll,
		// Bound how long a write may block before failing.
		WriteTimeout: writeTimeout,
	}

	return &Producer{
		writer: writer,
		topic:  topic,
	}
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

	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
	}

	var lastErr error
	for attempt := 0; attempt <= maxWriteRetries; attempt++ {
		if attempt > 0 {
			// Linear backoff, but respect context cancellation.
			select {
			case <-ctx.Done():
				return fmt.Errorf("failed to write message to kafka: %w", ctx.Err())
			case <-time.After(time.Duration(attempt) * writeRetryBackoff):
			}
		}

		err := p.writer.WriteMessages(ctx, msg)
		if err == nil {
			return nil
		}
		lastErr = err

		if !isRetryableWriteError(err) {
			return fmt.Errorf("failed to write message to kafka: %w", err)
		}
		log.Printf("kafka write failed (attempt %d/%d), retrying: %v", attempt+1, maxWriteRetries+1, err)
	}

	return fmt.Errorf("failed to write message to kafka after %d attempts: %w", maxWriteRetries+1, lastErr)
}

// isRetryableWriteError reports whether a write error is transient and worth
// retrying. Context errors are never retryable; kafka-go's temporary errors
// (and other non-permanent failures) are.
func isRetryableWriteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// kafka-go classifies protocol/IO errors via the Temporary() contract.
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) {
		return tempErr.Temporary()
	}

	// Unknown errors are treated as transient so a single hiccup does not
	// silently drop a message; permanent errors should implement Temporary().
	return true
}

// Close closes the Kafka producer
func (p *Producer) Close() error {
	return p.writer.Close()
}
