package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// Producer handles publishing events to Kafka
type Producer struct {
	writer *kafka.Writer
	topic  string
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
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

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// Close closes the Kafka producer
func (p *Producer) Close() error {
	return p.writer.Close()
}
