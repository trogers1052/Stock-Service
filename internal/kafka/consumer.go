package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// RawTradeRepository defines the interface for raw trade database operations
type RawTradeRepository interface {
	CreateRawTrade(t *models.RawTrade) error
	RawTradeExistsByOrderID(orderID, source string) (bool, error)
}

// Consumer handles consuming trade events from Kafka
// Note: This consumer only stores raw trades for audit purposes.
// Positions are managed separately via the PositionsConsumer which
// receives position snapshots directly from Robinhood.
type Consumer struct {
	reader *kafka.Reader
	repo   RawTradeRepository
}

// NewConsumer creates a new Kafka consumer for trade events
func NewConsumer(brokers []string, topic, groupID string, repo RawTradeRepository) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        1 * time.Second,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})

	return &Consumer{
		reader: reader,
		repo:   repo,
	}
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic: %s", c.reader.Config().Topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer shutting down...")
			return c.reader.Close()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil // Context cancelled, normal shutdown
				}
				log.Printf("Error reading message: %v", err)
				continue
			}

			if err := c.processMessage(msg); err != nil {
				log.Printf("Error processing message: %v", err)
				// Continue processing other messages
			}
		}
	}
}

// processMessage handles a single Kafka message
func (c *Consumer) processMessage(msg kafka.Message) error {
	log.Printf("Received message from partition %d offset %d: key=%s",
		msg.Partition, msg.Offset, string(msg.Key))

	var event models.TradeEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal trade event: %w", err)
	}

	// Only process TRADE_DETECTED events
	if event.EventType != "TRADE_DETECTED" {
		log.Printf("Ignoring event type: %s", event.EventType)
		return nil
	}

	// Check for duplicate (idempotency)
	exists, err := c.repo.RawTradeExistsByOrderID(event.Data.OrderID, event.Source)
	if err != nil {
		return fmt.Errorf("failed to check for duplicate trade: %w", err)
	}
	if exists {
		log.Printf("Trade %s from %s already exists, skipping", event.Data.OrderID, event.Source)
		return nil
	}

	// Convert event to RawTrade
	rawTrade, err := c.convertEventToRawTrade(event)
	if err != nil {
		return fmt.Errorf("failed to convert event to raw trade: %w", err)
	}

	// Save raw trade to database (audit trail only - positions come from Robinhood snapshots)
	if err := c.repo.CreateRawTrade(rawTrade); err != nil {
		return fmt.Errorf("failed to save raw trade: %w", err)
	}

	log.Printf("Saved raw trade: %s %s %s @ %s (order_id: %s)",
		rawTrade.Side, rawTrade.Quantity, rawTrade.Symbol, rawTrade.Price, rawTrade.OrderID)

	return nil
}

// convertEventToRawTrade maps a TradeEvent to a RawTrade model
func (c *Consumer) convertEventToRawTrade(event models.TradeEvent) (*models.RawTrade, error) {
	data := event.Data

	// Parse quantity
	quantity, err := decimal.NewFromString(data.Quantity)
	if err != nil {
		return nil, fmt.Errorf("invalid quantity %s: %w", data.Quantity, err)
	}

	// Parse price
	price, err := decimal.NewFromString(data.AveragePrice)
	if err != nil {
		return nil, fmt.Errorf("invalid price %s: %w", data.AveragePrice, err)
	}

	// Parse total cost
	totalCost, err := decimal.NewFromString(data.TotalNotional)
	if err != nil {
		// Fall back to quantity * price
		totalCost = quantity.Mul(price)
	}

	// Parse fees
	fees := decimal.Zero
	if data.Fees != "" {
		fees, _ = decimal.NewFromString(data.Fees)
	}

	// Convert side to uppercase
	side := strings.ToUpper(data.Side)
	if side != models.TradeTypeBuy && side != models.TradeTypeSell {
		return nil, fmt.Errorf("invalid trade side: %s", data.Side)
	}

	// Parse executed_at timestamp
	var executedAt time.Time
	if data.ExecutedAt != nil && *data.ExecutedAt != "" {
		executedAt, err = time.Parse(time.RFC3339, *data.ExecutedAt)
		if err != nil {
			// Try parsing without timezone
			executedAt, err = time.Parse("2006-01-02T15:04:05", *data.ExecutedAt)
			if err != nil {
				executedAt = time.Now()
			}
		}
	} else {
		executedAt = time.Now()
	}

	return &models.RawTrade{
		OrderID:    data.OrderID,
		Source:     event.Source,
		Symbol:     data.Symbol,
		Side:       side,
		Quantity:   quantity,
		Price:      price,
		TotalCost:  totalCost,
		Fees:       fees,
		ExecutedAt: executedAt,
	}, nil
}

// Close closes the Kafka consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}
