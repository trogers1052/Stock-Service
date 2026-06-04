package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/metrics"
	"github.com/trogers1052/stock-alert-system/internal/models"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
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
	group commonsConsumerGroup
	topic string
	repo  RawTradeRepository
}

// NewConsumer creates a new Kafka consumer for trade events. It joins the
// consumer group groupID and (for a brand-new group) starts at the OLDEST
// offset, matching the prior kafka-go StartOffset: FirstOffset behavior.
//
// On a handler (processing) error the message is NOT committed and is
// redelivered on the next session (Halt policy), preserving the prior
// "don't commit — message will be redelivered" semantics so a failed DB
// write is never silently dropped.
func NewConsumer(brokers []string, topic, groupID string, repo RawTradeRepository) (*Consumer, error) {
	c := &Consumer{
		topic: topic,
		repo:  repo,
	}

	group, err := commonskafka.NewConsumerGroup(
		brokers,
		groupID,
		[]string{topic},
		c.handle,
		commonskafka.WithInitialOffset(sarama.OffsetOldest),
		commonskafka.WithOnError(commonskafka.Halt),
		commonskafka.WithConsumerClientID("stock-service"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer group: %w", err)
	}
	c.group = group
	return c, nil
}

// Start begins consuming messages from Kafka. It blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic: %s", c.topic)
	return c.group.Run(ctx)
}

// handle is the ConsumerGroup Handler: it records metrics and dispatches each
// message to processMessage. Returning an error triggers the Halt policy
// (offset not committed; redelivered next session).
func (c *Consumer) handle(_ context.Context, msg *commonskafka.Message) error {
	metrics.KafkaConsumed.WithLabelValues(c.topic).Inc()

	if err := c.processMessage(msg.Value); err != nil {
		metrics.KafkaConsumerErrors.WithLabelValues(c.topic).Inc()
		log.Printf("Error processing message: %v", err)
		return err
	}
	return nil
}

// processMessage handles a single Kafka message payload
func (c *Consumer) processMessage(value []byte) error {
	var event models.TradeEvent
	if err := json.Unmarshal(value, &event); err != nil {
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
	dbStart := time.Now()
	if err := c.repo.CreateRawTrade(rawTrade); err != nil {
		metrics.DBWriteErrors.Inc()
		return fmt.Errorf("failed to save raw trade: %w", err)
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())

	log.Printf("Raw trade stored: %s %s %s @ %s (order=%s)",
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
	return c.group.Close()
}

// commonsConsumerGroup is the subset of the shared ConsumerGroup used here,
// extracted as an interface so the consumer lifecycle can be unit-tested with
// a fake group.
type commonsConsumerGroup interface {
	Run(ctx context.Context) error
	Close() error
}
