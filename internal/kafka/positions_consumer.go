package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/metrics"
	"github.com/trogers1052/stock-alert-system/internal/models"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
)

// PositionsRepository defines the interface for position database operations
type PositionsRepository interface {
	ReplaceAllPositions(positions []*models.Position) error
}

// PositionsConsumer handles consuming position snapshot events from Kafka
type PositionsConsumer struct {
	group commonsConsumerGroup
	topic string
	repo  PositionsRepository
}

// NewPositionsConsumer creates a new Kafka consumer for position events. It
// uses a dedicated consumer group (groupID + "-positions") and, for a brand-new
// group, starts at the NEWEST offset — only reading new messages, not history —
// matching the prior kafka-go StartOffset: LastOffset behavior.
//
// On a handler (processing) error the message is NOT committed and is
// redelivered on the next session (Halt policy), preserving the prior
// "don't commit — message will be redelivered" semantics.
func NewPositionsConsumer(brokers []string, topic, groupID string, repo PositionsRepository) (*PositionsConsumer, error) {
	c := &PositionsConsumer{
		topic: topic,
		repo:  repo,
	}

	group, err := commonskafka.NewConsumerGroup(
		brokers,
		groupID+"-positions", // Separate consumer group for positions
		[]string{topic},
		c.handle,
		commonskafka.WithInitialOffset(commonskafka.OffsetNewest), // Only read new messages (not historical)
		commonskafka.WithOnError(commonskafka.Halt),
		commonskafka.WithConsumerClientID("stock-service"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka positions consumer group: %w", err)
	}
	c.group = group
	return c, nil
}

// Start begins consuming messages from Kafka. It blocks until ctx is cancelled.
func (c *PositionsConsumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka positions consumer for topic: %s", c.topic)
	return c.group.Run(ctx)
}

// handle is the ConsumerGroup Handler: records metrics and dispatches each
// message to processMessage. Returning an error triggers the Halt policy.
func (c *PositionsConsumer) handle(_ context.Context, msg *commonskafka.Message) error {
	metrics.KafkaConsumed.WithLabelValues(c.topic).Inc()

	if err := c.processMessage(msg.Value); err != nil {
		metrics.KafkaConsumerErrors.WithLabelValues(c.topic).Inc()
		log.Printf("Error processing positions message: %v", err)
		return err
	}
	return nil
}

// processMessage handles a single Kafka message payload
func (c *PositionsConsumer) processMessage(value []byte) error {
	var event models.PositionsEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal positions event: %w", err)
	}

	// Only process POSITIONS_SNAPSHOT events
	if event.EventType != "POSITIONS_SNAPSHOT" {
		log.Printf("Ignoring event type: %s", event.EventType)
		return nil
	}

	log.Printf("Processing positions snapshot: %d positions, buying_power=%s",
		len(event.Data.Positions), event.Data.BuyingPower)

	// Convert event data to Position models
	positions := make([]*models.Position, 0, len(event.Data.Positions))
	now := time.Now()

	for _, pd := range event.Data.Positions {
		position, err := c.convertPositionData(pd, now)
		if err != nil {
			log.Printf("Warning: failed to convert position %s: %v", pd.Symbol, err)
			continue
		}
		positions = append(positions, position)
	}

	// Replace all positions in the database
	dbStart := time.Now()
	if err := c.repo.ReplaceAllPositions(positions); err != nil {
		metrics.DBWriteErrors.Inc()
		return fmt.Errorf("failed to replace positions: %w", err)
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())

	log.Printf("Positions snapshot applied: %d positions updated", len(positions))

	return nil
}

// convertPositionData converts Kafka position data to a Position model
func (c *PositionsConsumer) convertPositionData(pd models.PositionData, now time.Time) (*models.Position, error) {
	quantity, err := decimal.NewFromString(pd.Quantity)
	if err != nil {
		return nil, fmt.Errorf("invalid quantity %s: %w", pd.Quantity, err)
	}

	entryPrice, err := decimal.NewFromString(pd.AverageBuyPrice)
	if err != nil {
		return nil, fmt.Errorf("invalid average_buy_price %s: %w", pd.AverageBuyPrice, err)
	}

	equity, err := decimal.NewFromString(pd.Equity)
	if err != nil {
		equity = decimal.Zero
	}

	percentChange, err := decimal.NewFromString(pd.PercentChange)
	if err != nil {
		percentChange = decimal.Zero
	}

	// Calculate current price from equity and quantity
	var currentPrice decimal.Decimal
	if !quantity.IsZero() {
		currentPrice = equity.Div(quantity)
	}

	return &models.Position{
		Symbol:           pd.Symbol,
		Quantity:         quantity,
		EntryPrice:       entryPrice,
		EntryDate:        now, // We don't have the actual entry date from Robinhood snapshot
		CurrentPrice:     currentPrice,
		UnrealizedPnlPct: percentChange,
	}, nil
}

// Close closes the Kafka consumer
func (c *PositionsConsumer) Close() error {
	return c.group.Close()
}
