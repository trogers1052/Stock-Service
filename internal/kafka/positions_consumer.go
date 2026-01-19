package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// PositionsRepository defines the interface for position database operations
type PositionsRepository interface {
	ReplaceAllPositions(positions []*models.Position) error
}

// positionsReader is a small interface wrapper around kafka.Reader to enable unit testing.
type positionsReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
	Config() kafka.ReaderConfig
}

// PositionsConsumer handles consuming position snapshot events from Kafka
type PositionsConsumer struct {
	reader positionsReader
	repo   PositionsRepository
}

// NewPositionsConsumer creates a new Kafka consumer for position events
func NewPositionsConsumer(brokers []string, topic, groupID string, repo PositionsRepository) *PositionsConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID + "-positions", // Separate consumer group for positions
		MinBytes:       10e3,                   // 10KB
		MaxBytes:       10e6,                   // 10MB
		MaxWait:        1 * time.Second,
		StartOffset:    kafka.LastOffset, // Only read new messages (not historical)
		CommitInterval: time.Second,
	})

	return &PositionsConsumer{
		reader: reader,
		repo:   repo,
	}
}

// Start begins consuming messages from Kafka
func (c *PositionsConsumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka positions consumer for topic: %s", c.reader.Config().Topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Positions consumer shutting down...")
			return c.reader.Close()
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil // Context cancelled, normal shutdown
				}
				log.Printf("Error reading positions message: %v", err)
				continue
			}

			if err := c.processMessage(msg); err != nil {
				log.Printf("Error processing positions message: %v", err)
				// Continue processing other messages
			}
		}
	}
}

// processMessage handles a single Kafka message
func (c *PositionsConsumer) processMessage(msg kafka.Message) error {
	log.Printf("Received positions message from partition %d offset %d",
		msg.Partition, msg.Offset)

	var event models.PositionsEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
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
	if err := c.repo.ReplaceAllPositions(positions); err != nil {
		return fmt.Errorf("failed to replace positions: %w", err)
	}

	log.Printf("Successfully updated %d positions from snapshot", len(positions))

	// Log each position
	for _, p := range positions {
		log.Printf("  %s: %s shares @ $%s (current: $%s, P&L: %s%%)",
			p.Symbol, p.Quantity, p.EntryPrice, p.CurrentPrice, p.UnrealizedPnlPct)
	}

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
	return c.reader.Close()
}
