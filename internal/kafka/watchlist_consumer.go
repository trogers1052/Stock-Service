package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/trogers1052/stock-alert-system/internal/metrics"
	commonskafka "github.com/trogers1052/trading-go-commons/kafka"
)

// StockRepository defines the interface for stock database operations
type StockRepository interface {
	UpsertStockBasic(symbol, name string) error
	UpsertStockWithSector(symbol, name, sector, industry string) error
	StockExists(symbol string) (bool, error)
}

// WatchlistEvent represents a watchlist event from Kafka
type WatchlistEvent struct {
	EventType string             `json:"event_type"`
	Source    string             `json:"source"`
	Timestamp string             `json:"timestamp"`
	Data      WatchlistEventData `json:"data"`
}

// WatchlistEventData holds the data for different watchlist event types
type WatchlistEventData struct {
	// For WATCHLIST_UPDATED events
	AddedSymbols   []string         `json:"added_symbols,omitempty"`
	RemovedSymbols []string         `json:"removed_symbols,omitempty"`
	AllSymbols     []string         `json:"all_symbols,omitempty"`
	TotalCount     int              `json:"total_count,omitempty"`
	Stocks         []WatchlistStock `json:"stocks,omitempty"`

	// For WATCHLIST_SYMBOL_ADDED/REMOVED events
	Symbol   string `json:"symbol,omitempty"`
	Name     string `json:"name,omitempty"`
	Sector   string `json:"sector,omitempty"`
	Industry string `json:"industry,omitempty"`
}

// WatchlistStock represents stock details in the event
type WatchlistStock struct {
	Symbol        string `json:"symbol"`
	Name          string `json:"name"`
	InstrumentURL string `json:"instrument_url"`
	AddedAt       string `json:"added_at"`
	Sector        string `json:"sector,omitempty"`
	Industry      string `json:"industry,omitempty"`
}

// WatchlistConsumer handles consuming watchlist events from Kafka
type WatchlistConsumer struct {
	group commonsConsumerGroup
	topic string
	repo  StockRepository
}

// NewWatchlistConsumer creates a new Kafka consumer for watchlist events. It
// uses a dedicated consumer group (groupID + "-watchlist") and, for a brand-new
// group, starts at the OLDEST offset, matching the prior kafka-go StartOffset:
// FirstOffset behavior.
//
// On a handler (processing) error the message is NOT committed and is
// redelivered on the next session (Halt policy), preserving the prior
// "don't commit — message will be redelivered" semantics.
func NewWatchlistConsumer(brokers []string, topic, groupID string, repo StockRepository) (*WatchlistConsumer, error) {
	c := &WatchlistConsumer{
		topic: topic,
		repo:  repo,
	}

	group, err := commonskafka.NewConsumerGroup(
		brokers,
		groupID+"-watchlist",
		[]string{topic},
		c.handle,
		commonskafka.WithInitialOffset(sarama.OffsetOldest),
		commonskafka.WithOnError(commonskafka.Halt),
		commonskafka.WithConsumerClientID("stock-service"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka watchlist consumer group: %w", err)
	}
	c.group = group
	return c, nil
}

// Start begins consuming messages from Kafka. It blocks until ctx is cancelled.
func (c *WatchlistConsumer) Start(ctx context.Context) error {
	log.Printf("Starting watchlist consumer for topic: %s", c.topic)
	return c.group.Run(ctx)
}

// handle is the ConsumerGroup Handler: records metrics and dispatches each
// message to processMessage. Returning an error triggers the Halt policy.
func (c *WatchlistConsumer) handle(_ context.Context, msg *commonskafka.Message) error {
	metrics.KafkaConsumed.WithLabelValues(c.topic).Inc()
	metrics.WatchlistEvents.Inc()

	if err := c.processMessage(msg.Value); err != nil {
		metrics.KafkaConsumerErrors.WithLabelValues(c.topic).Inc()
		log.Printf("Error processing watchlist message: %v", err)
		return err
	}
	return nil
}

// processMessage handles a single Kafka message payload
func (c *WatchlistConsumer) processMessage(value []byte) error {
	var event WatchlistEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal watchlist event: %w", err)
	}

	log.Printf("Processing watchlist event: %s", event.EventType)

	switch event.EventType {
	case "WATCHLIST_UPDATED":
		return c.handleWatchlistUpdated(event)

	case "WATCHLIST_SYMBOL_ADDED":
		return c.handleSymbolAdded(event)

	case "WATCHLIST_SYMBOL_REMOVED":
		// For now, we don't delete stocks when removed from watchlist
		// We just log it - the stock data may still be useful
		log.Printf("Symbol removed from watchlist: %s (keeping in database)",
			event.Data.Symbol)
		return nil

	default:
		log.Printf("Ignoring unknown watchlist event type: %s", event.EventType)
		return nil
	}
}

// handleWatchlistUpdated processes a full watchlist update event
func (c *WatchlistConsumer) handleWatchlistUpdated(event WatchlistEvent) error {
	log.Printf("Processing watchlist update: %d added, %d removed, %d total",
		len(event.Data.AddedSymbols),
		len(event.Data.RemovedSymbols),
		event.Data.TotalCount)

	// Process added symbols
	for _, symbol := range event.Data.AddedSymbols {
		symbol = strings.ToUpper(symbol)
		name := symbol
		sector := ""
		industry := ""

		// Find name and sector from stocks list
		for _, stock := range event.Data.Stocks {
			if strings.ToUpper(stock.Symbol) == symbol {
				name = stock.Name
				sector = stock.Sector
				industry = stock.Industry
				break
			}
		}

		dbStart := time.Now()
		if sector != "" || industry != "" {
			if err := c.repo.UpsertStockWithSector(symbol, name, sector, industry); err != nil {
				metrics.DBWriteErrors.Inc()
				log.Printf("Error upserting stock %s: %v", symbol, err)
				continue
			}
		} else {
			if err := c.repo.UpsertStockBasic(symbol, name); err != nil {
				metrics.DBWriteErrors.Inc()
				log.Printf("Error upserting stock %s: %v", symbol, err)
				continue
			}
		}
		metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())
		log.Printf("Added/updated stock: %s (%s) sector=%s", symbol, name, sector)
	}

	return nil
}

// handleSymbolAdded processes a single symbol added event
func (c *WatchlistConsumer) handleSymbolAdded(event WatchlistEvent) error {
	symbol := strings.ToUpper(event.Data.Symbol)
	name := event.Data.Name
	if name == "" {
		name = symbol
	}

	sector := event.Data.Sector
	industry := event.Data.Industry

	dbStart := time.Now()
	if sector != "" || industry != "" {
		if err := c.repo.UpsertStockWithSector(symbol, name, sector, industry); err != nil {
			metrics.DBWriteErrors.Inc()
			return fmt.Errorf("failed to upsert stock %s: %w", symbol, err)
		}
	} else {
		if err := c.repo.UpsertStockBasic(symbol, name); err != nil {
			metrics.DBWriteErrors.Inc()
			return fmt.Errorf("failed to upsert stock %s: %w", symbol, err)
		}
	}
	metrics.DBWriteDuration.Observe(time.Since(dbStart).Seconds())

	log.Printf("Added/updated stock from watchlist: %s (%s) sector=%s", symbol, name, sector)
	return nil
}

// Close closes the Kafka consumer
func (c *WatchlistConsumer) Close() error {
	return c.group.Close()
}
