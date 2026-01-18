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

// Repository defines the interface for database operations
type Repository interface {
	// Raw trades
	CreateRawTrade(t *models.RawTrade) error
	RawTradeExistsByOrderID(orderID, source string) (bool, error)
	UpdateRawTradePositionID(tradeID int, positionID int) error
	GetRawTradesByPositionID(positionID int) ([]*models.RawTrade, error)
	LinkRawTradesToTradeHistory(positionID, historyID int) error

	// Positions
	GetPositionBySymbol(symbol string) (*models.Position, error)
	CreatePosition(p *models.Position) error
	UpdatePosition(p *models.Position) error
	DeletePosition(id int) error

	// Trade history
	CreateTradeHistory(t *models.TradeHistory) error
}

// Consumer handles consuming trade events from Kafka
type Consumer struct {
	reader *kafka.Reader
	repo   Repository
}

// NewConsumer creates a new Kafka consumer for trade events
func NewConsumer(brokers []string, topic, groupID string, repo Repository) *Consumer {
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

	// Save raw trade to database
	if err := c.repo.CreateRawTrade(rawTrade); err != nil {
		return fmt.Errorf("failed to save raw trade: %w", err)
	}

	log.Printf("Saved raw trade: %s %s %s @ %s (order_id: %s)",
		rawTrade.Side, rawTrade.Quantity, rawTrade.Symbol, rawTrade.Price, rawTrade.OrderID)

	// Aggregate into positions
	if err := c.aggregateToPosition(rawTrade); err != nil {
		log.Printf("Warning: failed to aggregate to position: %v", err)
		// Don't fail the whole message processing - raw trade is saved
	}

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

// aggregateToPosition updates or creates a position based on the raw trade
func (c *Consumer) aggregateToPosition(trade *models.RawTrade) error {
	// Get existing position for this symbol
	position, err := c.repo.GetPositionBySymbol(trade.Symbol)

	if err != nil && !strings.Contains(err.Error(), "not found") {
		return fmt.Errorf("failed to get position: %w", err)
	}

	if trade.Side == models.TradeTypeBuy {
		return c.handleBuy(trade, position)
	} else {
		return c.handleSell(trade, position)
	}
}

// handleBuy processes a BUY trade - creates or updates position
func (c *Consumer) handleBuy(trade *models.RawTrade, position *models.Position) error {
	if position == nil {
		// Create new position
		newPosition := &models.Position{
			Symbol:     trade.Symbol,
			Quantity:   trade.Quantity,
			EntryPrice: trade.Price,
			EntryDate:  trade.ExecutedAt,
		}

		if err := c.repo.CreatePosition(newPosition); err != nil {
			return fmt.Errorf("failed to create position: %w", err)
		}

		// Link raw trade to position
		if err := c.repo.UpdateRawTradePositionID(trade.ID, newPosition.ID); err != nil {
			log.Printf("Warning: failed to link raw trade to position: %v", err)
		}

		log.Printf("Created new position: %s %s @ %s",
			newPosition.Symbol, newPosition.Quantity, newPosition.EntryPrice)
	} else {
		// Update existing position with weighted average price
		// New avg = (old_qty * old_price + new_qty * new_price) / (old_qty + new_qty)
		oldTotal := position.Quantity.Mul(position.EntryPrice)
		newTotal := trade.Quantity.Mul(trade.Price)
		totalQty := position.Quantity.Add(trade.Quantity)

		position.EntryPrice = oldTotal.Add(newTotal).Div(totalQty)
		position.Quantity = totalQty

		if err := c.repo.UpdatePosition(position); err != nil {
			return fmt.Errorf("failed to update position: %w", err)
		}

		// Link raw trade to position
		if err := c.repo.UpdateRawTradePositionID(trade.ID, position.ID); err != nil {
			log.Printf("Warning: failed to link raw trade to position: %v", err)
		}

		log.Printf("Updated position: %s now %s @ avg %s",
			position.Symbol, position.Quantity, position.EntryPrice)
	}

	return nil
}

// handleSell processes a SELL trade - updates or closes position
func (c *Consumer) handleSell(trade *models.RawTrade, position *models.Position) error {
	if position == nil {
		// Selling without a position - this could be a short or data issue
		log.Printf("Warning: SELL for %s but no position exists", trade.Symbol)
		return nil
	}

	// Link raw trade to position
	if err := c.repo.UpdateRawTradePositionID(trade.ID, position.ID); err != nil {
		log.Printf("Warning: failed to link raw trade to position: %v", err)
	}

	// Calculate remaining quantity
	remainingQty := position.Quantity.Sub(trade.Quantity)

	if remainingQty.LessThanOrEqual(decimal.Zero) {
		// Position fully closed - move to trade history
		return c.closePosition(trade, position)
	}

	// Partial sell - update position quantity
	position.Quantity = remainingQty

	if err := c.repo.UpdatePosition(position); err != nil {
		return fmt.Errorf("failed to update position: %w", err)
	}

	log.Printf("Partial sell: %s remaining %s @ avg %s",
		position.Symbol, position.Quantity, position.EntryPrice)

	return nil
}

// closePosition moves a fully closed position to trade history
func (c *Consumer) closePosition(sellTrade *models.RawTrade, position *models.Position) error {
	// Get all raw trades for this position to calculate totals
	rawTrades, err := c.repo.GetRawTradesByPositionID(position.ID)
	if err != nil {
		log.Printf("Warning: could not get raw trades for position: %v", err)
		rawTrades = []*models.RawTrade{}
	}

	// Calculate aggregated values
	var totalBuyQty, totalBuyCost, totalSellQty, totalSellRevenue, totalFees decimal.Decimal

	for _, rt := range rawTrades {
		totalFees = totalFees.Add(rt.Fees)
		if rt.Side == models.TradeTypeBuy {
			totalBuyQty = totalBuyQty.Add(rt.Quantity)
			totalBuyCost = totalBuyCost.Add(rt.TotalCost)
		} else {
			totalSellQty = totalSellQty.Add(rt.Quantity)
			totalSellRevenue = totalSellRevenue.Add(rt.TotalCost)
		}
	}

	// Include the current sell trade
	totalFees = totalFees.Add(sellTrade.Fees)
	totalSellQty = totalSellQty.Add(sellTrade.Quantity)
	totalSellRevenue = totalSellRevenue.Add(sellTrade.TotalCost)

	// Calculate realized P&L
	realizedPnl := totalSellRevenue.Sub(totalBuyCost).Sub(totalFees)
	var realizedPnlPct decimal.Decimal
	if !totalBuyCost.IsZero() {
		realizedPnlPct = realizedPnl.Div(totalBuyCost).Mul(decimal.NewFromInt(100))
	}

	// Calculate holding period
	holdingHours := int(sellTrade.ExecutedAt.Sub(position.EntryDate).Hours())

	// Calculate average prices
	avgEntryPrice := position.EntryPrice
	var avgExitPrice decimal.Decimal
	if !totalSellQty.IsZero() {
		avgExitPrice = totalSellRevenue.Div(totalSellQty)
	}

	// Create trade history entry
	entryDate := position.EntryDate
	exitDate := sellTrade.ExecutedAt
	tradeHistory := &models.TradeHistory{
		Symbol:             position.Symbol,
		TradeType:          models.TradeTypeSell, // Closed position
		Quantity:           totalBuyQty,
		Price:              avgEntryPrice,
		TotalCost:          totalBuyCost,
		Fee:                totalFees,
		EntryDate:          &entryDate,
		ExitDate:           &exitDate,
		HoldingPeriodHours: &holdingHours,
		RealizedPnl:        realizedPnl,
		RealizedPnlPct:     realizedPnlPct,
		EntryReason:        position.EntryReason,
		ExecutedAt:         sellTrade.ExecutedAt,
	}

	// Set additional fields from position if available
	if !position.EntryRSI.IsZero() {
		tradeHistory.EntryRSI = position.EntryRSI
	}

	if err := c.repo.CreateTradeHistory(tradeHistory); err != nil {
		return fmt.Errorf("failed to create trade history: %w", err)
	}

	// Link all raw trades to this trade history
	if err := c.repo.LinkRawTradesToTradeHistory(position.ID, tradeHistory.ID); err != nil {
		log.Printf("Warning: failed to link raw trades to trade history: %v", err)
	}

	// Delete the position
	if err := c.repo.DeletePosition(position.ID); err != nil {
		log.Printf("Warning: failed to delete closed position: %v", err)
	}

	log.Printf("Closed position: %s | Entry: %s @ %s | Exit: %s @ %s | P&L: %s (%.2f%%)",
		position.Symbol,
		totalBuyQty, avgEntryPrice,
		totalSellQty, avgExitPrice,
		realizedPnl, realizedPnlPct.InexactFloat64())

	return nil
}

// Close closes the Kafka consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}
