package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreateRawTrade inserts a new raw trade record
func (db *DB) CreateRawTrade(t *models.RawTrade) error {
	query := `
		INSERT INTO raw_trades (
			order_id, source, symbol, side, quantity, price, total_cost, fees,
			executed_at, position_id, trade_history_id, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
		RETURNING id
	`
	now := time.Now()

	err := db.conn.QueryRow(query,
		t.OrderID, t.Source, t.Symbol, t.Side, t.Quantity, t.Price, t.TotalCost, t.Fees,
		t.ExecutedAt, t.PositionID, t.TradeHistoryID, now,
	).Scan(&t.ID)

	if err != nil {
		return fmt.Errorf("failed to create raw trade: %w", err)
	}
	t.CreatedAt = now
	return nil
}

// RawTradeExistsByOrderID checks if a raw trade with the given order_id and source already exists
func (db *DB) RawTradeExistsByOrderID(orderID, source string) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM raw_trades WHERE order_id = $1 AND source = $2)`
	var exists bool
	err := db.conn.QueryRow(query, orderID, source).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check raw trade existence: %w", err)
	}
	return exists, nil
}

// GetRawTradeByID retrieves a raw trade by ID
func (db *DB) GetRawTradeByID(id int) (*models.RawTrade, error) {
	query := `
		SELECT id, order_id, source, symbol, side, quantity, price, total_cost, fees,
		       executed_at, position_id, trade_history_id, created_at
		FROM raw_trades
		WHERE id = $1
	`
	return db.scanSingleRawTrade(db.conn.QueryRow(query, id))
}

// GetRawTradesBySymbol retrieves all raw trades for a symbol
func (db *DB) GetRawTradesBySymbol(symbol string, limit int) ([]*models.RawTrade, error) {
	query := `
		SELECT id, order_id, source, symbol, side, quantity, price, total_cost, fees,
		       executed_at, position_id, trade_history_id, created_at
		FROM raw_trades
		WHERE symbol = $1
		ORDER BY executed_at DESC
		LIMIT $2
	`
	return db.scanRawTrades(db.conn.Query(query, symbol, limit))
}

// GetRawTradesByPositionID retrieves all raw trades linked to a position
func (db *DB) GetRawTradesByPositionID(positionID int) ([]*models.RawTrade, error) {
	query := `
		SELECT id, order_id, source, symbol, side, quantity, price, total_cost, fees,
		       executed_at, position_id, trade_history_id, created_at
		FROM raw_trades
		WHERE position_id = $1
		ORDER BY executed_at ASC
	`
	return db.scanRawTrades(db.conn.Query(query, positionID))
}

// GetUnlinkedRawTradesBySymbol retrieves raw trades not yet linked to a position
func (db *DB) GetUnlinkedRawTradesBySymbol(symbol string) ([]*models.RawTrade, error) {
	query := `
		SELECT id, order_id, source, symbol, side, quantity, price, total_cost, fees,
		       executed_at, position_id, trade_history_id, created_at
		FROM raw_trades
		WHERE symbol = $1 AND position_id IS NULL
		ORDER BY executed_at ASC
	`
	return db.scanRawTrades(db.conn.Query(query, symbol))
}

// UpdateRawTradePositionID links a raw trade to a position
func (db *DB) UpdateRawTradePositionID(tradeID int, positionID int) error {
	query := `UPDATE raw_trades SET position_id = $2 WHERE id = $1`
	result, err := db.conn.Exec(query, tradeID, positionID)
	if err != nil {
		return fmt.Errorf("failed to update raw trade position_id: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("raw trade not found: %d", tradeID)
	}
	return nil
}

// UpdateRawTradeHistoryID links a raw trade to a trade history (closed position)
func (db *DB) UpdateRawTradeHistoryID(tradeID int, historyID int) error {
	query := `UPDATE raw_trades SET trade_history_id = $2 WHERE id = $1`
	result, err := db.conn.Exec(query, tradeID, historyID)
	if err != nil {
		return fmt.Errorf("failed to update raw trade trade_history_id: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("raw trade not found: %d", tradeID)
	}
	return nil
}

// LinkRawTradesToTradeHistory links all raw trades for a position to a trade history record
func (db *DB) LinkRawTradesToTradeHistory(positionID, historyID int) error {
	query := `UPDATE raw_trades SET trade_history_id = $2 WHERE position_id = $1`
	_, err := db.conn.Exec(query, positionID, historyID)
	if err != nil {
		return fmt.Errorf("failed to link raw trades to trade history: %w", err)
	}
	return nil
}

func (db *DB) scanSingleRawTrade(row *sql.Row) (*models.RawTrade, error) {
	var t models.RawTrade
	var positionID, tradeHistoryID sql.NullInt64
	var fees sql.NullString

	err := row.Scan(
		&t.ID, &t.OrderID, &t.Source, &t.Symbol, &t.Side, &t.Quantity, &t.Price, &t.TotalCost, &fees,
		&t.ExecutedAt, &positionID, &tradeHistoryID, &t.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("raw trade not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get raw trade: %w", err)
	}

	if fees.Valid {
		t.Fees, _ = decimal.NewFromString(fees.String)
	}
	if positionID.Valid {
		id := int(positionID.Int64)
		t.PositionID = &id
	}
	if tradeHistoryID.Valid {
		id := int(tradeHistoryID.Int64)
		t.TradeHistoryID = &id
	}

	return &t, nil
}

func (db *DB) scanRawTrades(rows *sql.Rows, err error) ([]*models.RawTrade, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to query raw trades: %w", err)
	}
	defer rows.Close()

	var trades []*models.RawTrade
	for rows.Next() {
		var t models.RawTrade
		var positionID, tradeHistoryID sql.NullInt64
		var fees sql.NullString

		err := rows.Scan(
			&t.ID, &t.OrderID, &t.Source, &t.Symbol, &t.Side, &t.Quantity, &t.Price, &t.TotalCost, &fees,
			&t.ExecutedAt, &positionID, &tradeHistoryID, &t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan raw trade: %w", err)
		}

		if fees.Valid {
			t.Fees, _ = decimal.NewFromString(fees.String)
		}
		if positionID.Valid {
			id := int(positionID.Int64)
			t.PositionID = &id
		}
		if tradeHistoryID.Valid {
			id := int(tradeHistoryID.Int64)
			t.TradeHistoryID = &id
		}

		trades = append(trades, &t)
	}

	return trades, nil
}
