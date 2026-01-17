package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreateMonitoredStock adds a stock to the monitoring watchlist
func (db *DB) CreateMonitoredStock(m *models.MonitoredStock) error {
	query := `
		INSERT INTO monitored_stocks (
			symbol, enabled, priority, buy_zone_low, buy_zone_high,
			target_price, stop_loss_price, alert_on_buy_zone, alert_on_rsi_oversold,
			rsi_oversold_threshold, notes, reason, added_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (symbol) DO UPDATE SET
			enabled = EXCLUDED.enabled,
			priority = EXCLUDED.priority,
			buy_zone_low = EXCLUDED.buy_zone_low,
			buy_zone_high = EXCLUDED.buy_zone_high,
			target_price = EXCLUDED.target_price,
			stop_loss_price = EXCLUDED.stop_loss_price,
			alert_on_buy_zone = EXCLUDED.alert_on_buy_zone,
			alert_on_rsi_oversold = EXCLUDED.alert_on_rsi_oversold,
			rsi_oversold_threshold = EXCLUDED.rsi_oversold_threshold,
			notes = EXCLUDED.notes,
			reason = EXCLUDED.reason,
			updated_at = EXCLUDED.updated_at
	`
	now := time.Now()
	if m.Priority == 0 {
		m.Priority = 1
	}

	_, err := db.conn.Exec(query,
		m.Symbol, m.Enabled, m.Priority, m.BuyZoneLow, m.BuyZoneHigh,
		m.TargetPrice, m.StopLossPrice, m.AlertOnBuyZone, m.AlertOnRSIOversold,
		m.RSIOversoldThreshold, m.Notes, m.Reason, now, now,
	)

	if err != nil {
		return fmt.Errorf("failed to create monitored stock: %w", err)
	}
	m.AddedAt = now
	m.UpdatedAt = now
	return nil
}

// GetMonitoredStockBySymbol retrieves a monitored stock by symbol
func (db *DB) GetMonitoredStockBySymbol(symbol string) (*models.MonitoredStock, error) {
	query := `
		SELECT symbol, enabled, priority, buy_zone_low, buy_zone_high,
		       target_price, stop_loss_price, alert_on_buy_zone, alert_on_rsi_oversold,
		       rsi_oversold_threshold, notes, reason, added_at, updated_at
		FROM monitored_stocks
		WHERE symbol = $1
	`
	var m models.MonitoredStock
	var buyZoneLow, buyZoneHigh, targetPrice, stopLossPrice, rsiThreshold sql.NullFloat64
	var notes, reason sql.NullString

	err := db.conn.QueryRow(query, symbol).Scan(
		&m.Symbol, &m.Enabled, &m.Priority, &buyZoneLow, &buyZoneHigh,
		&targetPrice, &stopLossPrice, &m.AlertOnBuyZone, &m.AlertOnRSIOversold,
		&rsiThreshold, &notes, &reason, &m.AddedAt, &m.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("monitored stock not found: %s", symbol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get monitored stock: %w", err)
	}

	if buyZoneLow.Valid {
		m.BuyZoneLow = &buyZoneLow.Float64
	}
	if buyZoneHigh.Valid {
		m.BuyZoneHigh = &buyZoneHigh.Float64
	}
	if targetPrice.Valid {
		m.TargetPrice = &targetPrice.Float64
	}
	if stopLossPrice.Valid {
		m.StopLossPrice = &stopLossPrice.Float64
	}
	if rsiThreshold.Valid {
		m.RSIOversoldThreshold = &rsiThreshold.Float64
	}
	if notes.Valid {
		m.Notes = notes.String
	}
	if reason.Valid {
		m.Reason = reason.String
	}

	return &m, nil
}

// GetAllMonitoredStocks retrieves all monitored stocks
func (db *DB) GetAllMonitoredStocks() ([]*models.MonitoredStock, error) {
	query := `
		SELECT symbol, enabled, priority, buy_zone_low, buy_zone_high,
		       target_price, stop_loss_price, alert_on_buy_zone, alert_on_rsi_oversold,
		       rsi_oversold_threshold, notes, reason, added_at, updated_at
		FROM monitored_stocks
		ORDER BY priority ASC, symbol ASC
	`
	return db.scanMonitoredStocks(db.conn.Query(query))
}

// GetEnabledMonitoredStocks retrieves all enabled monitored stocks
func (db *DB) GetEnabledMonitoredStocks() ([]*models.MonitoredStock, error) {
	query := `
		SELECT symbol, enabled, priority, buy_zone_low, buy_zone_high,
		       target_price, stop_loss_price, alert_on_buy_zone, alert_on_rsi_oversold,
		       rsi_oversold_threshold, notes, reason, added_at, updated_at
		FROM monitored_stocks
		WHERE enabled = true
		ORDER BY priority ASC, symbol ASC
	`
	return db.scanMonitoredStocks(db.conn.Query(query))
}

// GetMonitoredStocksByPriority retrieves monitored stocks by priority level
func (db *DB) GetMonitoredStocksByPriority(priority int) ([]*models.MonitoredStock, error) {
	query := `
		SELECT symbol, enabled, priority, buy_zone_low, buy_zone_high,
		       target_price, stop_loss_price, alert_on_buy_zone, alert_on_rsi_oversold,
		       rsi_oversold_threshold, notes, reason, added_at, updated_at
		FROM monitored_stocks
		WHERE priority = $1 AND enabled = true
		ORDER BY symbol ASC
	`
	return db.scanMonitoredStocks(db.conn.Query(query, priority))
}

// GetMonitoredSymbols returns just the symbols of enabled monitored stocks
func (db *DB) GetMonitoredSymbols() ([]string, error) {
	query := `
		SELECT symbol
		FROM monitored_stocks
		WHERE enabled = true
		ORDER BY priority ASC, symbol ASC
	`
	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get monitored symbols: %w", err)
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			return nil, fmt.Errorf("failed to scan symbol: %w", err)
		}
		symbols = append(symbols, symbol)
	}

	return symbols, nil
}

func (db *DB) scanMonitoredStocks(rows *sql.Rows, err error) ([]*models.MonitoredStock, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to query monitored stocks: %w", err)
	}
	defer rows.Close()

	var stocks []*models.MonitoredStock
	for rows.Next() {
		var m models.MonitoredStock
		var buyZoneLow, buyZoneHigh, targetPrice, stopLossPrice, rsiThreshold sql.NullFloat64
		var notes, reason sql.NullString

		err := rows.Scan(
			&m.Symbol, &m.Enabled, &m.Priority, &buyZoneLow, &buyZoneHigh,
			&targetPrice, &stopLossPrice, &m.AlertOnBuyZone, &m.AlertOnRSIOversold,
			&rsiThreshold, &notes, &reason, &m.AddedAt, &m.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan monitored stock: %w", err)
		}

		if buyZoneLow.Valid {
			m.BuyZoneLow = &buyZoneLow.Float64
		}
		if buyZoneHigh.Valid {
			m.BuyZoneHigh = &buyZoneHigh.Float64
		}
		if targetPrice.Valid {
			m.TargetPrice = &targetPrice.Float64
		}
		if stopLossPrice.Valid {
			m.StopLossPrice = &stopLossPrice.Float64
		}
		if rsiThreshold.Valid {
			m.RSIOversoldThreshold = &rsiThreshold.Float64
		}
		if notes.Valid {
			m.Notes = notes.String
		}
		if reason.Valid {
			m.Reason = reason.String
		}

		stocks = append(stocks, &m)
	}

	return stocks, nil
}

// UpdateMonitoredStock updates an existing monitored stock
func (db *DB) UpdateMonitoredStock(m *models.MonitoredStock) error {
	query := `
		UPDATE monitored_stocks SET
			enabled = $2, priority = $3, buy_zone_low = $4, buy_zone_high = $5,
			target_price = $6, stop_loss_price = $7, alert_on_buy_zone = $8,
			alert_on_rsi_oversold = $9, rsi_oversold_threshold = $10,
			notes = $11, reason = $12, updated_at = $13
		WHERE symbol = $1
	`
	m.UpdatedAt = time.Now()
	result, err := db.conn.Exec(query,
		m.Symbol, m.Enabled, m.Priority, m.BuyZoneLow, m.BuyZoneHigh,
		m.TargetPrice, m.StopLossPrice, m.AlertOnBuyZone,
		m.AlertOnRSIOversold, m.RSIOversoldThreshold,
		m.Notes, m.Reason, m.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update monitored stock: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("monitored stock not found: %s", m.Symbol)
	}
	return nil
}

// EnableMonitoredStock enables a monitored stock
func (db *DB) EnableMonitoredStock(symbol string) error {
	query := `UPDATE monitored_stocks SET enabled = true, updated_at = $2 WHERE symbol = $1`
	result, err := db.conn.Exec(query, symbol, time.Now())
	if err != nil {
		return fmt.Errorf("failed to enable monitored stock: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("monitored stock not found: %s", symbol)
	}
	return nil
}

// DisableMonitoredStock disables a monitored stock
func (db *DB) DisableMonitoredStock(symbol string) error {
	query := `UPDATE monitored_stocks SET enabled = false, updated_at = $2 WHERE symbol = $1`
	result, err := db.conn.Exec(query, symbol, time.Now())
	if err != nil {
		return fmt.Errorf("failed to disable monitored stock: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("monitored stock not found: %s", symbol)
	}
	return nil
}

// SetBuyZone updates the buy zone for a monitored stock
func (db *DB) SetBuyZone(symbol string, low, high float64) error {
	query := `
		UPDATE monitored_stocks
		SET buy_zone_low = $2, buy_zone_high = $3, updated_at = $4
		WHERE symbol = $1
	`
	result, err := db.conn.Exec(query, symbol, low, high, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set buy zone: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("monitored stock not found: %s", symbol)
	}
	return nil
}

// SetTargetAndStopLoss updates target price and stop loss for a monitored stock
func (db *DB) SetTargetAndStopLoss(symbol string, target, stopLoss float64) error {
	query := `
		UPDATE monitored_stocks
		SET target_price = $2, stop_loss_price = $3, updated_at = $4
		WHERE symbol = $1
	`
	result, err := db.conn.Exec(query, symbol, target, stopLoss, time.Now())
	if err != nil {
		return fmt.Errorf("failed to set target and stop loss: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("monitored stock not found: %s", symbol)
	}
	return nil
}

// DeleteMonitoredStock removes a stock from monitoring
func (db *DB) DeleteMonitoredStock(symbol string) error {
	query := `DELETE FROM monitored_stocks WHERE symbol = $1`
	result, err := db.conn.Exec(query, symbol)
	if err != nil {
		return fmt.Errorf("failed to delete monitored stock: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("monitored stock not found: %s", symbol)
	}
	return nil
}

// GetStocksInBuyZone returns stocks where current price is within the buy zone
func (db *DB) GetStocksInBuyZone() ([]*models.MonitoredStock, error) {
	query := `
		SELECT ms.symbol, ms.enabled, ms.priority, ms.buy_zone_low, ms.buy_zone_high,
		       ms.target_price, ms.stop_loss_price, ms.alert_on_buy_zone, ms.alert_on_rsi_oversold,
		       ms.rsi_oversold_threshold, ms.notes, ms.reason, ms.added_at, ms.updated_at
		FROM monitored_stocks ms
		JOIN stocks s ON ms.symbol = s.symbol
		WHERE ms.enabled = true
		  AND ms.buy_zone_low IS NOT NULL
		  AND ms.buy_zone_high IS NOT NULL
		  AND s.current_price BETWEEN ms.buy_zone_low AND ms.buy_zone_high
		ORDER BY ms.priority ASC, ms.symbol ASC
	`
	return db.scanMonitoredStocks(db.conn.Query(query))
}
