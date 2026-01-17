package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreateTechnicalIndicator inserts a new technical indicator record
func (db *DB) CreateTechnicalIndicator(t *models.TechnicalIndicator) error {
	query := `
		INSERT INTO technical_indicators (symbol, date, indicator_type, value, timeframe, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (symbol, date, indicator_type, timeframe) DO UPDATE SET
			value = EXCLUDED.value
		RETURNING id
	`
	if t.Timeframe == "" {
		t.Timeframe = "daily"
	}
	err := db.conn.QueryRow(query,
		t.Symbol, t.Date, t.IndicatorType, t.Value, t.Timeframe, time.Now(),
	).Scan(&t.ID)

	if err != nil {
		return fmt.Errorf("failed to create technical indicator: %w", err)
	}
	return nil
}

// CreateTechnicalIndicatorBatch inserts multiple technical indicator records efficiently
func (db *DB) CreateTechnicalIndicatorBatch(indicators []*models.TechnicalIndicator) error {
	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO technical_indicators (symbol, date, indicator_type, value, timeframe, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (symbol, date, indicator_type, timeframe) DO UPDATE SET
			value = EXCLUDED.value
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now()
	for _, t := range indicators {
		timeframe := t.Timeframe
		if timeframe == "" {
			timeframe = "daily"
		}
		_, err := stmt.Exec(t.Symbol, t.Date, t.IndicatorType, t.Value, timeframe, now)
		if err != nil {
			return fmt.Errorf("failed to insert indicator for %s: %w", t.Symbol, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// GetTechnicalIndicatorByID retrieves a technical indicator by ID
func (db *DB) GetTechnicalIndicatorByID(id int) (*models.TechnicalIndicator, error) {
	query := `
		SELECT id, symbol, date, indicator_type, value, timeframe, created_at
		FROM technical_indicators
		WHERE id = $1
	`
	var t models.TechnicalIndicator
	err := db.conn.QueryRow(query, id).Scan(
		&t.ID, &t.Symbol, &t.Date, &t.IndicatorType, &t.Value, &t.Timeframe, &t.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("technical indicator not found: %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get technical indicator: %w", err)
	}
	return &t, nil
}

// GetIndicator retrieves a specific indicator for a symbol on a date
func (db *DB) GetIndicator(symbol string, date time.Time, indicatorType string, timeframe string) (*models.TechnicalIndicator, error) {
	if timeframe == "" {
		timeframe = "daily"
	}
	query := `
		SELECT id, symbol, date, indicator_type, value, timeframe, created_at
		FROM technical_indicators
		WHERE symbol = $1 AND date = $2 AND indicator_type = $3 AND timeframe = $4
	`
	var t models.TechnicalIndicator
	err := db.conn.QueryRow(query, symbol, date, indicatorType, timeframe).Scan(
		&t.ID, &t.Symbol, &t.Date, &t.IndicatorType, &t.Value, &t.Timeframe, &t.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("indicator not found: %s %s on %s", symbol, indicatorType, date.Format("2006-01-02"))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get indicator: %w", err)
	}
	return &t, nil
}

// GetIndicatorsBySymbol retrieves all indicators for a symbol on a specific date
func (db *DB) GetIndicatorsBySymbol(symbol string, date time.Time) ([]*models.TechnicalIndicator, error) {
	query := `
		SELECT id, symbol, date, indicator_type, value, timeframe, created_at
		FROM technical_indicators
		WHERE symbol = $1 AND date = $2
		ORDER BY indicator_type
	`
	rows, err := db.conn.Query(query, symbol, date)
	if err != nil {
		return nil, fmt.Errorf("failed to get indicators: %w", err)
	}
	defer rows.Close()

	var indicators []*models.TechnicalIndicator
	for rows.Next() {
		var t models.TechnicalIndicator
		err := rows.Scan(
			&t.ID, &t.Symbol, &t.Date, &t.IndicatorType, &t.Value, &t.Timeframe, &t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan indicator: %w", err)
		}
		indicators = append(indicators, &t)
	}

	return indicators, nil
}

// GetIndicatorHistory retrieves historical values for a specific indicator
func (db *DB) GetIndicatorHistory(symbol string, indicatorType string, limit int) ([]*models.TechnicalIndicator, error) {
	query := `
		SELECT id, symbol, date, indicator_type, value, timeframe, created_at
		FROM technical_indicators
		WHERE symbol = $1 AND indicator_type = $2
		ORDER BY date DESC
		LIMIT $3
	`
	rows, err := db.conn.Query(query, symbol, indicatorType, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get indicator history: %w", err)
	}
	defer rows.Close()

	var indicators []*models.TechnicalIndicator
	for rows.Next() {
		var t models.TechnicalIndicator
		err := rows.Scan(
			&t.ID, &t.Symbol, &t.Date, &t.IndicatorType, &t.Value, &t.Timeframe, &t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan indicator: %w", err)
		}
		indicators = append(indicators, &t)
	}

	return indicators, nil
}

// GetLatestIndicators retrieves the most recent indicators for a symbol
func (db *DB) GetLatestIndicators(symbol string) ([]*models.TechnicalIndicator, error) {
	query := `
		SELECT DISTINCT ON (indicator_type)
			id, symbol, date, indicator_type, value, timeframe, created_at
		FROM technical_indicators
		WHERE symbol = $1
		ORDER BY indicator_type, date DESC
	`
	rows, err := db.conn.Query(query, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest indicators: %w", err)
	}
	defer rows.Close()

	var indicators []*models.TechnicalIndicator
	for rows.Next() {
		var t models.TechnicalIndicator
		err := rows.Scan(
			&t.ID, &t.Symbol, &t.Date, &t.IndicatorType, &t.Value, &t.Timeframe, &t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan indicator: %w", err)
		}
		indicators = append(indicators, &t)
	}

	return indicators, nil
}

// GetLatestRSI is a convenience method to get the most recent RSI value
func (db *DB) GetLatestRSI(symbol string) (decimal.Decimal, error) {
	query := `
		SELECT value
		FROM technical_indicators
		WHERE symbol = $1 AND indicator_type = 'RSI_14'
		ORDER BY date DESC
		LIMIT 1
	`
	var value decimal.Decimal
	err := db.conn.QueryRow(query, symbol).Scan(&value)

	if err == sql.ErrNoRows {
		return decimal.Zero, fmt.Errorf("no RSI data found for %s", symbol)
	}
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to get RSI: %w", err)
	}
	return value, nil
}

// DeleteTechnicalIndicator removes an indicator by ID
func (db *DB) DeleteTechnicalIndicator(id int) error {
	query := `DELETE FROM technical_indicators WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete indicator: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("indicator not found: %d", id)
	}
	return nil
}

// DeleteIndicatorsBySymbol removes all indicators for a symbol
func (db *DB) DeleteIndicatorsBySymbol(symbol string) error {
	query := `DELETE FROM technical_indicators WHERE symbol = $1`
	_, err := db.conn.Exec(query, symbol)
	if err != nil {
		return fmt.Errorf("failed to delete indicators for %s: %w", symbol, err)
	}
	return nil
}

// DeleteIndicatorsOlderThan removes indicators older than a specified date
func (db *DB) DeleteIndicatorsOlderThan(date time.Time) (int64, error) {
	query := `DELETE FROM technical_indicators WHERE date < $1`
	result, err := db.conn.Exec(query, date)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old indicators: %w", err)
	}
	return result.RowsAffected()
}
