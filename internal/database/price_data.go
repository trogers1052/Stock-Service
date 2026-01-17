package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreatePriceData inserts a new price data record
func (db *DB) CreatePriceData(p *models.PriceDataDaily) error {
	query := `
		INSERT INTO price_data_daily (symbol, date, open, high, low, close, volume, vwap, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (symbol, date) DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			vwap = EXCLUDED.vwap
		RETURNING id
	`
	err := db.conn.QueryRow(query,
		p.Symbol, p.Date, p.Open, p.High, p.Low, p.Close, p.Volume, p.VWAP, time.Now(),
	).Scan(&p.ID)

	if err != nil {
		return fmt.Errorf("failed to create price data: %w", err)
	}
	return nil
}

// CreatePriceDataBatch inserts multiple price data records efficiently
func (db *DB) CreatePriceDataBatch(prices []*models.PriceDataDaily) error {
	tx, err := db.conn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO price_data_daily (symbol, date, open, high, low, close, volume, vwap, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (symbol, date) DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			vwap = EXCLUDED.vwap
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now()
	for _, p := range prices {
		_, err := stmt.Exec(p.Symbol, p.Date, p.Open, p.High, p.Low, p.Close, p.Volume, p.VWAP, now)
		if err != nil {
			return fmt.Errorf("failed to insert price data for %s: %w", p.Symbol, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// GetPriceDataByID retrieves price data by ID
func (db *DB) GetPriceDataByID(id int) (*models.PriceDataDaily, error) {
	query := `
		SELECT id, symbol, date, open, high, low, close, volume, vwap, created_at
		FROM price_data_daily
		WHERE id = $1
	`
	var p models.PriceDataDaily
	var vwap sql.NullString

	err := db.conn.QueryRow(query, id).Scan(
		&p.ID, &p.Symbol, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume, &vwap, &p.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("price data not found: %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get price data: %w", err)
	}

	if vwap.Valid {
		p.VWAP, _ = decimal.NewFromString(vwap.String)
	}
	return &p, nil
}

// GetPriceDataBySymbolAndDate retrieves price data for a specific symbol and date
func (db *DB) GetPriceDataBySymbolAndDate(symbol string, date time.Time) (*models.PriceDataDaily, error) {
	query := `
		SELECT id, symbol, date, open, high, low, close, volume, vwap, created_at
		FROM price_data_daily
		WHERE symbol = $1 AND date = $2
	`
	var p models.PriceDataDaily
	var vwap sql.NullString

	err := db.conn.QueryRow(query, symbol, date).Scan(
		&p.ID, &p.Symbol, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume, &vwap, &p.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("price data not found for %s on %s", symbol, date.Format("2006-01-02"))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get price data: %w", err)
	}

	if vwap.Valid {
		p.VWAP, _ = decimal.NewFromString(vwap.String)
	}
	return &p, nil
}

// GetPriceDataBySymbol retrieves all price data for a symbol, ordered by date descending
func (db *DB) GetPriceDataBySymbol(symbol string, limit int) ([]*models.PriceDataDaily, error) {
	query := `
		SELECT id, symbol, date, open, high, low, close, volume, vwap, created_at
		FROM price_data_daily
		WHERE symbol = $1
		ORDER BY date DESC
		LIMIT $2
	`
	rows, err := db.conn.Query(query, symbol, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get price data: %w", err)
	}
	defer rows.Close()

	var prices []*models.PriceDataDaily
	for rows.Next() {
		var p models.PriceDataDaily
		var vwap sql.NullString

		err := rows.Scan(
			&p.ID, &p.Symbol, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume, &vwap, &p.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan price data: %w", err)
		}

		if vwap.Valid {
			p.VWAP, _ = decimal.NewFromString(vwap.String)
		}
		prices = append(prices, &p)
	}

	return prices, nil
}

// GetPriceDataRange retrieves price data for a symbol within a date range
func (db *DB) GetPriceDataRange(symbol string, startDate, endDate time.Time) ([]*models.PriceDataDaily, error) {
	query := `
		SELECT id, symbol, date, open, high, low, close, volume, vwap, created_at
		FROM price_data_daily
		WHERE symbol = $1 AND date >= $2 AND date <= $3
		ORDER BY date ASC
	`
	rows, err := db.conn.Query(query, symbol, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to get price data range: %w", err)
	}
	defer rows.Close()

	var prices []*models.PriceDataDaily
	for rows.Next() {
		var p models.PriceDataDaily
		var vwap sql.NullString

		err := rows.Scan(
			&p.ID, &p.Symbol, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume, &vwap, &p.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan price data: %w", err)
		}

		if vwap.Valid {
			p.VWAP, _ = decimal.NewFromString(vwap.String)
		}
		prices = append(prices, &p)
	}

	return prices, nil
}

// GetLatestPriceData retrieves the most recent price data for a symbol
func (db *DB) GetLatestPriceData(symbol string) (*models.PriceDataDaily, error) {
	query := `
		SELECT id, symbol, date, open, high, low, close, volume, vwap, created_at
		FROM price_data_daily
		WHERE symbol = $1
		ORDER BY date DESC
		LIMIT 1
	`
	var p models.PriceDataDaily
	var vwap sql.NullString

	err := db.conn.QueryRow(query, symbol).Scan(
		&p.ID, &p.Symbol, &p.Date, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume, &vwap, &p.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no price data found for %s", symbol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest price data: %w", err)
	}

	if vwap.Valid {
		p.VWAP, _ = decimal.NewFromString(vwap.String)
	}
	return &p, nil
}

// DeletePriceData removes price data by ID
func (db *DB) DeletePriceData(id int) error {
	query := `DELETE FROM price_data_daily WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete price data: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("price data not found: %d", id)
	}
	return nil
}

// DeletePriceDataBySymbol removes all price data for a symbol
func (db *DB) DeletePriceDataBySymbol(symbol string) error {
	query := `DELETE FROM price_data_daily WHERE symbol = $1`
	_, err := db.conn.Exec(query, symbol)
	if err != nil {
		return fmt.Errorf("failed to delete price data for %s: %w", symbol, err)
	}
	return nil
}

// DeletePriceDataOlderThan removes price data older than a specified date
func (db *DB) DeletePriceDataOlderThan(date time.Time) (int64, error) {
	query := `DELETE FROM price_data_daily WHERE date < $1`
	result, err := db.conn.Exec(query, date)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old price data: %w", err)
	}
	return result.RowsAffected()
}
