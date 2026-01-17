package database

import (
	"database/sql"
	"fmt"

	"github.com/trogers1052/stock-alert-system/internal/models"
)

// SaveStock inserts or updates a stock in the database
func (db *DB) SaveStock(stock *models.Stock) error {
	query := `
		INSERT INTO stocks (
			symbol, name, exchange, sector, industry,
			current_price, previous_close, change_amount, change_percent,
			day_high, day_low, volume, average_volume,
			week_52_high, week_52_low, market_cap, shares_outstanding,
			last_updated
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
		)
		ON CONFLICT (symbol)
		DO UPDATE SET
			name = EXCLUDED.name,
			exchange = EXCLUDED.exchange,
			sector = EXCLUDED.sector,
			industry = EXCLUDED.industry,
			current_price = EXCLUDED.current_price,
			previous_close = EXCLUDED.previous_close,
			change_amount = EXCLUDED.change_amount,
			change_percent = EXCLUDED.change_percent,
			day_high = EXCLUDED.day_high,
			day_low = EXCLUDED.day_low,
			volume = EXCLUDED.volume,
			average_volume = EXCLUDED.average_volume,
			week_52_high = EXCLUDED.week_52_high,
			week_52_low = EXCLUDED.week_52_low,
			market_cap = EXCLUDED.market_cap,
			shares_outstanding = EXCLUDED.shares_outstanding,
			last_updated = EXCLUDED.last_updated
		RETURNING id
	`

	err := db.conn.QueryRow(query,
		stock.Symbol, stock.Name, stock.Exchange, stock.Sector, stock.Industry,
		stock.CurrentPrice, stock.PreviousClose, stock.ChangeAmount, stock.ChangePercent,
		stock.DayHigh, stock.DayLow, stock.Volume, stock.AverageVolume,
		stock.Week52High, stock.Week52Low, stock.MarketCap, stock.SharesOutstanding,
		stock.LastUpdated,
	).Scan(&stock.ID)

	if err != nil {
		return fmt.Errorf("failed to save stock %s: %w", stock.Symbol, err)
	}

	return nil
}

// GetStock retrieves a stock by symbol
func (db *DB) GetStock(symbol string) (*models.Stock, error) {
	query := `
		SELECT id, symbol, name, exchange, sector, industry,
		       current_price, previous_close, change_amount, change_percent,
		       day_high, day_low, volume, average_volume,
		       week_52_high, week_52_low, market_cap, shares_outstanding,
		       last_updated, created_at
		FROM stocks
		WHERE symbol = $1
	`

	var stock models.Stock
	err := db.conn.QueryRow(query, symbol).Scan(
		&stock.ID, &stock.Symbol, &stock.Name, &stock.Exchange, &stock.Sector, &stock.Industry,
		&stock.CurrentPrice, &stock.PreviousClose, &stock.ChangeAmount, &stock.ChangePercent,
		&stock.DayHigh, &stock.DayLow, &stock.Volume, &stock.AverageVolume,
		&stock.Week52High, &stock.Week52Low, &stock.MarketCap, &stock.SharesOutstanding,
		&stock.LastUpdated, &stock.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("stock not found: %s", symbol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get stock %s: %w", symbol, err)
	}

	return &stock, nil
}

// GetStockByID retrieves a stock by UUID
func (db *DB) GetStockByID(id string) (*models.Stock, error) {
	query := `
		SELECT id, symbol, name, exchange, sector, industry,
		       current_price, previous_close, change_amount, change_percent,
		       day_high, day_low, volume, average_volume,
		       week_52_high, week_52_low, market_cap, shares_outstanding,
		       last_updated, created_at
		FROM stocks
		WHERE id = $1
	`

	var stock models.Stock
	err := db.conn.QueryRow(query, id).Scan(
		&stock.ID, &stock.Symbol, &stock.Name, &stock.Exchange, &stock.Sector, &stock.Industry,
		&stock.CurrentPrice, &stock.PreviousClose, &stock.ChangeAmount, &stock.ChangePercent,
		&stock.DayHigh, &stock.DayLow, &stock.Volume, &stock.AverageVolume,
		&stock.Week52High, &stock.Week52Low, &stock.MarketCap, &stock.SharesOutstanding,
		&stock.LastUpdated, &stock.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("stock not found with id: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get stock by id: %w", err)
	}

	return &stock, nil
}

// GetAllStocks returns all stocks in the database
func (db *DB) GetAllStocks() ([]*models.Stock, error) {
	query := `
		SELECT id, symbol, name, exchange, sector, industry,
		       current_price, previous_close, change_amount, change_percent,
		       day_high, day_low, volume, average_volume,
		       week_52_high, week_52_low, market_cap, shares_outstanding,
		       last_updated, created_at
		FROM stocks
		ORDER BY symbol
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all stocks: %w", err)
	}
	defer rows.Close()

	var stocks []*models.Stock
	for rows.Next() {
		var stock models.Stock
		err := rows.Scan(
			&stock.ID, &stock.Symbol, &stock.Name, &stock.Exchange, &stock.Sector, &stock.Industry,
			&stock.CurrentPrice, &stock.PreviousClose, &stock.ChangeAmount, &stock.ChangePercent,
			&stock.DayHigh, &stock.DayLow, &stock.Volume, &stock.AverageVolume,
			&stock.Week52High, &stock.Week52Low, &stock.MarketCap, &stock.SharesOutstanding,
			&stock.LastUpdated, &stock.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan stock: %w", err)
		}
		stocks = append(stocks, &stock)
	}

	return stocks, nil
}

// DeleteStock removes a stock by symbol
func (db *DB) DeleteStock(symbol string) error {
	query := `DELETE FROM stocks WHERE symbol = $1`
	result, err := db.conn.Exec(query, symbol)
	if err != nil {
		return fmt.Errorf("failed to delete stock %s: %w", symbol, err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("stock not found: %s", symbol)
	}
	return nil
}

// DeleteStockByID removes a stock by ID
func (db *DB) DeleteStockByID(id string) error {
	query := `DELETE FROM stocks WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete stock: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("stock not found with id: %s", id)
	}
	return nil
}

// GetStocksBySector retrieves all stocks in a specific sector
func (db *DB) GetStocksBySector(sector string) ([]*models.Stock, error) {
	query := `
		SELECT id, symbol, name, exchange, sector, industry,
		       current_price, previous_close, change_amount, change_percent,
		       day_high, day_low, volume, average_volume,
		       week_52_high, week_52_low, market_cap, shares_outstanding,
		       last_updated, created_at
		FROM stocks
		WHERE sector = $1
		ORDER BY symbol
	`

	rows, err := db.conn.Query(query, sector)
	if err != nil {
		return nil, fmt.Errorf("failed to get stocks by sector: %w", err)
	}
	defer rows.Close()

	var stocks []*models.Stock
	for rows.Next() {
		var stock models.Stock
		err := rows.Scan(
			&stock.ID, &stock.Symbol, &stock.Name, &stock.Exchange, &stock.Sector, &stock.Industry,
			&stock.CurrentPrice, &stock.PreviousClose, &stock.ChangeAmount, &stock.ChangePercent,
			&stock.DayHigh, &stock.DayLow, &stock.Volume, &stock.AverageVolume,
			&stock.Week52High, &stock.Week52Low, &stock.MarketCap, &stock.SharesOutstanding,
			&stock.LastUpdated, &stock.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan stock: %w", err)
		}
		stocks = append(stocks, &stock)
	}

	return stocks, nil
}
