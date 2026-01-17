package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreatePosition inserts a new position into the database
func (db *DB) CreatePosition(p *models.Position) error {
	query := `
		INSERT INTO positions (
			symbol, quantity, entry_price, entry_date, current_price,
			unrealized_pnl_pct, days_held, entry_rsi, entry_reason,
			sector, industry, position_size_pct, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		RETURNING id
	`
	now := time.Now()
	err := db.conn.QueryRow(query,
		p.Symbol, p.Quantity, p.EntryPrice, p.EntryDate, p.CurrentPrice,
		p.UnrealizedPnlPct, p.DaysHeld, p.EntryRSI, p.EntryReason,
		p.Sector, p.Industry, p.PositionSizePct, now, now,
	).Scan(&p.ID)

	if err != nil {
		return fmt.Errorf("failed to create position: %w", err)
	}
	p.CreatedAt = now
	p.UpdatedAt = now
	return nil
}

// GetPositionByID retrieves a position by its ID
func (db *DB) GetPositionByID(id int) (*models.Position, error) {
	query := `
		SELECT id, symbol, quantity, entry_price, entry_date, current_price,
		       unrealized_pnl_pct, days_held, entry_rsi, entry_reason,
		       sector, industry, position_size_pct, created_at, updated_at
		FROM positions
		WHERE id = $1
	`
	var p models.Position
	var currentPrice, unrealizedPnlPct, entryRSI, positionSizePct sql.NullString
	var daysHeld sql.NullInt64
	var entryReason, sector, industry sql.NullString

	err := db.conn.QueryRow(query, id).Scan(
		&p.ID, &p.Symbol, &p.Quantity, &p.EntryPrice, &p.EntryDate, &currentPrice,
		&unrealizedPnlPct, &daysHeld, &entryRSI, &entryReason,
		&sector, &industry, &positionSizePct, &p.CreatedAt, &p.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("position not found: %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get position: %w", err)
	}

	if currentPrice.Valid {
		p.CurrentPrice, _ = decimal.NewFromString(currentPrice.String)
	}
	if unrealizedPnlPct.Valid {
		p.UnrealizedPnlPct, _ = decimal.NewFromString(unrealizedPnlPct.String)
	}
	if daysHeld.Valid {
		p.DaysHeld = int(daysHeld.Int64)
	}
	if entryRSI.Valid {
		p.EntryRSI, _ = decimal.NewFromString(entryRSI.String)
	}
	if entryReason.Valid {
		p.EntryReason = entryReason.String
	}
	if sector.Valid {
		p.Sector = sector.String
	}
	if industry.Valid {
		p.Industry = industry.String
	}
	if positionSizePct.Valid {
		p.PositionSizePct, _ = decimal.NewFromString(positionSizePct.String)
	}

	return &p, nil
}

// GetPositionBySymbol retrieves a position by symbol
func (db *DB) GetPositionBySymbol(symbol string) (*models.Position, error) {
	query := `
		SELECT id, symbol, quantity, entry_price, entry_date, current_price,
		       unrealized_pnl_pct, days_held, entry_rsi, entry_reason,
		       sector, industry, position_size_pct, created_at, updated_at
		FROM positions
		WHERE symbol = $1
	`
	var p models.Position
	var currentPrice, unrealizedPnlPct, entryRSI, positionSizePct sql.NullString
	var daysHeld sql.NullInt64
	var entryReason, sector, industry sql.NullString

	err := db.conn.QueryRow(query, symbol).Scan(
		&p.ID, &p.Symbol, &p.Quantity, &p.EntryPrice, &p.EntryDate, &currentPrice,
		&unrealizedPnlPct, &daysHeld, &entryRSI, &entryReason,
		&sector, &industry, &positionSizePct, &p.CreatedAt, &p.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("position not found for symbol: %s", symbol)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get position: %w", err)
	}

	if currentPrice.Valid {
		p.CurrentPrice, _ = decimal.NewFromString(currentPrice.String)
	}
	if unrealizedPnlPct.Valid {
		p.UnrealizedPnlPct, _ = decimal.NewFromString(unrealizedPnlPct.String)
	}
	if daysHeld.Valid {
		p.DaysHeld = int(daysHeld.Int64)
	}
	if entryRSI.Valid {
		p.EntryRSI, _ = decimal.NewFromString(entryRSI.String)
	}
	if entryReason.Valid {
		p.EntryReason = entryReason.String
	}
	if sector.Valid {
		p.Sector = sector.String
	}
	if industry.Valid {
		p.Industry = industry.String
	}
	if positionSizePct.Valid {
		p.PositionSizePct, _ = decimal.NewFromString(positionSizePct.String)
	}

	return &p, nil
}

// GetAllPositions retrieves all positions
func (db *DB) GetAllPositions() ([]*models.Position, error) {
	query := `
		SELECT id, symbol, quantity, entry_price, entry_date, current_price,
		       unrealized_pnl_pct, days_held, entry_rsi, entry_reason,
		       sector, industry, position_size_pct, created_at, updated_at
		FROM positions
		ORDER BY entry_date DESC
	`
	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}
	defer rows.Close()

	var positions []*models.Position
	for rows.Next() {
		var p models.Position
		var currentPrice, unrealizedPnlPct, entryRSI, positionSizePct sql.NullString
		var daysHeld sql.NullInt64
		var entryReason, sector, industry sql.NullString

		err := rows.Scan(
			&p.ID, &p.Symbol, &p.Quantity, &p.EntryPrice, &p.EntryDate, &currentPrice,
			&unrealizedPnlPct, &daysHeld, &entryRSI, &entryReason,
			&sector, &industry, &positionSizePct, &p.CreatedAt, &p.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan position: %w", err)
		}

		if currentPrice.Valid {
			p.CurrentPrice, _ = decimal.NewFromString(currentPrice.String)
		}
		if unrealizedPnlPct.Valid {
			p.UnrealizedPnlPct, _ = decimal.NewFromString(unrealizedPnlPct.String)
		}
		if daysHeld.Valid {
			p.DaysHeld = int(daysHeld.Int64)
		}
		if entryRSI.Valid {
			p.EntryRSI, _ = decimal.NewFromString(entryRSI.String)
		}
		if entryReason.Valid {
			p.EntryReason = entryReason.String
		}
		if sector.Valid {
			p.Sector = sector.String
		}
		if industry.Valid {
			p.Industry = industry.String
		}
		if positionSizePct.Valid {
			p.PositionSizePct, _ = decimal.NewFromString(positionSizePct.String)
		}

		positions = append(positions, &p)
	}

	return positions, nil
}

// UpdatePosition updates an existing position
func (db *DB) UpdatePosition(p *models.Position) error {
	query := `
		UPDATE positions SET
			quantity = $2, entry_price = $3, entry_date = $4, current_price = $5,
			unrealized_pnl_pct = $6, days_held = $7, entry_rsi = $8, entry_reason = $9,
			sector = $10, industry = $11, position_size_pct = $12, updated_at = $13
		WHERE id = $1
	`
	p.UpdatedAt = time.Now()
	result, err := db.conn.Exec(query,
		p.ID, p.Quantity, p.EntryPrice, p.EntryDate, p.CurrentPrice,
		p.UnrealizedPnlPct, p.DaysHeld, p.EntryRSI, p.EntryReason,
		p.Sector, p.Industry, p.PositionSizePct, p.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update position: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("position not found: %d", p.ID)
	}
	return nil
}

// DeletePosition removes a position by ID
func (db *DB) DeletePosition(id int) error {
	query := `DELETE FROM positions WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete position: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("position not found: %d", id)
	}
	return nil
}

// DeletePositionBySymbol removes a position by symbol
func (db *DB) DeletePositionBySymbol(symbol string) error {
	query := `DELETE FROM positions WHERE symbol = $1`
	result, err := db.conn.Exec(query, symbol)
	if err != nil {
		return fmt.Errorf("failed to delete position: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("position not found for symbol: %s", symbol)
	}
	return nil
}
