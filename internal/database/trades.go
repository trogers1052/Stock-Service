package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreateTradeHistory inserts a new trade record
func (db *DB) CreateTradeHistory(t *models.TradeHistory) error {
	query := `
		INSERT INTO trades_history (
			symbol, trade_type, quantity, price, total_cost, fee,
			entry_date, exit_date, holding_period_hours,
			entry_rsi, exit_rsi, realized_pnl, realized_pnl_pct, max_drawdown_pct,
			entry_reason, exit_reason, emotional_state, conviction_level,
			market_conditions, what_went_right, what_went_wrong,
			trade_grade, strategy_tag, notes, executed_at, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
		)
		RETURNING id
	`
	now := time.Now()
	executedAt := t.ExecutedAt
	if executedAt.IsZero() {
		executedAt = now
	}

	err := db.conn.QueryRow(query,
		t.Symbol, t.TradeType, t.Quantity, t.Price, t.TotalCost, t.Fee,
		t.EntryDate, t.ExitDate, t.HoldingPeriodHours,
		t.EntryRSI, t.ExitRSI, t.RealizedPnl, t.RealizedPnlPct, t.MaxDrawdownPct,
		t.EntryReason, t.ExitReason, t.EmotionalState, t.ConvictionLevel,
		t.MarketConditions, t.WhatWentRight, t.WhatWentWrong,
		t.TradeGrade, t.StrategyTag, t.Notes, executedAt, now,
	).Scan(&t.ID)

	if err != nil {
		return fmt.Errorf("failed to create trade history: %w", err)
	}
	t.ExecutedAt = executedAt
	t.CreatedAt = now
	return nil
}

// GetTradeHistoryByID retrieves a trade record by ID
func (db *DB) GetTradeHistoryByID(id int) (*models.TradeHistory, error) {
	query := `
		SELECT id, symbol, trade_type, quantity, price, total_cost, fee,
		       entry_date, exit_date, holding_period_hours,
		       entry_rsi, exit_rsi, realized_pnl, realized_pnl_pct, max_drawdown_pct,
		       entry_reason, exit_reason, emotional_state, conviction_level,
		       market_conditions, what_went_right, what_went_wrong,
		       trade_grade, strategy_tag, notes, executed_at, created_at
		FROM trades_history
		WHERE id = $1
	`
	return db.scanSingleTrade(db.conn.QueryRow(query, id))
}

func (db *DB) scanSingleTrade(row *sql.Row) (*models.TradeHistory, error) {
	var t models.TradeHistory
	var entryDate, exitDate sql.NullTime
	var holdingPeriodHours sql.NullInt64
	var entryRSI, exitRSI, realizedPnl, realizedPnlPct, maxDrawdownPct, fee sql.NullString
	var entryReason, exitReason, marketConditions, whatWentRight, whatWentWrong sql.NullString
	var emotionalState, convictionLevel sql.NullInt64
	var tradeGrade, strategyTag, notes sql.NullString

	err := row.Scan(
		&t.ID, &t.Symbol, &t.TradeType, &t.Quantity, &t.Price, &t.TotalCost, &fee,
		&entryDate, &exitDate, &holdingPeriodHours,
		&entryRSI, &exitRSI, &realizedPnl, &realizedPnlPct, &maxDrawdownPct,
		&entryReason, &exitReason, &emotionalState, &convictionLevel,
		&marketConditions, &whatWentRight, &whatWentWrong,
		&tradeGrade, &strategyTag, &notes, &t.ExecutedAt, &t.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("trade not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get trade: %w", err)
	}

	if fee.Valid {
		t.Fee, _ = decimal.NewFromString(fee.String)
	}
	if entryDate.Valid {
		t.EntryDate = &entryDate.Time
	}
	if exitDate.Valid {
		t.ExitDate = &exitDate.Time
	}
	if holdingPeriodHours.Valid {
		hours := int(holdingPeriodHours.Int64)
		t.HoldingPeriodHours = &hours
	}
	if entryRSI.Valid {
		t.EntryRSI, _ = decimal.NewFromString(entryRSI.String)
	}
	if exitRSI.Valid {
		t.ExitRSI, _ = decimal.NewFromString(exitRSI.String)
	}
	if realizedPnl.Valid {
		t.RealizedPnl, _ = decimal.NewFromString(realizedPnl.String)
	}
	if realizedPnlPct.Valid {
		t.RealizedPnlPct, _ = decimal.NewFromString(realizedPnlPct.String)
	}
	if maxDrawdownPct.Valid {
		t.MaxDrawdownPct, _ = decimal.NewFromString(maxDrawdownPct.String)
	}
	if entryReason.Valid {
		t.EntryReason = entryReason.String
	}
	if exitReason.Valid {
		t.ExitReason = exitReason.String
	}
	if emotionalState.Valid {
		state := int(emotionalState.Int64)
		t.EmotionalState = &state
	}
	if convictionLevel.Valid {
		level := int(convictionLevel.Int64)
		t.ConvictionLevel = &level
	}
	if marketConditions.Valid {
		t.MarketConditions = marketConditions.String
	}
	if whatWentRight.Valid {
		t.WhatWentRight = whatWentRight.String
	}
	if whatWentWrong.Valid {
		t.WhatWentWrong = whatWentWrong.String
	}
	if tradeGrade.Valid {
		t.TradeGrade = tradeGrade.String
	}
	if strategyTag.Valid {
		t.StrategyTag = strategyTag.String
	}
	if notes.Valid {
		t.Notes = notes.String
	}

	return &t, nil
}

// GetTradeHistoryBySymbol retrieves trade history for a symbol
func (db *DB) GetTradeHistoryBySymbol(symbol string, limit int) ([]*models.TradeHistory, error) {
	query := `
		SELECT id, symbol, trade_type, quantity, price, total_cost, fee,
		       entry_date, exit_date, holding_period_hours,
		       entry_rsi, exit_rsi, realized_pnl, realized_pnl_pct, max_drawdown_pct,
		       entry_reason, exit_reason, emotional_state, conviction_level,
		       market_conditions, what_went_right, what_went_wrong,
		       trade_grade, strategy_tag, notes, executed_at, created_at
		FROM trades_history
		WHERE symbol = $1
		ORDER BY executed_at DESC
		LIMIT $2
	`
	return db.scanTrades(db.conn.Query(query, symbol, limit))
}

// GetAllTradeHistory retrieves all trade history with optional limit
func (db *DB) GetAllTradeHistory(limit int) ([]*models.TradeHistory, error) {
	query := `
		SELECT id, symbol, trade_type, quantity, price, total_cost, fee,
		       entry_date, exit_date, holding_period_hours,
		       entry_rsi, exit_rsi, realized_pnl, realized_pnl_pct, max_drawdown_pct,
		       entry_reason, exit_reason, emotional_state, conviction_level,
		       market_conditions, what_went_right, what_went_wrong,
		       trade_grade, strategy_tag, notes, executed_at, created_at
		FROM trades_history
		ORDER BY executed_at DESC
		LIMIT $1
	`
	return db.scanTrades(db.conn.Query(query, limit))
}

// GetTradeHistoryByDateRange retrieves trades within a date range
func (db *DB) GetTradeHistoryByDateRange(startDate, endDate time.Time) ([]*models.TradeHistory, error) {
	query := `
		SELECT id, symbol, trade_type, quantity, price, total_cost, fee,
		       entry_date, exit_date, holding_period_hours,
		       entry_rsi, exit_rsi, realized_pnl, realized_pnl_pct, max_drawdown_pct,
		       entry_reason, exit_reason, emotional_state, conviction_level,
		       market_conditions, what_went_right, what_went_wrong,
		       trade_grade, strategy_tag, notes, executed_at, created_at
		FROM trades_history
		WHERE executed_at >= $1 AND executed_at <= $2
		ORDER BY executed_at DESC
	`
	return db.scanTrades(db.conn.Query(query, startDate, endDate))
}

// GetTradeHistoryByStrategy retrieves trades with a specific strategy tag
func (db *DB) GetTradeHistoryByStrategy(strategyTag string, limit int) ([]*models.TradeHistory, error) {
	query := `
		SELECT id, symbol, trade_type, quantity, price, total_cost, fee,
		       entry_date, exit_date, holding_period_hours,
		       entry_rsi, exit_rsi, realized_pnl, realized_pnl_pct, max_drawdown_pct,
		       entry_reason, exit_reason, emotional_state, conviction_level,
		       market_conditions, what_went_right, what_went_wrong,
		       trade_grade, strategy_tag, notes, executed_at, created_at
		FROM trades_history
		WHERE strategy_tag = $1
		ORDER BY executed_at DESC
		LIMIT $2
	`
	return db.scanTrades(db.conn.Query(query, strategyTag, limit))
}

func (db *DB) scanTrades(rows *sql.Rows, err error) ([]*models.TradeHistory, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to query trades: %w", err)
	}
	defer rows.Close()

	var trades []*models.TradeHistory
	for rows.Next() {
		var t models.TradeHistory
		var entryDate, exitDate sql.NullTime
		var holdingPeriodHours sql.NullInt64
		var entryRSI, exitRSI, realizedPnl, realizedPnlPct, maxDrawdownPct, fee sql.NullString
		var entryReason, exitReason, marketConditions, whatWentRight, whatWentWrong sql.NullString
		var emotionalState, convictionLevel sql.NullInt64
		var tradeGrade, strategyTag, notes sql.NullString

		err := rows.Scan(
			&t.ID, &t.Symbol, &t.TradeType, &t.Quantity, &t.Price, &t.TotalCost, &fee,
			&entryDate, &exitDate, &holdingPeriodHours,
			&entryRSI, &exitRSI, &realizedPnl, &realizedPnlPct, &maxDrawdownPct,
			&entryReason, &exitReason, &emotionalState, &convictionLevel,
			&marketConditions, &whatWentRight, &whatWentWrong,
			&tradeGrade, &strategyTag, &notes, &t.ExecutedAt, &t.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan trade: %w", err)
		}

		if fee.Valid {
			t.Fee, _ = decimal.NewFromString(fee.String)
		}
		if entryDate.Valid {
			t.EntryDate = &entryDate.Time
		}
		if exitDate.Valid {
			t.ExitDate = &exitDate.Time
		}
		if holdingPeriodHours.Valid {
			hours := int(holdingPeriodHours.Int64)
			t.HoldingPeriodHours = &hours
		}
		if entryRSI.Valid {
			t.EntryRSI, _ = decimal.NewFromString(entryRSI.String)
		}
		if exitRSI.Valid {
			t.ExitRSI, _ = decimal.NewFromString(exitRSI.String)
		}
		if realizedPnl.Valid {
			t.RealizedPnl, _ = decimal.NewFromString(realizedPnl.String)
		}
		if realizedPnlPct.Valid {
			t.RealizedPnlPct, _ = decimal.NewFromString(realizedPnlPct.String)
		}
		if maxDrawdownPct.Valid {
			t.MaxDrawdownPct, _ = decimal.NewFromString(maxDrawdownPct.String)
		}
		if entryReason.Valid {
			t.EntryReason = entryReason.String
		}
		if exitReason.Valid {
			t.ExitReason = exitReason.String
		}
		if emotionalState.Valid {
			state := int(emotionalState.Int64)
			t.EmotionalState = &state
		}
		if convictionLevel.Valid {
			level := int(convictionLevel.Int64)
			t.ConvictionLevel = &level
		}
		if marketConditions.Valid {
			t.MarketConditions = marketConditions.String
		}
		if whatWentRight.Valid {
			t.WhatWentRight = whatWentRight.String
		}
		if whatWentWrong.Valid {
			t.WhatWentWrong = whatWentWrong.String
		}
		if tradeGrade.Valid {
			t.TradeGrade = tradeGrade.String
		}
		if strategyTag.Valid {
			t.StrategyTag = strategyTag.String
		}
		if notes.Valid {
			t.Notes = notes.String
		}

		trades = append(trades, &t)
	}

	return trades, nil
}

// UpdateTradeHistory updates an existing trade record
func (db *DB) UpdateTradeHistory(t *models.TradeHistory) error {
	query := `
		UPDATE trades_history SET
			symbol = $2, trade_type = $3, quantity = $4, price = $5, total_cost = $6, fee = $7,
			entry_date = $8, exit_date = $9, holding_period_hours = $10,
			entry_rsi = $11, exit_rsi = $12, realized_pnl = $13, realized_pnl_pct = $14, max_drawdown_pct = $15,
			entry_reason = $16, exit_reason = $17, emotional_state = $18, conviction_level = $19,
			market_conditions = $20, what_went_right = $21, what_went_wrong = $22,
			trade_grade = $23, strategy_tag = $24, notes = $25, executed_at = $26
		WHERE id = $1
	`
	result, err := db.conn.Exec(query,
		t.ID, t.Symbol, t.TradeType, t.Quantity, t.Price, t.TotalCost, t.Fee,
		t.EntryDate, t.ExitDate, t.HoldingPeriodHours,
		t.EntryRSI, t.ExitRSI, t.RealizedPnl, t.RealizedPnlPct, t.MaxDrawdownPct,
		t.EntryReason, t.ExitReason, t.EmotionalState, t.ConvictionLevel,
		t.MarketConditions, t.WhatWentRight, t.WhatWentWrong,
		t.TradeGrade, t.StrategyTag, t.Notes, t.ExecutedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update trade: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("trade not found: %d", t.ID)
	}
	return nil
}

// DeleteTradeHistory removes a trade record by ID
func (db *DB) DeleteTradeHistory(id int) error {
	query := `DELETE FROM trades_history WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete trade: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("trade not found: %d", id)
	}
	return nil
}

// GetTradeStats returns aggregated trade statistics
type TradeStats struct {
	TotalTrades   int             `json:"total_trades"`
	WinningTrades int             `json:"winning_trades"`
	LosingTrades  int             `json:"losing_trades"`
	WinRate       decimal.Decimal `json:"win_rate"`
	TotalPnl      decimal.Decimal `json:"total_pnl"`
	AvgPnlPct     decimal.Decimal `json:"avg_pnl_pct"`
	AvgWin        decimal.Decimal `json:"avg_win"`
	AvgLoss       decimal.Decimal `json:"avg_loss"`
}

func (db *DB) GetTradeStats() (*TradeStats, error) {
	query := `
		SELECT
			COUNT(*) as total_trades,
			COUNT(*) FILTER (WHERE realized_pnl > 0) as winning_trades,
			COUNT(*) FILTER (WHERE realized_pnl < 0) as losing_trades,
			COALESCE(SUM(realized_pnl), 0) as total_pnl,
			COALESCE(AVG(realized_pnl_pct), 0) as avg_pnl_pct,
			COALESCE(AVG(realized_pnl) FILTER (WHERE realized_pnl > 0), 0) as avg_win,
			COALESCE(AVG(realized_pnl) FILTER (WHERE realized_pnl < 0), 0) as avg_loss
		FROM trades_history
		WHERE trade_type = 'SELL' AND realized_pnl IS NOT NULL
	`
	var stats TradeStats
	err := db.conn.QueryRow(query).Scan(
		&stats.TotalTrades, &stats.WinningTrades, &stats.LosingTrades,
		&stats.TotalPnl, &stats.AvgPnlPct, &stats.AvgWin, &stats.AvgLoss,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get trade stats: %w", err)
	}

	if stats.TotalTrades > 0 {
		stats.WinRate = decimal.NewFromInt(int64(stats.WinningTrades)).
			Div(decimal.NewFromInt(int64(stats.TotalTrades))).
			Mul(decimal.NewFromInt(100))
	}

	return &stats, nil
}
