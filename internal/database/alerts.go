package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// CreateAlertRule inserts a new alert rule
func (db *DB) CreateAlertRule(a *models.AlertRule) error {
	query := `
		INSERT INTO alert_rules (
			symbol, rule_type, condition_value, comparison, enabled,
			cooldown_minutes, notification_channel, message_template, priority,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id
	`
	now := time.Now()
	err := db.conn.QueryRow(query,
		a.Symbol, a.RuleType, a.ConditionValue, a.Comparison, a.Enabled,
		a.CooldownMinutes, a.NotificationChannel, a.MessageTemplate, a.Priority,
		now, now,
	).Scan(&a.ID)

	if err != nil {
		return fmt.Errorf("failed to create alert rule: %w", err)
	}
	a.CreatedAt = now
	a.UpdatedAt = now
	return nil
}

// GetAlertRuleByID retrieves an alert rule by ID
func (db *DB) GetAlertRuleByID(id int) (*models.AlertRule, error) {
	query := `
		SELECT id, symbol, rule_type, condition_value, comparison, enabled,
		       triggered_count, last_triggered_at, cooldown_minutes,
		       notification_channel, message_template, priority, created_at, updated_at
		FROM alert_rules
		WHERE id = $1
	`
	var a models.AlertRule
	var conditionValue sql.NullString
	var lastTriggeredAt sql.NullTime
	var messageTemplate sql.NullString

	err := db.conn.QueryRow(query, id).Scan(
		&a.ID, &a.Symbol, &a.RuleType, &conditionValue, &a.Comparison, &a.Enabled,
		&a.TriggeredCount, &lastTriggeredAt, &a.CooldownMinutes,
		&a.NotificationChannel, &messageTemplate, &a.Priority, &a.CreatedAt, &a.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("alert rule not found: %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get alert rule: %w", err)
	}

	if conditionValue.Valid {
		a.ConditionValue, _ = decimal.NewFromString(conditionValue.String)
	}
	if lastTriggeredAt.Valid {
		a.LastTriggeredAt = &lastTriggeredAt.Time
	}
	if messageTemplate.Valid {
		a.MessageTemplate = messageTemplate.String
	}

	return &a, nil
}

// GetAlertRulesBySymbol retrieves all alert rules for a symbol
func (db *DB) GetAlertRulesBySymbol(symbol string) ([]*models.AlertRule, error) {
	query := `
		SELECT id, symbol, rule_type, condition_value, comparison, enabled,
		       triggered_count, last_triggered_at, cooldown_minutes,
		       notification_channel, message_template, priority, created_at, updated_at
		FROM alert_rules
		WHERE symbol = $1
		ORDER BY priority DESC, created_at DESC
	`
	return db.scanAlertRules(db.conn.Query(query, symbol))
}

// GetEnabledAlertRules retrieves all enabled alert rules
func (db *DB) GetEnabledAlertRules() ([]*models.AlertRule, error) {
	query := `
		SELECT id, symbol, rule_type, condition_value, comparison, enabled,
		       triggered_count, last_triggered_at, cooldown_minutes,
		       notification_channel, message_template, priority, created_at, updated_at
		FROM alert_rules
		WHERE enabled = true
		ORDER BY symbol, rule_type
	`
	return db.scanAlertRules(db.conn.Query(query))
}

// GetEnabledAlertRulesBySymbol retrieves enabled alert rules for a specific symbol
func (db *DB) GetEnabledAlertRulesBySymbol(symbol string) ([]*models.AlertRule, error) {
	query := `
		SELECT id, symbol, rule_type, condition_value, comparison, enabled,
		       triggered_count, last_triggered_at, cooldown_minutes,
		       notification_channel, message_template, priority, created_at, updated_at
		FROM alert_rules
		WHERE symbol = $1 AND enabled = true
		ORDER BY rule_type
	`
	return db.scanAlertRules(db.conn.Query(query, symbol))
}

func (db *DB) scanAlertRules(rows *sql.Rows, err error) ([]*models.AlertRule, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to query alert rules: %w", err)
	}
	defer rows.Close()

	var rules []*models.AlertRule
	for rows.Next() {
		var a models.AlertRule
		var conditionValue sql.NullString
		var lastTriggeredAt sql.NullTime
		var messageTemplate sql.NullString

		err := rows.Scan(
			&a.ID, &a.Symbol, &a.RuleType, &conditionValue, &a.Comparison, &a.Enabled,
			&a.TriggeredCount, &lastTriggeredAt, &a.CooldownMinutes,
			&a.NotificationChannel, &messageTemplate, &a.Priority, &a.CreatedAt, &a.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan alert rule: %w", err)
		}

		if conditionValue.Valid {
			a.ConditionValue, _ = decimal.NewFromString(conditionValue.String)
		}
		if lastTriggeredAt.Valid {
			a.LastTriggeredAt = &lastTriggeredAt.Time
		}
		if messageTemplate.Valid {
			a.MessageTemplate = messageTemplate.String
		}

		rules = append(rules, &a)
	}

	return rules, nil
}

// UpdateAlertRule updates an existing alert rule
func (db *DB) UpdateAlertRule(a *models.AlertRule) error {
	query := `
		UPDATE alert_rules SET
			rule_type = $2, condition_value = $3, comparison = $4, enabled = $5,
			cooldown_minutes = $6, notification_channel = $7, message_template = $8,
			priority = $9, updated_at = $10
		WHERE id = $1
	`
	a.UpdatedAt = time.Now()
	result, err := db.conn.Exec(query,
		a.ID, a.RuleType, a.ConditionValue, a.Comparison, a.Enabled,
		a.CooldownMinutes, a.NotificationChannel, a.MessageTemplate,
		a.Priority, a.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update alert rule: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("alert rule not found: %d", a.ID)
	}
	return nil
}

// MarkAlertTriggered updates the triggered count and timestamp for an alert rule
func (db *DB) MarkAlertTriggered(id int) error {
	query := `
		UPDATE alert_rules SET
			triggered_count = triggered_count + 1,
			last_triggered_at = $2,
			updated_at = $2
		WHERE id = $1
	`
	now := time.Now()
	_, err := db.conn.Exec(query, id, now)
	if err != nil {
		return fmt.Errorf("failed to mark alert triggered: %w", err)
	}
	return nil
}

// DeleteAlertRule removes an alert rule by ID
func (db *DB) DeleteAlertRule(id int) error {
	query := `DELETE FROM alert_rules WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete alert rule: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("alert rule not found: %d", id)
	}
	return nil
}

// DeleteAlertRulesBySymbol removes all alert rules for a symbol
func (db *DB) DeleteAlertRulesBySymbol(symbol string) error {
	query := `DELETE FROM alert_rules WHERE symbol = $1`
	_, err := db.conn.Exec(query, symbol)
	if err != nil {
		return fmt.Errorf("failed to delete alert rules for %s: %w", symbol, err)
	}
	return nil
}

// --- Alert History ---

// CreateAlertHistory records a triggered alert
func (db *DB) CreateAlertHistory(h *models.AlertHistory) error {
	query := `
		INSERT INTO alert_history (
			alert_rule_id, symbol, rule_type, triggered_value,
			message, notification_sent, notification_channel, triggered_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`
	var alertRuleID interface{}
	if h.AlertRuleID > 0 {
		alertRuleID = h.AlertRuleID
	}

	err := db.conn.QueryRow(query,
		alertRuleID, h.Symbol, h.RuleType, h.TriggeredValue,
		h.Message, h.NotificationSent, h.NotificationChannel, time.Now(),
	).Scan(&h.ID)

	if err != nil {
		return fmt.Errorf("failed to create alert history: %w", err)
	}
	return nil
}

// GetAlertHistoryByID retrieves an alert history record by ID
func (db *DB) GetAlertHistoryByID(id int) (*models.AlertHistory, error) {
	query := `
		SELECT id, alert_rule_id, symbol, rule_type, triggered_value,
		       message, notification_sent, notification_channel, triggered_at
		FROM alert_history
		WHERE id = $1
	`
	var h models.AlertHistory
	var alertRuleID sql.NullInt64
	var triggeredValue sql.NullString
	var message, notificationChannel sql.NullString

	err := db.conn.QueryRow(query, id).Scan(
		&h.ID, &alertRuleID, &h.Symbol, &h.RuleType, &triggeredValue,
		&message, &h.NotificationSent, &notificationChannel, &h.TriggeredAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("alert history not found: %d", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get alert history: %w", err)
	}

	if alertRuleID.Valid {
		h.AlertRuleID = int(alertRuleID.Int64)
	}
	if triggeredValue.Valid {
		h.TriggeredValue, _ = decimal.NewFromString(triggeredValue.String)
	}
	if message.Valid {
		h.Message = message.String
	}
	if notificationChannel.Valid {
		h.NotificationChannel = notificationChannel.String
	}

	return &h, nil
}

// GetAlertHistoryBySymbol retrieves alert history for a symbol
func (db *DB) GetAlertHistoryBySymbol(symbol string, limit int) ([]*models.AlertHistory, error) {
	query := `
		SELECT id, alert_rule_id, symbol, rule_type, triggered_value,
		       message, notification_sent, notification_channel, triggered_at
		FROM alert_history
		WHERE symbol = $1
		ORDER BY triggered_at DESC
		LIMIT $2
	`
	return db.scanAlertHistory(db.conn.Query(query, symbol, limit))
}

// GetRecentAlertHistory retrieves recent alert history across all symbols
func (db *DB) GetRecentAlertHistory(limit int) ([]*models.AlertHistory, error) {
	query := `
		SELECT id, alert_rule_id, symbol, rule_type, triggered_value,
		       message, notification_sent, notification_channel, triggered_at
		FROM alert_history
		ORDER BY triggered_at DESC
		LIMIT $1
	`
	return db.scanAlertHistory(db.conn.Query(query, limit))
}

func (db *DB) scanAlertHistory(rows *sql.Rows, err error) ([]*models.AlertHistory, error) {
	if err != nil {
		return nil, fmt.Errorf("failed to query alert history: %w", err)
	}
	defer rows.Close()

	var history []*models.AlertHistory
	for rows.Next() {
		var h models.AlertHistory
		var alertRuleID sql.NullInt64
		var triggeredValue sql.NullString
		var message, notificationChannel sql.NullString

		err := rows.Scan(
			&h.ID, &alertRuleID, &h.Symbol, &h.RuleType, &triggeredValue,
			&message, &h.NotificationSent, &notificationChannel, &h.TriggeredAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan alert history: %w", err)
		}

		if alertRuleID.Valid {
			h.AlertRuleID = int(alertRuleID.Int64)
		}
		if triggeredValue.Valid {
			h.TriggeredValue, _ = decimal.NewFromString(triggeredValue.String)
		}
		if message.Valid {
			h.Message = message.String
		}
		if notificationChannel.Valid {
			h.NotificationChannel = notificationChannel.String
		}

		history = append(history, &h)
	}

	return history, nil
}

// MarkNotificationSent updates an alert history record to indicate notification was sent
func (db *DB) MarkNotificationSent(id int) error {
	query := `UPDATE alert_history SET notification_sent = true WHERE id = $1`
	_, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to mark notification sent: %w", err)
	}
	return nil
}

// DeleteAlertHistory removes an alert history record by ID
func (db *DB) DeleteAlertHistory(id int) error {
	query := `DELETE FROM alert_history WHERE id = $1`
	result, err := db.conn.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete alert history: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("alert history not found: %d", id)
	}
	return nil
}

// DeleteAlertHistoryOlderThan removes alert history older than a specified date
func (db *DB) DeleteAlertHistoryOlderThan(date time.Time) (int64, error) {
	query := `DELETE FROM alert_history WHERE triggered_at < $1`
	result, err := db.conn.Exec(query, date)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old alert history: %w", err)
	}
	return result.RowsAffected()
}
