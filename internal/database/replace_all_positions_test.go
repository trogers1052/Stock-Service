package database

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

func TestReplaceAllPositions_Success(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := &DB{conn: sqlDB}

	entryDate := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	positions := []*models.Position{
		{
			Symbol:           "AAPL",
			Quantity:         decimal.NewFromFloat(1),
			EntryPrice:       decimal.NewFromFloat(100),
			EntryDate:        entryDate,
			CurrentPrice:     decimal.NewFromFloat(110),
			UnrealizedPnlPct: decimal.NewFromFloat(10),
			DaysHeld:         1,
		},
		{
			Symbol:           "MSFT",
			Quantity:         decimal.NewFromFloat(2),
			EntryPrice:       decimal.NewFromFloat(200),
			EntryDate:        entryDate,
			CurrentPrice:     decimal.NewFromFloat(180),
			UnrealizedPnlPct: decimal.NewFromFloat(-10),
			DaysHeld:         1,
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM positions").WillReturnResult(sqlmock.NewResult(0, 2))

	// Two inserts, one for each position.
	mock.ExpectQuery("INSERT INTO positions").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(101))
	mock.ExpectQuery("INSERT INTO positions").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(102))

	mock.ExpectCommit()
	// ReplaceAllPositions defers tx.Rollback(), but database/sql short-circuits Rollback after Commit,
	// so the underlying driver rollback is not executed (and sqlmock won't observe it).

	err = db.ReplaceAllPositions(positions)
	require.NoError(t, err)

	assert.Equal(t, 101, positions[0].ID)
	assert.Equal(t, 102, positions[1].ID)
	assert.False(t, positions[0].CreatedAt.IsZero())
	assert.False(t, positions[0].UpdatedAt.IsZero())
	assert.False(t, positions[1].CreatedAt.IsZero())
	assert.False(t, positions[1].UpdatedAt.IsZero())

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceAllPositions_ReturnsErrorIfBeginFails(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := &DB{conn: sqlDB}

	beginErr := errors.New("begin failed")
	mock.ExpectBegin().WillReturnError(beginErr)

	err = db.ReplaceAllPositions([]*models.Position{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin transaction")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceAllPositions_ReturnsErrorIfDeleteFails(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	db := &DB{conn: sqlDB}

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM positions").WillReturnError(errors.New("delete failed"))
	mock.ExpectRollback()

	err = db.ReplaceAllPositions([]*models.Position{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete existing positions")

	require.NoError(t, mock.ExpectationsWereMet())
}
