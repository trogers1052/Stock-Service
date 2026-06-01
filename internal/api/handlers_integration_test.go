package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/trogers1052/stock-alert-system/internal/database"
	"github.com/trogers1052/stock-alert-system/internal/models"
)

// setupHandlerWithDB starts a Postgres container, runs migrations, and returns a
// handler wired to a real database (no Kafka producer, no Redis).
func setupHandlerWithDB(t *testing.T) (*Handler, *database.DB, func()) {
	t.Helper()
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx,
		"postgres:15-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	connStr, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := database.New(connStr)
	require.NoError(t, err)

	// Run migrations.
	driver, err := migratepg.WithInstance(db.Conn(), &migratepg.Config{})
	require.NoError(t, err)
	_, filename, _, _ := runtime.Caller(0)
	migrationsPath := filepath.Join(filepath.Dir(filename), "..", "..", "db", "migrations")
	m, err := migrate.NewWithDatabaseInstance("file://"+migrationsPath, "postgres", driver)
	require.NoError(t, err)
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		t.Fatalf("migrations failed: %v", err)
	}

	h := NewHandler(db, nil, nil)
	cleanup := func() {
		_ = db.Close()
		_ = pg.Terminate(ctx)
	}
	return h, db, cleanup
}

func TestHandlersWithDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	h, db, cleanup := setupHandlerWithDB(t)
	defer cleanup()

	truncate := func(tables ...string) {
		for _, tbl := range tables {
			_, err := db.Conn().Exec("TRUNCATE TABLE " + tbl + " RESTART IDENTITY CASCADE")
			require.NoError(t, err)
		}
	}

	t.Run("AddStock then GetStock then GetAllStocks", func(t *testing.T) {
		truncate("monitored_stocks", "stocks")
		// AddStock reads back the stock row after inserting the monitored entry,
		// so a fully-populated stocks row must exist first (GetStock cannot scan NULLs).
		require.NoError(t, db.SaveStock(&models.Stock{
			Symbol: "AAPL", Name: "Apple", Exchange: "NASDAQ", Sector: "Technology",
			Industry: "Hardware", CurrentPrice: 175, LastUpdated: time.Now(),
		}))

		// AddStock success.
		req := httptest.NewRequest("POST", "/api/v1/stocks", strings.NewReader(`{"symbol":"AAPL"}`))
		w := httptest.NewRecorder()
		h.AddStock(w, req)
		require.Equal(t, http.StatusCreated, w.Code)

		var created models.Stock
		require.NoError(t, json.NewDecoder(w.Body).Decode(&created))
		assert.Equal(t, "AAPL", created.Symbol)

		// GetStock success via router (needs mux vars).
		router := SetupRoutes(h, "")
		gw := httptest.NewRecorder()
		greq := httptest.NewRequest("GET", "/api/v1/stocks/AAPL", nil)
		router.ServeHTTP(gw, greq)
		require.Equal(t, http.StatusOK, gw.Code)

		// GetStock not found.
		nfw := httptest.NewRecorder()
		nfreq := httptest.NewRequest("GET", "/api/v1/stocks/ZZZZ", nil)
		router.ServeHTTP(nfw, nfreq)
		assert.Equal(t, http.StatusNotFound, nfw.Code)

		// GetAllStocks success.
		aw := httptest.NewRecorder()
		areq := httptest.NewRequest("GET", "/api/v1/stocks", nil)
		h.GetAllStocks(aw, areq)
		require.Equal(t, http.StatusOK, aw.Code)
		var all []*models.Stock
		require.NoError(t, json.NewDecoder(aw.Body).Decode(&all))
		assert.Len(t, all, 1)
	})

	t.Run("RemoveStock success", func(t *testing.T) {
		truncate("monitored_stocks", "stocks")
		require.NoError(t, db.SaveStock(&models.Stock{
			Symbol: "TSLA", Name: "Tesla", Exchange: "NASDAQ", Sector: "Auto",
			Industry: "EV", CurrentPrice: 250, LastUpdated: time.Now(),
		}))

		aw := httptest.NewRecorder()
		areq := httptest.NewRequest("POST", "/api/v1/stocks", strings.NewReader(`{"symbol":"TSLA"}`))
		h.AddStock(aw, areq)
		require.Equal(t, http.StatusCreated, aw.Code)

		router := SetupRoutes(h, "")
		dw := httptest.NewRecorder()
		dreq := httptest.NewRequest("DELETE", "/api/v1/stocks/TSLA", nil)
		router.ServeHTTP(dw, dreq)
		assert.Equal(t, http.StatusNoContent, dw.Code)
	})

	t.Run("GetSectors success", func(t *testing.T) {
		truncate("monitored_stocks", "stocks")
		require.NoError(t, db.UpsertStockWithSector("AAPL", "Apple", "Technology", "Hardware"))

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/stocks/sectors", nil)
		h.GetSectors(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		var m map[string]string
		require.NoError(t, json.NewDecoder(w.Body).Decode(&m))
		assert.Equal(t, "Technology", m["AAPL"])
	})

	t.Run("HealthCheck with healthy DB", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/health", nil)
		h.HealthCheck(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		var result map[string]interface{}
		require.NoError(t, json.NewDecoder(w.Body).Decode(&result))
		services := result["services"].(map[string]interface{})
		assert.Equal(t, "healthy", services["postgres"])
	})

	t.Run("Feedback lifecycle", func(t *testing.T) {
		truncate("signal_feedback")

		// CreateFeedback success.
		body := `{"symbol":"AAPL","signal":"BUY","action":"traded","confidence":0.8,
			"rules_triggered":["golden_cross"],"regime_id":"BULL",
			"entry_price":100,"stop_price":95,"target_1":110,"target_2":120,
			"valid_until":"` + time.Now().Add(24*time.Hour).Format(time.RFC3339) + `"}`
		cw := httptest.NewRecorder()
		creq := httptest.NewRequest("POST", "/api/v1/feedback", strings.NewReader(body))
		h.CreateFeedback(cw, creq)
		require.Equal(t, http.StatusCreated, cw.Code)
		var created models.SignalFeedback
		require.NoError(t, json.NewDecoder(cw.Body).Decode(&created))
		require.NotZero(t, created.ID)

		// GetFeedback (no filters).
		gw := httptest.NewRecorder()
		greq := httptest.NewRequest("GET", "/api/v1/feedback", nil)
		h.GetFeedback(gw, greq)
		require.Equal(t, http.StatusOK, gw.Code)

		// GetFeedback with filters.
		fw := httptest.NewRecorder()
		freq := httptest.NewRequest("GET", "/api/v1/feedback?symbol=AAPL&since_days=7&limit=10", nil)
		h.GetFeedback(fw, freq)
		require.Equal(t, http.StatusOK, fw.Code)

		// GetFeedbackSummary.
		sw := httptest.NewRecorder()
		sreq := httptest.NewRequest("GET", "/api/v1/feedback/summary", nil)
		h.GetFeedbackSummary(sw, sreq)
		require.Equal(t, http.StatusOK, sw.Code)
		var summary models.FeedbackSummary
		require.NoError(t, json.NewDecoder(sw.Body).Decode(&summary))
		assert.Equal(t, 1, summary.Total)

		// GetUnresolvedSignals (has entry/stop, no outcome).
		uw := httptest.NewRecorder()
		ureq := httptest.NewRequest("GET", "/api/v1/feedback/unresolved?limit=50", nil)
		h.GetUnresolvedSignals(uw, ureq)
		require.Equal(t, http.StatusOK, uw.Code)

		// UpdateSignalOutcome via router.
		router := SetupRoutes(h, "")
		ow := httptest.NewRecorder()
		oreq := httptest.NewRequest("PUT",
			fmt.Sprintf("/api/v1/feedback/%d/outcome", created.ID),
			strings.NewReader(`{"outcome":"TARGET_1_HIT"}`))
		router.ServeHTTP(ow, oreq)
		require.Equal(t, http.StatusOK, ow.Code)

		// UpdateSignalOutcome on missing id => 500 (db reports not found).
		mw := httptest.NewRecorder()
		mreq := httptest.NewRequest("PUT", "/api/v1/feedback/999999/outcome",
			strings.NewReader(`{"outcome":"STOPPED_OUT"}`))
		router.ServeHTTP(mw, mreq)
		assert.Equal(t, http.StatusInternalServerError, mw.Code)

		// GetRuleAccuracy.
		raw := httptest.NewRecorder()
		rareq := httptest.NewRequest("GET", "/api/v1/feedback/accuracy?since_days=90&min_signals=1", nil)
		h.GetRuleAccuracy(raw, rareq)
		require.Equal(t, http.StatusOK, raw.Code)

		// GetRuleOutcomeQuality.
		qw := httptest.NewRecorder()
		qreq := httptest.NewRequest("GET", "/api/v1/feedback/outcome-quality?since_days=90&min_signals=1", nil)
		h.GetRuleOutcomeQuality(qw, qreq)
		require.Equal(t, http.StatusOK, qw.Code)
	})

	t.Run("UpdateFeedback action via router", func(t *testing.T) {
		truncate("signal_feedback")

		fb := &models.SignalFeedback{
			Symbol: "MSFT", Signal: "BUY", Action: "skipped", FeedbackTimestamp: time.Now(),
		}
		require.NoError(t, db.CreateSignalFeedback(fb))

		router := SetupRoutes(h, "")
		w := httptest.NewRecorder()
		req := httptest.NewRequest("PUT", fmt.Sprintf("/api/v1/feedback/%d", fb.ID),
			strings.NewReader(`{"action":"traded"}`))
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)

		// Missing action => 400.
		bw := httptest.NewRecorder()
		breq := httptest.NewRequest("PUT", fmt.Sprintf("/api/v1/feedback/%d", fb.ID),
			strings.NewReader(`{}`))
		router.ServeHTTP(bw, breq)
		assert.Equal(t, http.StatusBadRequest, bw.Code)

		// Missing id => 500.
		mw := httptest.NewRecorder()
		mreq := httptest.NewRequest("PUT", "/api/v1/feedback/999999",
			strings.NewReader(`{"action":"traded"}`))
		router.ServeHTTP(mw, mreq)
		assert.Equal(t, http.StatusInternalServerError, mw.Code)
	})

	t.Run("Tiers lifecycle", func(t *testing.T) {
		truncate("monitored_stocks", "backtest_tiers", "stocks")
		// backtest_tiers.symbol references stocks(symbol).
		require.NoError(t, db.UpsertStockBasic("AAPL", "Apple"))
		require.NoError(t, db.UpsertStockBasic("MSFT", "Microsoft"))

		router := SetupRoutes(h, "")

		// UpsertTier success.
		tierBody := `{"symbol":"AAPL","tier":"A","composite_score":90,"gates_passed":4,
			"gates_total":4,"confidence_multiplier":1.1,"position_size_multiplier":1.0,
			"ranking_date":"2026-01-01T00:00:00Z"}`
		uw := httptest.NewRecorder()
		ureq := httptest.NewRequest("PUT", "/api/v1/tiers", strings.NewReader(tierBody))
		router.ServeHTTP(uw, ureq)
		require.Equal(t, http.StatusOK, uw.Code)

		// UpsertTier missing fields => 400.
		bw := httptest.NewRecorder()
		breq := httptest.NewRequest("PUT", "/api/v1/tiers", strings.NewReader(`{"symbol":""}`))
		router.ServeHTTP(bw, breq)
		assert.Equal(t, http.StatusBadRequest, bw.Code)

		// GetTier success.
		gw := httptest.NewRecorder()
		greq := httptest.NewRequest("GET", "/api/v1/tiers/AAPL", nil)
		router.ServeHTTP(gw, greq)
		require.Equal(t, http.StatusOK, gw.Code)

		// GetTier not found.
		nfw := httptest.NewRecorder()
		nfreq := httptest.NewRequest("GET", "/api/v1/tiers/ZZZZ", nil)
		router.ServeHTTP(nfw, nfreq)
		assert.Equal(t, http.StatusNotFound, nfw.Code)

		// GetAllTiers.
		aw := httptest.NewRecorder()
		areq := httptest.NewRequest("GET", "/api/v1/tiers", nil)
		router.ServeHTTP(aw, areq)
		require.Equal(t, http.StatusOK, aw.Code)

		// GetAllTiers filtered by tier grade.
		fw := httptest.NewRecorder()
		freq := httptest.NewRequest("GET", "/api/v1/tiers?tier=A", nil)
		router.ServeHTTP(fw, freq)
		require.Equal(t, http.StatusOK, fw.Code)

		// BulkUpsertTiers: one valid, one missing fields.
		bulkBody := `[{"symbol":"MSFT","tier":"B","composite_score":70,"gates_passed":3,
			"gates_total":4,"confidence_multiplier":1.0,"position_size_multiplier":1.0,
			"ranking_date":"2026-01-01T00:00:00Z"},
			{"symbol":"","tier":""}]`
		bkw := httptest.NewRecorder()
		bkreq := httptest.NewRequest("PUT", "/api/v1/tiers/bulk", strings.NewReader(bulkBody))
		router.ServeHTTP(bkw, bkreq)
		require.Equal(t, http.StatusOK, bkw.Code)
		var bulkResult map[string]interface{}
		require.NoError(t, json.NewDecoder(bkw.Body).Decode(&bulkResult))
		assert.Equal(t, float64(1), bulkResult["succeeded"])

		// BulkUpsertTiers empty => 400.
		ew := httptest.NewRecorder()
		ereq := httptest.NewRequest("PUT", "/api/v1/tiers/bulk", strings.NewReader(`[]`))
		router.ServeHTTP(ew, ereq)
		assert.Equal(t, http.StatusBadRequest, ew.Code)

		// BulkUpsertTiers invalid json => 400.
		jw := httptest.NewRecorder()
		jreq := httptest.NewRequest("PUT", "/api/v1/tiers/bulk", strings.NewReader(`not json`))
		router.ServeHTTP(jw, jreq)
		assert.Equal(t, http.StatusBadRequest, jw.Code)
	})
}
