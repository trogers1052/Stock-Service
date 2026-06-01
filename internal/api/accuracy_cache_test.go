package api

import (
	"context"
	"net"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/trogers1052/stock-alert-system/internal/config"
	"github.com/trogers1052/stock-alert-system/internal/models"
	redisclient "github.com/trogers1052/stock-alert-system/internal/redis"
)

func setupRedisForAPI(t *testing.T) (*redisclient.Client, func()) {
	t.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	u, err := url.Parse(connStr)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	client, err := redisclient.New(config.RedisConfig{Host: host, Port: port})
	require.NoError(t, err)

	return client, func() {
		_ = client.Close()
		_ = container.Terminate(ctx)
	}
}

func TestUpsertTier_WithRedisCache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, db, dbCleanup := setupHandlerWithDB(t)
	defer dbCleanup()
	rc, redisCleanup := setupRedisForAPI(t)
	defer redisCleanup()

	// backtest_tiers.symbol references stocks(symbol).
	require.NoError(t, db.UpsertStockBasic("AAPL", "Apple"))
	require.NoError(t, db.UpsertStockBasic("MSFT", "Microsoft"))

	h := NewHandler(db, nil, rc)
	router := SetupRoutes(h, "")

	// UpsertTier caches the tier in Redis.
	tierBody := `{"symbol":"AAPL","tier":"A","composite_score":90,"gates_passed":4,
		"gates_total":4,"confidence_multiplier":1.1,"position_size_multiplier":1.0,
		"allowed_regimes":["BULL"],"ranking_date":"2026-01-01T00:00:00Z"}`
	uw := httptest.NewRecorder()
	ureq := httptest.NewRequest("PUT", "/api/v1/tiers", strings.NewReader(tierBody))
	router.ServeHTTP(uw, ureq)
	require.Equal(t, 200, uw.Code)

	td, err := rc.GetTierData(context.Background(), "AAPL")
	require.NoError(t, err)
	assert.Equal(t, "A", td.Tier)
	assert.Equal(t, []string{"BULL"}, td.AllowedRegimes)

	// BulkUpsertTiers also caches in Redis.
	bulkBody := `[{"symbol":"MSFT","tier":"B","composite_score":70,"gates_passed":3,
		"gates_total":4,"confidence_multiplier":1.0,"position_size_multiplier":1.0,
		"ranking_date":"2026-01-01T00:00:00Z"}]`
	bw := httptest.NewRecorder()
	breq := httptest.NewRequest("PUT", "/api/v1/tiers/bulk", strings.NewReader(bulkBody))
	router.ServeHTTP(bw, breq)
	require.Equal(t, 200, bw.Code)

	td2, err := rc.GetTierData(context.Background(), "MSFT")
	require.NoError(t, err)
	assert.Equal(t, "B", td2.Tier)
}

func TestStartAccuracyCacheWriter_NoRedis(t *testing.T) {
	// With no Redis configured, the writer returns immediately.
	h := NewHandler(nil, nil, nil)
	h.StartAccuracyCacheWriter(context.Background())
}

func TestAccuracyCacheWriter_WithDBAndRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, db, dbCleanup := setupHandlerWithDB(t)
	defer dbCleanup()

	rc, redisCleanup := setupRedisForAPI(t)
	defer redisCleanup()

	h := NewHandler(db, nil, rc)

	// Seed feedback with rules + outcomes so both caches have data to write.
	ts := time.Now()
	mk := func(sym, outcome string) {
		fb := &models.SignalFeedback{
			Symbol: sym, Signal: "BUY", Action: "traded", RegimeID: "BULL",
			RulesTriggered: []string{"golden_cross"}, EntryPrice: 10, StopPrice: 9,
			FeedbackTimestamp: ts,
		}
		require.NoError(t, db.CreateSignalFeedback(fb))
		if outcome != "" {
			require.NoError(t, db.UpdateSignalOutcome(fb.ID, outcome))
		}
	}
	for i := 0; i < 12; i++ {
		// Distinct timestamps so the unique (symbol,signal,ts) constraint holds.
		ts = ts.Add(time.Second)
		var outcome string
		if i%2 == 0 {
			outcome = models.OutcomeTarget1Hit
		} else {
			outcome = models.OutcomeStoppedOut
		}
		mk("SYM", outcome)
	}

	ctx := context.Background()

	// writeAccuracyCache also calls writeOutcomeQualityCache.
	h.writeAccuracyCache(ctx)

	accVal, err := rc.Get(ctx, accuracyCacheKey)
	require.NoError(t, err)
	assert.Contains(t, accVal, "golden_cross")

	oqVal, err := rc.Get(ctx, outcomeQualityCacheKey)
	require.NoError(t, err)
	assert.Contains(t, oqVal, "golden_cross")

	// StartAccuracyCacheWriter performs an immediate write then blocks on the
	// ticker; cancel right away to exercise the startup-write + ctx.Done path.
	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.StartAccuracyCacheWriter(cctx)
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("StartAccuracyCacheWriter did not stop on context cancel")
	}
}
