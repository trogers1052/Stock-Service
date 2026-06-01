package redis

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/trogers1052/stock-alert-system/internal/config"
)

// setupTestRedis spins up a Redis container and returns a connected Client.
func setupTestRedis(t *testing.T) (*Client, func()) {
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

	client, err := New(config.RedisConfig{Host: host, Port: port})
	require.NoError(t, err)

	cleanup := func() {
		_ = client.Close()
		_ = container.Terminate(ctx)
	}
	return client, cleanup
}

func TestRedisClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, cleanup := setupTestRedis(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("Ping", func(t *testing.T) {
		require.NoError(t, client.Ping(ctx))
	})

	t.Run("StockPrice round trip", func(t *testing.T) {
		require.NoError(t, client.SetStockPrice(ctx, "AAPL", 175.25, time.Minute))
		price, err := client.GetStockPrice(ctx, "AAPL")
		require.NoError(t, err)
		assert.Equal(t, 175.25, price)
	})

	t.Run("StockData round trip", func(t *testing.T) {
		data := &StockData{
			Symbol:        "MSFT",
			CurrentPrice:  380.0,
			PreviousClose: 375.0,
			DayHigh:       382.0,
			DayLow:        374.0,
			Volume:        1234567,
			UpdatedAt:     time.Now().UTC().Truncate(time.Second),
		}
		require.NoError(t, client.SetStockData(ctx, data, time.Minute))

		got, err := client.GetStockData(ctx, "MSFT")
		require.NoError(t, err)
		assert.Equal(t, "MSFT", got.Symbol)
		assert.Equal(t, 380.0, got.CurrentPrice)
		assert.Equal(t, int64(1234567), got.Volume)
	})

	t.Run("GetStockData missing key errors", func(t *testing.T) {
		_, err := client.GetStockData(ctx, "NOPE")
		require.Error(t, err)
	})

	t.Run("Indicator round trip", func(t *testing.T) {
		require.NoError(t, client.SetIndicator(ctx, "AAPL", "RSI", 42.5, time.Minute))
		val, err := client.GetIndicator(ctx, "AAPL", "RSI")
		require.NoError(t, err)
		assert.Equal(t, 42.5, val)
	})

	t.Run("TierData round trip", func(t *testing.T) {
		td := &TierData{
			Symbol:                 "NVDA",
			Tier:                   "A",
			CompositeScore:         88.0,
			ConfidenceMultiplier:   1.1,
			PositionSizeMultiplier: 1.0,
			Blacklisted:            false,
			AllowedRegimes:         []string{"BULL"},
		}
		require.NoError(t, client.SetTierData(ctx, td, time.Minute))

		got, err := client.GetTierData(ctx, "NVDA")
		require.NoError(t, err)
		assert.Equal(t, "A", got.Tier)
		assert.Equal(t, []string{"BULL"}, got.AllowedRegimes)
	})

	t.Run("GetTierData missing key errors", func(t *testing.T) {
		_, err := client.GetTierData(ctx, "MISSING")
		require.Error(t, err)
	})

	t.Run("Generic Set/Get/Exists/Delete", func(t *testing.T) {
		require.NoError(t, client.Set(ctx, "k1", "v1", time.Minute))

		val, err := client.Get(ctx, "k1")
		require.NoError(t, err)
		assert.Equal(t, "v1", val)

		exists, err := client.Exists(ctx, "k1")
		require.NoError(t, err)
		assert.True(t, exists)

		notExists, err := client.Exists(ctx, "does-not-exist")
		require.NoError(t, err)
		assert.False(t, notExists)

		require.NoError(t, client.Delete(ctx, "k1"))
		exists, err = client.Exists(ctx, "k1")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Publish and Subscribe", func(t *testing.T) {
		sub := client.Subscribe(ctx, "test-channel")
		defer sub.Close()

		// Wait for subscription to be ready.
		_, err := sub.Receive(ctx)
		require.NoError(t, err)

		ch := sub.Channel()
		require.NoError(t, client.Publish(ctx, "test-channel", map[string]string{"hello": "world"}))

		select {
		case msg := <-ch:
			assert.Contains(t, msg.Payload, "world")
		case <-time.After(3 * time.Second):
			t.Fatal("did not receive published message")
		}
	})

	t.Run("GetRawClient returns underlying client", func(t *testing.T) {
		assert.NotNil(t, client.GetRawClient())
	})
}

func TestRedisNew_ConnectionError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	// Port 1 is not a redis server; New should fail to ping.
	_, err := New(config.RedisConfig{Host: "127.0.0.1", Port: "1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Redis")
}
