package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// parseBrokers
// ---------------------------------------------------------------------------

func TestParseBrokers_SingleBroker(t *testing.T) {
	result := parseBrokers("localhost:9092")
	assert.Equal(t, []string{"localhost:9092"}, result)
}

func TestParseBrokers_MultipleBrokers(t *testing.T) {
	result := parseBrokers("broker1:9092,broker2:9092,broker3:9092")
	assert.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, result)
}

func TestParseBrokers_WhitespaceHandling(t *testing.T) {
	result := parseBrokers("  broker1:9092 , broker2:9092 , broker3:9092  ")
	assert.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, result)
}

func TestParseBrokers_EmptyString(t *testing.T) {
	result := parseBrokers("")
	assert.Empty(t, result)
}

func TestParseBrokers_TrailingComma(t *testing.T) {
	result := parseBrokers("broker1:9092,")
	assert.Equal(t, []string{"broker1:9092"}, result)
}

func TestParseBrokers_OnlyCommas(t *testing.T) {
	result := parseBrokers(",,,")
	assert.Empty(t, result)
}

// ---------------------------------------------------------------------------
// DatabaseConfig.ConnectionString
// ---------------------------------------------------------------------------

func TestConnectionString(t *testing.T) {
	db := DatabaseConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "trader",
		Password: "secret",
		DBName:   "trading_platform",
		SSLMode:  "disable",
	}
	expected := "postgres://trader:secret@localhost:5432/trading_platform?sslmode=disable"
	assert.Equal(t, expected, db.ConnectionString())
}

func TestConnectionString_CustomValues(t *testing.T) {
	db := DatabaseConfig{
		Host:     "db.prod",
		Port:     "5433",
		User:     "admin",
		Password: "p@$$w0rd",
		DBName:   "mydb",
		SSLMode:  "require",
	}
	expected := "postgres://admin:p@$$w0rd@db.prod:5433/mydb?sslmode=require"
	assert.Equal(t, expected, db.ConnectionString())
}

// ---------------------------------------------------------------------------
// RedisConfig.Address
// ---------------------------------------------------------------------------

func TestRedisAddress(t *testing.T) {
	r := RedisConfig{Host: "localhost", Port: "6379"}
	assert.Equal(t, "localhost:6379", r.Address())
}

func TestRedisAddress_CustomPort(t *testing.T) {
	r := RedisConfig{Host: "redis.prod", Port: "6380"}
	assert.Equal(t, "redis.prod:6380", r.Address())
}

// ---------------------------------------------------------------------------
// Load defaults
// ---------------------------------------------------------------------------

func TestLoad_Defaults(t *testing.T) {
	// Clear env vars that might interfere
	envVars := []string{
		"SERVER_PORT", "SERVER_HOST",
		"DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_SSLMODE",
		"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_TRADES_TOPIC",
		"KAFKA_POSITIONS_TOPIC", "KAFKA_WATCHLIST_TOPIC", "KAFKA_CONSUMER_GROUP",
		"REDIS_HOST", "REDIS_PORT", "REDIS_PASSWORD",
	}
	saved := make(map[string]string)
	for _, key := range envVars {
		saved[key] = os.Getenv(key)
		os.Unsetenv(key)
	}
	defer func() {
		for key, val := range saved {
			if val != "" {
				os.Setenv(key, val)
			}
		}
	}()

	cfg := Load()

	// Server defaults
	assert.Equal(t, "8081", cfg.Server.Port)
	assert.Equal(t, "0.0.0.0", cfg.Server.Host)

	// Database defaults
	assert.Equal(t, "postgres", cfg.Database.Host)
	assert.Equal(t, "5432", cfg.Database.Port)
	assert.Equal(t, "trader", cfg.Database.User)
	assert.Equal(t, "", cfg.Database.Password)
	assert.Equal(t, "trading_platform", cfg.Database.DBName)
	assert.Equal(t, "disable", cfg.Database.SSLMode)

	// Kafka defaults
	assert.Equal(t, []string{"localhost:19092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "stock-events", cfg.Kafka.Topic)
	assert.Equal(t, "trading.orders", cfg.Kafka.TradesTopic)
	assert.Equal(t, "trading.positions", cfg.Kafka.PositionsTopic)
	assert.Equal(t, "trading.watchlist", cfg.Kafka.WatchlistTopic)
	assert.Equal(t, "stock-service", cfg.Kafka.ConsumerGroup)

	// Redis defaults
	assert.Equal(t, "localhost", cfg.Redis.Host)
	assert.Equal(t, "6379", cfg.Redis.Port)
	assert.Equal(t, "", cfg.Redis.Password)
	assert.Equal(t, 0, cfg.Redis.DB)
}

func TestLoad_CustomEnvVars(t *testing.T) {
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("DB_HOST", "custom-db")
	os.Setenv("KAFKA_BROKERS", "b1:9092,b2:9092")
	os.Setenv("REDIS_HOST", "custom-redis")
	defer func() {
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("DB_HOST")
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("REDIS_HOST")
	}()

	cfg := Load()
	assert.Equal(t, "9090", cfg.Server.Port)
	assert.Equal(t, "custom-db", cfg.Database.Host)
	assert.Equal(t, []string{"b1:9092", "b2:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "custom-redis", cfg.Redis.Host)
}
