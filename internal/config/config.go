package config

import (
	"strings"

	"github.com/trogers1052/trading-go-commons/env"
)

// Config holds all application configuration
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Kafka    KafkaConfig
	Redis    RedisConfig
	APIKey   string
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port string
	Host string
}

// DatabaseConfig holds PostgreSQL configuration
type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// KafkaConfig holds Kafka/Redpanda configuration
type KafkaConfig struct {
	Brokers        []string
	Topic          string
	TradesTopic    string
	PositionsTopic string
	WatchlistTopic string
	ConsumerGroup  string
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Host     string
	Port     string
	Password string
	DB       int
}

// Load reads configuration from environment variables
func Load() *Config {
	return &Config{
		APIKey: env.String("API_KEY", ""),
		Server: ServerConfig{
			Port: env.String("SERVER_PORT", "8081"),
			Host: env.String("SERVER_HOST", "0.0.0.0"),
		},
		Database: DatabaseConfig{
			Host:     env.String("DB_HOST", "postgres"),
			Port:     env.String("DB_PORT", "5432"),
			User:     env.String("DB_USER", "trader"),
			Password: env.String("DB_PASSWORD", ""),
			DBName:   env.String("DB_NAME", "trading_platform"),
			SSLMode:  env.String("DB_SSLMODE", "disable"),
		},
		Kafka: KafkaConfig{
			Brokers:        parseBrokers(env.String("KAFKA_BROKERS", "localhost:19092")),
			Topic:          env.String("KAFKA_TOPIC", "stock-events"),
			TradesTopic:    env.String("KAFKA_TRADES_TOPIC", "trading.orders"),
			PositionsTopic: env.String("KAFKA_POSITIONS_TOPIC", "trading.positions"),
			WatchlistTopic: env.String("KAFKA_WATCHLIST_TOPIC", "trading.watchlist"),
			ConsumerGroup:  env.String("KAFKA_CONSUMER_GROUP", "stock-service"),
		},
		Redis: RedisConfig{
			Host:     env.String("REDIS_HOST", "localhost"),
			Port:     env.String("REDIS_PORT", "6379"),
			Password: env.String("REDIS_PASSWORD", ""),
			DB:       0,
		},
	}
}

// ConnectionString returns the PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return "postgres://" + d.User + ":" + d.Password + "@" + d.Host + ":" + d.Port + "/" + d.DBName + "?sslmode=" + d.SSLMode
}

// parseBrokers splits a comma-separated broker list
func parseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// Address returns the Redis address in host:port format
func (r *RedisConfig) Address() string {
	return r.Host + ":" + r.Port
}
