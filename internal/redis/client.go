package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/trogers1052/stock-alert-system/internal/config"
	"github.com/trogers1052/trading-go-commons/redisx"
)

// Client wraps the Redis client with stock-specific operations
type Client struct {
	rdb *redis.Client
}

// New creates a new Redis client
func New(cfg config.RedisConfig) (*Client, error) {
	rdb := redisx.NewClient(redisx.Options{
		Addr:     cfg.Address(),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Ping checks if Redis is reachable
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Stock price caching operations

// SetStockPrice caches a stock price with TTL
func (c *Client) SetStockPrice(ctx context.Context, symbol string, price float64, ttl time.Duration) error {
	key := fmt.Sprintf("stock:%s:price", symbol)
	return c.rdb.Set(ctx, key, price, ttl).Err()
}

// GetStockPrice retrieves a cached stock price
func (c *Client) GetStockPrice(ctx context.Context, symbol string) (float64, error) {
	key := fmt.Sprintf("stock:%s:price", symbol)
	return c.rdb.Get(ctx, key).Float64()
}

// StockData represents cached stock data
type StockData struct {
	Symbol        string    `json:"symbol"`
	CurrentPrice  float64   `json:"current_price"`
	PreviousClose float64   `json:"previous_close"`
	DayHigh       float64   `json:"day_high"`
	DayLow        float64   `json:"day_low"`
	Volume        int64     `json:"volume"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// SetStockData caches full stock data with TTL
func (c *Client) SetStockData(ctx context.Context, data *StockData, ttl time.Duration) error {
	key := fmt.Sprintf("stock:%s:data", data.Symbol)
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal stock data: %w", err)
	}
	return c.rdb.Set(ctx, key, jsonData, ttl).Err()
}

// GetStockData retrieves cached stock data
func (c *Client) GetStockData(ctx context.Context, symbol string) (*StockData, error) {
	key := fmt.Sprintf("stock:%s:data", symbol)
	jsonData, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var data StockData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stock data: %w", err)
	}
	return &data, nil
}

// Technical indicator caching

// SetIndicator caches a technical indicator value
func (c *Client) SetIndicator(ctx context.Context, symbol, indicatorType string, value float64, ttl time.Duration) error {
	key := fmt.Sprintf("stock:%s:indicator:%s", symbol, indicatorType)
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

// GetIndicator retrieves a cached indicator value
func (c *Client) GetIndicator(ctx context.Context, symbol, indicatorType string) (float64, error) {
	key := fmt.Sprintf("stock:%s:indicator:%s", symbol, indicatorType)
	return c.rdb.Get(ctx, key).Float64()
}

// Pub/Sub operations for real-time updates

// Publish publishes a message to a channel
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	jsonData, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	return c.rdb.Publish(ctx, channel, jsonData).Err()
}

// Subscribe returns a subscription to a channel
func (c *Client) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.rdb.Subscribe(ctx, channels...)
}

// Tier data caching

// TierData represents cached tier ranking data for the decision-engine.
type TierData struct {
	Symbol                 string   `json:"symbol"`
	Tier                   string   `json:"tier"`
	CompositeScore         float64  `json:"composite_score"`
	ConfidenceMultiplier   float64  `json:"confidence_multiplier"`
	PositionSizeMultiplier float64  `json:"position_size_multiplier"`
	Blacklisted            bool     `json:"blacklisted"`
	AllowedRegimes         []string `json:"allowed_regimes,omitempty"`
}

// SetTierData caches tier data with TTL
func (c *Client) SetTierData(ctx context.Context, data *TierData, ttl time.Duration) error {
	key := fmt.Sprintf("stock:%s:tier", data.Symbol)
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal tier data: %w", err)
	}
	return c.rdb.Set(ctx, key, jsonData, ttl).Err()
}

// GetTierData retrieves cached tier data
func (c *Client) GetTierData(ctx context.Context, symbol string) (*TierData, error) {
	key := fmt.Sprintf("stock:%s:tier", symbol)
	jsonData, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var data TierData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tier data: %w", err)
	}
	return &data, nil
}

// Generic operations

// Set stores a value with optional TTL
func (c *Client) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

// Get retrieves a string value
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
}

// Delete removes a key
func (c *Client) Delete(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}

// Exists checks if a key exists
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	result, err := c.rdb.Exists(ctx, key).Result()
	return result > 0, err
}

// GetRawClient returns the underlying redis client for advanced operations
func (c *Client) GetRawClient() *redis.Client {
	return c.rdb
}
