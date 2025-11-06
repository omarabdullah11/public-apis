package ratelimiter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisRateLimiter provides distributed rate limiting using Redis
type RedisRateLimiter struct {
	client      *redis.Client
	configs     map[string]*RateLimitConfig
	configMutex sync.RWMutex
	prefix      string
}

// RateLimitConfig defines rate limiting parameters for an exchange
type RateLimitConfig struct {
	Requests    int           `json:"requests"`     // Max requests per window
	Window      time.Duration `json:"window"`       // Time window
	Burst       int           `json:"burst"`        // Burst capacity
	Enabled     bool          `json:"enabled"`      // Whether rate limiting is enabled
}

// RateLimitResult represents the result of a rate limit check
type RateLimitResult struct {
	Allowed     bool          `json:"allowed"`
	Remaining   int           `json:"remaining"`
	ResetTime   time.Time     `json:"reset_time"`
	RetryAfter  time.Duration `json:"retry_after,omitempty"`
	Used        int           `json:"used"`
	Limit       int           `json:"limit"`
}

// CircuitBreaker implements circuit breaker pattern for exchange failures
type CircuitBreaker struct {
	failures     int
	maxFailures  int
	resetTimeout time.Duration
	lastFailTime time.Time
	state        CircuitState
	mutex        sync.RWMutex
}

type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// NewRedisRateLimiter creates a new Redis-based rate limiter
func NewRedisRateLimiter(redisAddr, prefix string) (*RedisRateLimiter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	limiter := &RedisRateLimiter{
		client:  client,
		configs: make(map[string]*RateLimitConfig),
		prefix:  prefix,
	}

	// Set default configurations for major exchanges
	limiter.setDefaultConfigs()

	return limiter, nil
}

// setDefaultConfigs sets default rate limits for major exchanges
func (r *RedisRateLimiter) setDefaultConfigs() {
	defaultConfigs := map[string]*RateLimitConfig{
		"binance": {
			Requests: 1200, // 1200 requests per minute
			Window:   time.Minute,
			Burst:    100,
			Enabled:  true,
		},
		"coinbase": {
			Requests: 100, // 100 requests per minute
			Window:   time.Minute,
			Burst:    20,
			Enabled:  true,
		},
		"kraken": {
			Requests: 60, // 60 requests per minute
			Window:   time.Minute,
			Burst:    10,
			Enabled:  true,
		},
		"bybit": {
			Requests: 600, // 600 requests per minute
			Window:   time.Minute,
			Burst:    50,
			Enabled:  true,
		},
		"okex": {
			Requests: 60, // 60 requests per minute
			Window:   time.Minute,
			Burst:    20,
			Enabled:  true,
		},
	}

	for exchange, config := range defaultConfigs {
		r.configs[exchange] = config
	}
}

// SetConfig sets the rate limit configuration for an exchange
func (r *RedisRateLimiter) SetConfig(exchange string, config *RateLimitConfig) {
	r.configMutex.Lock()
	defer r.configMutex.Unlock()
	r.configs[exchange] = config
}

// GetConfig gets the rate limit configuration for an exchange
func (r *RedisRateLimiter) GetConfig(exchange string) (*RateLimitConfig, bool) {
	r.configMutex.RLock()
	defer r.configMutex.RUnlock()
	config, exists := r.configs[exchange]
	return config, exists
}

// CheckRateLimit checks if a request is allowed under the rate limit
func (r *RedisRateLimiter) CheckRateLimit(ctx context.Context, exchange string) (*RateLimitResult, error) {
	config, exists := r.GetConfig(exchange)
	if !exists || !config.Enabled {
		// No rate limiting configured
		return &RateLimitResult{
			Allowed:   true,
			Remaining: -1,
			Limit:     -1,
		}, nil
	}

	key := r.getRedisKey(exchange)

	// Use Lua script for atomic operations
	luaScript := `
		local key = KEYS[1]
		local window = tonumber(ARGV[1])
		local limit = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])

		-- Clean up expired entries
		redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

		-- Count current requests
		local current = redis.call('ZCARD', key)

		-- Check if under limit
		if current < limit then
			-- Add this request
			redis.call('ZADD', key, now, now)
			redis.call('EXPIRE', key, math.ceil(window))
			return {1, limit - current - 1, now + window}
		else
			-- Find oldest request to calculate retry after
			local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
			local retry_after = 0
			if #oldest > 0 then
				retry_after = oldest[2] + window - now
			end
			return {0, 0, now + window, retry_after}
		end
	`

	now := time.Now().UnixMilli()
	windowMs := config.Window.Milliseconds()

	result, err := r.client.Eval(ctx, luaScript, []string{key},
		windowMs, config.Requests, now).Result()
	if err != nil {
		return nil, fmt.Errorf("rate limit check failed: %w", err)
	}

	// Parse Lua script result
	resultSlice, ok := result.([]interface{})
	if !ok || len(resultSlice) < 3 {
		return nil, fmt.Errorf("invalid rate limit result format")
	}

	allowed := resultSlice[0].(int64) == 1
	remaining := int(resultSlice[1].(int64))
	resetTime := time.UnixMilli(resultSlice[2].(int64))

	rateLimitResult := &RateLimitResult{
		Allowed:   allowed,
		Remaining: remaining,
		ResetTime: resetTime,
		Used:      config.Requests - remaining,
		Limit:     config.Requests,
	}

	// Add retry after if not allowed
	if !allowed && len(resultSlice) > 3 {
		rateLimitResult.RetryAfter = time.Duration(resultSlice[3].(int64)) * time.Millisecond
	}

	return rateLimitResult, nil
}

// WaitForSlot blocks until a request slot is available
func (r *RedisRateLimiter) WaitForSlot(ctx context.Context, exchange string) error {
	for {
		result, err := r.CheckRateLimit(ctx, exchange)
		if err != nil {
			return err
		}

		if result.Allowed {
			return nil
		}

		// Wait for retry after duration or 1 second minimum
		waitTime := result.RetryAfter
		if waitTime < time.Second {
			waitTime = time.Second
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			continue
		}
	}
}

// getRedisKey generates the Redis key for rate limiting
func (r *RedisRateLimiter) getRedisKey(exchange string) string {
	return fmt.Sprintf("%s:rate_limit:%s", r.prefix, exchange)
}

// GetStats gets current rate limit statistics for an exchange
func (r *RedisRateLimiter) GetStats(ctx context.Context, exchange string) (map[string]interface{}, error) {
	config, exists := r.GetConfig(exchange)
	if !exists {
		return nil, fmt.Errorf("no rate limit config for exchange: %s", exchange)
	}

	key := r.getRedisKey(exchange)

	// Get current count
	now := time.Now().UnixMilli()
	windowMs := config.Window.Milliseconds()

	// Clean up expired entries and get count
	luaScript := `
		local key = KEYS[1]
		local window = tonumber(ARGV[1])
		local now = tonumber(ARGV[2])

		redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
		local count = redis.call('ZCARD', key)
		local ttl = redis.call('TTL', key)

		return {count, ttl}
	`

	result, err := r.client.Eval(ctx, luaScript, []string{key}, windowMs, now).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get rate limit stats: %w", err)
	}

	resultSlice := result.([]interface{})
	currentCount := resultSlice[0].(int64)
	ttl := resultSlice[1].(int64)

	return map[string]interface{}{
		"exchange":       exchange,
		"requests":       currentCount,
		"limit":          config.Requests,
		"remaining":      int(config.Requests) - int(currentCount),
		"window_seconds": config.Window.Seconds(),
		"ttl_seconds":    ttl,
		"reset_time":     time.Now().Add(time.Duration(ttl) * time.Second),
	}, nil
}

// Reset resets the rate limit counter for an exchange
func (r *RedisRateLimiter) Reset(ctx context.Context, exchange string) error {
	key := r.getRedisKey(exchange)
	return r.client.Del(ctx, key).Err()
}

// Close closes the Redis connection
func (r *RedisRateLimiter) Close() error {
	return r.client.Close()
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		state:        StateClosed,
	}
}

// Call executes a function with circuit breaker protection
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	// Check if circuit should be reset
	if cb.state == StateOpen && time.Since(cb.lastFailTime) > cb.resetTimeout {
		cb.state = StateHalfOpen
		cb.failures = 0
	}

	// Reject calls if circuit is open
	if cb.state == StateOpen {
		return fmt.Errorf("circuit breaker is open")
	}

	// Execute the function
	err := fn()

	// Handle result
	if err != nil {
		cb.failures++
		cb.lastFailTime = time.Now()

		if cb.failures >= cb.maxFailures {
			cb.state = StateOpen
		}
		return err
	}

	// Success - reset failures
	cb.failures = 0
	cb.state = StateClosed
	return nil
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() CircuitState {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// IsHealthy returns true if the circuit breaker is not open
func (cb *CircuitBreaker) IsHealthy() bool {
	return cb.GetState() != StateOpen
}