package exchange

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ccxt/ccxt/go/v4/ccxt"
)

// CCXTClient implements ExchangeClient using the CCXT library
type CCXTClient struct {
	exchange   ccxt.IExchange
	name       string
	rateLimiter *RateLimiter
	mutex      sync.RWMutex
	healthy    bool
	lastCheck  time.Time
}

// RateLimiter manages API rate limiting per exchange
type RateLimiter struct {
	requests   int
	window     time.Duration
	lastReset  time.Time
	mutex      sync.Mutex
	maxReq     int
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(maxRequests int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		maxReq:    maxRequests,
		window:    window,
		lastReset: time.Now(),
	}
}

// Allow checks if a request is allowed under the rate limit
func (rl *RateLimiter) Allow() bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	now := time.Now()
	if now.Sub(rl.lastReset) > rl.window {
		rl.requests = 0
		rl.lastReset = now
	}

	if rl.requests >= rl.maxReq {
		return false
	}

	rl.requests++
	return true
}

// Wait blocks until a request is allowed
func (rl *RateLimiter) Wait() {
	for !rl.Allow() {
		time.Sleep(100 * time.Millisecond)
	}
}

// NewCCXTClient creates a new exchange client using CCXT
func NewCCXTClient(exchangeName string, maxRequests int) (*CCXTClient, error) {
	// Get exchange from CCXT
	exchange, err := ccxt.NewExchange(exchangeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create exchange %s: %w", exchangeName, err)
	}

	// Configure for public market data (no API keys needed)
	exchange.SetEnableRateLimit(true)
	exchange.SetSandboxMode(false) // Use real markets

	client := &CCXTClient{
		exchange:    exchange,
		name:        exchangeName,
		rateLimiter: NewRateLimiter(maxRequests, time.Minute),
		healthy:     true,
		lastCheck:   time.Now(),
	}

	// Test connection
	if err := client.testConnection(); err != nil {
		client.healthy = false
		log.Printf("Warning: Initial connection test failed for %s: %v", exchangeName, err)
	}

	return client, nil
}

// testConnection performs a basic connection test
func (c *CCXTClient) testConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to fetch a simple ticker
	_, err := c.exchange.FetchTicker(ctx, "BTC/USDT", nil)
	return err
}

// FetchTickers fetches tickers for multiple symbols
func (c *CCXTClient) FetchTickers(symbols []string) ([]Ticker, error) {
	c.mutex.RLock()
	if !c.healthy {
		c.mutex.RUnlock()
		return nil, fmt.Errorf("exchange %s is unhealthy", c.name)
	}
	c.mutex.RUnlock()

	c.rateLimiter.Wait()

	var tickers []Ticker
	var errors []error

	for _, symbol := range symbols {
		ticker, err := c.FetchTicker(symbol)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to fetch %s from %s: %w", symbol, c.name, err))
			continue
		}
		if ticker != nil {
			tickers = append(tickers, *ticker)
		}
	}

	if len(errors) > 0 && len(tickers) == 0 {
		return nil, fmt.Errorf("failed to fetch any tickers: %v", errors)
	}

	return tickers, nil
}

// FetchTicker fetches a single ticker
func (c *CCXTClient) FetchTicker(symbol string) (*Ticker, error) {
	c.mutex.RLock()
	if !c.healthy {
		c.mutex.RUnlock()
		return nil, fmt.Errorf("exchange %s is unhealthy", c.name)
	}
	c.mutex.RUnlock()

	c.rateLimiter.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Normalize symbol for this exchange
	normalizedSymbol := c.normalizeSymbolForExchange(symbol)

	// Fetch ticker from exchange
	ccxtTicker, err := c.exchange.FetchTicker(ctx, normalizedSymbol, nil)
	if err != nil {
		// Mark as unhealthy if this is a connection error
		if c.isConnectionError(err) {
			c.markUnhealthy(err)
		}
		return nil, fmt.Errorf("failed to fetch ticker %s from %s: %w", symbol, c.name, err)
	}

	// Convert to our normalized format
	ticker := c.convertCCXTTicker(ccxtTicker, symbol)

	// Mark as healthy on successful fetch
	c.markHealthy()

	return &ticker, nil
}

// normalizeSymbolForExchange converts symbol to the format expected by this exchange
func (c *CCXTClient) normalizeSymbolForExchange(symbol string) string {
	// Remove slashes if the exchange doesn't use them
	if !c.exchange.HasFeature(ccxt.FeatureFetchTickers) {
		return strings.ReplaceAll(symbol, "/", "")
	}
	return symbol
}

// convertCCXTTicker converts CCXT ticker to our normalized format
func (c *CCXTClient) convertCCXTTicker(ccxtTicker *ccxt.Ticker, symbol string) Ticker {
	var price, bid, ask, high, low, change, percent float64
	var volume float64

	if ccxtTicker != nil {
		price = ccxtTicker.Last
		bid = ccxtTicker.Bid
		ask = ccxtTicker.Ask
		high = ccxtTicker.High
		low = ccxtTicker.Low
		volume = ccxtTicker.BaseVolume
		change = ccxtTicker.Change
		percent = ccxtTicker.Percentage
	}

	return Ticker{
		Exchange:  c.name,
		Symbol:    symbol,
		Price:     price,
		Volume:    volume,
		Bid:       bid,
		Ask:       ask,
		High:      high,
		Low:       low,
		Change:    change,
		Percent:   percent,
		Timestamp: time.Now(),
	}
}

// GetExchangeName returns the exchange name
func (c *CCXTClient) GetExchangeName() string {
	return c.name
}

// GetSupportedSymbols returns the list of supported trading pairs
func (c *CCXTClient) GetSupportedSymbols() ([]string, error) {
	c.rateLimiter.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	markets, err := c.exchange.LoadMarkets(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to load markets for %s: %w", c.name, err)
	}

	var symbols []string
	for symbol := range markets {
		symbols = append(symbols, symbol)
	}

	return symbols, nil
}

// IsHealthy returns the health status of the exchange
func (c *CCXTClient) IsHealthy() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.healthy
}

// markHealthy marks the exchange as healthy
func (c *CCXTClient) markHealthy() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.healthy = true
	c.lastCheck = time.Now()
}

// markUnhealthy marks the exchange as unhealthy
func (c *CCXTClient) markUnhealthy(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.healthy = false
	c.lastCheck = time.Now()
	log.Printf("Exchange %s marked as unhealthy: %v", c.name, err)
}

// isConnectionError checks if an error is a connection-related error
func (c *CCXTClient) isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	connectionErrors := []string{
		"connection refused",
		"timeout",
		"network unreachable",
		"no such host",
		"connection reset",
		"connection timed out",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(errStr, connErr) {
			return true
		}
	}

	return false
}

// Close closes the exchange client and cleans up resources
func (c *CCXTClient) Close() error {
	if c.exchange != nil {
		c.exchange.Close()
	}
	return nil
}