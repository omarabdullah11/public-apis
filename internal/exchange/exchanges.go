package exchange

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ccxt/ccxt/go/v4/ccxt"
)

// ExchangeRegistry manages multiple exchange instances
type ExchangeRegistry struct {
	exchanges    map[string]*CCXTClient
	circuitBreakers map[string]*CircuitBreaker
	healthStatus map[string]*HealthStatus
	mutex        sync.RWMutex
	rateLimiter  RateLimiter
	config       *RegistryConfig
}

// HealthStatus tracks the health status of an exchange
type HealthStatus struct {
	Exchange      string    `json:"exchange"`
	Healthy       bool      `json:"healthy"`
	LastCheck     time.Time `json:"last_check"`
	LastSuccess   time.Time `json:"last_success"`
	ErrorCount    int       `json:"error_count"`
	LastError     string    `json:"last_error,omitempty"`
	ResponseTime  time.Duration `json:"response_time"`
	UptimePercent float64   `json:"uptime_percent"`
}

// RegistryConfig contains configuration for the exchange registry
type RegistryConfig struct {
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	MaxRetries          int           `json:"max_retries"`
	RetryDelay          time.Duration `json:"retry_delay"`
	EnableAutoRetry     bool          `json:"enable_auto_retry"`
	DisabledExchanges   []string      `json:"disabled_exchanges"`
}

// RateLimiter interface for dependency injection
type RateLimiter interface {
	CheckRateLimit(ctx context.Context, exchange string) (*RateLimitResult, error)
	WaitForSlot(ctx context.Context, exchange string) error
	GetStats(ctx context.Context, exchange string) (map[string]interface{}, error)
}

// NewExchangeRegistry creates a new exchange registry
func NewExchangeRegistry(rateLimiter RateLimiter, config *RegistryConfig) *ExchangeRegistry {
	if config == nil {
		config = &RegistryConfig{
			HealthCheckInterval: 30 * time.Second,
			MaxRetries:          3,
			RetryDelay:          1 * time.Second,
			EnableAutoRetry:     true,
			DisabledExchanges:   []string{},
		}
	}

	registry := &ExchangeRegistry{
		exchanges:        make(map[string]*CCXTClient),
		circuitBreakers:  make(map[string]*CircuitBreaker),
		healthStatus:     make(map[string]*HealthStatus),
		rateLimiter:      rateLimiter,
		config:           config,
	}

	// Start health monitoring
	go registry.startHealthMonitoring()

	return registry
}

// RegisterExchange registers a new exchange
func (er *ExchangeRegistry) RegisterExchange(exchangeName string, maxRequests int) error {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	// Check if already registered
	if _, exists := er.exchanges[exchangeName]; exists {
		return fmt.Errorf("exchange %s already registered", exchangeName)
	}

	// Check if disabled
	for _, disabled := range er.config.DisabledExchanges {
		if disabled == exchangeName {
			return fmt.Errorf("exchange %s is disabled", exchangeName)
		}
	}

	// Create exchange client
	client, err := NewCCXTClient(exchangeName, maxRequests)
	if err != nil {
		return fmt.Errorf("failed to create client for %s: %w", exchangeName, err)
	}

	// Create circuit breaker
	circuitBreaker := NewCircuitBreaker(5, 2*time.Minute)

	// Initialize health status
	healthStatus := &HealthStatus{
		Exchange:    exchangeName,
		Healthy:     client.IsHealthy(),
		LastCheck:   time.Now(),
		LastSuccess: time.Now(),
		ErrorCount:  0,
	}

	er.exchanges[exchangeName] = client
	er.circuitBreakers[exchangeName] = circuitBreaker
	er.healthStatus[exchangeName] = healthStatus

	log.Printf("Registered exchange: %s", exchangeName)
	return nil
}

// GetExchange gets a registered exchange client
func (er *ExchangeRegistry) GetExchange(exchangeName string) (*CCXTClient, error) {
	er.mutex.RLock()
	defer er.mutex.RUnlock()

	client, exists := er.exchanges[exchangeName]
	if !exists {
		return nil, fmt.Errorf("exchange %s not registered", exchangeName)
	}

	return client, nil
}

// GetAllExchanges returns all registered exchanges
func (er *ExchangeRegistry) GetAllExchanges() map[string]*CCXTClient {
	er.mutex.RLock()
	defer er.mutex.RUnlock()

	result := make(map[string]*CCXTClient)
	for name, client := range er.exchanges {
		result[name] = client
	}
	return result
}

// GetHealthyExchanges returns only healthy exchanges
func (er *ExchangeRegistry) GetHealthyExchanges() map[string]*CCXTClient {
	er.mutex.RLock()
	defer er.mutex.RUnlock()

	result := make(map[string]*CCXTClient)
	for name, client := range er.exchanges {
		if health, exists := er.healthStatus[name]; exists && health.Healthy {
			result[name] = client
		}
	}
	return result
}

// GetExchangeNames returns all registered exchange names
func (er *ExchangeRegistry) GetExchangeNames() []string {
	er.mutex.RLock()
	defer er.mutex.RUnlock()

	var names []string
	for name := range er.exchanges {
		names = append(names, name)
	}
	return names
}

// GetHealthStatus returns health status for all exchanges
func (er *ExchangeRegistry) GetHealthStatus() map[string]*HealthStatus {
	er.mutex.RLock()
	defer er.mutex.RUnlock()

	result := make(map[string]*HealthStatus)
	for name, status := range er.healthStatus {
		result[name] = status
	}
	return result
}

// GetHealthStatusForExchange returns health status for a specific exchange
func (er *ExchangeRegistry) GetHealthStatusForExchange(exchangeName string) (*HealthStatus, error) {
	er.mutex.RLock()
	defer er.mutex.RUnlock()

	status, exists := er.healthStatus[exchangeName]
	if !exists {
		return nil, fmt.Errorf("exchange %s not found", exchangeName)
	}
	return status, nil
}

// FetchTickersFromAll fetches tickers from all healthy exchanges
func (er *ExchangeRegistry) FetchTickersFromAll(ctx context.Context, symbols []string) (map[string][]Ticker, error) {
	healthyExchanges := er.GetHealthyExchanges()
	if len(healthyExchanges) == 0 {
		return nil, fmt.Errorf("no healthy exchanges available")
	}

	results := make(map[string][]Ticker)
	var wg sync.WaitGroup
	var mutex sync.Mutex

	for exchangeName, client := range healthyExchanges {
		wg.Add(1)
		go func(name string, c *CCXTClient) {
			defer wg.Done()

			// Check rate limit
			if err := er.rateLimiter.WaitForSlot(ctx, name); err != nil {
				log.Printf("Rate limit error for %s: %v", name, err)
				return
			}

			// Use circuit breaker
			circuitBreaker := er.circuitBreakers[name]
			err := circuitBreaker.Call(func() error {
				tickers, fetchErr := c.FetchTickers(symbols)
				if fetchErr != nil {
					er.updateHealthStatus(name, false, fetchErr)
					return fetchErr
				}

				mutex.Lock()
				results[name] = tickers
				mutex.Unlock()

				er.updateHealthStatus(name, true, nil)
				return nil
			})

			if err != nil {
				log.Printf("Failed to fetch tickers from %s: %v", name, err)
			}
		}(exchangeName, client)
	}

	wg.Wait()
	return results, nil
}

// FetchTickerFromAll fetches a single ticker from all healthy exchanges
func (er *ExchangeRegistry) FetchTickerFromAll(ctx context.Context, symbol string) (map[string]*Ticker, error) {
	healthyExchanges := er.GetHealthyExchanges()
	if len(healthyExchanges) == 0 {
		return nil, fmt.Errorf("no healthy exchanges available")
	}

	results := make(map[string]*Ticker)
	var wg sync.WaitGroup
	var mutex sync.Mutex

	for exchangeName, client := range healthyExchanges {
		wg.Add(1)
		go func(name string, c *CCXTClient) {
			defer wg.Done()

			// Check rate limit
			if err := er.rateLimiter.WaitForSlot(ctx, name); err != nil {
				log.Printf("Rate limit error for %s: %v", name, err)
				return
			}

			// Use circuit breaker
			circuitBreaker := er.circuitBreakers[name]
			err := circuitBreaker.Call(func() error {
				ticker, fetchErr := c.FetchTicker(symbol)
				if fetchErr != nil {
					er.updateHealthStatus(name, false, fetchErr)
					return fetchErr
				}

				mutex.Lock()
				results[name] = ticker
				mutex.Unlock()

				er.updateHealthStatus(name, true, nil)
				return nil
			})

			if err != nil {
				log.Printf("Failed to fetch ticker from %s: %v", name, err)
			}
		}(exchangeName, client)
	}

	wg.Wait()
	return results, nil
}

// updateHealthStatus updates the health status for an exchange
func (er *ExchangeRegistry) updateHealthStatus(exchangeName string, healthy bool, err error) {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	status, exists := er.healthStatus[exchangeName]
	if !exists {
		return
	}

	status.LastCheck = time.Now()
	status.Healthy = healthy

	if healthy {
		status.LastSuccess = time.Now()
		status.ErrorCount = 0
		status.LastError = ""
	} else {
		status.ErrorCount++
		if err != nil {
			status.LastError = err.Error()
		}
	}
}

// startHealthMonitoring starts the background health monitoring
func (er *ExchangeRegistry) startHealthMonitoring() {
	ticker := time.NewTicker(er.config.HealthCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		er.performHealthChecks()
	}
}

// performHealthChecks checks the health of all registered exchanges
func (er *ExchangeRegistry) performHealthChecks() {
	er.mutex.RLock()
	exchanges := make(map[string]*CCXTClient)
	for name, client := range er.exchanges {
		exchanges[name] = client
	}
	er.mutex.RUnlock()

	var wg sync.WaitGroup
	for exchangeName, client := range exchanges {
		wg.Add(1)
		go func(name string, c *CCXTClient) {
			defer wg.Done()

			start := time.Now()
			healthy := c.IsHealthy()
			responseTime := time.Since(start)

			// If not healthy, try a simple connection test
			if !healthy {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				err := c.testConnection()
				healthy = err == nil
			}

			er.updateHealthStatusWithTiming(name, healthy, responseTime)
		}(exchangeName, client)
	}
	wg.Wait()
}

// updateHealthStatusWithTiming updates health status with response time
func (er *ExchangeRegistry) updateHealthStatusWithTiming(exchangeName string, healthy bool, responseTime time.Duration) {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	status, exists := er.healthStatus[exchangeName]
	if !exists {
		return
	}

	status.LastCheck = time.Now()
	status.Healthy = healthy
	status.ResponseTime = responseTime

	// Calculate uptime percentage (simplified)
	if healthy {
		status.LastSuccess = time.Now()
		if status.ErrorCount > 0 {
			status.ErrorCount = max(0, status.ErrorCount-1) // Gradually reduce error count
		}
	} else {
		status.ErrorCount++
	}
}

// EnableExchange enables a previously disabled exchange
func (er *ExchangeRegistry) EnableExchange(exchangeName string) error {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	client, exists := er.exchanges[exchangeName]
	if !exists {
		return fmt.Errorf("exchange %s not registered", exchangeName)
	}

	// Re-enable the circuit breaker
	if circuitBreaker, exists := er.circuitBreakers[exchangeName]; exists {
		// Circuit breaker re-enabling is handled by the Call method
	}

	// Update health status
	if status, exists := er.healthStatus[exchangeName]; exists {
		status.Healthy = client.IsHealthy()
	}

	log.Printf("Enabled exchange: %s", exchangeName)
	return nil
}

// DisableExchange disables an exchange temporarily
func (er *ExchangeRegistry) DisableExchange(exchangeName string) error {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	if _, exists := er.exchanges[exchangeName]; !exists {
		return fmt.Errorf("exchange %s not registered", exchangeName)
	}

	if status, exists := er.healthStatus[exchangeName]; exists {
		status.Healthy = false
		status.LastError = "Manually disabled"
	}

	log.Printf("Disabled exchange: %s", exchangeName)
	return nil
}

// Close closes all exchange connections
func (er *ExchangeRegistry) Close() error {
	er.mutex.Lock()
	defer er.mutex.Unlock()

	var errors []error
	for name, client := range er.exchanges {
		if err := client.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close %s: %w", name, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors closing exchanges: %v", errors)
	}

	return nil
}

// GetAvailableExchanges returns a list of exchanges supported by CCXT
func GetAvailableExchanges() []string {
	return ccxt.Exchanges
}

// IsExchangeSupported checks if an exchange is supported by CCXT
func IsExchangeSupported(exchangeName string) bool {
	for _, exchange := range ccxt.Exchanges {
		if exchange == exchangeName {
			return true
		}
	}
	return false
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}