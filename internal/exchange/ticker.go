package exchange

import (
	"time"
)

// Ticker represents normalized ticker data across all exchanges
type Ticker struct {
	Exchange  string    `json:"exchange"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    float64   `json:"volume"`
	Bid       float64   `json:"bid"`
	Ask       float64   `json:"ask"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Change    float64   `json:"change"`
	Percent   float64   `json:"percent"`
	Timestamp time.Time `json:"timestamp"`
}

// ExchangeClient defines the interface for exchange interactions
type ExchangeClient interface {
	FetchTickers(symbols []string) ([]Ticker, error)
	FetchTicker(symbol string) (*Ticker, error)
	GetExchangeName() string
	GetSupportedSymbols() ([]string, error)
	IsHealthy() bool
}

// ExchangeInfo contains metadata about an exchange
type ExchangeInfo struct {
	Name         string   `json:"name"`
	DisplayName  string   `json:"display_name"`
	Supported    bool     `json:"supported"`
	LastChecked  time.Time `json:"last_checked"`
	ErrorMessage string   `json:"error_message,omitempty"`
}

// NormalizeSymbol converts different symbol formats to a standard format
// e.g., "BTC/USDT", "BTCUSDT", "BTC-USDT" -> "BTC/USDT"
func NormalizeSymbol(symbol string) string {
	// Basic normalization - can be expanded as needed
	if len(symbol) == 0 {
		return symbol
	}

	// Convert to uppercase
	symbol = strings.ToUpper(symbol)

	// Replace common separators with "/"
	symbol = strings.ReplaceAll(symbol, "-", "/")
	symbol = strings.ReplaceAll(symbol, "_", "/")

	// If no separator, try to infer one (e.g., BTCUSDT -> BTC/USDT)
	if !strings.Contains(symbol, "/") && len(symbol) >= 6 {
		// Common quote currencies
		quoteCurrencies := []string{"USDT", "USDC", "BUSD", "USD", "EUR", "BTC", "ETH"}
		for _, quote := range quoteCurrencies {
			if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
				base := symbol[:len(symbol)-len(quote)]
				symbol = base + "/" + quote
				break
			}
		}
	}

	return symbol
}