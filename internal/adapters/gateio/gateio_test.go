package gateio

import (
	"context"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Connect* методы после H1-фикса БЛОКИРУЮТСЯ на время жизни соединения.
// Предотменённый контекст должен заставить их вернуться сразу (без сети и без зависания).
func TestGateio_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
	adapter := NewAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до вызова => процессы должны сразу выйти

	tickChan := make(chan domain.MarketTick, 1)

	done := make(chan error, 3)
	go func() { done <- adapter.ConnectSpot(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFutures(ctx, tickChan) }()
	go func() { done <- adapter.ConnectFunding(ctx, nil) }()

	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			_ = err // prompt return is the contract; a cancelled-ctx dial error is expected
		case <-time.After(2 * time.Second):
			t.Fatalf("Connect* did not return promptly on cancelled context")
		}
	}
}

func TestGateio_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestGateio_TickerToTick(t *testing.T) {
	ts := time.Now()
	// Спот.
	mt, ok := spotTickerToTick(&tickerData{CurrencyPair: "BTC_USDT", HighestBid: "100", LowestAsk: "101", QuoteVolume: "3000"}, ts)
	require.True(t, ok)
	require.Equal(t, "GATEIO", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(3000)))
	// Фьючерс: BTC_USDT → BTCUSDT.
	mt, ok = futuresTickerToTick(&futuresTickerData{Contract: "BTC_USDT", Bid1: "100", Ask1: "101", Volume: "4000"}, ts)
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(4000)))
	// Нулевой bid и перекрёстный рынок отбраковываются на обоих рынках.
	_, ok = spotTickerToTick(&tickerData{CurrencyPair: "BTC_USDT", HighestBid: "0", LowestAsk: "101", QuoteVolume: "1"}, ts)
	require.False(t, ok)
	_, ok = futuresTickerToTick(&futuresTickerData{Contract: "BTC_USDT", Bid1: "102", Ask1: "101", Volume: "1"}, ts)
	require.False(t, ok)
}
