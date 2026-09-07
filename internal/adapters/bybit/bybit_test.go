package bybit

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
func TestBybit_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
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

func TestBybit_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestBybit_ToMarketTick(t *testing.T) {
	ts := time.Now()
	// Валидный тикер.
	mt, ok := toMarketTick(&tickerPayload{Symbol: "BTCUSDT", Bid1: "100", Ask1: "101", TurnOver: "500000"}, domain.MarketTypeFutures, ts)
	require.True(t, ok)
	require.Equal(t, "BYBIT", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.BestBid.Equal(decimal.NewFromInt(100)))
	require.True(t, mt.BestAsk.Equal(decimal.NewFromInt(101)))
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(500000)))
	// Нулевой bid.
	_, ok = toMarketTick(&tickerPayload{Symbol: "BTCUSDT", Bid1: "0", Ask1: "101"}, domain.MarketTypeFutures, ts)
	require.False(t, ok)
	// Перекрёстный рынок.
	_, ok = toMarketTick(&tickerPayload{Symbol: "BTCUSDT", Bid1: "102", Ask1: "101"}, domain.MarketTypeFutures, ts)
	require.False(t, ok)
	// Нечисловая котировка.
	_, ok = toMarketTick(&tickerPayload{Symbol: "BTCUSDT", Bid1: "abc", Ask1: "101"}, domain.MarketTypeFutures, ts)
	require.False(t, ok)
}
