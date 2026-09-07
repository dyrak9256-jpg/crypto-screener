package bingx

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
func TestBingx_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
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

func TestBingx_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestBingx_ToMarketTick(t *testing.T) {
	ts := time.Now()
	// Спот: поля b/a.
	mt, ok := toMarketTick(&tickerData{Symbol: "BTC-USDT", BidPr: "100", AskPr: "101", QVolume: "5000"}, domain.MarketTypeSpot, ts)
	require.True(t, ok)
	require.Equal(t, "BINGX", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(5000)))
	// Фьючерс: поля bidPrice/askPrice.
	mt, ok = toMarketTick(&tickerData{Symbol: "BTC-USDT", BidPrice: "100", AskPrice: "101", QVolume: "5000"}, domain.MarketTypeFutures, ts)
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	// Не-USDT символ отбрасывается.
	_, ok = toMarketTick(&tickerData{Symbol: "BTC-ETH", BidPr: "100", AskPr: "101", QVolume: "1"}, domain.MarketTypeSpot, ts)
	require.False(t, ok)
	// Перекрёстный рынок.
	_, ok = toMarketTick(&tickerData{Symbol: "BTC-USDT", BidPr: "102", AskPr: "101", QVolume: "1"}, domain.MarketTypeSpot, ts)
	require.False(t, ok)
}
