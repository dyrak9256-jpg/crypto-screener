package bitget

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
func TestBitget_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
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

func TestBitget_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestBitget_ToMarketTick(t *testing.T) {
	ts := time.Now()
	mt, ok := toMarketTick(&tickerData{InstID: "BTCUSDT", BidPr: "100", AskPr: "101", QuoteVol: "1000"}, domain.MarketTypeSpot, ts)
	require.True(t, ok)
	require.Equal(t, "BITGET", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(1000)))
	// Символ с дефисом нормализуется: BTC-USDT → BTCUSDT.
	mt, ok = toMarketTick(&tickerData{InstID: "BTC-USDT", BidPr: "100", AskPr: "101", QuoteVol: "1000"}, domain.MarketTypeSpot, ts)
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	// Нулевой bid / перекрёстный рынок / битый объём — отбраковка.
	_, ok = toMarketTick(&tickerData{InstID: "BTCUSDT", BidPr: "0", AskPr: "101", QuoteVol: "1"}, domain.MarketTypeSpot, ts)
	require.False(t, ok)
	_, ok = toMarketTick(&tickerData{InstID: "BTCUSDT", BidPr: "102", AskPr: "101", QuoteVol: "1"}, domain.MarketTypeSpot, ts)
	require.False(t, ok)
	_, ok = toMarketTick(&tickerData{InstID: "BTCUSDT", BidPr: "100", AskPr: "101", QuoteVol: "n/a"}, domain.MarketTypeSpot, ts)
	require.False(t, ok)
}
