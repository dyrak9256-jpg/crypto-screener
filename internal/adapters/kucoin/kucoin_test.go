package kucoin

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
func TestKucoin_ConnectMethods_ReturnPromptlyOnCanceledContext(t *testing.T) {
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

func TestKucoin_ImplementsExchangeConnector(t *testing.T) {
	var _ domain.ExchangeConnector = NewAdapter()
	assert.NotNil(t, NewAdapter())
}

func TestKucoin_ToMarketTick(t *testing.T) {
	// Спот: BTC-USDT → BTCUSDT.
	mt, ok := toMarketTick(&tickerData{Symbol: "BTC-USDT", BestBid: "100", BestAsk: "101", VolValue: "2000"}, domain.MarketTypeSpot)
	require.True(t, ok)
	require.Equal(t, "KUCOIN", mt.Exchange)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(2000)))
	// Фьючерс: XBTUSDTM → BTCUSDT (XBT — старое обозначение Bitcoin).
	mt, ok = toMarketTick(&tickerData{Symbol: "XBTUSDTM", BestBidPrice: "100", BestAskPrice: "101", VolValue: "2000"}, domain.MarketTypeFutures)
	require.True(t, ok)
	require.Equal(t, "BTCUSDT", mt.Symbol)
	// Фьючерс с пустыми BestBid/BestAsk использует поля *Price.
	mt, ok = toMarketTick(&tickerData{Symbol: "ETHUSDTM", BestBid: "0", BestAsk: "0", BestBidPrice: "50", BestAskPrice: "51", Turnover24h: "777"}, domain.MarketTypeFutures)
	require.True(t, ok)
	require.True(t, mt.BestBid.Equal(decimal.NewFromInt(50)))
	require.True(t, mt.QuoteVolume.Equal(decimal.NewFromInt(777)))
	// Квартальный контракт (не perpetual) отбрасывается.
	_, ok = toMarketTick(&tickerData{Symbol: "XBTMM24", BestBid: "100", BestAsk: "101", VolValue: "1"}, domain.MarketTypeFutures)
	require.False(t, ok)
	// Перекрёстный рынок.
	_, ok = toMarketTick(&tickerData{Symbol: "BTC-USDT", BestBid: "102", BestAsk: "101", VolValue: "1"}, domain.MarketTypeSpot)
	require.False(t, ok)
}
