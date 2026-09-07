package app

import (
	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func tick(ex string, mt domain.MarketType, bid, ask string, ts time.Time) domain.MarketTick {
	return domain.MarketTick{Exchange: ex, Symbol: "BTCUSDT", MarketType: mt, BestBid: decimal.RequireFromString(bid), BestAsk: decimal.RequireFromString(ask), QuoteVolume: decimal.RequireFromString("1000000"), EventTime: ts, ReceivedAt: ts, Timestamp: ts}
}
func seedFunding(t *testing.T, fm *FundingManager, now time.Time) {
	t.Helper()
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.RequireFromString("0.001"), now.Add(time.Hour), now))
	require.NoError(t, fm.UpdateFunding("BYBIT", "BTCUSDT", decimal.RequireFromString("0.001"), now.Add(time.Hour), now))
}
func TestShardedAggregator_AllProfitableCrossExchangePairs(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	require.Len(t, ch, 1)
	ev := <-ch
	require.Equal(t, domain.CrossExchange, ev.SpreadType)
	require.Equal(t, "BINANCE", ev.BuyExchange)
	require.Equal(t, "BYBIT", ev.SellExchange)
	require.True(t, ev.Spread.Equal(decimal.RequireFromString("0.021")))
}
func TestShardedAggregator_ClosesWhenSpreadDisappears(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	<-ch
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "99", "100", now.Add(time.Second)))
	require.NotEmpty(t, ch)
	ev := <-ch
	require.Equal(t, domain.SignalClosed, ev.Lifecycle)
}
func TestShardedAggregator_IntraRequiresFundingAndIsSpotLongOnly(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeSpot, "100", "100", now))
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "105", "105", now))
	require.Empty(t, ch)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.RequireFromString("0.001"), now.Add(time.Hour), now))
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "105", "105", now.Add(time.Second)))
	require.NotEmpty(t, ch)
	ev := <-ch
	require.Equal(t, domain.IntraExchange, ev.SpreadType)
	require.Equal(t, domain.MarketTypeSpot, ev.BuyMarket)
	require.Equal(t, domain.MarketTypeFutures, ev.SellMarket)
}
