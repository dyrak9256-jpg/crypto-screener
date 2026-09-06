package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func tick(ex string, mt domain.MarketType, bid, ask string, ts time.Time) domain.MarketTick {
	return domain.MarketTick{Exchange: ex, Symbol: "BTCUSDT", MarketType: mt, BestBid: decimal.RequireFromString(bid), BestAsk: decimal.RequireFromString(ask), QuoteVolume: decimal.RequireFromString("1000000"), EventTime: ts, ReceivedAt: ts, Timestamp: ts}
}

func TestShardedAggregator_AllProfitableCrossExchangePairs(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	require.Len(t, ch, 1)
	ev := <-ch
	require.Equal(t, domain.CrossExchange, ev.SpreadType)
	require.Equal(t, "BINANCE", ev.BuyExchange)
	require.Equal(t, "BYBIT", ev.SellExchange)
	require.Equal(t, domain.SignalOpened, ev.Lifecycle)
	require.True(t, ev.Spread.Equal(decimal.RequireFromString("0.02")))
}

func TestShardedAggregator_ClosesWhenSpreadDisappears(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	<-ch
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "99", "100", now.Add(time.Second)))
	require.NotEmpty(t, ch)
	ev := <-ch
	require.Equal(t, domain.SignalClosed, ev.Lifecycle)
}

func TestShardedAggregator_IntraWorksWithoutFundingFeed(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeSpot, "100", "100", now))
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "105", "105", now))
	require.NotEmpty(t, ch)
	ev := <-ch
	require.Equal(t, domain.IntraExchange, ev.SpreadType)
	require.Equal(t, "BINANCE", ev.BuyExchange)
	require.Equal(t, "BINANCE", ev.SellExchange)
}
