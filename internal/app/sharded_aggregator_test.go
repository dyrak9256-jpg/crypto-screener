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

func TestShardedAggregator_BidAskValidation(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	funding := NewFundingManager(cfg)
	trackerChan := make(chan domain.SpreadEvent, 100)
	sa := NewShardedAggregator(trackerChan, funding, cfg)
	now := time.Now()

	// Invalid: bid > ask.
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: "INVALID1", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("200"), BestAsk: decimal.RequireFromString("100"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: now,
	})
	// Invalid: non-positive bid/ask.
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: "INVALID2", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.Zero, BestAsk: decimal.RequireFromString("100"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: now,
	})
	assert.Empty(t, trackerChan, "invalid ticks must be rejected before any spread calc")

	// Valid cross-exchange (BINANCE vs BYBIT futures) should emit an event.
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: "VALID", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("99.9"), BestAsk: decimal.RequireFromString("100.1"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: now,
	})
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BYBIT", Symbol: "VALID", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("104.9"), BestAsk: decimal.RequireFromString("105.1"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: now,
	})
	assert.NotEmpty(t, trackerChan, "valid ticks must produce a cross-exchange event")
}

func TestShardedAggregator_StaleTicksExcluded(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	funding := NewFundingManager(cfg)
	trackerChan := make(chan domain.SpreadEvent, 100)
	sa := NewShardedAggregator(trackerChan, funding, cfg)
	now := time.Now()

	// Stale timestamp (well beyond staleWindow) must be excluded.
	stale := now.Add(-2 * staleWindow)
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: "STALE", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("99.9"), BestAsk: decimal.RequireFromString("100.1"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: stale,
	})
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BYBIT", Symbol: "STALE", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("104.9"), BestAsk: decimal.RequireFromString("105.1"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: stale,
	})
	assert.Empty(t, trackerChan, "stale ticks must be excluded from arbitrage")
}

func TestShardedAggregator_PerExchangeFundingFilter(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	funding := NewFundingManager(cfg)
	trackerChan := make(chan domain.SpreadEvent, 100)
	sa := NewShardedAggregator(trackerChan, funding, cfg)
	now := time.Now()

	// Funding on an UNRELATED exchange (OKX) with an enormous rate must NOT
	// suppress the cross-exchange signal between BINANCE and BYBIT.
	funding.UpdateFunding("OKX", "BTCUSDT", decimal.RequireFromString("0.5"), now.Add(2*time.Hour))

	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("99.9"), BestAsk: decimal.RequireFromString("100.1"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: now,
	})
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BYBIT", Symbol: "BTCUSDT", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("104.9"), BestAsk: decimal.RequireFromString("105.1"),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: now,
	})

	found := false
	for len(trackerChan) > 0 {
		ev := <-trackerChan
		if ev.SpreadType == domain.CrossExchange {
			found = true
		}
	}
	assert.True(t, found, "cross-exchange signal must NOT be suppressed by funding on an unrelated exchange")
}
