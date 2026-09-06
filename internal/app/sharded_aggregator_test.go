package app

import (
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedAggregator_ShardRouting(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	funding := NewFundingManager(cfg)
	trackerChan := make(chan domain.SpreadEvent, 100)
	sa := NewShardedAggregator(trackerChan, funding, cfg)

	symbols := []string{
		"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT",
		"XRPUSDT", "ADAUSDT", "PEPEUSDT", "AVAXUSDT",
	}

	now := time.Now()

	for _, symbol := range symbols {
		symbol := symbol
		t.Run(fmt.Sprintf("routing_%s", symbol), func(t *testing.T) {
			expectedShard := int(crc32.ChecksumIEEE([]byte(symbol)) % numShards)

			// Process a Spot tick from BINANCE
			sa.ProcessTick(domain.MarketTick{
				Exchange:    "BINANCE",
				Symbol:      symbol,
				MarketType:  domain.MarketTypeSpot,
				BestBid:     decimal.RequireFromString("100"),
				BestAsk:     decimal.RequireFromString("102"),
				QuoteVolume: decimal.RequireFromString("50000"),
				Timestamp:   now,
			})

			// Process a Futures tick from BYBIT for the same symbol
			sa.ProcessTick(domain.MarketTick{
				Exchange:    "BYBIT",
				Symbol:      symbol,
				MarketType:  domain.MarketTypeFutures,
				BestBid:     decimal.RequireFromString("103"),
				BestAsk:     decimal.RequireFromString("105"),
				QuoteVolume: decimal.RequireFromString("60000"),
				Timestamp:   now,
			})

			// Target shard must contain prices for both exchanges
			targetShard := sa.shards[expectedShard]
			targetShard.mu.Lock()
			require.NotNil(t, targetShard.prices[symbol], "target shard must have prices for symbol")
			require.NotNil(t, targetShard.prices[symbol]["BINANCE"], "target shard must have BINANCE prices")
			require.NotNil(t, targetShard.prices[symbol]["BYBIT"], "target shard must have BYBIT prices")

			// Check Spot mid price: (100 + 102) / 2 = 101
			assert.True(t, targetShard.prices[symbol]["BINANCE"].SpotBid.Equal(decimal.RequireFromString("100")))
			assert.True(t, targetShard.prices[symbol]["BINANCE"].SpotAsk.Equal(decimal.RequireFromString("102")))
			// Check Futures mid price: (103 + 105) / 2 = 104
			assert.True(t, targetShard.prices[symbol]["BYBIT"].FuturesBid.Equal(decimal.RequireFromString("103")))
			assert.True(t, targetShard.prices[symbol]["BYBIT"].FuturesAsk.Equal(decimal.RequireFromString("105")))
			targetShard.mu.Unlock()

			// Check all OTHER shards to ensure no state leakage
			for i := 0; i < numShards; i++ {
				if i == expectedShard {
					continue
				}
				otherShard := sa.shards[i]
				otherShard.mu.Lock()
				assert.Nil(t, otherShard.prices[symbol], "other shard %d must not contain prices for %s", i, symbol)
				assert.Nil(t, otherShard.volumes[symbol], "other shard %d must not contain volume for %s", i, symbol)
				otherShard.mu.Unlock()
			}
		})
	}
}

func TestShardedAggregator_MinMaxSpreadCalculation(t *testing.T) {
	t.Parallel()

	t.Run("calculates correct min/max spread across 4 mock exchanges", func(t *testing.T) {
		t.Parallel()

		cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000")) // 1% hard limit
		funding := NewFundingManager(cfg)
		trackerChan := make(chan domain.SpreadEvent, 100)
		sa := NewShardedAggregator(trackerChan, funding, cfg)

		now := time.Now()
		symbol := "BTCUSDT"

		// 4 exchanges with different Futures prices:
		// BINANCE: 100.0
		// BYBIT:   102.0
		// OKX:     105.0 (Absolute Max)
		// KRAKEN:  98.0  (Absolute Min)
		exchanges := []struct {
			name    string
			bestBid decimal.Decimal
			bestAsk decimal.Decimal
		}{
			{"BINANCE", decimal.RequireFromString("99.9"), decimal.RequireFromString("100.1")}, // mid = 100
			{"BYBIT", decimal.RequireFromString("101.9"), decimal.RequireFromString("102.1")},  // mid = 102
			{"OKX", decimal.RequireFromString("104.9"), decimal.RequireFromString("105.1")},    // mid = 105
			{"KRAKEN", decimal.RequireFromString("97.9"), decimal.RequireFromString("98.1")},   // mid = 98
		}

		for _, ex := range exchanges {
			sa.ProcessTick(domain.MarketTick{
				Exchange:    ex.name,
				Symbol:      symbol,
				MarketType:  domain.MarketTypeFutures,
				BestBid:     ex.bestBid,
				BestAsk:     ex.bestAsk,
				QuoteVolume: decimal.RequireFromString("1000000"),
				Timestamp:   now,
			})
		}

		// Drain all emitted cross exchange events
		var crossEvents []domain.SpreadEvent
		for len(trackerChan) > 0 {
			ev := <-trackerChan
			if ev.SpreadType == domain.CrossExchange {
				crossEvents = append(crossEvents, ev)
			}
		}

		// At least 3 cross events emitted (after 2nd, 3rd, and 4th exchange ticks)
		require.NotEmpty(t, crossEvents)

		// The final cross event should reflect KRAKEN (98) as Min and OKX (105) as Max
		finalEvent := crossEvents[len(crossEvents)-1]
		assert.Equal(t, symbol, finalEvent.Symbol)
		assert.Equal(t, "KRAKEN", finalEvent.ExchangeA, "ExchangeA should be minimum exchange")
		assert.Equal(t, "OKX", finalEvent.ExchangeB, "ExchangeB should be maximum exchange")

		// Spread = (maxBid - minAsk) / minAsk = (104.9 - 98.1) / 98.1
		expectedSpread := decimal.RequireFromString("104.9").Sub(decimal.RequireFromString("98.1")).Div(decimal.RequireFromString("98.1"))
		assert.True(t, finalEvent.Spread.Equal(expectedSpread), "expected spread %s, got %s", expectedSpread, finalEvent.Spread)
	})

	t.Run("ignores spreads below hard min spread limit", func(t *testing.T) {
		t.Parallel()

		cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.05"), decimal.RequireFromString("1000")) // 5% hard limit
		funding := NewFundingManager(cfg)
		trackerChan := make(chan domain.SpreadEvent, 100)
		sa := NewShardedAggregator(trackerChan, funding, cfg)

		now := time.Now()
		symbol := "ETHUSDT"

		// Spread will be (101 - 100) / 100 = 1% < 5%
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeFutures,
			BestBid:     decimal.RequireFromString("100"),
			BestAsk:     decimal.RequireFromString("100"),
			QuoteVolume: decimal.RequireFromString("100000"),
			Timestamp:   now,
		})
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BYBIT",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeFutures,
			BestBid:     decimal.RequireFromString("101"),
			BestAsk:     decimal.RequireFromString("101"),
			QuoteVolume: decimal.RequireFromString("100000"),
			Timestamp:   now,
		})

		assert.Empty(t, trackerChan, "no event should be emitted when spread is below hard limit")
	})

	t.Run("does not emit cross exchange event with single exchange", func(t *testing.T) {
		t.Parallel()

		cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
		funding := NewFundingManager(cfg)
		trackerChan := make(chan domain.SpreadEvent, 100)
		sa := NewShardedAggregator(trackerChan, funding, cfg)

		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      "SOLUSDT",
			MarketType:  domain.MarketTypeFutures,
			BestBid:     decimal.RequireFromString("50"),
			BestAsk:     decimal.RequireFromString("50"),
			QuoteVolume: decimal.RequireFromString("100000"),
			Timestamp:   time.Now(),
		})

		assert.Empty(t, trackerChan, "single exchange cannot form a cross-exchange spread")
	})

	t.Run("calculates intra-exchange spread between Spot and Futures", func(t *testing.T) {
		t.Parallel()

		cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
		funding := NewFundingManager(cfg)
		trackerChan := make(chan domain.SpreadEvent, 100)
		sa := NewShardedAggregator(trackerChan, funding, cfg)

		now := time.Now()
		symbol := "DOGEUSDT"

		// Spot bid/ask = 0.10
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.RequireFromString("0.10"),
			BestAsk:     decimal.RequireFromString("0.10"),
			QuoteVolume: decimal.RequireFromString("50000"),
			Timestamp:   now,
		})

		// Futures bid/ask = 0.105 -> executable intra spread = (0.105 - 0.10)/0.10 = 0.05 (5%)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeFutures,
			BestBid:     decimal.RequireFromString("0.105"),
			BestAsk:     decimal.RequireFromString("0.105"),
			QuoteVolume: decimal.RequireFromString("50000"),
			Timestamp:   now,
		})

		var intraEvents []domain.SpreadEvent
		for len(trackerChan) > 0 {
			ev := <-trackerChan
			if ev.SpreadType == domain.IntraExchange {
				intraEvents = append(intraEvents, ev)
			}
		}

		require.NotEmpty(t, intraEvents)
		ev := intraEvents[len(intraEvents)-1]
		assert.Equal(t, "BINANCE", ev.ExchangeA)
		assert.Equal(t, "BINANCE", ev.ExchangeB)
		assert.True(t, ev.Spread.Equal(decimal.RequireFromString("0.05")))
	})

	t.Run("suppresses event when funding is unprofitable", func(t *testing.T) {
		t.Parallel()

		cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
		funding := NewFundingManager(cfg)
		trackerChan := make(chan domain.SpreadEvent, 100)
		sa := NewShardedAggregator(trackerChan, funding, cfg)

		now := time.Now()
		symbol := "XRPUSDT"

		// Set funding rate higher than spread: rate = 10%
		funding.UpdateFunding("BINANCE", symbol, decimal.RequireFromString("0.10"), now.Add(2*time.Hour))

		// Spread will be (103 - 100) / 100 = 3% < 10% funding
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeFutures,
			BestBid:     decimal.RequireFromString("100"),
			BestAsk:     decimal.RequireFromString("100"),
			QuoteVolume: decimal.RequireFromString("100000"),
			Timestamp:   now,
		})
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BYBIT",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeFutures,
			BestBid:     decimal.RequireFromString("103"),
			BestAsk:     decimal.RequireFromString("103"),
			QuoteVolume: decimal.RequireFromString("100000"),
			Timestamp:   now,
		})

		assert.Empty(t, trackerChan, "event should be suppressed when funding rate exceeds spread")
	})
}

func TestShardedAggregator_VolumeBucketing(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	funding := NewFundingManager(cfg)
	trackerChan := make(chan domain.SpreadEvent, 100)
	sa := NewShardedAggregator(trackerChan, funding, cfg)
	sa.staleWindow = 7 * 24 * time.Hour // historical fixtures are not "stale" here

	t0 := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	symbol := "BTCUSDT"

	shardIdx := int(crc32.ChecksumIEEE([]byte(symbol)) % numShards)
	shard := sa.shards[shardIdx]

	t.Run("calculates positive deltas and accumulates into minute buckets", func(t *testing.T) {
		// Tick 1: First tick sets LastQuoteVol = 1000 (no bucket updated)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.NewFromInt(100),
			BestAsk:     decimal.NewFromInt(100),
			QuoteVolume: decimal.NewFromInt(1000),
			Timestamp:   t0,
		})

		vol1 := sa.GetSymbolVolume(symbol, domain.TF_1m, t0)
		assert.True(t, vol1.IsZero(), "first tick should not generate bucket volume")

		// Tick 2: 15s later, QuoteVol = 1600 (Delta = 600)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.NewFromInt(100),
			BestAsk:     decimal.NewFromInt(100),
			QuoteVolume: decimal.NewFromInt(1600),
			Timestamp:   t0.Add(15 * time.Second),
		})

		vol2 := sa.GetSymbolVolume(symbol, domain.TF_1m, t0.Add(15*time.Second))
		assert.True(t, vol2.Equal(decimal.NewFromInt(600)), "expected volume 600, got %s", vol2)

		// Tick 3: 30s later, QuoteVol = 2100 (Delta = 500, total in minute = 1100)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.NewFromInt(100),
			BestAsk:     decimal.NewFromInt(100),
			QuoteVolume: decimal.NewFromInt(2100),
			Timestamp:   t0.Add(30 * time.Second),
		})

		vol3 := sa.GetSymbolVolume(symbol, domain.TF_1m, t0.Add(30*time.Second))
		assert.True(t, vol3.Equal(decimal.NewFromInt(1100)), "expected volume 1100, got %s", vol3)

		// Tick 4: 45s later, QuoteVol = 2100 (Delta = 0, no bucket increment)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.NewFromInt(100),
			BestAsk:     decimal.NewFromInt(100),
			QuoteVolume: decimal.NewFromInt(2100),
			Timestamp:   t0.Add(45 * time.Second),
		})

		vol4 := sa.GetSymbolVolume(symbol, domain.TF_1m, t0.Add(45*time.Second))
		assert.True(t, vol4.Equal(decimal.NewFromInt(1100)), "zero delta should not change bucket volume")

		// Tick 5: Next minute (t0 + 75s), QuoteVol = 3000 (Delta = 900)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.NewFromInt(100),
			BestAsk:     decimal.NewFromInt(100),
			QuoteVolume: decimal.NewFromInt(3000),
			Timestamp:   t0.Add(75 * time.Second),
		})

		// Query TF_1m at t0+75s: cutoff is (t0+1m).Add(-1m) = t0
		// Both minute buckets t0 and t0+1m are included (1100 + 900 = 2000)
		volTF1m := sa.GetSymbolVolume(symbol, domain.TF_1m, t0.Add(75*time.Second))
		assert.True(t, volTF1m.Equal(decimal.NewFromInt(2000)), "expected TF_1m volume 2000, got %s", volTF1m)

		// Query TF_5m: covers both minutes
		volTF5m := sa.GetSymbolVolume(symbol, domain.TF_5m, t0.Add(75*time.Second))
		assert.True(t, volTF5m.Equal(decimal.NewFromInt(2000)), "expected TF_5m volume 2000, got %s", volTF5m)
	})

	t.Run("prunes buckets older than 24 hours", func(t *testing.T) {
		shard.mu.Lock()
		volState := shard.volumes[symbol]
		require.NotNil(t, volState)

		// Manually inject an old bucket from 25 hours ago
		oldKey := t0.Add(-25 * time.Hour).Truncate(time.Minute)
		recentKey := t0.Add(-2 * time.Hour).Truncate(time.Minute)
		volState.Buckets[oldKey] = decimal.NewFromInt(5000)
		volState.Buckets[recentKey] = decimal.NewFromInt(3000)
		volState.LastPruneTime = t0
		shard.mu.Unlock()

		// Trigger tick at t0 + 2 minutes, which exceeds LastPruneTime by > 1 minute
		pruneTriggerTime := t0.Add(2 * time.Minute)
		sa.ProcessTick(domain.MarketTick{
			Exchange:    "BINANCE",
			Symbol:      symbol,
			MarketType:  domain.MarketTypeSpot,
			BestBid:     decimal.NewFromInt(100),
			BestAsk:     decimal.NewFromInt(100),
			QuoteVolume: decimal.NewFromInt(3500),
			Timestamp:   pruneTriggerTime,
		})

		shard.mu.Lock()
		defer shard.mu.Unlock()

		// Old bucket (> 24h) must be deleted
		_, oldExists := volState.Buckets[oldKey]
		assert.False(t, oldExists, "bucket older than 24 hours should be pruned")

		// Recent bucket (< 24h) must be preserved
		_, recentExists := volState.Buckets[recentKey]
		assert.True(t, recentExists, "bucket within 24 hours should be preserved")

		// LastPruneTime must be updated
		assert.Equal(t, pruneTriggerTime, volState.LastPruneTime)
	})
}

func TestShardedAggregator_VolumeTimeframes_And_EdgeCases(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	funding := NewFundingManager(cfg)
	trackerChan := make(chan domain.SpreadEvent, 100)
	sa := NewShardedAggregator(trackerChan, funding, cfg)
	sa.staleWindow = 7 * 24 * time.Hour // historical fixtures are not "stale" here

	t0 := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	// 1. Non-existent symbol volume query
	volZero := sa.GetSymbolVolume("NON_EXISTENT_SYMBOL", domain.TF_15m, t0)
	assert.True(t, volZero.IsZero(), "non-existent symbol should return zero volume")

	// 2. Test all timeframes in getSymbolVolumeInternal
	symbol := "TIME_COIN"
	// Init volume
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: symbol, MarketType: domain.MarketTypeSpot,
		BestBid: decimal.NewFromInt(10), BestAsk: decimal.NewFromInt(10),
		QuoteVolume: decimal.NewFromInt(1000), Timestamp: t0,
	})
	// Delta 500
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: symbol, MarketType: domain.MarketTypeSpot,
		BestBid: decimal.NewFromInt(10), BestAsk: decimal.NewFromInt(10),
		QuoteVolume: decimal.NewFromInt(1500), Timestamp: t0.Add(30 * time.Second),
	})

	allTFs := []domain.Timeframe{
		domain.TF_1m, domain.TF_5m, domain.TF_15m, domain.TF_30m,
		domain.TF_1h, domain.TF_4h, domain.TF_24h, domain.Timeframe("UNKNOWN_TF"),
	}
	for _, tf := range allTFs {
		v := sa.GetSymbolVolume(symbol, tf, t0.Add(30*time.Second))
		assert.True(t, v.Equal(decimal.NewFromInt(500)), "expected volume 500 for timeframe %s, got %s", tf, v)
	}

	// 3. sendEvent delivers without silent drop when a reader is present.
	fullChan := make(chan domain.SpreadEvent, 1)
	saFull := NewShardedAggregator(fullChan, funding, cfg)

	delivered := make(chan domain.SpreadEvent, 1)
	go func() {
		saFull.sendEvent(&domain.SpreadEvent{Symbol: "DELIVERED"})
		delivered <- <-fullChan
	}()

	select {
	case got := <-delivered:
		assert.Equal(t, "DELIVERED", got.Symbol)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sendEvent did not deliver event")
	}

	// 4. Intra-exchange with missing/zero prices
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BINANCE", Symbol: "ZERO_TEST", MarketType: domain.MarketTypeSpot,
		BestBid: decimal.Zero, BestAsk: decimal.Zero, QuoteVolume: decimal.NewFromInt(100),
		Timestamp: t0,
	})
	// No intra event
	assert.Empty(t, trackerChan)

	// Intra-exchange with spread below hardMinSpread (0.1% vs 1% hard limit)
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BYBIT", Symbol: "LOW_INTRA", MarketType: domain.MarketTypeSpot,
		BestBid: decimal.NewFromInt(1000), BestAsk: decimal.NewFromInt(1000),
		QuoteVolume: decimal.NewFromInt(100), Timestamp: t0,
	})
	sa.ProcessTick(domain.MarketTick{
		Exchange: "BYBIT", Symbol: "LOW_INTRA", MarketType: domain.MarketTypeFutures,
		BestBid: decimal.RequireFromString("1001"), BestAsk: decimal.RequireFromString("1001"),
		QuoteVolume: decimal.NewFromInt(100), Timestamp: t0,
	})
	assert.Empty(t, trackerChan, "intra spread below hard limit should not emit event")
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
