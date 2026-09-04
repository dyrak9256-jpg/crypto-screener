package app

import (
	"crypto-screener/internal/domain"
	"hash/crc32"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const numShards = 1024

type PriceState struct{ Spot, Futures decimal.Decimal }

type SymbolVolumeState struct {
	LastQuoteVol  decimal.Decimal
	Buckets       map[time.Time]decimal.Decimal
	LastPruneTime time.Time
}

type Shard struct {
	mu      sync.Mutex
	prices  map[string]map[string]*PriceState
	volumes map[string]*SymbolVolumeState
}

type ShardedAggregator struct {
	shards      [numShards]*Shard
	trackerChan chan<- domain.SpreadEvent
	funding     *FundingManager
	config      *domain.ScreenerConfig
}

func NewShardedAggregator(trackerChan chan<- domain.SpreadEvent, funding *FundingManager, cfg *domain.ScreenerConfig) *ShardedAggregator {
	sa := &ShardedAggregator{shards: [numShards]*Shard{}, trackerChan: trackerChan, funding: funding, config: cfg}
	for i := 0; i < numShards; i++ {
		sa.shards[i] = &Shard{prices: make(map[string]map[string]*PriceState), volumes: make(map[string]*SymbolVolumeState)}
	}
	return sa
}

func (sa *ShardedAggregator) ProcessTick(tick domain.MarketTick) {
	shardIdx := int(crc32.ChecksumIEEE([]byte(tick.Symbol)) % numShards)
	shard := sa.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.prices[tick.Symbol] == nil {
		shard.prices[tick.Symbol] = make(map[string]*PriceState)
	}
	if shard.prices[tick.Symbol][tick.Exchange] == nil {
		shard.prices[tick.Symbol][tick.Exchange] = &PriceState{}
	}

	midPrice := tick.BestBid.Add(tick.BestAsk).Div(decimal.NewFromInt(2))
	state := shard.prices[tick.Symbol][tick.Exchange]
	if tick.MarketType == domain.MarketTypeSpot {
		state.Spot = midPrice
	} else {
		state.Futures = midPrice
	}

	sa.updateVolumeState(shard, tick.Symbol, tick.QuoteVolume, tick.Timestamp)

	if !sa.config.IsPairEnabled(tick.Symbol) {
		return
	}
	currentVol := sa.getSymbolVolume(shard, tick.Symbol, sa.config.GetUserTimeframe(), tick.Timestamp)
	if currentVol.LessThan(sa.config.GetEffectiveMinVolume()) {
		return
	}

	sa.calculateCrossExchange(tick.Symbol, shard.prices[tick.Symbol], tick.Timestamp)
	sa.calculateIntraExchange(tick.Symbol, tick.Exchange, state, tick.Timestamp)
}

func (sa *ShardedAggregator) updateVolumeState(shard *Shard, symbol string, currentQVol decimal.Decimal, ts time.Time) {
	if shard.volumes[symbol] == nil {
		shard.volumes[symbol] = &SymbolVolumeState{Buckets: make(map[time.Time]decimal.Decimal)}
	}
	volState := shard.volumes[symbol]

	if !volState.LastQuoteVol.IsZero() {
		delta := currentQVol.Sub(volState.LastQuoteVol)
		if delta.IsPositive() {
			minuteKey := ts.Truncate(time.Minute)
			volState.Buckets[minuteKey] = volState.Buckets[minuteKey].Add(delta)
		}
	}
	volState.LastQuoteVol = currentQVol

	if ts.Sub(volState.LastPruneTime) > time.Minute {
		cutoff := ts.Add(-24 * time.Hour)
		for k := range volState.Buckets {
			if k.Before(cutoff) {
				delete(volState.Buckets, k)
			}
		}
		volState.LastPruneTime = ts
	}
}

func (sa *ShardedAggregator) getSymbolVolume(shard *Shard, symbol string, tf domain.Timeframe, ts time.Time) decimal.Decimal {
	volState, exists := shard.volumes[symbol]
	if !exists {
		return decimal.Zero
	}

	var duration time.Duration
	switch tf {
	case domain.TF_1m:
		duration = time.Minute
	case domain.TF_5m:
		duration = 5 * time.Minute
	case domain.TF_15m:
		duration = 15 * time.Minute
	case domain.TF_30m:
		duration = 30 * time.Minute
	case domain.TF_1h:
		duration = time.Hour
	case domain.TF_4h:
		duration = 4 * time.Hour
	default:
		duration = 24 * time.Hour
	}

	cutoff := ts.Truncate(time.Minute).Add(-duration)
	totalVol := decimal.Zero
	for k, v := range volState.Buckets {
		if k.After(cutoff) || k.Equal(cutoff) {
			totalVol = totalVol.Add(v)
		}
	}
	return totalVol
}

func (sa *ShardedAggregator) calculateCrossExchange(symbol string, exchanges map[string]*PriceState, ts time.Time) {
	var minPrice, maxPrice decimal.Decimal
	var minEx, maxEx string
	first := true

	for ex, state := range exchanges {
		if state.Futures.IsZero() {
			continue
		}
		if first {
			minPrice, maxPrice = state.Futures, state.Futures
			minEx, maxEx = ex, ex
			first = false
			continue
		}
		if state.Futures.LessThan(minPrice) {
			minPrice = state.Futures
			minEx = ex
		}
		if state.Futures.GreaterThan(maxPrice) {
			maxPrice = state.Futures
			maxEx = ex
		}
	}
	if first {
		return
	}

	spread := maxPrice.Sub(minPrice).Div(minPrice)
	if spread.GreaterThanOrEqual(sa.config.GetEffectiveCrossSpread()) {
		if sa.funding.IsArbProfitable(symbol, spread, ts) {
			sa.sendEventNonBlocking(domain.SpreadEvent{Symbol: symbol, SpreadType: domain.CrossExchange, Spread: spread, ExchangeA: minEx, ExchangeB: maxEx, Timestamp: ts})
		}
	}
}

func (sa *ShardedAggregator) calculateIntraExchange(symbol, exchange string, state *PriceState, ts time.Time) {
	if state.Spot.IsZero() || state.Futures.IsZero() {
		return
	}
	spread := state.Futures.Sub(state.Spot).Abs().Div(state.Spot)

	if spread.GreaterThanOrEqual(sa.config.GetEffectiveIntraSpread()) {
		if sa.funding.IsArbProfitable(symbol, spread, ts) {
			sa.sendEventNonBlocking(domain.SpreadEvent{Symbol: symbol, SpreadType: domain.IntraExchange, Spread: spread, ExchangeA: exchange, ExchangeB: exchange, Timestamp: ts})
		}
	}
}

func (sa *ShardedAggregator) sendEventNonBlocking(event domain.SpreadEvent) {
	select {
	case sa.trackerChan <- event:
	default:
	}
}
