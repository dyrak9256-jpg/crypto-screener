package app

import (
	"hash/crc32"
	"sync"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

const numShards = 1024

// staleWindow — максимально допустимый возраст тика. Данные старше считаются
// устаревшими и исключаются из расчёта (нельзя арбитражить по устаревшей цене).
const staleWindow = 15 * time.Second

// intervalVolumeTF — таймфрейм, по которому считается "интервальный" объём,
// попадающий в SpreadEvent.QuoteVolume. Это НЕ 24h-rolling с биржи: объём
// выводcтся из накопленных минутных дельт (см. getSymbolVolumeInternal).
const intervalVolumeTF = domain.TF_5m

// PriceState хранит ЛУЧШИЕ цены спроса/предложения отдельно для спота и фьючерсов.
// Mid-price больше НЕ используется для исполняемого спреда: спред считается по
// bid/ask-маршруту (купля по ask, продажа по bid).
type PriceState struct {
	SpotBid    decimal.Decimal
	SpotAsk    decimal.Decimal
	FuturesBid decimal.Decimal
	FuturesAsk decimal.Decimal
}

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
	// staleWindow — максимальный допустимый возраст тика (переопределяется в тестах).
	staleWindow time.Duration
}

func NewShardedAggregator(trackerChan chan<- domain.SpreadEvent, funding *FundingManager, cfg *domain.ScreenerConfig) *ShardedAggregator {
	sa := &ShardedAggregator{shards: [numShards]*Shard{}, trackerChan: trackerChan, funding: funding, config: cfg}
	for i := 0; i < numShards; i++ {
		sa.shards[i] = &Shard{prices: make(map[string]map[string]*PriceState), volumes: make(map[string]*SymbolVolumeState)}
	}
	sa.staleWindow = staleWindow
	return sa
}

// ProcessTick валидирует и нормализует входящий тик, затем проверяет спреды.
func (sa *ShardedAggregator) ProcessTick(tick domain.MarketTick) {
	// Валидация bid/ask: bid>0, ask>0, bid<=ask. Некорректные тики отбрасываются.
	if !tick.BestBid.IsPositive() || !tick.BestAsk.IsPositive() || tick.BestBid.GreaterThan(tick.BestAsk) {
		return
	}
	// Исключение устаревших данных.
	if now := time.Now(); now.Sub(tick.Timestamp) > sa.staleWindow {
		return
	}

	shardIdx := int(crc32.ChecksumIEEE([]byte(tick.Symbol)) % numShards)
	shard := sa.shards[shardIdx]

	shard.mu.Lock()

	if shard.prices[tick.Symbol] == nil {
		shard.prices[tick.Symbol] = make(map[string]*PriceState)
	}
	if shard.prices[tick.Symbol][tick.Exchange] == nil {
		shard.prices[tick.Symbol][tick.Exchange] = &PriceState{}
	}

	state := shard.prices[tick.Symbol][tick.Exchange]
	if tick.MarketType == domain.MarketTypeSpot {
		state.SpotBid = tick.BestBid
		state.SpotAsk = tick.BestAsk
	} else {
		state.FuturesBid = tick.BestBid
		state.FuturesAsk = tick.BestAsk
	}

	sa.updateVolumeState(shard, tick.Symbol, tick.QuoteVolume, tick.Timestamp)

	if !sa.config.IsPairEnabled(tick.Symbol) {
		shard.mu.Unlock()
		return
	}

	// Интервальный объём (из минутных дельт), а НЕ 24h-rolling с биржи.
	intervalVol := sa.getSymbolVolumeInternal(shard, tick.Symbol, intervalVolumeTF, tick.Timestamp)

	// Считаем события УДЕРЖИВАЯ мьютекс, но отправляем ПОСЛЕ снятия —
	// не блокируем запись в канал под блокировкой на shard.
	cross := sa.calculateCrossExchange(tick.Symbol, shard.prices[tick.Symbol], tick.Timestamp, intervalVol)
	intra := sa.calculateIntraExchange(tick.Symbol, tick.Exchange, state, tick.Timestamp, intervalVol)
	shard.mu.Unlock()

	if cross != nil {
		sa.sendEvent(cross)
	}
	if intra != nil {
		sa.sendEvent(intra)
	}
}

// GetSymbolVolume реализует интерфейс domain.VolumeProvider
func (sa *ShardedAggregator) GetSymbolVolume(symbol string, tf domain.Timeframe, ts time.Time) decimal.Decimal {
	shardIdx := int(crc32.ChecksumIEEE([]byte(symbol)) % numShards)
	shard := sa.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	return sa.getSymbolVolumeInternal(shard, symbol, tf, ts)
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

func (sa *ShardedAggregator) getSymbolVolumeInternal(shard *Shard, symbol string, tf domain.Timeframe, ts time.Time) decimal.Decimal {
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

// calculateCrossExchange считает ИСПОЛНЯЕМЫЙ межбиржевой спред по bid/ask:
// мы покупаем фьючерс на бирже с минимальным ask и продаём на бирже с
// максимальным bid. Спред = (maxBid - minAsk) / minAsk.
func (sa *ShardedAggregator) calculateCrossExchange(symbol string, exchanges map[string]*PriceState, ts time.Time, qVol decimal.Decimal) *domain.SpreadEvent {
	var minAsk, maxBid decimal.Decimal
	var buyEx, sellEx string
	first := true

	for ex, state := range exchanges {
		if state.FuturesBid.IsZero() || state.FuturesAsk.IsZero() {
			continue
		}
		if first {
			minAsk, maxBid = state.FuturesAsk, state.FuturesBid
			buyEx, sellEx = ex, ex
			first = false
			continue
		}
		if state.FuturesAsk.LessThan(minAsk) {
			minAsk = state.FuturesAsk
			buyEx = ex
		}
		if state.FuturesBid.GreaterThan(maxBid) {
			maxBid = state.FuturesBid
			sellEx = ex
		}
	}
	if first || minAsk.IsZero() {
		return nil
	}

	spread := maxBid.Sub(minAsk).Div(minAsk)

	if spread.GreaterThanOrEqual(sa.config.GetHardMinSpread()) {
		if sa.funding.IsArbProfitable(symbol, spread, ts, buyEx, sellEx) {
			return &domain.SpreadEvent{
				Symbol: symbol, SpreadType: domain.CrossExchange, Spread: spread,
				ExchangeA: buyEx, ExchangeB: sellEx, QuoteVolume: qVol, Timestamp: ts,
			}
		}
	}
	return nil
}

// calculateIntraExchange считает ИСПОЛНЯЕМЫЙ внутрибиржевой спред (базис) по
// bid/ask: берём положительный (арбитражируемый) вариант — либо продать
// фьючерс/купить спот, либо продать спот/купить фьючерс.
func (sa *ShardedAggregator) calculateIntraExchange(symbol, exchange string, state *PriceState, ts time.Time, qVol decimal.Decimal) *domain.SpreadEvent {
	if state.SpotBid.IsZero() || state.SpotAsk.IsZero() || state.FuturesBid.IsZero() || state.FuturesAsk.IsZero() {
		return nil
	}

	// Продать фьючерс (по FuturesBid), купить спот (по SpotAsk).
	spreadFutPremium := state.FuturesBid.Sub(state.SpotAsk).Div(state.SpotAsk)
	// Продать спот (по SpotBid), купить фьючерс (по FuturesAsk).
	spreadSpotPremium := state.SpotBid.Sub(state.FuturesAsk).Div(state.FuturesAsk)

	spread := spreadFutPremium
	if spreadSpotPremium.GreaterThan(spread) {
		spread = spreadSpotPremium
	}
	if spread.IsNegative() {
		return nil
	}

	if spread.GreaterThanOrEqual(sa.config.GetHardMinSpread()) {
		if sa.funding.IsArbProfitable(symbol, spread, ts, exchange) {
			return &domain.SpreadEvent{
				Symbol: symbol, SpreadType: domain.IntraExchange, Spread: spread,
				ExchangeA: exchange, ExchangeB: exchange, QuoteVolume: qVol, Timestamp: ts,
			}
		}
	}
	return nil
}

// sendEvent отправляет событие в трекер БЛОКИРУЮЩЕ: событие никогда не теряется
// молча. Вызывается только ПОСЛЕ снятия мьютекса на shard.
func (sa *ShardedAggregator) sendEvent(event *domain.SpreadEvent) {
	sa.trackerChan <- *event
}
