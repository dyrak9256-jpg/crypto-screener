package app

import (
	"hash/crc32"
	"strings"
	"sync"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
)

const (
	numShards       = 1024
	defaultPriceTTL = 5 * time.Second
)

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
	Bid, Ask       decimal.Decimal
	ReceivedAt     time.Time
	QuoteVolume24h decimal.Decimal
}

type routeState struct {
	peak     decimal.Decimal
	last     domain.SpreadEvent
	openedAt time.Time
}

type Shard struct {
	mu     sync.Mutex
	prices map[string]map[string]map[domain.MarketType]*PriceState
	active map[string]*routeState
}

type ShardedAggregator struct {
	shards      [numShards]*Shard
	trackerChan chan<- domain.SpreadEvent
	funding     *FundingManager
	config      *domain.ScreenerConfig
	users       *domain.UserManager
	volume      *VolumeEngine
	priceTTL    time.Duration
}

func NewShardedAggregator(trackerChan chan<- domain.SpreadEvent, funding *FundingManager, cfg *domain.ScreenerConfig, extras ...any) *ShardedAggregator {
	var users *domain.UserManager
	var volume *VolumeEngine
	for _, x := range extras {
		switch v := x.(type) {
		case *domain.UserManager:
			users = v
		case *VolumeEngine:
			volume = v
		}
	}
	if cfg == nil {
		cfg = domain.NewScreenerConfig(decimal.Zero, decimal.Zero)
	}
	sa := &ShardedAggregator{trackerChan: trackerChan, funding: funding, config: cfg, users: users, volume: volume, priceTTL: defaultPriceTTL}
	for i := 0; i < numShards; i++ {
		sa.shards[i] = &Shard{
			prices: make(map[string]map[string]map[domain.MarketType]*PriceState),
			active: make(map[string]*routeState),
		}
	}
	sa.staleWindow = staleWindow
	return sa
}

func shardFor(symbol string) int { return int(crc32.ChecksumIEEE([]byte(symbol)) % numShards) }

// ProcessTick updates market state and emits only lifecycle/peak events. It never
// drops those events: the downstream lifecycle queue is intentionally backpressured.
// Redundant quote updates are not persisted, which keeps DB traffic proportional to
// actual arbitrage state changes rather than exchange ticker frequency.
func (sa *ShardedAggregator) ProcessTick(tick domain.MarketTick) {
	if !validTick(tick) {
		return
	}
	tick.Exchange = strings.ToUpper(strings.TrimSpace(tick.Exchange))
	tick.Symbol = normalizeSymbol(tick.Symbol)
	if tick.Symbol == "" {
		return
	}
	if tick.ReceivedAt.IsZero() {
		tick.ReceivedAt = tick.EventTime
	}
	if tick.EventTime.IsZero() {
		tick.EventTime = tick.ReceivedAt
	}
	if tick.Timestamp.IsZero() {
		tick.Timestamp = tick.EventTime
	}

	shard := sa.shards[shardFor(tick.Symbol)]
	shard.mu.Lock()
	byExchange := shard.prices[tick.Symbol]
	if byExchange == nil {
		byExchange = make(map[string]map[domain.MarketType]*PriceState)
		shard.prices[tick.Symbol] = byExchange
	}
	markets := byExchange[tick.Exchange]
	if markets == nil {
		markets = make(map[domain.MarketType]*PriceState)
		byExchange[tick.Exchange] = markets
	}
	if old := markets[tick.MarketType]; old != nil && !old.ReceivedAt.IsZero() && tick.ReceivedAt.Before(old.ReceivedAt) {
		shard.mu.Unlock()
		return
	}
	markets[tick.MarketType] = &PriceState{Bid: tick.BestBid, Ask: tick.BestAsk, ReceivedAt: tick.ReceivedAt, QuoteVolume24h: tick.QuoteVolume}

	now := tick.ReceivedAt
	if now.IsZero() {
		now = tick.EventTime
	}
	if now.IsZero() {
		now = tick.Timestamp
	}

	var events []domain.SpreadEvent
	currentKeys := make(map[string]struct{}, 2)
	if sa.config.IsPairEnabled(tick.Symbol) {
		for _, ev := range sa.crossCandidates(byExchange, tick.Symbol, now) {
			if sa.routeVolumeEligible(ev) {
				if event, emit := sa.observe(shard, ev, currentKeys); emit {
					events = append(events, event)
				}
			}
		}
		if ev, ok := sa.intraCandidate(byExchange, tick.Exchange, tick.Symbol, now); ok {
			if sa.routeVolumeEligible(ev) {
				if event, emit := sa.observe(shard, ev, currentKeys); emit {
					events = append(events, event)
				}
			}
		}
	}

	// A route can disappear because one side became stale. Such a route must be
	// closed instead of remaining active forever.
	prefix := tick.Symbol + ":"
	for key := range shard.active {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, exists := currentKeys[key]; exists {
			continue
		}
		state := shard.active[key]
		delete(shard.active, key)
		if state != nil {
			ev := state.last
			ev.Spread = decimal.Zero
			ev.Timestamp = now
			ev.Lifecycle = domain.SignalClosed
			events = append(events, ev)
		}
	}
	shard.mu.Unlock()

	for _, event := range events {
		sa.sendEvent(event)
	}
}

// FlushActive closes every still-open route during a controlled shutdown. This
// prevents active DB rows from surviving forever merely because the process stopped.
func (sa *ShardedAggregator) FlushActive(now time.Time) []domain.SpreadEvent {
	if now.IsZero() {
		now = time.Now()
	}
	var events []domain.SpreadEvent
	for _, shard := range sa.shards {
		shard.mu.Lock()
		for key, state := range shard.active {
			if state == nil {
				delete(shard.active, key)
				continue
			}
			ev := state.last
			ev.Timestamp = now
			ev.Spread = decimal.Zero
			ev.Lifecycle = domain.SignalClosed
			events = append(events, ev)
			delete(shard.active, key)
		}
		shard.mu.Unlock()
	}
	return events
}

func (sa *ShardedAggregator) crossCandidates(byExchange map[string]map[domain.MarketType]*PriceState, symbol string, now time.Time) []domain.SpreadEvent {
	// The number of exchanges is intentionally small (single digits). O(E²)
	// here is preferable to emitting only the global min/max pair: every
	// independently profitable exchange pair is a real arbitrage opportunity.
	exchanges := make([]string, 0, len(byExchange))
	for ex := range byExchange {
		exchanges = append(exchanges, ex)
	}
	events := make([]domain.SpreadEvent, 0, len(exchanges))
	for i := 0; i < len(exchanges); i++ {
		left := byExchange[exchanges[i]][domain.MarketTypeFutures]
		if !fresh(left, now, sa.priceTTL) {
			continue
		}
		for j := i + 1; j < len(exchanges); j++ {
			right := byExchange[exchanges[j]][domain.MarketTypeFutures]
			if !fresh(right, now, sa.priceTTL) {
				continue
			}
			var buyEx, sellEx string
			var buyAsk, sellBid, buyVol, sellVol decimal.Decimal
			if left.Ask.LessThan(right.Bid) {
				buyEx, sellEx = exchanges[i], exchanges[j]
				buyAsk, sellBid = left.Ask, right.Bid
				buyVol, sellVol = left.QuoteVolume24h, right.QuoteVolume24h
			} else if right.Ask.LessThan(left.Bid) {
				buyEx, sellEx = exchanges[j], exchanges[i]
				buyAsk, sellBid = right.Ask, left.Bid
				buyVol, sellVol = right.QuoteVolume24h, left.QuoteVolume24h
			} else {
				continue
			}
			spread := sellBid.Sub(buyAsk).Div(buyAsk)
			if !spread.IsPositive() {
				continue
			}
			events = append(events, domain.SpreadEvent{
				Symbol: symbol, SpreadType: domain.CrossExchange, Spread: spread,
				BuyExchange: buyEx, SellExchange: sellEx,
				BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures,
				BuyAsk: buyAsk, SellBid: sellBid, QuoteVolume: minDecimal(buyVol, sellVol), Timestamp: now,
			})
		}
	}
	return events
}

func (sa *ShardedAggregator) intraCandidate(byExchange map[string]map[domain.MarketType]*PriceState, exchange, symbol string, now time.Time) (domain.SpreadEvent, bool) {
	states := byExchange[exchange]
	if states == nil {
		return domain.SpreadEvent{}, false
	}
	spot, fut := states[domain.MarketTypeSpot], states[domain.MarketTypeFutures]
	if !fresh(spot, now, sa.priceTTL) || !fresh(fut, now, sa.priceTTL) {
		return domain.SpreadEvent{}, false
	}
	var spread, buyAsk, sellBid decimal.Decimal
	var direction ArbDirection
	if fut.Bid.GreaterThan(spot.Ask) {
		spread = fut.Bid.Sub(spot.Ask).Div(spot.Ask)
		direction, buyAsk, sellBid = SpotLongFuturesShort, spot.Ask, fut.Bid
	} else if spot.Bid.GreaterThan(fut.Ask) {
		spread = spot.Bid.Sub(fut.Ask).Div(fut.Ask)
		direction, buyAsk, sellBid = SpotShortFuturesLong, fut.Ask, spot.Bid
	} else {
		return domain.SpreadEvent{}, false
	}
	if spread.IsNegative() || spread.IsZero() {
		return domain.SpreadEvent{}, false
	}

	result := ArbResult{Profitable: true, NetSpread: spread, Reason: ReasonProfitable}
	if sa.funding != nil {
		result = sa.funding.EvaluateArb(exchange, symbol, spread, direction, now)
		if !result.Profitable {
			// Funding is optional. The FundingManager only blocks this route when
			// RequireFunding=true; otherwise a missing/stale funding feed does not
			// disable spot/futures detection on an otherwise healthy exchange.
			return domain.SpreadEvent{}, false
		}
	}
	return domain.SpreadEvent{
		Symbol: symbol, SpreadType: domain.IntraExchange, Spread: result.NetSpread,
		BuyExchange: exchange, SellExchange: exchange,
		BuyMarket: domain.MarketTypeSpot, SellMarket: domain.MarketTypeFutures,
		BuyAsk: buyAsk, SellBid: sellBid,
		QuoteVolume: minDecimal(spot.QuoteVolume24h, fut.QuoteVolume24h),
		FundingRate: result.FundingRate, NextFunding: result.NextFundingTime, Timestamp: now,
	}, true
}

func (sa *ShardedAggregator) observe(shard *Shard, ev domain.SpreadEvent, current map[string]struct{}) (domain.SpreadEvent, bool) {
	key := routeKey(ev)
	current[key] = struct{}{}
	state, active := shard.active[key]
	closeThreshold := sa.config.GetCloseThreshold()
	openThreshold := sa.config.GetHardMinSpread()

	if !active {
		if ev.Spread.LessThan(openThreshold) {
			return domain.SpreadEvent{}, false
		}
		ev.Lifecycle = domain.SignalOpened
		shard.active[key] = &routeState{peak: ev.Spread, last: ev, openedAt: ev.Timestamp}
		return ev, true
	}

	state.last = ev
	if ev.Spread.LessThanOrEqual(closeThreshold) {
		delete(shard.active, key)
		ev.Lifecycle = domain.SignalClosed
		return ev, true
	}
	if ev.Spread.GreaterThan(state.peak) {
		state.peak = ev.Spread
		ev.Lifecycle = domain.SignalUpdated
		return ev, true
	}
	return domain.SpreadEvent{}, false
}

func (sa *ShardedAggregator) routeVolumeEligible(ev domain.SpreadEvent) bool {
	if sa.users == nil {
		return true
	}
	eligible := false
	sa.users.Range(func(u domain.User) bool {
		if u.MinVolume.IsNegative() {
			return true
		}
		var volume decimal.Decimal
		if u.Timeframe == domain.TF_24h {
			volume = ev.QuoteVolume
		} else if sa.volume != nil {
			volume = sa.volume.GetRouteVolume(ev.BuyExchange, ev.BuyMarket, ev.SellExchange, ev.SellMarket, ev.Symbol, u.Timeframe, ev.Timestamp)
		}
		if volume.GreaterThanOrEqual(u.MinVolume) {
			eligible = true
			return false
		}
		return true
	})
	return eligible
}

func routeKey(e domain.SpreadEvent) string {
	buy, sell := e.BuyExchange, e.SellExchange
	if buy == "" {
		buy = e.ExchangeA
	}
	if sell == "" {
		sell = e.ExchangeB
	}
	return strings.Join([]string{e.Symbol, string(e.SpreadType), buy, sell, string(e.BuyMarket), string(e.SellMarket)}, ":")
}

func normalizeSymbol(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	s = strings.NewReplacer("-", "", "_", "", "/", "").Replace(s)
	return s
}

func fresh(s *PriceState, now time.Time, ttl time.Duration) bool {
	if s == nil || s.ReceivedAt.IsZero() || now.Before(s.ReceivedAt) {
		return false
	}
	return now.Sub(s.ReceivedAt) <= ttl
}
func validTick(t domain.MarketTick) bool {
	if strings.TrimSpace(t.Exchange) == "" || strings.TrimSpace(t.Symbol) == "" {
		return false
	}
	if t.MarketType != domain.MarketTypeSpot && t.MarketType != domain.MarketTypeFutures {
		return false
	}
	if !t.BestBid.IsPositive() || !t.BestAsk.IsPositive() || t.BestBid.GreaterThan(t.BestAsk) {
		return false
	}
	return !t.ReceivedAt.IsZero() || !t.EventTime.IsZero() || !t.Timestamp.IsZero()
}
func minDecimal(a, b decimal.Decimal) decimal.Decimal {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.LessThan(b) {
		return a
	}
	return b
}
func (sa *ShardedAggregator) GetSymbolVolume(symbol string, tf domain.Timeframe, ts time.Time) decimal.Decimal {
	// Only 24h rolling quote volume is available from the ticker adapters. A
	// timeframe-specific volume must be built from trades/klines and is not
	// fabricated here.
	if tf != domain.TF_24h {
		return decimal.Zero
	}
	shard := sa.shards[shardFor(normalizeSymbol(symbol))]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	byExchange := shard.prices[normalizeSymbol(symbol)]
	var max decimal.Decimal
	for _, markets := range byExchange {
		for _, st := range markets {
			if st != nil && (ts.IsZero() || st.ReceivedAt.After(ts.Add(-sa.priceTTL))) && st.QuoteVolume24h.GreaterThan(max) {
				max = st.QuoteVolume24h
			}
		}
	}
	return max
}
func (sa *ShardedAggregator) sendEvent(event domain.SpreadEvent) {
	if sa.trackerChan == nil {
		return
	}
	sa.trackerChan <- event
}
