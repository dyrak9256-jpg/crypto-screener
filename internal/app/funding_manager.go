package app

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type ArbReason string

const (
	ReasonProfitable               ArbReason = "PROFITABLE"
	ReasonBelowMinSpread           ArbReason = "BELOW_MIN_SPREAD"
	ReasonDataStale                ArbReason = "DATA_STALE"
	ReasonNoData                   ArbReason = "NO_DATA"
	ReasonStreamUnhealthy          ArbReason = "STREAM_UNHEALTHY"
	ReasonInvalidInput             ArbReason = "INVALID_INPUT"
	ReasonConfigurationUnavailable ArbReason = "CONFIGURATION_UNAVAILABLE"
)

type FundingConfig struct {
	MinSpread        decimal.Decimal
	MaxDataStaleness time.Duration
}

func DefaultFundingConfig() FundingConfig {
	return FundingConfig{MaxDataStaleness: 60 * time.Second}
}

func (c FundingConfig) normalize() FundingConfig {
	d := DefaultFundingConfig()
	if c.MaxDataStaleness == 0 {
		c.MaxDataStaleness = d.MaxDataStaleness
	}
	return c
}

func (c FundingConfig) Validate() error {
	if c.MinSpread.IsNegative() {
		return errors.New("min spread cannot be negative")
	}
	if c.MaxDataStaleness <= 0 {
		return errors.New("max data staleness must be positive")
	}
	return nil
}

type FundingRecord struct {
	Exchange, Symbol                            string
	Rate                                        decimal.Decimal
	NextFundingTime, EventTime, LocalReceivedAt time.Time
}

func (r FundingRecord) IsStale(now time.Time, max time.Duration) bool {
	if r.LocalReceivedAt.IsZero() || max <= 0 {
		return true
	}
	// Only true age beyond the budget makes a record stale. A slightly negative
	// age (record received right after the price tick it is evaluated against)
	// means the funding snapshot is fresh, not stale.
	age := now.Sub(r.LocalReceivedAt)
	return age > max
}

type ArbResult struct {
	Profitable          bool
	Reason              ArbReason
	NetSpread           decimal.Decimal
	FundingRate         decimal.Decimal
	NextFundingTime     time.Time
	BuyFundingRate      decimal.Decimal
	SellFundingRate     decimal.Decimal
	BuyNextFundingTime  time.Time
	SellNextFundingTime time.Time
}

type fundingKey struct{ Exchange, Symbol string }

type FundingManager struct {
	mu     sync.RWMutex
	rates  map[fundingKey]FundingRecord
	config atomic.Pointer[FundingConfig]
	health sync.Map // exchange -> *atomic.Bool
	// lastUpdate: exchange -> время последнего funding-обновления (для метрик).
	lastUpdate sync.Map // exchange -> *atomic.Int64 (UnixNano)
	clock      Clock
}

func NewFundingManager(cfg FundingConfig, clock Clock) (*FundingManager, error) {
	cfg = cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate funding config: %w", err)
	}
	if clock == nil {
		clock = realClock{}
	}
	fm := &FundingManager{rates: make(map[fundingKey]FundingRecord), clock: clock}
	fm.config.Store(&cfg)
	return fm, nil
}

func (fm *FundingManager) UpdateConfig(cfg FundingConfig) error {
	cfg = cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate funding config: %w", err)
	}
	fm.config.Store(&cfg)
	return nil
}

func (fm *FundingManager) SetStreamHealth(exchange string, healthy bool) {
	fm.SetExchangeStreamHealth(exchange, healthy)
}

func (fm *FundingManager) SetExchangeStreamHealth(exchange string, healthy bool) {
	exchange = strings.ToUpper(strings.TrimSpace(exchange))
	if exchange == "" {
		return
	}
	v := new(atomic.Bool)
	if old, ok := fm.health.LoadOrStore(exchange, v); ok {
		v = old.(*atomic.Bool)
	}
	v.Store(healthy)
}

func (fm *FundingManager) isHealthy(exchange string) bool {
	v, ok := fm.health.Load(strings.ToUpper(strings.TrimSpace(exchange)))
	if !ok {
		return false
	}
	return v.(*atomic.Bool).Load()
}

func normalizeFundingKey(exchange, symbol string) fundingKey {
	return fundingKey{
		Exchange: strings.ToUpper(strings.TrimSpace(exchange)),
		Symbol:   strings.ToUpper(strings.NewReplacer("-", "", "_", "", "/", "").Replace(strings.TrimSpace(symbol))),
	}
}

func (fm *FundingManager) UpdateFunding(exchange, symbol string, rate decimal.Decimal, next, event time.Time) error {
	key := normalizeFundingKey(exchange, symbol)
	if key.Exchange == "" || key.Symbol == "" {
		return errors.New("exchange and symbol must not be empty")
	}
	now := fm.clock.Now()
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if old, ok := fm.rates[key]; ok && !event.IsZero() && !old.EventTime.IsZero() && event.Before(old.EventTime) {
		return nil
	}
	fm.rates[key] = FundingRecord{Exchange: key.Exchange, Symbol: key.Symbol, Rate: rate, NextFundingTime: next, EventTime: event, LocalReceivedAt: now}
	fm.SetExchangeStreamHealth(key.Exchange, true)
	// Всегда фиксируем время получения (LoadOrStore возвращает загруженное ИЛИ
	// новое значение — Store обязателен в обоих случаях, иначе ПЕРВЫЙ апдейт
	// биржи оставляет lastUpdate=0 и FundingAge зря репортит "unknown").
	v, _ := fm.lastUpdate.LoadOrStore(key.Exchange, new(atomic.Int64))
	v.(*atomic.Int64).Store(now.UnixNano())
	return nil
}

func (fm *FundingManager) GetFunding(exchange, symbol string) (FundingRecord, bool) {
	key := normalizeFundingKey(exchange, symbol)
	fm.mu.RLock()
	r, ok := fm.rates[key]
	fm.mu.RUnlock()
	return r, ok
}

// FundingAge возвращает время с последнего funding-обновления биржи.
// Если данных не было вовсе — возвращается -1 (метрика "unknown").
func (fm *FundingManager) FundingAge(exchange string) time.Duration {
	if fm == nil {
		return -1
	}
	v, ok := fm.lastUpdate.Load(strings.ToUpper(strings.TrimSpace(exchange)))
	if !ok {
		return -1
	}
	ts := v.(*atomic.Int64).Load()
	if ts <= 0 {
		return -1
	}
	return fm.clock.Now().Sub(time.Unix(0, ts))
}

// KnownExchanges перечисляет биржи, от которых приходили funding-данные.
func (fm *FundingManager) KnownExchanges() []string {
	if fm == nil {
		return nil
	}
	var out []string
	fm.lastUpdate.Range(func(key, _ any) bool {
		if name, ok := key.(string); ok {
			out = append(out, name)
		}
		return true
	})
	sort.Strings(out)
	return out
}

func (fm *FundingManager) EvictStale() {
	p := fm.config.Load()
	if p == nil {
		return
	}
	now := fm.clock.Now()
	fm.mu.Lock()
	for k, r := range fm.rates {
		if r.IsStale(now, p.MaxDataStaleness) {
			delete(fm.rates, k)
		}
	}
	fm.mu.Unlock()
}

// CalcNetSpread applies funding according to the actual position directions:
// buy futures pays/receives buyRate, while a short futures leg contributes
// sellRate. For SPOT/FUTURES the spot leg is deliberately represented by zero.
func CalcNetSpread(spread, buyRate, sellRate decimal.Decimal) decimal.Decimal {
	return spread.Add(sellRate).Sub(buyRate)
}

func (fm *FundingManager) EvaluateSpotFutures(exchange, symbol string, spread decimal.Decimal, now time.Time) ArbResult {
	if now.IsZero() || exchange == "" || symbol == "" || spread.IsNegative() {
		return ArbResult{Reason: ReasonInvalidInput}
	}
	p := fm.config.Load()
	if p == nil {
		return ArbResult{Reason: ReasonConfigurationUnavailable}
	}
	// SPOT BUY has no funding. FUTURES SHORT is the sell leg and therefore its
	// funding rate is added to the executable spread.
	sell, ok := fm.validRecord(exchange, symbol, now, *p)
	if !ok {
		return sell
	}
	net := CalcNetSpread(spread, decimal.Zero, sell.FundingRate)
	result := ArbResult{
		NetSpread:           net,
		FundingRate:         sell.FundingRate,
		NextFundingTime:     sell.NextFundingTime,
		BuyFundingRate:      decimal.Zero,
		SellFundingRate:     sell.FundingRate,
		BuyNextFundingTime:  time.Time{},
		SellNextFundingTime: sell.NextFundingTime,
	}
	if net.LessThan(p.MinSpread) {
		result.Reason = ReasonBelowMinSpread
		return result
	}
	result.Profitable = true
	result.Reason = ReasonProfitable
	return result
}

func (fm *FundingManager) EvaluateFuturesPair(buyEx, buySymbol, sellEx, sellSymbol string, spread decimal.Decimal, now time.Time) ArbResult {
	if now.IsZero() || buyEx == "" || buySymbol == "" || sellEx == "" || sellSymbol == "" || spread.IsNegative() {
		return ArbResult{Reason: ReasonInvalidInput}
	}
	p := fm.config.Load()
	if p == nil {
		return ArbResult{Reason: ReasonConfigurationUnavailable}
	}
	buy, ok := fm.validRecord(buyEx, buySymbol, now, *p)
	if !ok {
		return buy
	}
	sell, ok := fm.validRecord(sellEx, sellSymbol, now, *p)
	if !ok {
		return sell
	}
	net := CalcNetSpread(spread, buy.FundingRate, sell.FundingRate)
	next := earliestFunding(buy.NextFundingTime, sell.NextFundingTime)
	result := ArbResult{
		NetSpread:           net,
		FundingRate:         buy.FundingRate,
		NextFundingTime:     next,
		BuyFundingRate:      buy.FundingRate,
		SellFundingRate:     sell.FundingRate,
		BuyNextFundingTime:  buy.NextFundingTime,
		SellNextFundingTime: sell.NextFundingTime,
	}
	if net.LessThan(p.MinSpread) {
		result.Reason = ReasonBelowMinSpread
		return result
	}
	result.Profitable = true
	result.Reason = ReasonProfitable
	return result
}

func (fm *FundingManager) validRecord(exchange, symbol string, now time.Time, cfg FundingConfig) (ArbResult, bool) {
	r, ok := fm.GetFunding(exchange, symbol)
	if !ok {
		return ArbResult{Reason: ReasonNoData}, false
	}
	if !fm.isHealthy(exchange) {
		return ArbResult{Reason: ReasonStreamUnhealthy}, false
	}
	if r.NextFundingTime.IsZero() {
		return ArbResult{Reason: ReasonNoData}, false
	}
	if r.IsStale(now, cfg.MaxDataStaleness) {
		return ArbResult{Reason: ReasonDataStale, BuyFundingRate: r.Rate, SellFundingRate: r.Rate, BuyNextFundingTime: r.NextFundingTime, SellNextFundingTime: r.NextFundingTime}, false
	}
	return ArbResult{Profitable: true, Reason: ReasonProfitable, FundingRate: r.Rate, NextFundingTime: r.NextFundingTime}, true
}

func earliestFunding(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
