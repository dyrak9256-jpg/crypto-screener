package app

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type ArbDirection int8

const (
	DirectionUnknown ArbDirection = iota
	SpotLongFuturesShort
	SpotShortFuturesLong
)

func (d ArbDirection) Valid() bool { return d == SpotLongFuturesShort || d == SpotShortFuturesLong }

type ArbReason string

const (
	ReasonProfitable               ArbReason = "PROFITABLE"
	ReasonBelowMinSpread           ArbReason = "BELOW_MIN_SPREAD"
	ReasonHazardWindow             ArbReason = "HAZARD_WINDOW"
	ReasonGracePeriod              ArbReason = "GRACE_PERIOD"
	ReasonWindowUnknown            ArbReason = "WINDOW_UNKNOWN"
	ReasonDataStale                ArbReason = "DATA_STALE"
	ReasonNoData                   ArbReason = "NO_DATA"
	ReasonStreamUnhealthy          ArbReason = "STREAM_UNHEALTHY"
	ReasonInvalidInput             ArbReason = "INVALID_INPUT"
	ReasonConfigurationUnavailable ArbReason = "CONFIGURATION_UNAVAILABLE"
)

type FundingConfig struct {
	MinSpread              decimal.Decimal
	MaxDataStaleness       time.Duration
	PreFundingHazardWindow time.Duration
	PostFundingGracePeriod time.Duration
	RequireFunding         bool
}

func DefaultFundingConfig() FundingConfig {
	return FundingConfig{MinSpread: decimal.Zero, MaxDataStaleness: 15 * time.Second, PreFundingHazardWindow: 5 * time.Minute, PostFundingGracePeriod: time.Minute, RequireFunding: false}
}
func (c FundingConfig) normalize() FundingConfig {
	d := DefaultFundingConfig()
	if c.MaxDataStaleness == 0 {
		c.MaxDataStaleness = d.MaxDataStaleness
	}
	if c.PreFundingHazardWindow == 0 {
		c.PreFundingHazardWindow = d.PreFundingHazardWindow
	}
	if c.PostFundingGracePeriod == 0 {
		c.PostFundingGracePeriod = d.PostFundingGracePeriod
	}
	return c
}
func (c FundingConfig) Validate() error {
	if c.MinSpread.IsNegative() {
		return errors.New("MinSpread cannot be negative")
	}
	if c.MaxDataStaleness <= 0 {
		return errors.New("MaxDataStaleness must be positive")
	}
	if c.PreFundingHazardWindow < 0 {
		return errors.New("PreFundingHazardWindow cannot be negative")
	}
	if c.PostFundingGracePeriod < 0 {
		return errors.New("PostFundingGracePeriod cannot be negative")
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
	if now.Before(r.LocalReceivedAt) {
		return r.LocalReceivedAt.Sub(now) > time.Second
	}
	return now.Sub(r.LocalReceivedAt) > max
}

type ArbResult struct {
	Profitable      bool
	Reason          ArbReason
	NetSpread       decimal.Decimal
	FundingRate     decimal.Decimal
	NextFundingTime time.Time
}

type fundingKey struct{ Exchange, Symbol string }

type FundingManager struct {
	mu     sync.RWMutex
	rates  map[fundingKey]FundingRecord
	config atomic.Pointer[FundingConfig]
	health sync.Map // exchange -> *atomic.Bool
	clock  Clock
}

func NewFundingManager(cfg FundingConfig, clock Clock) (*FundingManager, error) {
	cfg = cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
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
		return err
	}
	fm.config.Store(&cfg)
	return nil
}
func (fm *FundingManager) SetStreamHealth(exchange string, healthy bool) {
	fm.SetExchangeStreamHealth(exchange, healthy)
}
func (fm *FundingManager) SetExchangeStreamHealth(exchange string, healthy bool) {
	v := new(atomic.Bool)
	if old, ok := fm.health.LoadOrStore(exchange, v); ok {
		v = old.(*atomic.Bool)
	}
	v.Store(healthy)
}
func (fm *FundingManager) isHealthy(exchange string) bool {
	if v, ok := fm.health.Load(exchange); ok {
		return v.(*atomic.Bool).Load()
	}
	if v, ok := fm.health.Load("*"); ok {
		return v.(*atomic.Bool).Load()
	}
	return false
}
func (fm *FundingManager) UpdateFunding(exchange, symbol string, rate decimal.Decimal, nextFundingTime, eventTime time.Time) error {
	if exchange == "" || symbol == "" {
		return errors.New("exchange and symbol must not be empty")
	}
	now := fm.clock.Now()
	key := fundingKey{Exchange: exchange, Symbol: symbol}
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if old, ok := fm.rates[key]; ok && !eventTime.IsZero() && !old.EventTime.IsZero() && eventTime.Before(old.EventTime) {
		return nil
	}
	fm.rates[key] = FundingRecord{Exchange: exchange, Symbol: symbol, Rate: rate, NextFundingTime: nextFundingTime, EventTime: eventTime, LocalReceivedAt: now}
	fm.SetExchangeStreamHealth(exchange, true)
	return nil
}
func (fm *FundingManager) GetFunding(exchange, symbol string) (FundingRecord, bool) {
	fm.mu.RLock()
	r, ok := fm.rates[fundingKey{exchange, symbol}]
	fm.mu.RUnlock()
	return r, ok
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
func CalcNetSpread(spread, rate decimal.Decimal, direction ArbDirection) (decimal.Decimal, error) {
	switch direction {
	case SpotLongFuturesShort:
		return spread.Add(rate), nil
	case SpotShortFuturesLong:
		return spread.Sub(rate), nil
	default:
		return decimal.Zero, errors.New("invalid arb direction")
	}
}
func (fm *FundingManager) EvaluateArb(exchange, symbol string, spread decimal.Decimal, direction ArbDirection, now time.Time) ArbResult {
	if now.IsZero() || exchange == "" || symbol == "" || !direction.Valid() {
		return ArbResult{Reason: ReasonInvalidInput}
	}
	p := fm.config.Load()
	if p == nil {
		return ArbResult{Reason: ReasonConfigurationUnavailable}
	}
	rec, ok := fm.GetFunding(exchange, symbol)
	if !ok {
		if p.RequireFunding {
			return ArbResult{Reason: ReasonNoData}
		}
		return ArbResult{Profitable: true, Reason: ReasonProfitable, NetSpread: spread}
	}
	if !fm.isHealthy(exchange) {
		if p.RequireFunding {
			return ArbResult{Reason: ReasonStreamUnhealthy}
		}
		return ArbResult{Profitable: true, Reason: ReasonProfitable, NetSpread: spread}
	}
	if rec.IsStale(now, p.MaxDataStaleness) {
		if p.RequireFunding {
			return ArbResult{Reason: ReasonDataStale, FundingRate: rec.Rate, NextFundingTime: rec.NextFundingTime}
		}
		return ArbResult{Profitable: true, Reason: ReasonProfitable, NetSpread: spread}
	}
	net, err := CalcNetSpread(spread, rec.Rate, direction)
	if err != nil {
		return ArbResult{Reason: ReasonInvalidInput}
	}
	reason := evaluateWindow(now, rec.NextFundingTime, *p)
	if reason != ReasonProfitable {
		return ArbResult{Reason: reason, NetSpread: net, FundingRate: rec.Rate, NextFundingTime: rec.NextFundingTime}
	}
	if net.LessThan(p.MinSpread) {
		return ArbResult{Reason: ReasonBelowMinSpread, NetSpread: net, FundingRate: rec.Rate, NextFundingTime: rec.NextFundingTime}
	}
	return ArbResult{Profitable: true, Reason: ReasonProfitable, NetSpread: net, FundingRate: rec.Rate, NextFundingTime: rec.NextFundingTime}
}
func evaluateWindow(now, next time.Time, c FundingConfig) ArbReason {
	if next.IsZero() {
		return ReasonWindowUnknown
	}
	if now.Before(next) {
		if next.Sub(now) <= c.PreFundingHazardWindow {
			return ReasonHazardWindow
		}
		return ReasonProfitable
	}
	if now.Sub(next) <= c.PostFundingGracePeriod {
		return ReasonGracePeriod
	}
	return ReasonWindowUnknown
}
