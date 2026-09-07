package app

import (
	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type volumeBucket struct {
	start time.Time
	quote decimal.Decimal
}
type volumeSeries struct {
	buckets  map[int64]volumeBucket
	lastSeen time.Time
}

type VolumeEstimate struct {
	Volume        decimal.Decimal
	Complete      bool
	KnownMinutes  int
	WindowMinutes int
}

type VolumeEngine struct {
	mu        sync.RWMutex
	data      map[string]*volumeSeries
	retention time.Duration
	clock     func() time.Time
	updates   atomic.Uint64
}

func NewVolumeEngine() *VolumeEngine {
	return &VolumeEngine{data: make(map[string]*volumeSeries), retention: 24*time.Hour + 2*time.Minute, clock: time.Now}
}
func volumeKey(exchange, symbol string, market domain.MarketType) string {
	return strings.ToUpper(exchange) + "|" + strings.ToUpper(symbol) + "|" + string(market)
}
func minuteStart(t time.Time) time.Time { return t.UTC().Truncate(time.Minute) }
func (v *VolumeEngine) UpdateCandle(c domain.MarketCandle) error {
	if v == nil || c.Exchange == "" || c.Symbol == "" || c.OpenTime.IsZero() || c.MarketType == "" || c.QuoteVolume.IsNegative() {
		return nil
	}
	start := minuteStart(c.OpenTime)
	key := volumeKey(c.Exchange, c.Symbol, c.MarketType)
	now := v.clock().UTC()
	v.mu.Lock()
	s := v.data[key]
	if s == nil {
		s = &volumeSeries{buckets: make(map[int64]volumeBucket)}
		v.data[key] = s
	}
	s.buckets[start.Unix()] = volumeBucket{start: start, quote: c.QuoteVolume}
	if now.After(s.lastSeen) {
		s.lastSeen = now
	}
	cut := now.Add(-v.retention).Unix()
	for ts := range s.buckets {
		if ts < cut {
			delete(s.buckets, ts)
		}
	}
	v.mu.Unlock()
	if v.updates.Add(1)%1000 == 0 {
		v.Janitor()
	}
	return nil
}
func timeframeDuration(tf domain.Timeframe) (time.Duration, bool) {
	switch tf {
	case domain.TF_1m:
		return time.Minute, true
	case domain.TF_5m:
		return 5 * time.Minute, true
	case domain.TF_15m:
		return 15 * time.Minute, true
	case domain.TF_30m:
		return 30 * time.Minute, true
	case domain.TF_1h:
		return time.Hour, true
	case domain.TF_4h:
		return 4 * time.Hour, true
	case domain.TF_24h:
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}
func (v *VolumeEngine) EstimateMarketVolume(exchange, symbol string, market domain.MarketType, tf domain.Timeframe, now time.Time) VolumeEstimate {
	d, ok := timeframeDuration(tf)
	if !ok || v == nil {
		return VolumeEstimate{}
	}
	if now.IsZero() {
		now = v.clock()
	}
	end := minuteStart(now)
	minutes := int(d / time.Minute)
	begin := end.Add(-d + time.Minute)

	v.mu.RLock()
	s := v.data[volumeKey(exchange, symbol, market)]
	if s == nil {
		v.mu.RUnlock()
		return VolumeEstimate{WindowMinutes: minutes}
	}

	// Only a contiguous run ending at the newest known minute is projected.
	// Missing minutes are not silently treated as zero or as observations.
	latest := time.Time{}
	for t := end; !t.Before(begin); t = t.Add(-time.Minute) {
		if b, exists := s.buckets[t.Unix()]; exists {
			latest = b.start
			break
		}
	}
	if latest.IsZero() || !latest.Equal(end) {
		v.mu.RUnlock()
		return VolumeEstimate{WindowMinutes: minutes}
	}

	var total decimal.Decimal
	known := 0
	for t := latest; !t.Before(begin); t = t.Add(-time.Minute) {
		b, exists := s.buckets[t.Unix()]
		if !exists {
			break
		}
		total = total.Add(b.quote)
		known++
	}
	v.mu.RUnlock()

	if known == 0 {
		return VolumeEstimate{WindowMinutes: minutes}
	}
	if known == minutes && latest.Equal(end) {
		return VolumeEstimate{Volume: total, Complete: true, KnownMinutes: known, WindowMinutes: minutes}
	}

	// Cold-start projection is deliberately based only on the contiguous current
	// run. With one observed minute, 2,000 USDT/min projects to 60,000 USDT for
	// a 30-minute filter; a gap immediately stops the sample.
	est := total.Div(decimal.NewFromInt(int64(known))).Mul(decimal.NewFromInt(int64(minutes)))
	return VolumeEstimate{Volume: est, KnownMinutes: known, WindowMinutes: minutes}
}

func (v *VolumeEngine) GetMarketVolume(exchange, symbol string, market domain.MarketType, tf domain.Timeframe, now time.Time) decimal.Decimal {
	return v.EstimateMarketVolume(exchange, symbol, market, tf, now).Volume
}
func (v *VolumeEngine) GetRouteVolumeEstimate(buyEx string, buyMarket domain.MarketType, sellEx string, sellMarket domain.MarketType, symbol string, tf domain.Timeframe, now time.Time) VolumeEstimate {
	a := v.EstimateMarketVolume(buyEx, symbol, buyMarket, tf, now)
	b := v.EstimateMarketVolume(sellEx, symbol, sellMarket, tf, now)
	if a.KnownMinutes == 0 || b.KnownMinutes == 0 {
		return VolumeEstimate{WindowMinutes: a.WindowMinutes}
	}
	vol := a.Volume
	if b.Volume.LessThan(vol) {
		vol = b.Volume
	}
	return VolumeEstimate{Volume: vol, Complete: a.Complete && b.Complete, KnownMinutes: minInt(a.KnownMinutes, b.KnownMinutes), WindowMinutes: maxInt(a.WindowMinutes, b.WindowMinutes)}
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (v *VolumeEngine) GetRouteVolume(buyEx string, buyMarket domain.MarketType, sellEx string, sellMarket domain.MarketType, symbol string, tf domain.Timeframe, now time.Time) decimal.Decimal {
	return v.GetRouteVolumeEstimate(buyEx, buyMarket, sellEx, sellMarket, symbol, tf, now).Volume
}
func (v *VolumeEngine) Janitor() {
	if v == nil {
		return
	}
	now := v.clock().UTC()
	cut := now.Add(-v.retention)
	v.mu.Lock()
	defer v.mu.Unlock()
	for k, s := range v.data {
		for ts := range s.buckets {
			if time.Unix(ts, 0).Before(cut) {
				delete(s.buckets, ts)
			}
		}
		if s.lastSeen.IsZero() || now.Sub(s.lastSeen) > v.retention {
			delete(v.data, k)
		}
	}
}
