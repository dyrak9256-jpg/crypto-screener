package app

import (
	"strings"
	"sync"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
)

type volumeBucket struct {
	start time.Time
	quote decimal.Decimal
}

type volumeSeries struct {
	buckets map[int64]volumeBucket
}

// VolumeEngine keeps only minute quote-turnover buckets. It is deliberately
// independent from the arbitrage engine so candle ingestion never needs to
// walk order books or retain trade history.
type VolumeEngine struct {
	mu        sync.RWMutex
	data      map[string]*volumeSeries
	retention time.Duration
}

func NewVolumeEngine() *VolumeEngine {
	return &VolumeEngine{data: make(map[string]*volumeSeries), retention: 24*time.Hour + 2*time.Minute}
}

func volumeKey(exchange, symbol string, market domain.MarketType) string {
	return strings.ToUpper(exchange) + "|" + strings.ToUpper(symbol) + "|" + string(market)
}

func minuteStart(t time.Time) time.Time { return t.UTC().Truncate(time.Minute) }

func (v *VolumeEngine) UpdateCandle(c domain.MarketCandle) error {
	if v == nil || c.Exchange == "" || c.Symbol == "" || !c.QuoteVolume.IsPositive() || c.OpenTime.IsZero() {
		return nil
	}
	start := minuteStart(c.OpenTime)
	key := volumeKey(c.Exchange, c.Symbol, c.MarketType)
	v.mu.Lock()
	s := v.data[key]
	if s == nil {
		s = &volumeSeries{buckets: make(map[int64]volumeBucket)}
		v.data[key] = s
	}
	s.buckets[start.Unix()] = volumeBucket{start: start, quote: c.QuoteVolume}
	cutoff := time.Now().UTC().Add(-v.retention).Unix()
	for ts := range s.buckets {
		if ts < cutoff {
			delete(s.buckets, ts)
		}
	}
	v.mu.Unlock()
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

func (v *VolumeEngine) GetMarketVolume(exchange, symbol string, market domain.MarketType, tf domain.Timeframe, now time.Time) decimal.Decimal {
	d, ok := timeframeDuration(tf)
	if !ok || v == nil {
		return decimal.Zero
	}
	if now.IsZero() {
		now = time.Now()
	}
	end := minuteStart(now)
	begin := end.Add(-d + time.Minute)
	key := volumeKey(exchange, symbol, market)
	v.mu.RLock()
	s := v.data[key]
	if s == nil {
		v.mu.RUnlock()
		return decimal.Zero
	}
	var total decimal.Decimal
	for t := begin; !t.After(end); t = t.Add(time.Minute) {
		if b, ok := s.buckets[t.Unix()]; ok {
			total = total.Add(b.quote)
		}
	}
	v.mu.RUnlock()
	return total
}

// GetRouteVolume is intentionally the minimum of both legs: an arbitrage
// route is only as liquid as its weaker leg.
func (v *VolumeEngine) GetRouteVolume(buyEx string, buyMarket domain.MarketType, sellEx string, sellMarket domain.MarketType, symbol string, tf domain.Timeframe, now time.Time) decimal.Decimal {
	a := v.GetMarketVolume(buyEx, symbol, buyMarket, tf, now)
	b := v.GetMarketVolume(sellEx, symbol, sellMarket, tf, now)
	if a.IsZero() || b.IsZero() {
		return decimal.Zero
	}
	if a.LessThan(b) {
		return a
	}
	return b
}

func (v *VolumeEngine) GetSymbolVolume(symbol string, tf domain.Timeframe, ts time.Time) decimal.Decimal {
	// Kept for the VolumeProvider interface. Route-specific checks should use
	// GetRouteVolume so one liquid leg cannot hide an illiquid leg.
	return decimal.Zero
}
