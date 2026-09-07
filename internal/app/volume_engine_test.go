package app

import (
	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestVolumeEngine_ColdStartProjectsMinuteAverage(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC)
	v.clock = func() time.Time { return now }
	for i := 0; i < 1; i++ {
		require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot, OpenTime: now.Add(-time.Duration(i) * time.Minute), QuoteVolume: decimal.RequireFromString("1666.666666")}))
	}
	est := v.EstimateMarketVolume("BINANCE", "BTCUSDT", domain.MarketTypeSpot, domain.TF_30m, now)
	require.False(t, est.Complete)
	require.Equal(t, 1, est.KnownMinutes)
	require.InDelta(t, 50000, est.Volume.InexactFloat64(), 0.01)
}
func TestVolumeEngine_RepeatedCurrentCandleReplacesBucket(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC)
	v.clock = func() time.Time { return now }
	c := domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot, OpenTime: now, QuoteVolume: decimal.NewFromInt(1000)}
	require.NoError(t, v.UpdateCandle(c))
	c.QuoteVolume = decimal.NewFromInt(2500)
	require.NoError(t, v.UpdateCandle(c))
	require.True(t, v.GetMarketVolume("BINANCE", "BTCUSDT", domain.MarketTypeSpot, domain.TF_1m, now).Equal(decimal.NewFromInt(2500)))
}
func TestVolumeEngine_ZeroVolumeIsKnown(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC)
	v.clock = func() time.Time { return now }
	require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot, OpenTime: now, QuoteVolume: decimal.Zero}))
	est := v.EstimateMarketVolume("BINANCE", "BTCUSDT", domain.MarketTypeSpot, domain.TF_1m, now)
	require.True(t, est.Complete)
	require.Equal(t, 1, est.KnownMinutes)
	require.True(t, est.Volume.IsZero())
}

func TestVolumeEngine_ProjectionStopsAtGap(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC)
	v.clock = func() time.Time { return now }
	for _, offset := range []int{-1, -3} {
		require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot, OpenTime: now.Add(time.Duration(offset) * time.Minute), QuoteVolume: decimal.NewFromInt(1000)}))
	}
	est := v.EstimateMarketVolume("BINANCE", "BTCUSDT", domain.MarketTypeSpot, domain.TF_30m, now)
	require.Equal(t, 0, est.KnownMinutes)
	require.False(t, est.Complete)
	require.True(t, est.Volume.IsZero())
}
