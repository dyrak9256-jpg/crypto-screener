package app

import (
	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestVolumeEngineReplacesCurrentCandle(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Now().UTC().Truncate(time.Minute)
	require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeFutures, OpenTime: now, QuoteVolume: decimal.NewFromInt(100)}))
	require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeFutures, OpenTime: now, QuoteVolume: decimal.NewFromInt(250)}))
	require.True(t, v.GetMarketVolume("BINANCE", "BTCUSDT", domain.MarketTypeFutures, domain.TF_1m, now).Equal(decimal.NewFromInt(250)))
}

func TestVolumeEngineSumsMinutes(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 5; i++ {
		require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: domain.MarketTypeSpot, OpenTime: now.Add(time.Duration(-i) * time.Minute), QuoteVolume: decimal.NewFromInt(int64(100 + i))}))
	}
	require.True(t, v.GetMarketVolume("BINANCE", "BTCUSDT", domain.MarketTypeSpot, domain.TF_5m, now).Equal(decimal.NewFromInt(510)))
}

func TestVolumeEngineRouteUsesWeakerLeg(t *testing.T) {
	v := NewVolumeEngine()
	now := time.Now().UTC().Truncate(time.Minute)
	for _, x := range []struct {
		ex string
		m  domain.MarketType
		q  int64
	}{{"BINANCE", domain.MarketTypeSpot, 10000}, {"BYBIT", domain.MarketTypeFutures, 7000}} {
		require.NoError(t, v.UpdateCandle(domain.MarketCandle{Exchange: x.ex, Symbol: "BTCUSDT", MarketType: x.m, OpenTime: now, QuoteVolume: decimal.NewFromInt(x.q)}))
	}
	require.True(t, v.GetRouteVolume("BINANCE", domain.MarketTypeSpot, "BYBIT", domain.MarketTypeFutures, "BTCUSDT", domain.TF_1m, now).Equal(decimal.NewFromInt(7000)))
}
