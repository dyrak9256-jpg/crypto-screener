package app

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

func newTestFundingManager(t *testing.T, spread decimal.Decimal) *FundingManager {
	t.Helper()
	cfg := DefaultFundingConfig()
	cfg.MinSpread = spread
	cfg.PreFundingHazardWindow = 30 * time.Minute
	fm, err := NewFundingManager(cfg, testClock{now: time.Now()})
	require.NoError(t, err)
	return fm
}

func TestFundingManager_DirectionAndExchangeIsolation(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	fm := newTestFundingManager(t, decimal.RequireFromString("0.01"))
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.RequireFromString("0.001"), now.Add(2*time.Hour), now))
	require.NoError(t, fm.UpdateFunding("BYBIT", "BTCUSDT", decimal.RequireFromString("0.05"), now.Add(2*time.Hour), now))
	got := fm.EvaluateArb("BINANCE", "BTCUSDT", decimal.RequireFromString("0.02"), SpotLongFuturesShort, now)
	require.True(t, got.Profitable)
	got = fm.EvaluateArb("BYBIT", "BTCUSDT", decimal.RequireFromString("0.02"), SpotLongFuturesShort, now)
	require.False(t, got.Profitable)
}

func TestFundingManager_StaleAndHazard(t *testing.T) {
	now := time.Now()
	fm := newTestFundingManager(t, decimal.Zero)
	require.NoError(t, fm.UpdateFunding("BINANCE", "ETHUSDT", decimal.Zero, now.Add(10*time.Minute), now))
	got := fm.EvaluateArb("BINANCE", "ETHUSDT", decimal.RequireFromString("0.02"), SpotLongFuturesShort, now)
	require.Equal(t, ReasonHazardWindow, got.Reason)
	require.NoError(t, fm.UpdateFunding("BINANCE", "ETHUSDT", decimal.Zero, now.Add(2*time.Hour), now.Add(time.Second)))
	got = fm.EvaluateArb("BINANCE", "ETHUSDT", decimal.RequireFromString("0.02"), SpotLongFuturesShort, now.Add(20*time.Second))
	require.Equal(t, ReasonProfitable, got.Reason)
	got = fm.EvaluateArb("BINANCE", "ETHUSDT", decimal.RequireFromString("0.02"), SpotLongFuturesShort, now.Add(20*time.Second+16*time.Second))
	require.Equal(t, ReasonDataStale, got.Reason)
}
