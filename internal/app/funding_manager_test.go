package app

import (
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }
func newTestFundingManager(t *testing.T, spread decimal.Decimal) *FundingManager {
	t.Helper()
	cfg := DefaultFundingConfig()
	cfg.MinSpread = spread
	fm, err := NewFundingManager(cfg, testClock{now: time.Now()})
	require.NoError(t, err)
	return fm
}
func TestFundingManager_SpotFuturesDirection(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	fm := newTestFundingManager(t, decimal.Zero)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.RequireFromString("0.001"), now.Add(2*time.Hour), now))
	got := fm.EvaluateSpotFutures("BINANCE", "BTCUSDT", decimal.RequireFromString("0.02"), now)
	require.True(t, got.Profitable)
	require.True(t, got.NetSpread.Equal(decimal.RequireFromString("0.021")))
}
func TestFundingManager_FuturesPairFundingIsRouteAware(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	fm := newTestFundingManager(t, decimal.Zero)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.RequireFromString("0.01"), now.Add(2*time.Hour), now))
	require.NoError(t, fm.UpdateFunding("BYBIT", "BTCUSDT", decimal.RequireFromString("0.001"), now.Add(3*time.Hour), now))
	got := fm.EvaluateFuturesPair("BINANCE", "BTCUSDT", "BYBIT", "BTCUSDT", decimal.RequireFromString("0.02"), now)
	require.True(t, got.Profitable)
	require.True(t, got.NetSpread.Equal(decimal.RequireFromString("0.011")))
	require.True(t, got.BuyFundingRate.Equal(decimal.RequireFromString("0.01")))
	require.True(t, got.SellFundingRate.Equal(decimal.RequireFromString("0.001")))
}
func TestFundingManager_FuturesPairBlocksWhenFundingConsumesSpread(t *testing.T) {
	now := time.Now()
	fm := newTestFundingManager(t, decimal.Zero)
	require.NoError(t, fm.UpdateFunding("BINANCE", "ETHUSDT", decimal.RequireFromString("0.03"), now.Add(time.Hour), now))
	require.NoError(t, fm.UpdateFunding("BYBIT", "ETHUSDT", decimal.Zero, now.Add(time.Hour), now))
	got := fm.EvaluateFuturesPair("BINANCE", "ETHUSDT", "BYBIT", "ETHUSDT", decimal.RequireFromString("0.02"), now)
	require.False(t, got.Profitable)
	require.Equal(t, ReasonBelowMinSpread, got.Reason)
}
func TestFundingManager_FailClosedOnMissingOrStale(t *testing.T) {
	now := time.Now()
	fm := newTestFundingManager(t, decimal.Zero)
	got := fm.EvaluateSpotFutures("BINANCE", "ETHUSDT", decimal.RequireFromString("0.02"), now)
	require.False(t, got.Profitable)
	require.Equal(t, ReasonNoData, got.Reason)
	require.NoError(t, fm.UpdateFunding("BINANCE", "ETHUSDT", decimal.Zero, now.Add(time.Hour), now))
	got = fm.EvaluateSpotFutures("BINANCE", "ETHUSDT", decimal.RequireFromString("0.02"), now.Add(20*time.Second))
	require.True(t, got.Profitable)
	got = fm.EvaluateSpotFutures("BINANCE", "ETHUSDT", decimal.RequireFromString("0.02"), now.Add(61*time.Second))
	require.False(t, got.Profitable)
	require.Equal(t, ReasonDataStale, got.Reason)
}

func TestFundingManager_SpotFuturesAdverseFundingBlocksSignal(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	fm := newTestFundingManager(t, decimal.Zero)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.RequireFromString("-0.03"), now.Add(time.Hour), now))
	got := fm.EvaluateSpotFutures("BINANCE", "BTCUSDT", decimal.RequireFromString("0.02"), now)
	require.False(t, got.Profitable)
	require.Equal(t, ReasonBelowMinSpread, got.Reason)
	require.True(t, got.NetSpread.Equal(decimal.RequireFromString("-0.01")))
}

func TestFundingManager_MissingNextFundingFailsClosed(t *testing.T) {
	now := time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC)
	fm, err := NewFundingManager(FundingConfig{MinSpread: decimal.RequireFromString("0.01"), MaxDataStaleness: time.Minute}, testClock{now: now})
	require.NoError(t, err)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", decimal.Zero, time.Time{}, now))
	result := fm.EvaluateSpotFutures("BINANCE", "BTCUSDT", decimal.RequireFromString("0.02"), now)
	require.False(t, result.Profitable)
	require.Equal(t, ReasonNoData, result.Reason)
}
