package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFundingManager_TimeBuffer(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	// Default fundingTimeBuffer is 30 minutes
	require.Equal(t, 30, cfg.GetFundingTimeBuffer())

	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	spread := decimal.RequireFromString("0.05")   // 5% spread
	lowRate := decimal.RequireFromString("0.001") // 0.1% rate (well below spread)

	tests := []struct {
		name              string
		nextFundingOffset time.Duration
		expectedResult    bool
	}{
		{
			name:              "funding imminent (10 minutes away, buffer is 30)",
			nextFundingOffset: 10 * time.Minute,
			expectedResult:    false,
		},
		{
			name:              "funding imminent (29 minutes away, buffer is 30)",
			nextFundingOffset: 29 * time.Minute,
			expectedResult:    false,
		},
		{
			name:              "funding in past (negative time offset)",
			nextFundingOffset: -5 * time.Minute,
			expectedResult:    false,
		},
		{
			name:              "funding safely ahead (35 minutes away)",
			nextFundingOffset: 35 * time.Minute,
			expectedResult:    true,
		},
		{
			name:              "funding safely ahead (2 hours away)",
			nextFundingOffset: 2 * time.Hour,
			expectedResult:    true,
		},
		{
			name:              "funding safely ahead (8 hours away)",
			nextFundingOffset: 8 * time.Hour,
			expectedResult:    true,
		},
		{
			name:              "boundary check (exactly 30 minutes away)",
			nextFundingOffset: 30 * time.Minute,
			expectedResult:    true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fm := NewFundingManager(cfg)
			fm.UpdateFunding("BINANCE", "BTCUSDT", lowRate, now.Add(tc.nextFundingOffset))

			isProfitable := fm.IsArbProfitable("BTCUSDT", spread, now, "BINANCE")
			assert.Equal(t, tc.expectedResult, isProfitable)
		})
	}
}

func TestFundingManager_Profitability(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	// Safe funding time: 2 hours in the future (> 30 min buffer)
	safeFundingTime := now.Add(2 * time.Hour)

	tests := []struct {
		name           string
		rate           decimal.Decimal
		spread         decimal.Decimal
		expectedResult bool
	}{
		{
			name:           "positive rate greater than spread",
			rate:           decimal.RequireFromString("0.03"), // 3%
			spread:         decimal.RequireFromString("0.02"), // 2%
			expectedResult: false,
		},
		{
			name:           "positive rate equal to spread",
			rate:           decimal.RequireFromString("0.02"),
			spread:         decimal.RequireFromString("0.02"),
			expectedResult: false,
		},
		{
			name:           "positive rate lower than spread",
			rate:           decimal.RequireFromString("0.005"), // 0.5%
			spread:         decimal.RequireFromString("0.02"),
			expectedResult: true,
		},
		{
			name:           "negative rate with abs greater than spread",
			rate:           decimal.RequireFromString("-0.035"), // |-3.5%| = 3.5%
			spread:         decimal.RequireFromString("0.02"),
			expectedResult: false,
		},
		{
			name:           "negative rate with abs equal to spread",
			rate:           decimal.RequireFromString("-0.02"),
			spread:         decimal.RequireFromString("0.02"),
			expectedResult: false,
		},
		{
			name:           "negative rate with abs lower than spread",
			rate:           decimal.RequireFromString("-0.008"), // |-0.8%| = 0.8%
			spread:         decimal.RequireFromString("0.02"),
			expectedResult: true,
		},
		{
			name:           "zero funding rate",
			rate:           decimal.Zero,
			spread:         decimal.RequireFromString("0.01"),
			expectedResult: true,
		},
		{
			name:           "zero spread with zero funding rate",
			rate:           decimal.Zero,
			spread:         decimal.Zero,
			expectedResult: false, // 0 >= 0 is true -> not profitable
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fm := NewFundingManager(cfg)
			fm.UpdateFunding("BINANCE", "ETHUSDT", tc.rate, safeFundingTime)

			isProfitable := fm.IsArbProfitable("ETHUSDT", tc.spread, now, "BINANCE")
			assert.Equal(t, tc.expectedResult, isProfitable)
		})
	}
}

func TestFundingManager_UnregisteredSymbol(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	fm := NewFundingManager(cfg)
	now := time.Now()

	// Symbol without prior UpdateFunding call should default to profitable
	isProfitable := fm.IsArbProfitable("UNREGISTERED_COIN", decimal.RequireFromString("0.02"), now)
	assert.True(t, isProfitable, "unregistered symbol should be considered profitable by default")
}

func TestFundingManager_UpdateFunding_OverwritesPreviousRate(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	fm := NewFundingManager(cfg)
	now := time.Now()
	safeTime := now.Add(2 * time.Hour)

	// Initially unprofitable due to high rate
	fm.UpdateFunding("BINANCE", "SOLUSDT", decimal.RequireFromString("0.05"), safeTime)
	assert.False(t, fm.IsArbProfitable("SOLUSDT", decimal.RequireFromString("0.02"), now, "BINANCE"))

	// Update to low rate -> should now be profitable
	fm.UpdateFunding("BINANCE", "SOLUSDT", decimal.RequireFromString("0.001"), safeTime)
	assert.True(t, fm.IsArbProfitable("SOLUSDT", decimal.RequireFromString("0.02"), now, "BINANCE"))
}
