package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArbitrageSignal_NewAndLifecycle(t *testing.T) {
	t.Parallel()

	openedAt := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	event := SpreadEvent{
		Symbol:      "BTCUSDT",
		SpreadType:  CrossExchange,
		Spread:      decimal.RequireFromString("0.02"),
		ExchangeA:   "BINANCE",
		ExchangeB:   "BYBIT",
		QuoteVolume: decimal.RequireFromString("500000"),
		Timestamp:   openedAt,
	}

	sig := NewArbitrageSignal(event, openedAt)
	require.NotNil(t, sig)

	// Verify initialization
	_, err := uuid.Parse(sig.ID)
	require.NoError(t, err, "ID must be a valid UUID")
	assert.Equal(t, "BTCUSDT", sig.Symbol)
	assert.Equal(t, CrossExchange, sig.SpreadType)
	assert.Equal(t, "BINANCE", sig.ExchangeA)
	assert.Equal(t, "BYBIT", sig.ExchangeB)
	assert.Equal(t, openedAt, sig.OpenedAt)
	assert.True(t, sig.IsActive)
	assert.True(t, sig.InitialSpread.Equal(decimal.RequireFromString("0.02")))
	assert.True(t, sig.PeakSpread.Equal(decimal.RequireFromString("0.02")))
	assert.True(t, sig.QuoteVolume.Equal(decimal.RequireFromString("500000")))
	assert.True(t, sig.FinalSpread.IsZero())
	assert.Equal(t, time.Duration(0), sig.Duration)

	// UpdatePeak with higher spread
	sig.UpdatePeak(decimal.RequireFromString("0.05"))
	assert.True(t, sig.PeakSpread.Equal(decimal.RequireFromString("0.05")), "peak spread should increase to 0.05")

	// UpdatePeak with lower spread (must not decrease)
	sig.UpdatePeak(decimal.RequireFromString("0.03"))
	assert.True(t, sig.PeakSpread.Equal(decimal.RequireFromString("0.05")), "peak spread should not decrease")

	// UpdatePeak with equal spread
	sig.UpdatePeak(decimal.RequireFromString("0.05"))
	assert.True(t, sig.PeakSpread.Equal(decimal.RequireFromString("0.05")))

	// Close signal
	closedAt := openedAt.Add(90 * time.Second)
	finalSpread := decimal.RequireFromString("0.005")
	sig.Close(closedAt, finalSpread)

	assert.False(t, sig.IsActive)
	assert.Equal(t, closedAt, sig.ClosedAt)
	assert.True(t, sig.FinalSpread.Equal(finalSpread))
	assert.Equal(t, 90*time.Second, sig.Duration)
}

func TestScreenerConfig_Getters(t *testing.T) {
	t.Parallel()

	hardSpread := decimal.RequireFromString("0.015")
	hardVol := decimal.RequireFromString("500000")
	cfg := NewScreenerConfig(hardSpread, hardVol)

	assert.True(t, cfg.GetHardMinSpread().Equal(hardSpread))
	assert.True(t, cfg.GetHardMinVolume().Equal(hardVol))
	assert.True(t, cfg.GetEffectiveIntraSpread().Equal(hardSpread))
	assert.True(t, cfg.GetEffectiveMinVolume().Equal(hardVol))
	assert.Equal(t, TF_15m, cfg.GetUserTimeframe())
	assert.Equal(t, 30, cfg.GetFundingTimeBuffer())
	assert.True(t, cfg.IsPairEnabled("BTCUSDT"))
	assert.True(t, cfg.IsPairEnabled("ETHUSDT"))
}
