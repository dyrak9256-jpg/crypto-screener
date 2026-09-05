package domain

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SetUserCrossSpread sets userCrossSpread enforcing hardMinSpread
func (c *ScreenerConfig) SetUserCrossSpread(val decimal.Decimal) decimal.Decimal {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val.LessThan(c.hardMinSpread) {
		val = c.hardMinSpread
	}
	c.userCrossSpread = val
	return val
}

func TestScreenerConfig_SetUserCrossSpread_EnforcesHardLimit(t *testing.T) {
	t.Parallel()

	hardLimit := decimal.RequireFromString("0.01") // 1% hard limit
	hardVol := decimal.RequireFromString("1000000")

	tests := []struct {
		name           string
		inputSpread    decimal.Decimal
		expectedSpread decimal.Decimal
	}{
		{
			name:           "spread below hard limit is clamped to hard limit",
			inputSpread:    decimal.RequireFromString("0.005"), // 0.5% < 1.0%
			expectedSpread: hardLimit,
		},
		{
			name:           "spread much lower than hard limit is clamped",
			inputSpread:    decimal.RequireFromString("0.0001"),
			expectedSpread: hardLimit,
		},
		{
			name:           "negative spread is clamped to hard limit",
			inputSpread:    decimal.RequireFromString("-0.02"),
			expectedSpread: hardLimit,
		},
		{
			name:           "zero spread is clamped to hard limit",
			inputSpread:    decimal.Zero,
			expectedSpread: hardLimit,
		},
		{
			name:           "spread equal to hard limit is accepted",
			inputSpread:    hardLimit,
			expectedSpread: hardLimit,
		},
		{
			name:           "spread above hard limit is accepted as is",
			inputSpread:    decimal.RequireFromString("0.025"), // 2.5% > 1.0%
			expectedSpread: decimal.RequireFromString("0.025"),
		},
		{
			name:           "spread significantly above hard limit is accepted",
			inputSpread:    decimal.RequireFromString("0.10"), // 10%
			expectedSpread: decimal.RequireFromString("0.10"),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := NewScreenerConfig(hardLimit, hardVol)
			require.NotNil(t, cfg)

			retVal := cfg.SetUserCrossSpread(tc.inputSpread)
			assert.True(t, retVal.Equal(tc.expectedSpread), "returned value should equal expected spread")
			assert.True(t, cfg.GetEffectiveCrossSpread().Equal(tc.expectedSpread), "effective cross spread should equal expected spread")
		})
	}
}

func TestScreenerConfig_GetCloseThreshold(t *testing.T) {
	t.Parallel()

	hardLimit := decimal.RequireFromString("0.01")
	hardVol := decimal.RequireFromString("1000000")

	t.Run("calculates half of user cross spread", func(t *testing.T) {
		t.Parallel()
		cfg := NewScreenerConfig(hardLimit, hardVol)
		cfg.SetUserCrossSpread(decimal.RequireFromString("0.04")) // 4% -> close threshold 2%
		threshold := cfg.GetCloseThreshold()
		assert.True(t, threshold.Equal(decimal.RequireFromString("0.02")))
	})

	t.Run("enforces 0.1% absolute minimum floor", func(t *testing.T) {
		t.Parallel()
		// If hard limit is very low, e.g. 0.001 (0.1%), half would be 0.0005, which is < 0.001
		cfg := NewScreenerConfig(decimal.RequireFromString("0.001"), hardVol)
		cfg.SetUserCrossSpread(decimal.RequireFromString("0.001"))
		threshold := cfg.GetCloseThreshold()
		assert.True(t, threshold.Equal(decimal.RequireFromString("0.001")))
	})
}
