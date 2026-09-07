package domain

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScreenerConfig_SetUserCrossSpread_EnforcesHardLimit(t *testing.T) {
	t.Parallel()

	hardLimit := decimal.RequireFromString("0.01") // 1% hard limit

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

			cfg := NewScreenerConfig(hardLimit)
			require.NotNil(t, cfg)

			retVal := cfg.SetUserCrossSpread(tc.inputSpread)
			assert.True(t, retVal.Equal(tc.expectedSpread), "returned value should equal expected spread")
			assert.True(t, cfg.GetEffectiveCrossSpread().Equal(hardLimit), "global effective spread must remain the administrative floor")
		})
	}
}

func TestScreenerConfig_GetCloseThreshold(t *testing.T) {
	t.Parallel()

	hardLimit := decimal.RequireFromString("0.01")

	t.Run("calculates half of hard spread", func(t *testing.T) {
		t.Parallel()
		cfg := NewScreenerConfig(hardLimit)
		cfg.SetUserCrossSpread(decimal.RequireFromString("0.04")) // user setting does not change lifecycle threshold
		threshold := cfg.GetCloseThreshold()
		assert.True(t, threshold.Equal(decimal.RequireFromString("0.02")))
	})

	t.Run("never exceeds the open threshold", func(t *testing.T) {
		t.Parallel()
		cfg := NewScreenerConfig(decimal.RequireFromString("0.001"))
		threshold := cfg.GetCloseThreshold()
		assert.True(t, threshold.Equal(decimal.RequireFromString("0.005")))
	})
}
