package domain

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestScreenerConfig_GetCloseThreshold(t *testing.T) {
	t.Parallel()

	hardVol := decimal.RequireFromString("1000000")

	t.Run("calculates half of hard min spread", func(t *testing.T) {
		t.Parallel()
		cfg := NewScreenerConfig(decimal.RequireFromString("0.04"), hardVol) // 4% -> 2%
		assert.True(t, cfg.GetCloseThreshold().Equal(decimal.RequireFromString("0.02")))
	})

	t.Run("enforces 0.1% absolute minimum floor", func(t *testing.T) {
		t.Parallel()
		// hard limit 0.001 (0.1%) -> half would be 0.0005 (< floor 0.001)
		cfg := NewScreenerConfig(decimal.RequireFromString("0.001"), hardVol)
		assert.True(t, cfg.GetCloseThreshold().Equal(decimal.RequireFromString("0.001")))
	})
}
