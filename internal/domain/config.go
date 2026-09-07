package domain

import (
	"sync"

	"github.com/shopspring/decimal"
)

type Timeframe string

const (
	TF_1m  Timeframe = "1m"
	TF_5m  Timeframe = "5m"
	TF_15m Timeframe = "15m"
	TF_30m Timeframe = "30m"
	TF_1h  Timeframe = "1h"
	TF_4h  Timeframe = "4h"
	TF_24h Timeframe = "24h"
)

const hardMinSpread = "0.01"

type ScreenerConfig struct {
	mu            sync.RWMutex
	hardMinSpread decimal.Decimal
}

func NewScreenerConfig(hardSpread decimal.Decimal, _ ...decimal.Decimal) *ScreenerConfig {
	min := decimal.RequireFromString(hardMinSpread)
	if hardSpread.LessThan(min) {
		hardSpread = min
	}
	return &ScreenerConfig{hardMinSpread: hardSpread}
}

func (c *ScreenerConfig) GetHardMinSpread() decimal.Decimal {
	if c == nil {
		return decimal.RequireFromString(hardMinSpread)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hardMinSpread
}

func (c *ScreenerConfig) GetCloseThreshold() decimal.Decimal {
	return c.GetHardMinSpread().Div(decimal.NewFromInt(2))
}

func (c *ScreenerConfig) SetHardMinSpread(v decimal.Decimal) decimal.Decimal {
	min := decimal.RequireFromString(hardMinSpread)
	if v.LessThan(min) {
		v = min
	}
	c.mu.Lock()
	c.hardMinSpread = v
	c.mu.Unlock()
	return v
}

// SetUserCrossSpread validates/clamps a user's requested threshold against the
// administrative floor. The canonical value is stored on User, not here.
func (c *ScreenerConfig) SetUserCrossSpread(v decimal.Decimal) decimal.Decimal {
	return c.clamp(v)
}

func (c *ScreenerConfig) GetEffectiveCrossSpread() decimal.Decimal { return c.GetHardMinSpread() }
func (c *ScreenerConfig) GetEffectiveIntraSpread() decimal.Decimal { return c.GetHardMinSpread() }
func (c *ScreenerConfig) IsPairEnabled(string) bool                { return true }

func (c *ScreenerConfig) clamp(v decimal.Decimal) decimal.Decimal {
	h := c.GetHardMinSpread()
	if v.LessThan(h) {
		return h
	}
	return v
}
