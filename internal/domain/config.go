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

type ScreenerConfig struct {
	mu              sync.RWMutex
	hardMinSpread   decimal.Decimal
	hardMinVolume   decimal.Decimal
	userCrossSpread decimal.Decimal
	userIntraSpread decimal.Decimal
	userMinVolume   decimal.Decimal
	userTimeframe   Timeframe
}

func NewScreenerConfig(hardSpread, hardVol decimal.Decimal) *ScreenerConfig {
	return &ScreenerConfig{
		hardMinSpread:   hardSpread,
		hardMinVolume:   hardVol,
		userCrossSpread: hardSpread,
		userIntraSpread: hardSpread,
		userMinVolume:   hardVol,
		userTimeframe:   TF_15m,
	}
}

func (c *ScreenerConfig) SetUserCrossSpread(val decimal.Decimal) decimal.Decimal {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val.LessThan(c.hardMinSpread) {
		val = c.hardMinSpread
	}
	c.userCrossSpread = val
	return val
}

func (c *ScreenerConfig) SetUserIntraSpread(val decimal.Decimal) decimal.Decimal {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val.LessThan(c.hardMinSpread) {
		val = c.hardMinSpread
	}
	c.userIntraSpread = val
	return val
}

func (c *ScreenerConfig) SetUserMinVolume(val decimal.Decimal) decimal.Decimal {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val.LessThan(c.hardMinVolume) {
		val = c.hardMinVolume
	}
	c.userMinVolume = val
	return val
}

func (c *ScreenerConfig) SetUserTimeframe(tf Timeframe) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.userTimeframe = tf
}

func (c *ScreenerConfig) GetEffectiveCrossSpread() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userCrossSpread
}
func (c *ScreenerConfig) GetEffectiveIntraSpread() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userIntraSpread
}
func (c *ScreenerConfig) GetEffectiveMinVolume() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userMinVolume
}
func (c *ScreenerConfig) GetUserTimeframe() Timeframe {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userTimeframe
}
func (c *ScreenerConfig) GetHardMinSpread() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hardMinSpread
}
func (c *ScreenerConfig) GetCloseThreshold() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	close := c.userCrossSpread.Div(decimal.NewFromInt(2))
	minClose := decimal.NewFromFloat(0.001)
	if close.LessThan(minClose) {
		return minClose
	}
	return close
}
func (c *ScreenerConfig) IsPairEnabled(symbol string) bool { return true }
