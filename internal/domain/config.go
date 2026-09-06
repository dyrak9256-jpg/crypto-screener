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
	mu sync.RWMutex

	hardMinSpread     decimal.Decimal
	hardMinVolume     decimal.Decimal
	fundingTimeBuffer int // Буфер в минутах до выплаты funding

	userCrossSpread decimal.Decimal
	userIntraSpread decimal.Decimal
	userMinVolume   decimal.Decimal
	userTimeframe   Timeframe
}

func NewScreenerConfig(hardSpread, hardVol decimal.Decimal) *ScreenerConfig {
	minSpread := decimal.RequireFromString("0.01")
	if hardSpread.LessThan(minSpread) {
		hardSpread = minSpread
	}
	return &ScreenerConfig{
		hardMinSpread:     hardSpread,
		hardMinVolume:     hardVol,
		fundingTimeBuffer: 30, // По умолчанию игнорируем сигналы за 30 минут до funding
		userCrossSpread:   hardSpread,
		userIntraSpread:   hardSpread,
		userMinVolume:     hardVol,
		userTimeframe:     TF_15m,
	}
}

// --- Getters ---

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

func (c *ScreenerConfig) GetHardMinVolume() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hardMinVolume
}

func (c *ScreenerConfig) GetFundingTimeBuffer() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fundingTimeBuffer
}

func (c *ScreenerConfig) GetCloseThreshold() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.hardMinSpread.IsNegative() {
		return decimal.Zero
	}
	return c.hardMinSpread.Div(decimal.NewFromInt(2))
}

func (c *ScreenerConfig) SetHardMinSpread(val decimal.Decimal) decimal.Decimal {
	min := decimal.RequireFromString("0.01")
	if val.LessThan(min) {
		val = min
	}
	c.mu.Lock()
	c.hardMinSpread = val
	if c.userCrossSpread.LessThan(val) {
		c.userCrossSpread = val
	}
	if c.userIntraSpread.LessThan(val) {
		c.userIntraSpread = val
	}
	c.mu.Unlock()
	return val
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

func (c *ScreenerConfig) IsPairEnabled(symbol string) bool {
	return true
}
