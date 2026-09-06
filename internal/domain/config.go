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

// ScreenerConfig — потокобезопасная конфигурация жёстких порогов и политик.
// Персональные настройки пользователей (мин. спред/объём/таймфрейм) живут в User
// и применяются в NotificationRouter; здесь — только глобальные guardrails.
type ScreenerConfig struct {
	mu sync.RWMutex

	hardMinSpread     decimal.Decimal
	hardMinVolume     decimal.Decimal
	fundingTimeBuffer int // Буфер в минутах до выплаты funding
}

func NewScreenerConfig(hardSpread, hardVol decimal.Decimal) *ScreenerConfig {
	return &ScreenerConfig{
		hardMinSpread:     hardSpread,
		hardMinVolume:     hardVol,
		fundingTimeBuffer: 30, // По умолчанию игнорируем сигналы за 30 минут до funding
	}
}

// --- Getters ---

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

// GetCloseThreshold возвращает порог, при котором активный сигнал считается
// закрывшимся. Используется явный гистерезис: половина жёсткого минимального
// спреда с абсолютным полом 0.1%, чтобы избежать «дребезга» на границе порогов.
func (c *ScreenerConfig) GetCloseThreshold() decimal.Decimal {
	c.mu.RLock()
	defer c.mu.RUnlock()
	close := c.hardMinSpread.Div(decimal.NewFromInt(2))
	minClose := decimal.NewFromFloat(0.001) // 0.1% absolute minimum
	if close.LessThan(minClose) {
		return minClose
	}
	return close
}

// IsPairEnabled — признак того, что пара допущена к анализу.
// Сейчас фильтр пар отключён (все пары включены); можно расширить allow-list'ом.
func (c *ScreenerConfig) IsPairEnabled(symbol string) bool {
	return true
}
