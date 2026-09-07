package domain

import (
	"fmt"
	"sort"
	"strings"
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

// defaultTakerFee — консервативная оценка taker-комиссии одной стороны сделки
// (5 базисных пунктов). Используется, пока оператор не задал свои значения
// через переменную окружения FEES или команду /setfees.
const defaultTakerFee = "0.0005"

// maxFee ограничивает сверху вводимую комиссию, чтобы опечатка вида "0.5"
// (50%) не превратила скринер в генератор пустых результатов.
const maxFee = "0.01"

// FeeKeyDefault — ключ карты комиссий, применяемый ко всем биржам без
// собственного значения.
const FeeKeyDefault = "DEFAULT"

type ScreenerConfig struct {
	mu            sync.RWMutex
	hardMinSpread decimal.Decimal
	fees          map[string]decimal.Decimal
}

func NewScreenerConfig(hardSpread decimal.Decimal, _ ...decimal.Decimal) *ScreenerConfig {
	min := decimal.RequireFromString(hardMinSpread)
	if hardSpread.LessThan(min) {
		hardSpread = min
	}
	return &ScreenerConfig{
		hardMinSpread: hardSpread,
		fees:          map[string]decimal.Decimal{FeeKeyDefault: decimal.RequireFromString(defaultTakerFee)},
	}
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

// SetFee задаёт taker-комиссию одной стороны сделки для биржи (доля, не
// проценты: 0.0004 = 4 б.п.). Ключ "DEFAULT" действует как значение по
// умолчанию для всех бирж без явной настройки.
func (c *ScreenerConfig) SetFee(exchange string, v decimal.Decimal) {
	exchange = strings.ToUpper(strings.TrimSpace(exchange))
	if exchange == "" {
		return
	}
	lo := decimal.Zero
	hi := decimal.RequireFromString(maxFee)
	if v.LessThan(lo) {
		v = lo
	}
	if v.GreaterThan(hi) {
		v = hi
	}
	c.mu.Lock()
	if c.fees == nil {
		c.fees = make(map[string]decimal.Decimal)
	}
	c.fees[exchange] = v
	c.mu.Unlock()
}

// FeeFor возвращает комиссию биржи; при отсутствии точного значения — DEFAULT,
// при не настроенной карте — встроенный консервативный ориентир.
func (c *ScreenerConfig) FeeFor(exchange string) decimal.Decimal {
	fallback := decimal.RequireFromString(defaultTakerFee)
	if c == nil {
		return fallback
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.fees != nil {
		if v, ok := c.fees[strings.ToUpper(strings.TrimSpace(exchange))]; ok {
			return v
		}
		if v, ok := c.fees[FeeKeyDefault]; ok {
			return v
		}
	}
	return fallback
}

// FeesString возвращает текущую карту комиссий в формате, пригодном для
// команды /setfees и переменной окружения FEES (например "DEFAULT:0.0005,BINANCE:0.0004").
func (c *ScreenerConfig) FeesString() string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.fees))
	for k := range c.fees {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+":"+c.fees[k].String())
	}
	return strings.Join(parts, ",")
}

// ParseFees разбирает строку вида "BINANCE:0.0004,DEFAULT:0.0005" в карту
// комиссий. Используется переменной окружения FEES и командой /setfees.
func ParseFees(raw string) (map[string]decimal.Decimal, error) {
	out := make(map[string]decimal.Decimal)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			return nil, errBadFeePart(part)
		}
		key := strings.ToUpper(strings.TrimSpace(kv[0]))
		if key == "" {
			return nil, errBadFeePart(part)
		}
		v, err := decimal.NewFromString(strings.TrimSpace(kv[1]))
		if err != nil || v.IsNegative() || v.GreaterThan(decimal.RequireFromString(maxFee)) {
			return nil, errBadFeePart(part)
		}
		out[key] = v
	}
	return out, nil
}

func errBadFeePart(part string) error {
	return fmt.Errorf("некорректная запись комиссии %q (ожидается БИРЖА:ДОЛЯ, напр. BINANCE:0.0004; 0 ≤ доля ≤ 0.01)", part)
}

// ApplyFees заменяет всю карту комиссий значениями из строки FEES-формата.
func (c *ScreenerConfig) ApplyFees(raw string) error {
	parsed, err := ParseFees(raw)
	if err != nil {
		return err
	}
	if len(parsed) == 0 {
		return nil
	}
	c.mu.Lock()
	c.fees = parsed
	c.mu.Unlock()
	return nil
}
