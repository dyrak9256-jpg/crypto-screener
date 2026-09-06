package app

import (
	"sync"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

// FundingManager хранит ставки финансирования ПО БИРЖАМ: symbol -> exchange -> rate.
// Это корректнее для кросс-биржевого арбитража: ставка и время её выплаты зависят
// от конкретного фьючерсного рынка, а не от тикера как такового.
type FundingManager struct {
	mu     sync.RWMutex
	rates  map[string]map[string]*domain.FundingRate // symbol -> exchange -> rate
	config *domain.ScreenerConfig
}

func NewFundingManager(cfg *domain.ScreenerConfig) *FundingManager {
	return &FundingManager{
		rates:  make(map[string]map[string]*domain.FundingRate),
		config: cfg,
	}
}

// UpdateFunding регистрирует ставку финансирования для символа на конкретной бирже.
// Повторные вызовы перезаписывают запись (биржа/символ).
func (fm *FundingManager) UpdateFunding(exchange string, symbol string, rate decimal.Decimal, nextTime time.Time) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	byExchange, ok := fm.rates[symbol]
	if !ok {
		byExchange = make(map[string]*domain.FundingRate)
		fm.rates[symbol] = byExchange
	}
	byExchange[exchange] = &domain.FundingRate{
		Exchange:        exchange,
		Symbol:          symbol,
		Rate:            rate,
		NextFundingTime: nextTime,
	}
}

// IsArbProfitable проверяет, что для всех вовлечённых бирж выполняется условие
// прибыльности: 1) до выплаты funding осталось больше буфера; 2) |ставка| < спред.
// Если по какой-то бирже данных нет — считаем её «пермиссивной» (не фильтруем),
// т.к. отсутствие данных ещё не означает убыточности.
func (fm *FundingManager) IsArbProfitable(symbol string, spread decimal.Decimal, now time.Time, exchanges ...string) bool {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if len(exchanges) == 0 {
		return true
	}

	for _, ex := range exchanges {
		byExchange, ok := fm.rates[symbol]
		if !ok {
			continue // нет данных о funding для символа вообще
		}
		fr, ok := byExchange[ex]
		if !ok || fr == nil {
			continue // нет данных для конкретной биржи
		}

		minutesUntilFunding := fr.NextFundingTime.Sub(now).Minutes()
		if minutesUntilFunding < float64(fm.config.GetFundingTimeBuffer()) {
			return false
		}
		if fr.Rate.Abs().GreaterThanOrEqual(spread) {
			return false
		}
	}

	return true
}
