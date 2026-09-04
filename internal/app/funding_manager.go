package app

import (
	"sync"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

type FundingManager struct {
	mu     sync.RWMutex
	rates  map[string]*domain.FundingRate
	config *domain.ScreenerConfig
}

func NewFundingManager(cfg *domain.ScreenerConfig) *FundingManager {
	return &FundingManager{
		rates:  make(map[string]*domain.FundingRate),
		config: cfg,
	}
}

func (fm *FundingManager) UpdateFunding(symbol string, rate decimal.Decimal, nextTime time.Time) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.rates[symbol] = &domain.FundingRate{Symbol: symbol, Rate: rate, NextFundingTime: nextTime}
}

func (fm *FundingManager) IsArbProfitable(symbol string, spread decimal.Decimal, now time.Time) bool {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	fr, exists := fm.rates[symbol]
	if !exists {
		return true
	}

	minutesUntilFunding := fr.NextFundingTime.Sub(now).Minutes()

	if minutesUntilFunding < float64(fm.config.GetFundingTimeBuffer()) {
		return false
	}

	if fr.Rate.Abs().GreaterThanOrEqual(spread) {
		return false
	}

	return true
}
