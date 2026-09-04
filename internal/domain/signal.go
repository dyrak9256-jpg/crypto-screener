package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const (
	CrossExchange SpreadType = "CROSS_EXCHANGE"
	IntraExchange SpreadType = "INTRA_EXCHANGE"
)

// Событие SpreadEvent передается от агрегатора к трекеру.
type SpreadEvent struct {
	Symbol     string
	SpreadType SpreadType
	Spread     decimal.Decimal
	ExchangeA  string
	ExchangeB  string
	Timestamp  time.Time
}

type ArbitrageSignal struct {
	ID            string
	Symbol        string
	SpreadType    SpreadType
	ExchangeA     string
	ExchangeB     string
	OpenedAt      time.Time
	ClosedAt      time.Time
	IsActive      bool
	InitialSpread decimal.Decimal
	PeakSpread    decimal.Decimal
	FinalSpread   decimal.Decimal
	Duration      time.Duration
}

func NewArbitrageSignal(event SpreadEvent, ts time.Time) *ArbitrageSignal {
	return &ArbitrageSignal{
		ID:            uuid.NewString(),
		Symbol:        event.Symbol,
		SpreadType:    event.SpreadType,
		ExchangeA:     event.ExchangeA,
		ExchangeB:     event.ExchangeB,
		OpenedAt:      ts,
		IsActive:      true,
		InitialSpread: event.Spread,
		PeakSpread:    event.Spread,
	}
}

func (s *ArbitrageSignal) UpdatePeak(currentSpread decimal.Decimal) {
	if currentSpread.GreaterThan(s.PeakSpread) {
		s.PeakSpread = currentSpread
	}
}

func (s *ArbitrageSignal) Close(ts time.Time, finalSpread decimal.Decimal) {
	s.IsActive = false
	s.ClosedAt = ts
	s.FinalSpread = finalSpread
	s.Duration = ts.Sub(s.OpenedAt)
}
