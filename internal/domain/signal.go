package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type SpreadType string

const (
	CrossExchange SpreadType = "CROSS_EXCHANGE"
	IntraExchange SpreadType = "INTRA_EXCHANGE"
)

type SpreadEvent struct {
	Symbol      string
	SpreadType  SpreadType
	Spread      decimal.Decimal
	ExchangeA   string
	ExchangeB   string
	QuoteVolume decimal.Decimal // 24h rolling volume для базовой оценки
	Timestamp   time.Time
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
	QuoteVolume   decimal.Decimal
}

func NewArbitrageSignal(event SpreadEvent, ts time.Time) *ArbitrageSignal {
	return &ArbitrageSignal{
		ID: uuid.NewString(), Symbol: event.Symbol, SpreadType: event.SpreadType,
		ExchangeA: event.ExchangeA, ExchangeB: event.ExchangeB, OpenedAt: ts,
		IsActive: true, InitialSpread: event.Spread, PeakSpread: event.Spread,
		QuoteVolume: event.QuoteVolume,
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
