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

type SignalLifecycle uint8

const (
	SignalOpened SignalLifecycle = iota + 1
	SignalUpdated
	SignalClosed
)

type SpreadEvent struct {
	Symbol          string
	SpreadType      SpreadType
	Spread          decimal.Decimal
	BuyExchange     string
	SellExchange    string
	ExchangeA       string // legacy alias
	ExchangeB       string // legacy alias
	BuyMarket       MarketType
	SellMarket      MarketType
	BuyAsk          decimal.Decimal
	SellBid         decimal.Decimal
	QuoteVolume     decimal.Decimal
	BuyFundingRate  decimal.Decimal
	SellFundingRate decimal.Decimal
	BuyNextFunding  time.Time
	SellNextFunding time.Time
	Timestamp       time.Time
	Lifecycle       SignalLifecycle
}

type ArbitrageSignal struct {
	ID              string
	Symbol          string
	SpreadType      SpreadType
	BuyExchange     string
	SellExchange    string
	ExchangeA       string
	ExchangeB       string
	BuyMarket       MarketType
	SellMarket      MarketType
	OpenedAt        time.Time
	ClosedAt        time.Time
	IsActive        bool
	InitialSpread   decimal.Decimal
	PeakSpread      decimal.Decimal
	FinalSpread     decimal.Decimal
	Duration        time.Duration
	QuoteVolume     decimal.Decimal
	BuyFundingRate  decimal.Decimal
	SellFundingRate decimal.Decimal
	BuyNextFunding  time.Time
	SellNextFunding time.Time
	NotifiedChatIDs []int64
}

func NewArbitrageSignal(event SpreadEvent, ts time.Time) *ArbitrageSignal {
	buy, sell := event.BuyExchange, event.SellExchange
	if buy == "" {
		buy = event.ExchangeA
	}
	if sell == "" {
		sell = event.ExchangeB
	}
	return &ArbitrageSignal{
		ID: uuid.NewString(), Symbol: event.Symbol, SpreadType: event.SpreadType,
		BuyExchange: buy, SellExchange: sell, ExchangeA: buy, ExchangeB: sell,
		BuyMarket: event.BuyMarket, SellMarket: event.SellMarket,
		OpenedAt: ts, IsActive: true, InitialSpread: event.Spread, PeakSpread: event.Spread,
		QuoteVolume:    event.QuoteVolume,
		BuyFundingRate: event.BuyFundingRate, SellFundingRate: event.SellFundingRate,
		BuyNextFunding: event.BuyNextFunding, SellNextFunding: event.SellNextFunding,
	}
}

func (s *ArbitrageSignal) Update(event SpreadEvent) {
	if s == nil || !s.IsActive || event.Timestamp.Before(s.OpenedAt) {
		return
	}
	if event.Spread.GreaterThan(s.PeakSpread) {
		s.PeakSpread = event.Spread
	}
	if event.QuoteVolume.GreaterThan(s.QuoteVolume) {
		s.QuoteVolume = event.QuoteVolume
	}
	// Funding is part of the route snapshot. Assign it even when the current
	// rate becomes exactly zero; otherwise a zero-rate update would leave stale
	// funding in the active signal.
	s.BuyFundingRate, s.SellFundingRate = event.BuyFundingRate, event.SellFundingRate
	s.BuyNextFunding, s.SellNextFunding = event.BuyNextFunding, event.SellNextFunding
}

// UpdatePeak is kept as a small compatibility API for callers/tests that only
// have a spread observation and do not need to construct a full event.
func (s *ArbitrageSignal) UpdatePeak(spread decimal.Decimal) {
	if s == nil || !s.IsActive {
		return
	}
	if spread.GreaterThan(s.PeakSpread) {
		s.PeakSpread = spread
	}
}

func (s *ArbitrageSignal) Close(ts time.Time, finalSpread decimal.Decimal) {
	if s == nil || !s.IsActive {
		return
	}
	if ts.Before(s.OpenedAt) {
		ts = s.OpenedAt
	}
	s.IsActive = false
	s.ClosedAt = ts
	s.FinalSpread = finalSpread
	s.Duration = ts.Sub(s.OpenedAt)
}
func (s *ArbitrageSignal) Snapshot() *ArbitrageSignal {
	if s == nil {
		return nil
	}
	copy := *s
	copy.NotifiedChatIDs = append([]int64(nil), s.NotifiedChatIDs...)
	return &copy
}
