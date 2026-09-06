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
	Symbol       string
	SpreadType   SpreadType
	Spread       decimal.Decimal
	BuyExchange  string
	SellExchange string
	// Deprecated compatibility aliases. New code must use Buy/Sell fields.
	ExchangeA   string
	ExchangeB   string
	BuyMarket   MarketType
	SellMarket  MarketType
	BuyAsk      decimal.Decimal
	SellBid     decimal.Decimal
	QuoteVolume decimal.Decimal // route liquidity snapshot, rolling 24h
	FundingRate decimal.Decimal
	NextFunding time.Time
	Timestamp   time.Time
	Lifecycle   SignalLifecycle
}

type ArbitrageSignal struct {
	ID            string
	Symbol        string
	SpreadType    SpreadType
	BuyExchange   string
	SellExchange  string
	ExchangeA     string // deprecated compatibility alias
	ExchangeB     string // deprecated compatibility alias
	BuyMarket     MarketType
	SellMarket    MarketType
	OpenedAt      time.Time
	ClosedAt      time.Time
	IsActive      bool
	InitialSpread decimal.Decimal
	PeakSpread    decimal.Decimal
	FinalSpread   decimal.Decimal
	Duration      time.Duration
	QuoteVolume   decimal.Decimal
	FundingRate   decimal.Decimal
	NextFunding   time.Time

	// Chat IDs that actually received the OPEN notification. This is runtime
	// state and is intentionally not persisted with the market signal itself.
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
		OpenedAt: ts, IsActive: true, InitialSpread: event.Spread,
		PeakSpread: event.Spread, QuoteVolume: event.QuoteVolume,
		FundingRate: event.FundingRate, NextFunding: event.NextFunding,
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
	if !event.FundingRate.IsZero() || !event.NextFunding.IsZero() {
		s.FundingRate = event.FundingRate
		s.NextFunding = event.NextFunding
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
