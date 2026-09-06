package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// MarketType описывает тип рынка (Спот или Фьючерс)
type MarketType string

const (
	MarketTypeSpot    MarketType = "SPOT"
	MarketTypeFutures MarketType = "FUTURES"
)

// MarketTick — единый нормализованный тикер цен с биржи
type MarketTick struct {
	Exchange    string
	Symbol      string
	MarketType  MarketType
	BestBid     decimal.Decimal
	BestAsk     decimal.Decimal
	QuoteVolume decimal.Decimal
	Timestamp   time.Time
}

type FundingRate struct {
	Exchange        string // Биржа-источник ставки (например, "BINANCE")
	Symbol          string
	Rate            decimal.Decimal
	NextFundingTime time.Time
}
