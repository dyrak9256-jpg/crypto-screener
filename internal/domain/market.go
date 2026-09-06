package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type MarketType string

const (
	MarketTypeSpot    MarketType = "SPOT"
	MarketTypeFutures MarketType = "FUTURES"
)

type MarketTick struct {
	Exchange    string
	Symbol      string
	MarketType  MarketType
	BestBid     decimal.Decimal
	BestAsk     decimal.Decimal
	QuoteVolume decimal.Decimal // rolling 24h quote volume; not an interval delta
	EventTime   time.Time       // exchange timestamp when available
	ReceivedAt  time.Time       // local receive timestamp
	Timestamp   time.Time       // deprecated compatibility field; use EventTime/ReceivedAt
}

// FundingRate is an immutable snapshot of one exchange/instrument funding rate.
type FundingRate struct {
	Exchange        string
	Symbol          string
	Rate            decimal.Decimal
	NextFundingTime time.Time
	EventTime       time.Time
	ReceivedAt      time.Time
}
