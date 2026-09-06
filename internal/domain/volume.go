package domain

import (
	"context"
	"github.com/shopspring/decimal"
	"time"
)

// MarketCandle is a 1-minute quote-turnover update. Updates for the same
// exchange/symbol/market/open-time replace the previous value; they are not
// accumulated. This is important because exchanges continuously update the
// currently open candle.
type MarketCandle struct {
	Exchange    string
	Symbol      string
	MarketType  MarketType
	OpenTime    time.Time
	CloseTime   time.Time
	QuoteVolume decimal.Decimal
	EventTime   time.Time
	Closed      bool
}

type CandleConnector interface {
	ConnectCandles(ctx context.Context, sink CandleSink) error
}

type CandleSink interface {
	UpdateCandle(MarketCandle) error
}
