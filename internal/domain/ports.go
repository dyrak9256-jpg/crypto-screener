package domain

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
)

type ExchangeConnector interface {
	ConnectSpot(ctx context.Context, outChan chan<- MarketTick) error
	ConnectFutures(ctx context.Context, outChan chan<- MarketTick) error
	ConnectFunding(ctx context.Context, sink FundingSink) error
}

type FundingSink interface {
	UpdateFunding(symbol string, rate decimal.Decimal, nextTime time.Time)
}

type TelegramSender interface {
	SendMessage(text string)
	Close()
}

type SignalRepository interface {
	SaveSignal(ctx context.Context, signal *ArbitrageSignal) error
}

type CommandHandler interface {
	HandleCommand(cmd string, args []string) string
}
