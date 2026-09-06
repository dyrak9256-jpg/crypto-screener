package domain

//go:generate mockgen -destination mocks/mock_ports.go -package mocks crypto-screener/internal/domain ExchangeConnector,FundingSink,TelegramSender,SignalRepository,UserRepository,CommandHandler,VolumeProvider

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
)

// MarketConnector provides spot and/or futures market data.
// Connect methods block until the stream stops or ctx is cancelled.
type MarketConnector interface {
	ConnectSpot(ctx context.Context, outChan chan<- MarketTick) error
	ConnectFutures(ctx context.Context, outChan chan<- MarketTick) error
}

// FundingConnector is optional: exchanges that do not expose funding data
// can still be used as market connectors.
type FundingConnector interface {
	ConnectFunding(ctx context.Context, sink FundingSink) error
}

// ExchangeConnector is the common market-data capability.
type ExchangeConnector interface {
	MarketConnector
}

type FundingSink interface {
	UpdateFunding(exchange, symbol string, rate decimal.Decimal, nextTime, eventTime time.Time) error
	SetStreamHealth(exchange string, healthy bool)
}

type TelegramSender interface {
	SendPrivateMessage(chatID int64, text string)
	Broadcast(text string, chatIDs []int64)
	Close()
}

type SignalRepository interface {
	SaveSignal(ctx context.Context, signal *ArbitrageSignal) error
}

type UserRepository interface {
	SaveUser(ctx context.Context, user *User) error
	DeleteUser(ctx context.Context, chatID int64) error
	GetAllUsers(ctx context.Context) ([]*User, error)
}

type CommandHandler interface {
	HandleCommand(chatID int64, username string, cmd string, args []string) string
}

type VolumeProvider interface {
	GetSymbolVolume(symbol string, tf Timeframe, ts time.Time) decimal.Decimal
}
