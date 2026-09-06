package domain

//go:generate mockgen -destination mocks/mock_ports.go -package mocks crypto-screener/internal/domain ExchangeConnector,FundingSink,TelegramSender,SignalRepository,UserRepository,CommandHandler,VolumeProvider

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
	// UpdateFunding регистрирует ставку финансирования для конкретной биржи.
	// exchange — идентификатор биржи-источника; ставки хранятся отдельно на биржу.
	UpdateFunding(exchange string, symbol string, rate decimal.Decimal, nextTime time.Time)
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
