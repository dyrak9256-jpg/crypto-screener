package domain

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

// SettingsRepository — опциональная возможность репозитория: персистентность
// настроек оператора между рестартами. Приложение проверяет её наличие по
// образцу ReconcileActiveSignals и деградирует корректно, если её нет.
type SettingsRepository interface {
	GetSetting(ctx context.Context, key string) (string, bool, error)
	SetSetting(ctx context.Context, key, value string) error
}

// StatsRepository — опциональная способность: агрегированная статистика
// сигналов для команды /stats.
type StatsRepository interface {
	SignalStats24h(ctx context.Context) (SignalStats, error)
}

// SignalStats — агрегат по сигналам за последние 24 часа.
type SignalStats struct {
	Opened24h     int
	Closed24h     int
	AvgPeakSpread decimal.Decimal
	AvgDuration   time.Duration
}

type CommandHandler interface {
	// botID identifies the Telegram bot instance that received the command
	// (0 when a single bot is deployed). It is stored with the user so
	// notifications can be routed back through the same bot.
	HandleCommand(botID, chatID int64, username string, cmd string, args []string) string
}
