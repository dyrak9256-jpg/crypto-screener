package app

import (
	"fmt"
	"time"

	"crypto-screener/internal/domain"

	"github.com/shopspring/decimal"
)

type NotificationRouter struct {
	userMgr     *domain.UserManager
	volProvider domain.VolumeProvider
	telegram    domain.TelegramSender
}

func NewNotificationRouter(userMgr *domain.UserManager, volProvider domain.VolumeProvider, tg domain.TelegramSender) *NotificationRouter {
	return &NotificationRouter{userMgr: userMgr, volProvider: volProvider, telegram: tg}
}

func (nr *NotificationRouter) ProcessSignal(signal *domain.ArbitrageSignal, isOpened bool) {
	users := nr.userMgr.GetAllUsers()
	if len(users) == 0 {
		return
	}

	// Группируем пользователей по таймфреймам, чтобы не считать объем для каждого отдельно
	tfGroups := make(map[domain.Timeframe][]*domain.User)
	for _, u := range users {
		tfGroups[u.Timeframe] = append(tfGroups[u.Timeframe], u)
	}

	// Считаем объемы для каждого уникального таймфрейма один раз
	tfVolumes := make(map[domain.Timeframe]decimal.Decimal)
	for tf := range tfGroups {
		tfVolumes[tf] = nr.volProvider.GetSymbolVolume(signal.Symbol, tf, signal.OpenedAt)
	}

	var targets []int64
	for tf, groupUsers := range tfGroups {
		vol := tfVolumes[tf]
		for _, u := range groupUsers {
			// Персональная фильтрация
			if signal.PeakSpread.GreaterThanOrEqual(u.MinSpread) && vol.GreaterThanOrEqual(u.MinVolume) {
				targets = append(targets, u.ChatID)
			}
		}
	}

	if len(targets) == 0 {
		return
	}

	var text string
	if isOpened {
		text = fmt.Sprintf("🚨 *SIGNAL OPENED*\nSymbol: `%s`\nType: %s\nExchanges: %s vs %s\nSpread: %s%%\nTime: %s",
			signal.Symbol, signal.SpreadType, signal.ExchangeA, signal.ExchangeB,
			signal.InitialSpread.Mul(decimal.NewFromInt(100)).StringFixed(2),
			signal.OpenedAt.Format(time.RFC3339))
	} else {
		text = fmt.Sprintf("✅ *SIGNAL CLOSED*\nSymbol: `%s`\nType: %s\nPeak: %s%%\nFinal: %s%%\nDuration: %s",
			signal.Symbol, signal.SpreadType,
			signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(2),
			signal.FinalSpread.Mul(decimal.NewFromInt(100)).StringFixed(2),
			signal.Duration.Round(time.Second))
	}

	nr.telegram.Broadcast(text, targets)
}
