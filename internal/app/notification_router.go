package app

import (
	"fmt"
	"sync"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
)

type notificationJob struct {
	text    string
	chatIDs []int64
}

type NotificationRouter struct {
	userMgr  *domain.UserManager
	telegram domain.TelegramSender
	volume   *VolumeEngine
	jobs     chan notificationJob
	wg       sync.WaitGroup
	once     sync.Once
}

func NewNotificationRouter(userMgr *domain.UserManager, tg domain.TelegramSender, volumes ...*VolumeEngine) *NotificationRouter {
	var volume *VolumeEngine
	if len(volumes) > 0 {
		volume = volumes[0]
	}
	nr := &NotificationRouter{userMgr: userMgr, telegram: tg, volume: volume, jobs: make(chan notificationJob, 10000)}
	nr.wg.Add(1)
	go nr.worker()
	return nr
}

func (nr *NotificationRouter) worker() {
	defer nr.wg.Done()
	for job := range nr.jobs {
		if nr.telegram != nil {
			nr.telegram.Broadcast(job.text, job.chatIDs)
		}
	}
}

func (nr *NotificationRouter) Close() {
	if nr == nil {
		return
	}
	nr.once.Do(func() { close(nr.jobs); nr.wg.Wait() })
}

// ProcessSignal returns the exact users that qualified for an OPEN notification.
// CLOSE is queued only for that same set, so a user cannot receive a close event
// for an opportunity they never qualified for.
func (nr *NotificationRouter) ProcessSignal(signal *domain.ArbitrageSignal, isOpened bool) []int64 {
	if nr == nil || signal == nil || nr.userMgr == nil || nr.telegram == nil {
		return nil
	}

	var targets []int64
	if isOpened {
		nr.userMgr.Range(func(u domain.User) bool {
			if signal.PeakSpread.LessThan(u.MinSpread) {
				return true
			}
			volume := signal.QuoteVolume
			if u.Timeframe != domain.TF_24h && nr.volume != nil {
				volume = nr.volume.GetRouteVolume(signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.Symbol, u.Timeframe, signal.OpenedAt)
			}
			if volume.GreaterThanOrEqual(u.MinVolume) {
				targets = append(targets, u.ChatID)
			}
			return true
		})
	} else {
		targets = append(targets, signal.NotifiedChatIDs...)
	}
	if len(targets) == 0 {
		return nil
	}

	var text string
	if isOpened {
		text = fmt.Sprintf("🚨 SIGNAL OPENED\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nSpread: %s%%\nTime: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.InitialSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), signal.OpenedAt.Format(time.RFC3339Nano))
	} else {
		text = fmt.Sprintf("✅ SIGNAL CLOSED\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nPeak: %s%%\nFinal: %s%%\nDuration: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), signal.FinalSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), signal.Duration.Round(time.Millisecond))
	}

	// Notification jobs are bounded and ordered. If Telegram is overloaded the
	// market lifecycle is backpressured instead of silently losing an alert.
	nr.jobs <- notificationJob{text: text, chatIDs: append([]int64(nil), targets...)}
	return targets
}
