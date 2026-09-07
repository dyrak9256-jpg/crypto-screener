package app

import (
	"crypto-screener/internal/domain"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type notificationJob struct {
	text    string
	chatIDs []int64
}

type NotificationRouter struct {
	userMgr       *domain.UserManager
	telegram      domain.TelegramSender
	volume        *VolumeEngine
	jobs          chan notificationJob
	stop          chan struct{}
	wg            sync.WaitGroup
	mu            sync.RWMutex
	closed        bool
	clock         Clock
	telegramReady chan struct{}
}

func NewNotificationRouter(userMgr *domain.UserManager, tg domain.TelegramSender, volumes ...*VolumeEngine) *NotificationRouter {
	var v *VolumeEngine
	if len(volumes) > 0 {
		v = volumes[0]
	}
	nr := &NotificationRouter{
		userMgr:       userMgr,
		telegram:      tg,
		volume:        v,
		jobs:          make(chan notificationJob, 10000),
		stop:          make(chan struct{}),
		clock:         realClock{},
		telegramReady: make(chan struct{}),
	}
	const workerCount = 4
	nr.wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go nr.worker()
	}
	return nr
}

func (nr *NotificationRouter) SetClock(clock Clock) {
	if clock == nil {
		return
	}
	nr.mu.Lock()
	nr.clock = clock
	nr.mu.Unlock()
}

func (nr *NotificationRouter) SetTelegramSender(tg domain.TelegramSender) {
	nr.mu.Lock()
	wasNil := nr.telegram == nil
	nr.telegram = tg
	if wasNil && tg != nil {
		select {
		case <-nr.telegramReady:
		default:
			close(nr.telegramReady)
		}
	}
	nr.mu.Unlock()
}

func (nr *NotificationRouter) worker() {
	defer nr.wg.Done()
	for {
		select {
		case <-nr.stop:
			return
		default:
		}
		select {
		case job := <-nr.jobs:
			nr.sendJob(job)
		case <-nr.stop:
			// Shutdown must be bounded. Jobs already accepted into the queue are
			// transient notifications; persistence/lifecycle state is handled by
			// the tracker separately. Do not spend an unbounded amount of time
			// draining Telegram work during process termination.
			return
		}
	}
}

func (nr *NotificationRouter) sendJob(job notificationJob) {
	for {
		nr.mu.RLock()
		tg := nr.telegram
		closed := nr.closed
		nr.mu.RUnlock()
		if tg != nil {
			tg.Broadcast(job.text, job.chatIDs)
			return
		}
		if closed {
			return
		}
		// Keep accepted notifications until transport becomes available instead
		// of coupling business target calculation to Telegram initialization.
		select {
		case <-nr.telegramReady:
		case <-nr.stop:
			return
		}
	}
}

func (nr *NotificationRouter) Close() {
	if nr == nil {
		return
	}
	nr.mu.Lock()
	if nr.closed {
		nr.mu.Unlock()
		return
	}
	nr.closed = true
	close(nr.stop)
	nr.mu.Unlock()
	nr.wg.Wait()
}

func minFundingTime(signal *domain.ArbitrageSignal) time.Time {
	var t time.Time
	for _, x := range []time.Time{signal.BuyNextFunding, signal.SellNextFunding} {
		if !x.IsZero() && (t.IsZero() || x.Before(t)) {
			t = x
		}
	}
	return t
}

func fundingAllowed(signal *domain.ArbitrageSignal, u domain.User, now time.Time) bool {
	if signal.BuyMarket != domain.MarketTypeFutures && signal.SellMarket != domain.MarketTypeFutures {
		return true
	}
	next := minFundingTime(signal)
	if next.IsZero() {
		return false
	}
	return next.Sub(now) >= time.Duration(u.MinFundingMinutes)*time.Minute
}

func (nr *NotificationRouter) ProcessSignal(signal *domain.ArbitrageSignal, isOpened bool) []int64 {
	if nr == nil || signal == nil || nr.userMgr == nil {
		return nil
	}
	nr.mu.RLock()
	closed := nr.closed
	now := nr.clock.Now()
	nr.mu.RUnlock()
	if closed {
		return nil
	}

	var targets []int64
	if isOpened {
		nr.userMgr.Range(func(u domain.User) bool {
			if signal.PeakSpread.LessThan(u.MinSpread) || !fundingAllowed(signal, u, now) {
				return true
			}
			volume := signal.QuoteVolume
			if u.Timeframe != domain.TF_24h && nr.volume != nil {
				volume = nr.volume.GetRouteVolumeEstimate(signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.Symbol, u.Timeframe, signal.OpenedAt).Volume
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

	fundingText := "Funding: N/A"
	if !signal.BuyNextFunding.IsZero() || !signal.SellNextFunding.IsZero() {
		fundingText = fmt.Sprintf("Funding buy: %s (%s) | sell: %s (%s)", signal.BuyFundingRate.StringFixed(6), formatFundingTime(signal.BuyNextFunding), signal.SellFundingRate.StringFixed(6), formatFundingTime(signal.SellNextFunding))
	}
	var text string
	if isOpened {
		text = fmt.Sprintf("🚨 SIGNAL OPENED\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nSpread (net): %s%%\n%s\nTime: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.InitialSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), fundingText, signal.OpenedAt.Format(time.RFC3339Nano))
	} else {
		text = fmt.Sprintf("✅ SIGNAL CLOSED\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nPeak (net): %s%%\nFinal (net): %s%%\n%s\nDuration: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), signal.FinalSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), fundingText, signal.Duration.Round(time.Millisecond))
	}

	job := notificationJob{text: text, chatIDs: append([]int64(nil), targets...)}
	nr.mu.RLock()
	closed = nr.closed
	nr.mu.RUnlock()
	if closed {
		return nil
	}
	// The queue is intentionally large and serviced by a worker pool so Telegram
	// latency cannot serialize all notifications behind one slow chat. We still
	// use a blocking send: an OPEN/CLOSE signal is business-critical and must not
	// be silently discarded.
	select {
	case nr.jobs <- job:
		return targets
	case <-nr.stop:
		return nil
	}
}

func formatFundingTime(t time.Time) string {
	if t.IsZero() {
		return "N/A"
	}
	return t.Format(time.RFC3339)
}
