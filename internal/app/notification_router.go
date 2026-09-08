package app

import (
	"context"
	"crypto-screener/internal/domain"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type notificationJob struct {
	key        string
	text       string
	chatIDs    []int64
	signal     *domain.ArbitrageSignal
	revalidate bool
}

// updateState prevents duplicate UPDATEs when several peak observations cross
// the same user threshold concurrently. It is intentionally in-memory: the
// notification ledger is runtime state, not analytics data.
type updateKey struct {
	signalID string
	chatID   int64
}

type NotificationRouter struct {
	userMgr        *domain.UserManager
	telegram       domain.TelegramSender
	volume         *VolumeEngine
	config         *domain.ScreenerConfig
	wake           chan struct{}
	pending        map[string]notificationJob
	stop           chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	closed         bool
	clock          Clock
	telegramReady  chan struct{}
	revalidator    func(*domain.ArbitrageSignal) (*domain.ArbitrageSignal, bool)
	updateMu       sync.Mutex
	lastUpdate     map[updateKey]decimal.Decimal
	lifecycleMu    sync.Mutex
	reliableCtx    context.Context
	reliableCancel context.CancelFunc
	invalidated    map[string]struct{}
}

func NewNotificationRouter(userMgr *domain.UserManager, tg domain.TelegramSender, extras ...any) *NotificationRouter {
	var v *VolumeEngine
	var cfg *domain.ScreenerConfig
	for _, extra := range extras {
		switch x := extra.(type) {
		case *VolumeEngine:
			v = x
		case *domain.ScreenerConfig:
			cfg = x
		}
	}
	reliableCtx, reliableCancel := context.WithCancel(context.Background())
	nr := &NotificationRouter{
		userMgr:        userMgr,
		telegram:       tg,
		volume:         v,
		config:         cfg,
		wake:           make(chan struct{}, 1),
		pending:        make(map[string]notificationJob),
		stop:           make(chan struct{}),
		clock:          realClock{},
		telegramReady:  make(chan struct{}),
		lastUpdate:     make(map[updateKey]decimal.Decimal),
		invalidated:    make(map[string]struct{}),
		reliableCtx:    reliableCtx,
		reliableCancel: reliableCancel,
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

func (nr *NotificationRouter) SetRevalidator(fn func(*domain.ArbitrageSignal) (*domain.ArbitrageSignal, bool)) {
	if nr == nil {
		return
	}
	nr.mu.Lock()
	nr.revalidator = fn
	nr.mu.Unlock()
}

func (nr *NotificationRouter) worker() {
	defer nr.wg.Done()
	for {
		select {
		case <-nr.stop:
			return
		case <-nr.wake:
			for {
				job, ok := nr.takePending()
				if !ok {
					break
				}
				nr.sendJob(job)
			}
		}
	}
}

func (nr *NotificationRouter) takePending() (notificationJob, bool) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	for key, job := range nr.pending {
		delete(nr.pending, key)
		return job, true
	}
	return notificationJob{}, false
}

func (nr *NotificationRouter) enqueue(job notificationJob) bool {
	if nr == nil || job.key == "" || len(job.chatIDs) == 0 {
		return false
	}
	nr.mu.Lock()
	if nr.closed {
		nr.mu.Unlock()
		return false
	}
	nr.pending[job.key] = job
	nr.mu.Unlock()
	select {
	case nr.wake <- struct{}{}:
	default:
	}
	return true
}

func (nr *NotificationRouter) sendJob(job notificationJob) {
	isClose := strings.HasSuffix(job.key, ":close")
	if job.signal != nil && !isClose {
		nr.lifecycleMu.Lock()
		_, invalid := nr.invalidated[job.signal.ID]
		nr.lifecycleMu.Unlock()
		if invalid {
			return
		}
	}
	if job.revalidate && job.signal != nil {
		nr.mu.RLock()
		revalidator := nr.revalidator
		nr.mu.RUnlock()
		if revalidator != nil {
			current, ok := revalidator(job.signal)
			if !ok || current == nil {
				return
			}
			nr.mu.RLock()
			now := nr.clock.Now()
			config := nr.config
			nr.mu.RUnlock()
			if config != nil && current.PeakSpread.LessThan(config.GetHardMinSpread()) {
				return
			}
			job.chatIDs = nr.currentEligibleChatIDs(current, job.chatIDs, now)
			if len(job.chatIDs) == 0 {
				return
			}
			job.text = renderSignalNotification(current, !isClose, strings.HasSuffix(job.key, ":update"))
		}
	}
	for {
		nr.mu.RLock()
		tg := nr.telegram
		closed := nr.closed
		nr.mu.RUnlock()
		if tg != nil {
			// Serialize the final lifecycle check with CLOSE invalidation and the
			// actual transport call. Thus a queued UPDATE can never be sent after
			// Tracker has committed the signal to CLOSED.
			nr.lifecycleMu.Lock()
			if job.signal != nil && !isClose {
				if _, invalid := nr.invalidated[job.signal.ID]; invalid {
					nr.lifecycleMu.Unlock()
					return
				}
			}
			if reliable, ok := tg.(domain.ReliableTelegramSender); ok && job.signal != nil {
				nr.mu.RLock()
				reliableCtx := nr.reliableCtx
				nr.mu.RUnlock()
				err := reliable.BroadcastReliable(reliableCtx, job.text, job.chatIDs)
				nr.lifecycleMu.Unlock()
				if err != nil {
					slog.Warn("reliable telegram delivery failed", "signal_id", job.signal.ID, "error", err)
				}
			} else {
				tg.Broadcast(job.text, job.chatIDs)
				nr.lifecycleMu.Unlock()
			}
			return
		}
		if closed {
			return
		}
		select {
		case <-nr.telegramReady:
		case <-nr.stop:
			return
		}
	}
}

func (nr *NotificationRouter) currentEligibleChatIDs(signal *domain.ArbitrageSignal, chatIDs []int64, now time.Time) []int64 {
	if nr == nil || nr.userMgr == nil || signal == nil {
		return nil
	}
	wanted := make(map[int64]struct{}, len(chatIDs))
	for _, id := range chatIDs {
		wanted[id] = struct{}{}
	}
	out := make([]int64, 0, len(chatIDs))
	nr.userMgr.Range(func(u domain.User) bool {
		if _, ok := wanted[u.ChatID]; !ok || signal.PeakSpread.LessThan(u.MinSpread) || !fundingAllowed(signal, u, now) {
			return true
		}
		volume := signal.QuoteVolume
		if u.Timeframe != domain.TF_24h && nr.volume != nil {
			volume = nr.volume.GetRouteVolumeEstimate(signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.Symbol, u.Timeframe, signal.OpenedAt).Volume
		}
		if volume.GreaterThanOrEqual(u.MinVolume) {
			out = append(out, u.ChatID)
		}
		return true
	})
	return out
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
	if nr.reliableCancel != nil {
		nr.reliableCancel()
	}
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

func renderSignalNotification(signal *domain.ArbitrageSignal, opened, update bool) string {
	if signal == nil {
		return ""
	}
	fundingText := "Funding: N/A"
	if !signal.BuyNextFunding.IsZero() || !signal.SellNextFunding.IsZero() {
		fundingText = fmt.Sprintf("Funding buy: %s (%s) | sell: %s (%s)", signal.BuyFundingRate.StringFixed(6), formatFundingTime(signal.BuyNextFunding), signal.SellFundingRate.StringFixed(6), formatFundingTime(signal.SellNextFunding))
	}
	if update {
		return fmt.Sprintf("📈 SIGNAL UPDATE\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nPeak (net): %s%%\nTime: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), signal.OpenedAt.Format(time.RFC3339Nano))
	}
	if opened {
		return fmt.Sprintf("🚨 SIGNAL OPENED\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nSpread (net): %s%%\n%s\nTime: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.InitialSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), fundingText, signal.OpenedAt.Format(time.RFC3339Nano))
	}
	return fmt.Sprintf("✅ SIGNAL CLOSED\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nPeak (net): %s%%\nFinal (net): %s%%\n%s\nDuration: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), signal.FinalSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), fundingText, signal.Duration.Round(time.Millisecond))
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

	text := renderSignalNotification(signal, isOpened, false)

	jobKind := "close"
	if isOpened {
		jobKind = "open"
	}
	nr.enqueue(notificationJob{key: signal.ID + ":" + jobKind, text: text, chatIDs: append([]int64(nil), targets...), signal: signal.Snapshot(), revalidate: isOpened})
	return targets
}

// ProcessSignalUpdate sends a per-user UPDATE only when the current peak crosses
// that user's configured percentage-point step. A jump across multiple steps
// produces one message containing the current peak, never a burst of catch-up
// messages.
func (nr *NotificationRouter) ProcessSignalUpdate(signal *domain.ArbitrageSignal) []int64 {
	if nr == nil || signal == nil || nr.userMgr == nil || !signal.IsActive {
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
	nr.userMgr.Range(func(u domain.User) bool {
		if !u.UpdateStep.IsPositive() || signal.PeakSpread.LessThan(u.MinSpread) || !fundingAllowed(signal, u, now) {
			return true
		}
		volume := signal.QuoteVolume
		if u.Timeframe != domain.TF_24h && nr.volume != nil {
			volume = nr.volume.GetRouteVolumeEstimate(signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.Symbol, u.Timeframe, signal.OpenedAt).Volume
		}
		if volume.LessThan(u.MinVolume) {
			return true
		}
		steps := signal.PeakSpread.Sub(signal.InitialSpread).Div(u.UpdateStep).IntPart()
		if steps < 1 {
			return true
		}
		threshold := signal.InitialSpread.Add(u.UpdateStep.Mul(decimal.NewFromInt(steps)))
		key := updateKey{signalID: signal.ID, chatID: u.ChatID}
		nr.updateMu.Lock()
		last := nr.lastUpdate[key]
		if threshold.LessThanOrEqual(last) {
			nr.updateMu.Unlock()
			return true
		}
		nr.lastUpdate[key] = threshold
		nr.updateMu.Unlock()
		targets = append(targets, u.ChatID)
		return true
	})
	if len(targets) == 0 {
		return nil
	}
	text := fmt.Sprintf("📈 SIGNAL UPDATE\nSymbol: %s\nType: %s\nRoute: %s [%s] → %s [%s]\nPeak (net): %s%%\nTime: %s", signal.Symbol, signal.SpreadType, signal.BuyExchange, signal.BuyMarket, signal.SellExchange, signal.SellMarket, signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(4), now.Format(time.RFC3339Nano))
	nr.enqueue(notificationJob{key: signal.ID + ":update", text: text, chatIDs: append([]int64(nil), targets...), signal: signal.Snapshot(), revalidate: true})
	return targets
}

func (nr *NotificationRouter) invalidateSignal(signalID string) {
	if nr == nil || signalID == "" {
		return
	}
	nr.lifecycleMu.Lock()
	nr.invalidated[signalID] = struct{}{}
	nr.lifecycleMu.Unlock()
}

func (nr *NotificationRouter) dropPending(key string) {
	if nr == nil || key == "" {
		return
	}
	nr.mu.Lock()
	delete(nr.pending, key)
	nr.mu.Unlock()
}

func (nr *NotificationRouter) ClearSignalUpdates(signalID string) {
	if nr == nil || signalID == "" {
		return
	}
	nr.updateMu.Lock()
	for key := range nr.lastUpdate {
		if key.signalID == signalID {
			delete(nr.lastUpdate, key)
		}
	}
	nr.updateMu.Unlock()
}

func formatFundingTime(t time.Time) string {
	if t.IsZero() {
		return "N/A"
	}
	return t.Format(time.RFC3339)
}
