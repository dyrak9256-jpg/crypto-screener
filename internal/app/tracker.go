package app

import (
	"strings"
	"sync"
	"time"

	"crypto-screener/internal/domain"
)

type Tracker struct {
	mu            sync.Mutex
	activeSignals map[string]*domain.ArbitrageSignal
	config        *domain.ScreenerConfig
	dbChan        chan<- *domain.ArbitrageSignal
	router        *NotificationRouter
}

func NewTracker(cfg *domain.ScreenerConfig, dbChan chan<- *domain.ArbitrageSignal, router *NotificationRouter) *Tracker {
	return &Tracker{activeSignals: make(map[string]*domain.ArbitrageSignal), config: cfg, dbChan: dbChan, router: router}
}

func signalKey(e domain.SpreadEvent) string {
	buy, sell := e.BuyExchange, e.SellExchange
	if buy == "" {
		buy = e.ExchangeA
	}
	if sell == "" {
		sell = e.ExchangeB
	}
	return strings.Join([]string{e.Symbol, string(e.SpreadType), buy, sell, string(e.BuyMarket), string(e.SellMarket)}, ":")
}

func (t *Tracker) HandleEvent(event domain.SpreadEvent) {
	if event.Timestamp.IsZero() || event.Symbol == "" || event.Spread.IsNegative() {
		return
	}
	key := signalKey(event)
	var persist *domain.ArbitrageSignal
	var notify *domain.ArbitrageSignal
	var opened bool

	t.mu.Lock()
	signal := t.activeSignals[key]
	switch event.Lifecycle {
	case domain.SignalOpened:
		if signal == nil {
			signal = domain.NewArbitrageSignal(event, event.Timestamp)
			t.activeSignals[key] = signal
			persist = signal.Snapshot()
			notify = signal.Snapshot()
			opened = true
		}
	case domain.SignalUpdated:
		if signal != nil && !event.Timestamp.Before(signal.OpenedAt) {
			before := signal.PeakSpread
			signal.Update(event)
			if signal.PeakSpread.GreaterThan(before) {
				persist = signal.Snapshot()
			}
		}
	case domain.SignalClosed:
		if signal != nil && !event.Timestamp.Before(signal.OpenedAt) {
			signal.Update(event)
			signal.Close(event.Timestamp, event.Spread)
			delete(t.activeSignals, key)
			persist = signal.Snapshot()
			notify = signal.Snapshot()
			opened = false
		}
	default:
		// Legacy producers without lifecycle metadata are treated as observations.
		if signal == nil {
			signal = domain.NewArbitrageSignal(event, event.Timestamp)
			t.activeSignals[key] = signal
			persist = signal.Snapshot()
			notify = signal.Snapshot()
			opened = true
		} else if !event.Timestamp.Before(signal.OpenedAt) {
			before := signal.PeakSpread
			signal.Update(event)
			if signal.PeakSpread.GreaterThan(before) {
				persist = signal.Snapshot()
			}
			if event.Spread.LessThanOrEqual(t.config.GetCloseThreshold()) {
				signal.Close(event.Timestamp, event.Spread)
				delete(t.activeSignals, key)
				persist = signal.Snapshot()
				notify = signal.Snapshot()
				opened = false
			}
		}
	}
	t.mu.Unlock()

	if persist != nil && t.dbChan != nil {
		// Lifecycle/peak events are rare and must not be silently dropped.
		t.dbChan <- persist
	}
	if notify != nil && t.router != nil {
		if opened {
			ids := t.router.ProcessSignal(notify, true)
			notify.NotifiedChatIDs = ids
			// Persisting runtime recipient IDs is intentionally avoided; the DB row
			// remains a market-level signal, not a user notification ledger.
			if len(ids) > 0 && persist != nil && persist.IsActive {
				// The open snapshot was already queued. Notification state only
				// affects the subsequent CLOSE notification in this process.
				t.mu.Lock()
				if current := t.activeSignals[key]; current != nil {
					current.NotifiedChatIDs = append([]int64(nil), ids...)
				}
				t.mu.Unlock()
			}
		} else {
			t.router.ProcessSignal(notify, false)
		}
	}
}

func (t *Tracker) Stop(timeout time.Duration) error { _ = timeout; return nil }
