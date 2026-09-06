package app

import (
	"sync"

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
	return &Tracker{
		activeSignals: make(map[string]*domain.ArbitrageSignal),
		config:        cfg,
		dbChan:        dbChan,
		router:        router,
	}
}

func (t *Tracker) HandleEvent(event domain.SpreadEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	exA, exB := event.ExchangeA, event.ExchangeB
	if exA > exB {
		exA, exB = exB, exA
	}
	key := event.Symbol + ":" + string(event.SpreadType) + ":" + exA + ":" + exB

	signal, exists := t.activeSignals[key]
	closeThreshold := t.config.GetCloseThreshold()

	if exists {
		signal.UpdatePeak(event.Spread)

		if event.Spread.LessThanOrEqual(closeThreshold) {
			signal.Close(event.Timestamp, event.Spread)
			delete(t.activeSignals, key)

			select {
			case t.dbChan <- signal:
			default:
			}

			// Уведомляем роутер о закрытии.
			// Передаём СНАПШОТ (копию по значению): обработка в отдельной горутине
			// не должна гоняться с мутацией PeakSpread/FinalSpread в HandleEvent.
			if t.router != nil {
				snapshot := *signal
				go t.router.ProcessSignal(&snapshot, false)
			}
		}
	} else {
		newSignal := domain.NewArbitrageSignal(event, event.Timestamp)
		t.activeSignals[key] = newSignal

		// Уведомляем роутер об открытии (тоже передаём копию-снапшот).
		if t.router != nil {
			snapshot := *newSignal
			go t.router.ProcessSignal(&snapshot, true)
		}
	}
}
