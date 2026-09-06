package app

import (
	"sync"

	"crypto-screener/internal/domain"
)

// Tracker ведёт активные сигналы и гарантирует последовательную обработку
// событий одного ключа. Отправка в персистентность выполняется БЛОКИРУЮЩЕ и
// ВНЕ мьютекса (никакой тихой потери сигналов). Уведомления — отслеживаемые
// горутины (routerWg), чтобы graceful shutdown дождался их завершения.
type Tracker struct {
	mu            sync.Mutex
	activeSignals map[string]*domain.ArbitrageSignal
	config        *domain.ScreenerConfig
	dbChan        chan<- *domain.ArbitrageSignal
	router        *NotificationRouter
	routerWg      *sync.WaitGroup
}

func NewTracker(cfg *domain.ScreenerConfig, dbChan chan<- *domain.ArbitrageSignal, router *NotificationRouter, routerWg *sync.WaitGroup) *Tracker {
	return &Tracker{
		activeSignals: make(map[string]*domain.ArbitrageSignal),
		config:        cfg,
		dbChan:        dbChan,
		router:        router,
		routerWg:      routerWg,
	}
}

func (t *Tracker) HandleEvent(event domain.SpreadEvent) {
	var toPersist, toNotify *domain.ArbitrageSignal
	notifyOpened := false

	t.mu.Lock()
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
			toPersist = signal
			toNotify = signal
		}
	} else {
		newSignal := domain.NewArbitrageSignal(event, event.Timestamp)
		t.activeSignals[key] = newSignal
		toNotify = newSignal
		notifyOpened = true
	}
	t.mu.Unlock()

	// Блокирующая запись в персистентность ВНЕ мьютекса: сигнал не теряется.
	if toPersist != nil {
		t.dbChan <- toPersist
	}

	// Снапшот-копия + отслеживаемая горутина уведомлений.
	if t.router != nil && toNotify != nil {
		snapshot := *toNotify
		if t.routerWg != nil {
			t.routerWg.Add(1)
			go func(s *domain.ArbitrageSignal, opened bool) {
				defer t.routerWg.Done()
				t.router.ProcessSignal(s, opened)
			}(&snapshot, notifyOpened)
		} else {
			go t.router.ProcessSignal(&snapshot, notifyOpened)
		}
	}
}
