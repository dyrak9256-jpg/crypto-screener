package app

import (
	"crypto-screener/internal/domain"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

type Tracker struct {
	mu            sync.Mutex
	activeSignals map[string]*domain.ArbitrageSignal
	config        *domain.ScreenerConfig
	dbChan        chan<- *domain.ArbitrageSignal
	telegram      domain.TelegramSender
}

func NewTracker(cfg *domain.ScreenerConfig, dbChan chan<- *domain.ArbitrageSignal) *Tracker {
	return &Tracker{activeSignals: make(map[string]*domain.ArbitrageSignal), config: cfg, dbChan: dbChan}
}

func (t *Tracker) SetTelegramSender(tg domain.TelegramSender) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.telegram = tg
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

			if t.telegram != nil {
				msg := fmt.Sprintf("✅ *SIGNAL CLOSED*\nSymbol: `%s`\nType: %s\nPeak: %s%%\nFinal: %s%%\nDuration: %s",
					signal.Symbol, signal.SpreadType,
					signal.PeakSpread.Mul(decimal.NewFromInt(100)).StringFixed(2),
					signal.FinalSpread.Mul(decimal.NewFromInt(100)).StringFixed(2),
					signal.Duration.Round(time.Second))
				t.telegram.SendMessage(msg)
			}
		}
	} else {
		newSignal := domain.NewArbitrageSignal(event, event.Timestamp)
		t.activeSignals[key] = newSignal

		if t.telegram != nil {
			msg := fmt.Sprintf("🚨 *SIGNAL OPENED*\nSymbol: `%s`\nType: %s\nExchanges: %s vs %s\nSpread: %s%%\nTime: %s",
				newSignal.Symbol, newSignal.SpreadType, newSignal.ExchangeA, newSignal.ExchangeB,
				newSignal.InitialSpread.Mul(decimal.NewFromInt(100)).StringFixed(2),
				newSignal.OpenedAt.Format(time.RFC3339))
			t.telegram.SendMessage(msg)
		}
	}
}
