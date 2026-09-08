package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

type blockingReliableSender struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	ctxDone chan struct{}
}

func (s *blockingReliableSender) SendPrivateMessage(int64, string) {}
func (s *blockingReliableSender) Broadcast(string, []int64)        {}
func (s *blockingReliableSender) Close()                           {}
func (s *blockingReliableSender) BroadcastReliable(ctx context.Context, _ string, _ []int64) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	select {
	case <-ctx.Done():
		close(s.ctxDone)
		return ctx.Err()
	}
}

func TestNotificationRouter_CloseCancelsReliableSignalDelivery(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 1, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h})
	tg := &blockingReliableSender{started: make(chan struct{}), ctxDone: make(chan struct{})}
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	s := &domain.ArbitrageSignal{ID: "cancel-1", Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"), QuoteVolume: decimal.NewFromInt(1000), OpenedAt: time.Now(), IsActive: true}
	r.ProcessSignal(s, true)
	select {
	case <-tg.started:
	case <-time.After(time.Second):
		t.Fatal("reliable delivery did not start")
	}
	r.invalidateSignal(s.ID)
	select {
	case <-tg.ctxDone:
	case <-time.After(time.Second):
		t.Fatal("reliable delivery was not cancelled")
	}
}

func TestNotificationRouter_UpdateBurstIsCoalescedToLatestPeak(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 1, MinSpread: decimal.RequireFromString("0.02"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h, UpdateStep: decimal.RequireFromString("0.003")})
	tg := &captureSender{calls: make(chan string, 4)}
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	now := time.Now()
	s := &domain.ArbitrageSignal{ID: "burst-1", Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, BuyExchange: "A", SellExchange: "B", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"), QuoteVolume: decimal.NewFromInt(1000), OpenedAt: now, IsActive: true}
	r.ProcessSignal(s, true)
	select {
	case <-tg.calls:
	case <-time.After(time.Second):
		t.Fatal("open notification was not delivered")
	}
	s.PeakSpread = decimal.RequireFromString("0.023")
	r.ProcessSignalUpdate(s)
	s.PeakSpread = decimal.RequireFromString("0.026")
	r.ProcessSignalUpdate(s)
	select {
	case text := <-tg.calls:
		require.Contains(t, text, "2.6000%")
	case <-time.After(time.Second):
		t.Fatal("coalesced update was not delivered")
	}
	select {
	case extra := <-tg.calls:
		t.Fatalf("unexpected second update: %q", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

type captureSender struct{ calls chan string }

func (s *captureSender) SendPrivateMessage(int64, string) {}
func (s *captureSender) Broadcast(text string, _ []int64) { s.calls <- text }
func (s *captureSender) Close()                           {}
