package app

import (
	"strings"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestTracker_Lifecycle_OpenUpdatePeakClose(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Config with hardSpread = 0.02 -> closeThreshold = 0.02 / 2 = 0.01 (1%)
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.RequireFromString("1000"))
	require.True(t, cfg.GetCloseThreshold().Equal(decimal.RequireFromString("0.01")))

	dbChan := make(chan *domain.ArbitrageSignal, 10)

	// User manager with 1 subscribed user
	userMgr := domain.NewUserManager()
	user := &domain.User{
		ChatID:    123456,
		Username:  "crypto_trader",
		MinSpread: decimal.RequireFromString("0.01"),
		MinVolume: decimal.RequireFromString("500"),
		Timeframe: domain.TF_15m,
	}
	userMgr.SetUser(user)

	mockVolProvider := mocks.NewMockVolumeProvider(ctrl)
	mockTgSender := mocks.NewMockTelegramSender(ctrl)

	// Mock expectations for router notifications
	mockVolProvider.EXPECT().
		GetSymbolVolume("BTCUSDT", domain.TF_15m, gomock.Any()).
		Return(decimal.RequireFromString("10000")).
		AnyTimes()

	var wg sync.WaitGroup
	wg.Add(2) // 1 Open notification + 1 Close notification

	mockTgSender.EXPECT().
		Broadcast(gomock.Any(), []int64{123456}).
		Do(func(text string, targets []int64) {
			defer wg.Done()
			assert.True(t, strings.Contains(text, "SIGNAL OPENED") || strings.Contains(text, "SIGNAL CLOSED"))
		}).
		Times(2)

	router := NewNotificationRouter(userMgr, mockVolProvider, mockTgSender)
	tracker := NewTracker(cfg, dbChan, router)

	t0 := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	expectedKey := "BTCUSDT:CROSS_EXCHANGE:BINANCE:BYBIT"

	// -------------------------------------------------------------
	// 1. OPEN: First event triggers signal creation
	// -------------------------------------------------------------
	eventOpen := domain.SpreadEvent{
		Symbol:      "BTCUSDT",
		SpreadType:  domain.CrossExchange,
		Spread:      decimal.RequireFromString("0.03"), // 3%
		ExchangeA:   "BINANCE",
		ExchangeB:   "BYBIT",
		QuoteVolume: decimal.RequireFromString("10000"),
		Timestamp:   t0,
	}
	tracker.HandleEvent(eventOpen)

	tracker.mu.Lock()
	signal, exists := tracker.activeSignals[expectedKey]
	require.True(t, exists, "signal must be created in activeSignals")
	assert.NotEmpty(t, signal.ID)
	assert.True(t, signal.IsActive)
	assert.True(t, signal.InitialSpread.Equal(decimal.RequireFromString("0.03")))
	assert.True(t, signal.PeakSpread.Equal(decimal.RequireFromString("0.03")))
	assert.Equal(t, t0, signal.OpenedAt)
	tracker.mu.Unlock()

	assert.Empty(t, dbChan, "no signal should be written to dbChan on open")

	// -------------------------------------------------------------
	// 2. UPDATE PEAK: Higher spread updates peak
	// -------------------------------------------------------------
	t1 := t0.Add(10 * time.Second)
	eventHigher := domain.SpreadEvent{
		Symbol:      "BTCUSDT",
		SpreadType:  domain.CrossExchange,
		Spread:      decimal.RequireFromString("0.07"), // 7% > 3%
		ExchangeA:   "BINANCE",
		ExchangeB:   "BYBIT",
		QuoteVolume: decimal.RequireFromString("15000"),
		Timestamp:   t1,
	}
	tracker.HandleEvent(eventHigher)

	tracker.mu.Lock()
	signal = tracker.activeSignals[expectedKey]
	require.NotNil(t, signal)
	assert.True(t, signal.IsActive)
	assert.True(t, signal.PeakSpread.Equal(decimal.RequireFromString("0.07")), "peak spread should update to 0.07")
	assert.True(t, signal.InitialSpread.Equal(decimal.RequireFromString("0.03")), "initial spread should remain 0.03")
	tracker.mu.Unlock()

	// Lower spread that doesn't trigger close (0.04 > 0.01 threshold)
	t2 := t0.Add(20 * time.Second)
	eventLower := domain.SpreadEvent{
		Symbol:      "BTCUSDT",
		SpreadType:  domain.CrossExchange,
		Spread:      decimal.RequireFromString("0.04"), // 4%
		ExchangeA:   "BINANCE",
		ExchangeB:   "BYBIT",
		QuoteVolume: decimal.RequireFromString("18000"),
		Timestamp:   t2,
	}
	tracker.HandleEvent(eventLower)

	tracker.mu.Lock()
	signal = tracker.activeSignals[expectedKey]
	require.NotNil(t, signal)
	assert.True(t, signal.PeakSpread.Equal(decimal.RequireFromString("0.07")), "peak spread should remain 0.07")
	tracker.mu.Unlock()

	// -------------------------------------------------------------
	// 3. CLOSE: Spread drops to <= closeThreshold (0.01)
	// -------------------------------------------------------------
	t3 := t0.Add(45 * time.Second)
	eventClose := domain.SpreadEvent{
		Symbol:      "BTCUSDT",
		SpreadType:  domain.CrossExchange,
		Spread:      decimal.RequireFromString("0.008"), // 0.8% <= 1.0% threshold
		ExchangeA:   "BINANCE",
		ExchangeB:   "BYBIT",
		QuoteVolume: decimal.RequireFromString("20000"),
		Timestamp:   t3,
	}
	tracker.HandleEvent(eventClose)

	tracker.mu.Lock()
	_, exists = tracker.activeSignals[expectedKey]
	assert.False(t, exists, "signal must be removed from activeSignals upon closing")
	tracker.mu.Unlock()

	// Verify closed signal is sent to dbChan
	select {
	case closedSignal := <-dbChan:
		assert.False(t, closedSignal.IsActive)
		assert.Equal(t, t3, closedSignal.ClosedAt)
		assert.True(t, closedSignal.PeakSpread.Equal(decimal.RequireFromString("0.07")))
		assert.True(t, closedSignal.FinalSpread.Equal(decimal.RequireFromString("0.008")))
		assert.Equal(t, 45*time.Second, closedSignal.Duration)
	case <-time.After(2 * time.Second):
		t.Fatal("expected closed signal in dbChan, timed out")
	}

	// Wait for Telegram notification goroutines to complete
	wg.Wait()
}

func TestTracker_ReEmergence(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.RequireFromString("1000"))
	dbChan := make(chan *domain.ArbitrageSignal, 10)
	tracker := NewTracker(cfg, dbChan, nil)

	t0 := time.Date(2026, 9, 5, 14, 0, 0, 0, time.UTC)
	key := "ETHUSDT:CROSS_EXCHANGE:BINANCE:BYBIT"

	// 1. Open first signal
	tracker.HandleEvent(domain.SpreadEvent{
		Symbol:     "ETHUSDT",
		SpreadType: domain.CrossExchange,
		Spread:     decimal.RequireFromString("0.03"),
		ExchangeA:  "BINANCE",
		ExchangeB:  "BYBIT",
		Timestamp:  t0,
	})

	tracker.mu.Lock()
	firstSignal := tracker.activeSignals[key]
	require.NotNil(t, firstSignal)
	firstSignalID := firstSignal.ID
	tracker.mu.Unlock()

	// 2. Close first signal
	tracker.HandleEvent(domain.SpreadEvent{
		Symbol:     "ETHUSDT",
		SpreadType: domain.CrossExchange,
		Spread:     decimal.RequireFromString("0.005"), // <= 0.01 threshold
		ExchangeA:  "BINANCE",
		ExchangeB:  "BYBIT",
		Timestamp:  t0.Add(30 * time.Second),
	})

	tracker.mu.Lock()
	assert.Nil(t, tracker.activeSignals[key], "signal must be removed after closing")
	tracker.mu.Unlock()

	// Drain dbChan
	select {
	case <-dbChan:
	default:
	}

	// 3. Re-emergence: New event arrives for the same pair
	tReemerge := t0.Add(5 * time.Minute)
	tracker.HandleEvent(domain.SpreadEvent{
		Symbol:     "ETHUSDT",
		SpreadType: domain.CrossExchange,
		Spread:     decimal.RequireFromString("0.04"),
		ExchangeA:  "BINANCE",
		ExchangeB:  "BYBIT",
		Timestamp:  tReemerge,
	})

	tracker.mu.Lock()
	reemergedSignal, exists := tracker.activeSignals[key]
	require.True(t, exists, "new signal must be created upon re-emergence")
	assert.NotEqual(t, firstSignalID, reemergedSignal.ID, "re-emerged signal must have a new unique ID")
	assert.True(t, reemergedSignal.IsActive)
	assert.Equal(t, tReemerge, reemergedSignal.OpenedAt)
	assert.True(t, reemergedSignal.InitialSpread.Equal(decimal.RequireFromString("0.04")))
	assert.True(t, reemergedSignal.PeakSpread.Equal(decimal.RequireFromString("0.04")))
	tracker.mu.Unlock()
}

func TestTracker_KeyNormalization(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.RequireFromString("1000"))
	dbChan := make(chan *domain.ArbitrageSignal, 10)
	tracker := NewTracker(cfg, dbChan, nil)

	t0 := time.Now()

	// Event 1: ExchangeA = OKX, ExchangeB = BINANCE (out of alphabetical order)
	tracker.HandleEvent(domain.SpreadEvent{
		Symbol:     "SOLUSDT",
		SpreadType: domain.CrossExchange,
		Spread:     decimal.RequireFromString("0.03"),
		ExchangeA:  "OKX",
		ExchangeB:  "BINANCE",
		Timestamp:  t0,
	})

	expectedNormalizedKey := "SOLUSDT:CROSS_EXCHANGE:BINANCE:OKX"

	tracker.mu.Lock()
	require.Contains(t, tracker.activeSignals, expectedNormalizedKey, "key should be normalized to BINANCE:OKX")
	assert.NotContains(t, tracker.activeSignals, "SOLUSDT:CROSS_EXCHANGE:OKX:BINANCE")
	signal := tracker.activeSignals[expectedNormalizedKey]
	require.NotNil(t, signal)
	assert.True(t, signal.PeakSpread.Equal(decimal.RequireFromString("0.03")))
	tracker.mu.Unlock()

	// Event 2: Reverse order (ExchangeA = BINANCE, ExchangeB = OKX) with higher spread
	tracker.HandleEvent(domain.SpreadEvent{
		Symbol:     "SOLUSDT",
		SpreadType: domain.CrossExchange,
		Spread:     decimal.RequireFromString("0.06"),
		ExchangeA:  "BINANCE",
		ExchangeB:  "OKX",
		Timestamp:  t0.Add(5 * time.Second),
	})

	tracker.mu.Lock()
	assert.Equal(t, 1, len(tracker.activeSignals), "should not create duplicate signal for reverse exchange order")
	updatedSignal := tracker.activeSignals[expectedNormalizedKey]
	require.NotNil(t, updatedSignal)
	assert.True(t, updatedSignal.PeakSpread.Equal(decimal.RequireFromString("0.06")), "existing signal peak should be updated")
	tracker.mu.Unlock()
}

func TestTracker_DbChanFullDrop(t *testing.T) {
	t.Parallel()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.RequireFromString("1000"))
	// dbChan capacity 1, pre-filled
	dbChan := make(chan *domain.ArbitrageSignal, 1)
	dbChan <- &domain.ArbitrageSignal{ID: "PRE_EXISTING"}

	tracker := NewTracker(cfg, dbChan, nil)
	t0 := time.Now()

	// Open signal
	tracker.HandleEvent(domain.SpreadEvent{
		Symbol:     "ADAUSDT",
		SpreadType: domain.CrossExchange,
		Spread:     decimal.RequireFromString("0.03"),
		ExchangeA:  "BINANCE",
		ExchangeB:  "BYBIT",
		Timestamp:  t0,
	})

	// Close signal while dbChan is full -> hits default: branch without blocking
	done := make(chan struct{})
	go func() {
		tracker.HandleEvent(domain.SpreadEvent{
			Symbol:     "ADAUSDT",
			SpreadType: domain.CrossExchange,
			Spread:     decimal.RequireFromString("0.005"),
			ExchangeA:  "BINANCE",
			ExchangeB:  "BYBIT",
			Timestamp:  t0.Add(10 * time.Second),
		})
		close(done)
	}()

	select {
	case <-done:
		// Succeeded without blocking
	case <-time.After(500 * time.Millisecond):
		t.Fatal("closing signal blocked on full dbChan")
	}

	assert.Len(t, dbChan, 1)
	existing := <-dbChan
	assert.Equal(t, "PRE_EXISTING", existing.ID)
}
