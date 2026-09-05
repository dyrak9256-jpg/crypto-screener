package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNotificationRouter_OT_VolumeOptimization(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	userMgr := domain.NewUserManager()

	// 6 users across only 2 timeframes (3 users each)
	timeframes := []domain.Timeframe{domain.TF_15m, domain.TF_1h}
	for i := 0; i < 6; i++ {
		tf := timeframes[i%2]
		userMgr.SetUser(&domain.User{
			ChatID:    int64(i + 1),
			Username:  "trader",
			MinSpread: decimal.RequireFromString("0.01"),
			MinVolume: decimal.RequireFromString("100"),
			Timeframe: tf,
		})
	}

	openedAt := time.Date(2026, 9, 5, 15, 0, 0, 0, time.UTC)
	signal := &domain.ArbitrageSignal{
		Symbol:        "BTCUSDT",
		SpreadType:    domain.CrossExchange,
		ExchangeA:     "BINANCE",
		ExchangeB:     "BYBIT",
		OpenedAt:      openedAt,
		PeakSpread:    decimal.RequireFromString("0.03"),
		InitialSpread: decimal.RequireFromString("0.03"),
	}

	mockVolProvider := mocks.NewMockVolumeProvider(ctrl)
	mockTg := mocks.NewMockTelegramSender(ctrl)

	// Volume provider must be called EXACTLY ONCE for TF_15m and EXACTLY ONCE for TF_1h
	mockVolProvider.EXPECT().
		GetSymbolVolume("BTCUSDT", domain.TF_15m, openedAt).
		Return(decimal.RequireFromString("5000")).
		Times(1)

	mockVolProvider.EXPECT().
		GetSymbolVolume("BTCUSDT", domain.TF_1h, openedAt).
		Return(decimal.RequireFromString("20000")).
		Times(1)

	mockTg.EXPECT().
		Broadcast(gomock.Any(), gomock.Len(6)).
		Times(1)

	router := NewNotificationRouter(userMgr, mockVolProvider, mockTg)
	router.ProcessSignal(signal, true)
}

func TestNotificationRouter_PersonalFiltering(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	userMgr := domain.NewUserManager()

	// User 1: Passes spread & volume
	userMgr.SetUser(&domain.User{
		ChatID: 101, Username: "u1",
		MinSpread: decimal.RequireFromString("0.02"),
		MinVolume: decimal.RequireFromString("1000"),
		Timeframe: domain.TF_15m,
	})
	// User 2: Fails spread (0.05 > 0.03)
	userMgr.SetUser(&domain.User{
		ChatID: 102, Username: "u2",
		MinSpread: decimal.RequireFromString("0.05"),
		MinVolume: decimal.RequireFromString("1000"),
		Timeframe: domain.TF_15m,
	})
	// User 3: Fails volume (50000 > 20000)
	userMgr.SetUser(&domain.User{
		ChatID: 103, Username: "u3",
		MinSpread: decimal.RequireFromString("0.01"),
		MinVolume: decimal.RequireFromString("50000"),
		Timeframe: domain.TF_1h,
	})
	// User 4: Passes spread & volume
	userMgr.SetUser(&domain.User{
		ChatID: 104, Username: "u4",
		MinSpread: decimal.RequireFromString("0.01"),
		MinVolume: decimal.RequireFromString("10000"),
		Timeframe: domain.TF_1h,
	})

	openedAt := time.Date(2026, 9, 5, 15, 0, 0, 0, time.UTC)
	signal := &domain.ArbitrageSignal{
		Symbol:        "ETHUSDT",
		SpreadType:    domain.CrossExchange,
		ExchangeA:     "BINANCE",
		ExchangeB:     "OKX",
		OpenedAt:      openedAt,
		PeakSpread:    decimal.RequireFromString("0.03"), // 3%
		InitialSpread: decimal.RequireFromString("0.03"),
	}

	mockVolProvider := mocks.NewMockVolumeProvider(ctrl)
	mockTg := mocks.NewMockTelegramSender(ctrl)

	mockVolProvider.EXPECT().
		GetSymbolVolume("ETHUSDT", domain.TF_15m, openedAt).
		Return(decimal.RequireFromString("5000")).
		Times(1)

	mockVolProvider.EXPECT().
		GetSymbolVolume("ETHUSDT", domain.TF_1h, openedAt).
		Return(decimal.RequireFromString("20000")).
		Times(1)

	// Targets must only contain User 101 and User 104
	mockTg.EXPECT().
		Broadcast(gomock.Any(), gomock.Any()).
		Do(func(text string, targets []int64) {
			assert.Len(t, targets, 2)
			targetMap := make(map[int64]bool)
			for _, id := range targets {
				targetMap[id] = true
			}
			assert.True(t, targetMap[101], "User 101 should be targeted")
			assert.True(t, targetMap[104], "User 104 should be targeted")
			assert.False(t, targetMap[102], "User 102 should be filtered out (spread)")
			assert.False(t, targetMap[103], "User 103 should be filtered out (volume)")
		}).
		Times(1)

	router := NewNotificationRouter(userMgr, mockVolProvider, mockTg)
	router.ProcessSignal(signal, true)
}

func TestNotificationRouter_MessageFormatting_OpenedAndClosed(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	userMgr := domain.NewUserManager()
	userMgr.SetUser(&domain.User{
		ChatID:    200,
		Username:  "tester",
		MinSpread: decimal.RequireFromString("0.01"),
		MinVolume: decimal.RequireFromString("100"),
		Timeframe: domain.TF_15m,
	})

	mockVolProvider := mocks.NewMockVolumeProvider(ctrl)
	mockVolProvider.EXPECT().
		GetSymbolVolume(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(decimal.RequireFromString("100000")).
		AnyTimes()

	mockTg := mocks.NewMockTelegramSender(ctrl)

	tOpen := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	signal := &domain.ArbitrageSignal{
		Symbol:        "SOLUSDT",
		SpreadType:    domain.CrossExchange,
		ExchangeA:     "BINANCE",
		ExchangeB:     "BYBIT",
		OpenedAt:      tOpen,
		ClosedAt:      tOpen.Add(75 * time.Second),
		Duration:      75 * time.Second,
		InitialSpread: decimal.RequireFromString("0.0245"),
		PeakSpread:    decimal.RequireFromString("0.0520"),
		FinalSpread:   decimal.RequireFromString("0.0080"),
	}

	// 1. Test isOpened = true format
	mockTg.EXPECT().
		Broadcast(gomock.Any(), []int64{200}).
		Do(func(text string, targets []int64) {
			assert.Contains(t, text, "🚨 *SIGNAL OPENED*")
			assert.Contains(t, text, "Symbol: `SOLUSDT`")
			assert.Contains(t, text, "Type: CROSS_EXCHANGE")
			assert.Contains(t, text, "Exchanges: BINANCE vs BYBIT")
			assert.Contains(t, text, "Spread: 2.45%")
			assert.Contains(t, text, "Time: 2026-09-05T12:00:00Z")
		}).
		Times(1)

	router := NewNotificationRouter(userMgr, mockVolProvider, mockTg)
	router.ProcessSignal(signal, true)

	// 2. Test isOpened = false format
	mockTg.EXPECT().
		Broadcast(gomock.Any(), []int64{200}).
		Do(func(text string, targets []int64) {
			assert.Contains(t, text, "✅ *SIGNAL CLOSED*")
			assert.Contains(t, text, "Symbol: `SOLUSDT`")
			assert.Contains(t, text, "Type: CROSS_EXCHANGE")
			assert.Contains(t, text, "Peak: 5.20%")
			assert.Contains(t, text, "Final: 0.80%")
			assert.Contains(t, text, "Duration: 1m15s")
		}).
		Times(1)

	router.ProcessSignal(signal, false)
}

func TestNotificationRouter_NoSubscribersOrMatches(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("no subscribers in system", func(t *testing.T) {
		emptyUserMgr := domain.NewUserManager()
		mockVol := mocks.NewMockVolumeProvider(ctrl)
		mockTg := mocks.NewMockTelegramSender(ctrl)

		// Neither volume provider nor telegram should be called
		router := NewNotificationRouter(emptyUserMgr, mockVol, mockTg)
		router.ProcessSignal(&domain.ArbitrageSignal{Symbol: "BTCUSDT"}, true)
	})

	t.Run("subscribers exist but none match thresholds", func(t *testing.T) {
		userMgr := domain.NewUserManager()
		userMgr.SetUser(&domain.User{
			ChatID:    300,
			Username:  "high_roller",
			MinSpread: decimal.RequireFromString("0.10"), // 10% min spread
			MinVolume: decimal.RequireFromString("1000"),
			Timeframe: domain.TF_15m,
		})

		mockVol := mocks.NewMockVolumeProvider(ctrl)
		mockVol.EXPECT().
			GetSymbolVolume("BTCUSDT", domain.TF_15m, gomock.Any()).
			Return(decimal.RequireFromString("50000")).
			Times(1)

		mockTg := mocks.NewMockTelegramSender(ctrl)
		// Broadcast should NOT be called

		router := NewNotificationRouter(userMgr, mockVol, mockTg)
		signal := &domain.ArbitrageSignal{
			Symbol:     "BTCUSDT",
			PeakSpread: decimal.RequireFromString("0.02"), // 2% < 10%
		}
		router.ProcessSignal(signal, true)
	})
}
