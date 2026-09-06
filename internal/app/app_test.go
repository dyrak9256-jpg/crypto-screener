package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestApplication_HandleCommandSecurityAndPersistence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	sigRepo := mocks.NewMockSignalRepository(ctrl)
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().SaveUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	userRepo.EXPECT().DeleteUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	a := NewApplication(cfg, sigRepo, userRepo)
	defer a.router.Close()
	a.accepting.Store(true)
	a.SetAdminIDs([]int64{1})
	a.SetConnectorFactory(func(string) (domain.ExchangeConnector, bool) { return nil, false })
	require.Contains(t, a.HandleCommand(2, "u", "addex", []string{"BINANCE"}), "Access Denied")
	require.Contains(t, a.HandleCommand(2, "u", "start", nil), "Добро пожаловать")
	require.Contains(t, a.HandleCommand(2, "u", "setcross", []string{"2.5"}), "2.50%")
	require.Contains(t, a.HandleCommand(2, "u", "stop", nil), "отписались")
}

func TestApplication_RunShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	sigRepo := mocks.NewMockSignalRepository(ctrl)
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().GetAllUsers(gomock.Any()).Return([]*domain.User{{ChatID: 1}}, nil).Times(1)
	a := NewApplication(cfg, sigRepo, userRepo)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	require.NoError(t, a.Run(ctx, sigRepo))
}

func TestApplication_Run_AllowsNotificationToFinish(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.RequireFromString("1000"))
	// closeThreshold = 0.02/2 = 0.01

	mockSignalRepo := mocks.NewMockSignalRepository(ctrl)
	mockUserRepo := mocks.NewMockUserRepository(ctrl)
	mockUserRepo.EXPECT().GetAllUsers(gomock.Any()).Return(nil, nil).Times(1)
	mockSignalRepo.EXPECT().SaveSignal(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	app := NewApplication(cfg, mockUserRepo)

	notifyDone := make(chan struct{})
	uid := int64(777)
	app.userMgr.SetUser(&domain.User{ChatID: uid, MinSpread: decimal.Zero, MinVolume: decimal.Zero, Timeframe: domain.TF_15m})
	mockTg := mocks.NewMockTelegramSender(ctrl)
	// An open + a close -> two broadcasts.
	var bcCount atomic.Int32
	mockTg.EXPECT().Broadcast(gomock.Any(), gomock.Any()).Do(func(string, []int64) {
		if bcCount.Add(1) == 2 {
			close(notifyDone)
		}
	}).Times(2)
	app.SetTelegramSender(mockTg)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- app.Run(ctx, mockSignalRepo) }()
	time.Sleep(150 * time.Millisecond) // let workers start

	t0 := time.Now()
	key := "BTCUSDT:CROSS_EXCHANGE:BINANCE:BYBIT"
	_ = key

	// Open then immediately close the same signal via the tracker channel.
	app.trackerChan <- domain.SpreadEvent{
		Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, Spread: decimal.RequireFromString("0.03"),
		ExchangeA: "BINANCE", ExchangeB: "BYBIT", Timestamp: t0,
	}
	app.trackerChan <- domain.SpreadEvent{
		Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, Spread: decimal.RequireFromString("0.005"),
		ExchangeA: "BINANCE", ExchangeB: "BYBIT", Timestamp: t0.Add(30 * time.Second),
	}

	// Shutdown: Run must drain the tracker, persist the closed signal, and wait
	// for the notification goroutine BEFORE returning nil.
	cancel()
	require.NoError(t, <-runErr)

	select {
	case <-notifyDone:
		// notification goroutine completed before Run returned (routerWg.Wait)
	case <-time.After(2 * time.Second):
		t.Fatal("notification goroutine did not finish before Application.Run returned")
	}
}
