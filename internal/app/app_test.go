package app

import (
	"context"
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
	a, err := NewApplication(cfg, sigRepo, userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)
	a.SetAdminIDs([]int64{1})
	a.SetConnectorFactory(func(string) (domain.ExchangeConnector, bool) { return nil, false })
	require.Contains(t, a.HandleCommand(2, "u", "addex", []string{"BINANCE"}), "Access Denied")
	require.Contains(t, a.HandleCommand(2, "u", "start", nil), "Добро пожаловать")
	require.Contains(t, a.HandleCommand(2, "u", "setcross", []string{"2.5"}), "2.50%")
	require.Contains(t, a.HandleCommand(1, "admin", "sethardspread", []string{"2"}), "2.00%")
	now := time.Now()
	require.NoError(t, a.fundingMgr.UpdateFunding("BINANCE", "BTCUSDT", decimal.Zero, now.Add(time.Hour), now))
	result := a.fundingMgr.EvaluateSpotFutures("BINANCE", "BTCUSDT", decimal.RequireFromString("0.015"), now)
	require.False(t, result.Profitable)
	require.Equal(t, ReasonBelowMinSpread, result.Reason)
	require.Contains(t, a.HandleCommand(2, "u", "stop", nil), "отписались")
}

func TestApplication_RunShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	sigRepo := mocks.NewMockSignalRepository(ctrl)
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().GetAllUsers(gomock.Any()).Return([]*domain.User{{ChatID: 1}}, nil).Times(1)
	a, err := NewApplication(cfg, sigRepo, userRepo)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	require.NoError(t, a.Run(ctx, sigRepo))
}
