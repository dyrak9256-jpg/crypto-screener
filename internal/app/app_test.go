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
	require.Contains(t, a.HandleCommand(0, 2, "u", "addex", []string{"BINANCE"}), "Access Denied")
	require.Contains(t, a.HandleCommand(0, 2, "u", "start", nil), "Добро пожаловать")
	require.Contains(t, a.HandleCommand(0, 2, "u", "setcross", []string{"2.5"}), "2.50%")
	require.Contains(t, a.HandleCommand(0, 1, "admin", "sethardspread", []string{"2"}), "2.00%")
	now := time.Now()
	require.NoError(t, a.fundingMgr.UpdateFunding("BINANCE", "BTCUSDT", decimal.Zero, now.Add(time.Hour), now))
	result := a.fundingMgr.EvaluateSpotFutures("BINANCE", "BTCUSDT", decimal.RequireFromString("0.015"), now)
	require.False(t, result.Profitable)
	require.Equal(t, ReasonBelowMinSpread, result.Reason)
	require.Contains(t, a.HandleCommand(0, 2, "u", "stop", nil), "отписались")
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

// settingsStatsStub — репозиторий с опциональными возможностями настроек и
// статистики для тестирования /setfees, /sethardspread и /stats.
type settingsStatsStub struct {
	domain.SignalRepository
	saved map[string]string
	stats domain.SignalStats
	err   error
}

func (s *settingsStatsStub) GetSetting(_ context.Context, key string) (string, bool, error) {
	v, ok := s.saved[key]
	return v, ok, nil
}

func (s *settingsStatsStub) SetSetting(_ context.Context, key, value string) error {
	if s.saved == nil {
		s.saved = make(map[string]string)
	}
	s.saved[key] = value
	return nil
}

func (s *settingsStatsStub) SignalStats24h(context.Context) (domain.SignalStats, error) {
	return s.stats, s.err
}

func TestApplication_SetFeesCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().SaveUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	stub := &settingsStatsStub{}
	a, err := NewApplication(cfg, stub, userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)
	a.SetAdminIDs([]int64{1})

	// Не-админ получает отказ.
	require.Contains(t, a.HandleCommand(0, 2, "u", "setfees", []string{"DEFAULT:0.001"}), "Access Denied")
	// Без аргументов — текущие комиссии.
	require.Contains(t, a.HandleCommand(0, 1, "admin", "setfees", nil), "DEFAULT:0.0005")
	// Валидные значения применяются и персистятся.
	out := a.HandleCommand(0, 1, "admin", "setfees", []string{"DEFAULT:0.0008", "BINANCE:0.0002"})
	require.Contains(t, out, "0.0008")
	require.Equal(t, "BINANCE:0.0002,DEFAULT:0.0008", stub.saved["fees"])
	require.True(t, cfg.FeeFor("BINANCE").Equal(decimal.RequireFromString("0.0002")))
	require.True(t, cfg.FeeFor("MEXC").Equal(decimal.RequireFromString("0.0008")))
	// Некорректная запись — ошибка, состояние не меняется.
	require.Contains(t, a.HandleCommand(0, 1, "admin", "setfees", []string{"BAD"}), "❌")
	require.True(t, cfg.FeeFor("MEXC").Equal(decimal.RequireFromString("0.0008")))
}

func TestApplication_SetHardSpreadPersists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	userRepo := mocks.NewMockUserRepository(ctrl)
	stub := &settingsStatsStub{}
	a, err := NewApplication(cfg, stub, userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)
	a.SetAdminIDs([]int64{1})

	out := a.HandleCommand(0, 1, "admin", "sethardspread", []string{"2"})
	require.Contains(t, out, "2.00%")
	require.Contains(t, out, "сохранено")
	require.Equal(t, "0.02", stub.saved["hard_min_spread"])
}

func TestApplication_StatsCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	userRepo := mocks.NewMockUserRepository(ctrl)
	stub := &settingsStatsStub{stats: domain.SignalStats{
		Opened24h:     5,
		Closed24h:     3,
		AvgPeakSpread: decimal.RequireFromString("0.021"),
		AvgDuration:   90 * time.Second,
	}}
	a, err := NewApplication(cfg, stub, userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)
	a.SetAdminIDs([]int64{1})

	out := a.HandleCommand(0, 1, "admin", "stats", nil)
	require.Contains(t, out, "5")
	require.Contains(t, out, "3")
	require.Contains(t, out, "2.1000%")
	require.Contains(t, out, "1m30s")
	// Не-админ — отказ.
	require.Contains(t, a.HandleCommand(0, 2, "u", "stats", nil), "Access Denied")
}

func TestApplication_RouteBot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().SaveUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	a, err := NewApplication(cfg, mocks.NewMockSignalRepository(ctrl), userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)
	a.SetAdminIDs([]int64{1})

	// Подписка через бота #3 закрепляет пользователя за ним.
	require.Contains(t, a.HandleCommand(3, 2, "u", "start", nil), "Добро пожаловать")
	require.Equal(t, int64(3), a.RouteBot(2))
	// Неизвестный чат — бот по умолчанию (id 0).
	require.Equal(t, int64(0), a.RouteBot(999))
}
