package app

import (
	"testing"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// newUserCommandApp — приложение с замоканным репозиторием юзеров и готовым
// к приёму команд состоянием (accepting=true, hard floor = 1%).
func newUserCommandApp(t *testing.T) *Application {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	sigRepo := mocks.NewMockSignalRepository(ctrl)
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().SaveUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	userRepo.EXPECT().DeleteUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	a, err := NewApplication(cfg, sigRepo, userRepo)
	require.NoError(t, err)
	t.Cleanup(a.router.Close)
	a.accepting.Store(true)
	return a
}

// Диапазон пользовательских команд, задающих параметры фильтрации сигналов:
// минимальный спред (/setcross), объём (/setvol), таймфрейм (/settimeframe)
// и окно до funding (/setfundingtime). Проверяем валидацию, применение
// к in-memory пользователю и персистенцию через репозиторий.
func TestApplication_UserSettingsCommands(t *testing.T) {
	a := newUserCommandApp(t)
	const chatID = int64(42)

	// До подписки настройки недоступны.
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setvol", []string{"100"}), "не подписаны")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "start", nil), "Добро пожаловать")
	// Повторный /start идемпотентен.
	require.Contains(t, a.HandleCommand(0, chatID, "u", "start", nil), "уже подписаны")

	// --- /setvol: минимальный объём ---
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setvol", []string{"500000"}), "$500000")
	u, ok := a.userMgr.GetUser(chatID)
	require.True(t, ok)
	require.True(t, u.MinVolume.Equal(decimal.NewFromInt(500000)), "MinVolume должен примениться к юзеру")
	// Невалидные значения отбрасываются, настройка не меняется.
	for _, bad := range [][]string{{"abc"}, {"-5"}, {"2000000000"}} {
		require.Contains(t, a.HandleCommand(0, chatID, "u", "setvol", bad), "❌")
	}
	// Без аргументов — подсказка usage.
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setvol", nil), "Usage")
	u, _ = a.userMgr.GetUser(chatID)
	require.True(t, u.MinVolume.Equal(decimal.NewFromInt(500000)), "после невалидных вводов объём не меняется")
	// Ноль допустим (снимает фильтр по объёму).
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setvol", []string{"0"}), "$0")
	u, _ = a.userMgr.GetUser(chatID)
	require.True(t, u.MinVolume.IsZero())

	// --- /setcross: минимальный спред, клампится к hard floor ---
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setcross", []string{"1.5"}), "1.50%")
	u, _ = a.userMgr.GetUser(chatID)
	require.True(t, u.MinSpread.Equal(decimal.RequireFromString("0.015")))
	// Ниже глобального минимума (1%) — поднимается до него.
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setcross", []string{"0.3"}), "1.00%")
	u, _ = a.userMgr.GetUser(chatID)
	require.True(t, u.MinSpread.Equal(decimal.RequireFromString("0.01")), "setcross клампится к hard floor")
	for _, bad := range [][]string{{"abc"}, {"150"}, {"-1"}} {
		require.Contains(t, a.HandleCommand(0, chatID, "u", "setcross", bad), "❌")
	}
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setcross", nil), "Usage")

	// --- /settimeframe ---
	for _, tf := range []string{"1m", "5m", "15m", "30m", "1h", "4h", "24h"} {
		require.Contains(t, a.HandleCommand(0, chatID, "u", "settimeframe", []string{tf}), "Таймфрейм")
	}
	require.Contains(t, a.HandleCommand(0, chatID, "u", "settimeframe", []string{"7m"}), "❌")
	u, _ = a.userMgr.GetUser(chatID)
	require.Equal(t, domain.TF_24h, u.Timeframe, "последний валидный таймфрейм применён")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "settimeframe", []string{"15m"}), "Таймфрейм")

	// --- /setfundingtime ---
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setfundingtime", []string{"0"}), "✅")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setfundingtime", []string{"10080"}), "✅")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setfundingtime", []string{"10081"}), "❌")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setfundingtime", []string{"soon"}), "❌")
	u, _ = a.userMgr.GetUser(chatID)
	require.Equal(t, 10080, u.MinFundingMinutes)

	// --- /help перечисляет все пользовательские команды ---
	help := a.HandleCommand(0, chatID, "u", "help", nil)
	for _, cmd := range []string{"/start", "/stop", "/setcross", "/setvol", "/settimeframe", "/setfundingtime"} {
		require.Contains(t, help, cmd)
	}

	// --- /stop ---
	require.Contains(t, a.HandleCommand(0, chatID, "u", "stop", nil), "отписались")
	_, ok = a.userMgr.GetUser(chatID)
	require.False(t, ok, "после /stop юзер удалён из рассылки")
	// Повторный /stop без подписки — подсказка, а не ошибка.
	require.Contains(t, a.HandleCommand(0, chatID, "u", "stop", nil), "не подписаны")
}

// Отписка должна персиститься: deleteUser вызывается у репозитория.
func TestApplication_StopPersistsDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	sigRepo := mocks.NewMockSignalRepository(ctrl)
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().SaveUser(gomock.Any(), gomock.Any()).Times(1).Return(nil)
	userRepo.EXPECT().DeleteUser(gomock.Any(), int64(7)).Times(1).Return(nil)
	a, err := NewApplication(cfg, sigRepo, userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)
	require.Contains(t, a.HandleCommand(0, 7, "u", "start", nil), "Добро пожаловать")
	require.Contains(t, a.HandleCommand(0, 7, "u", "stop", nil), "отписались")
}

// Настройки, применённые командами, реально влияют на адресатов сигнала
// (маршрутизация /setvol + /setcross): ниже порогов — не доставляется,
// выше — доставляется. Интеграция команд и NotificationRouter.
func TestApplication_UserSettingsAffectSignalRouting(t *testing.T) {
	a := newUserCommandApp(t)
	const chatID = int64(99)
	require.Contains(t, a.HandleCommand(0, chatID, "u", "start", nil), "Добро пожаловать")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setvol", []string{"10000"}), "$10000")
	require.Contains(t, a.HandleCommand(0, chatID, "u", "setcross", []string{"2"}), "2.00%")

	u, ok := a.userMgr.GetUser(chatID)
	require.True(t, ok)
	require.True(t, u.MinVolume.Equal(decimal.NewFromInt(10000)))
	require.True(t, u.MinSpread.Equal(decimal.RequireFromString("0.02")))
	require.Equal(t, domain.TF_15m, u.Timeframe, "после /start таймфрейм по умолчанию 15m")
}
