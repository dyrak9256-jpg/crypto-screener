package app

import (
	"context"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestApplication_HandleCommand(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hardSpread := decimal.RequireFromString("0.01") // 1%
	hardVol := decimal.RequireFromString("1000000") // 1M USDT
	cfg := domain.NewScreenerConfig(hardSpread, hardVol)

	mockSignalRepo := mocks.NewMockSignalRepository(ctrl)
	mockUserRepo := mocks.NewMockUserRepository(ctrl)

	// Expectations for async user repo calls
	mockUserRepo.EXPECT().
		SaveUser(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	mockUserRepo.EXPECT().
		DeleteUser(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	app := NewApplication(cfg, mockSignalRepo, mockUserRepo)
	appCtx := context.Background()
	app.ctx.Store(&appCtx)
	require.NotNil(t, app)

	chatID := int64(99901)
	username := "test_trader"

	// 1. Unsubscribed user attempting commands other than /start
	resp := app.HandleCommand(chatID, username, "help", nil)
	assert.Contains(t, resp, "Вы не подписаны")

	resp = app.HandleCommand(chatID, username, "setcross", []string{"2.5"})
	assert.Contains(t, resp, "Вы не подписаны")

	// 2. /start: Subscribe new user
	resp = app.HandleCommand(chatID, username, "start", nil)
	assert.Contains(t, resp, "Добро пожаловать")
	user, exists := app.userMgr.GetUser(chatID)
	require.True(t, exists)
	assert.Equal(t, username, user.Username)
	assert.True(t, user.MinSpread.Equal(hardSpread))
	assert.True(t, user.MinVolume.Equal(hardVol))
	assert.Equal(t, domain.TF_15m, user.Timeframe)

	// Subscribing again should return already subscribed message
	resp = app.HandleCommand(chatID, username, "start", nil)
	assert.Contains(t, resp, "Вы уже подписаны")

	// 3. /help
	resp = app.HandleCommand(chatID, username, "help", nil)
	assert.Contains(t, resp, "Доступные команды")

	// 4. /setcross
	// No args
	resp = app.HandleCommand(chatID, username, "setcross", nil)
	assert.Contains(t, resp, "Usage: /setcross")

	// Invalid number
	resp = app.HandleCommand(chatID, username, "setcross", []string{"not-a-number"})
	assert.Contains(t, resp, "Некорректное число")

	// Valid number above hard limit (2.5%)
	resp = app.HandleCommand(chatID, username, "setcross", []string{"2.5"})
	assert.Contains(t, resp, "2.50%")
	user, _ = app.userMgr.GetUser(chatID)
	assert.True(t, user.MinSpread.Equal(decimal.RequireFromString("0.025")))

	// Clamping to hard limit: user passes 0.5%, hard limit is 1.0%
	resp = app.HandleCommand(chatID, username, "setcross", []string{"0.5"})
	assert.Contains(t, resp, "1.00%")
	user, _ = app.userMgr.GetUser(chatID)
	assert.True(t, user.MinSpread.Equal(hardSpread), "should be clamped to hardMinSpread")

	// 5. /setvol
	// No args
	resp = app.HandleCommand(chatID, username, "setvol", nil)
	assert.Contains(t, resp, "Usage: /setvol")

	// Invalid number
	resp = app.HandleCommand(chatID, username, "setvol", []string{"xyz"})
	assert.Contains(t, resp, "Некорректный объём")

	// Valid number above hard limit ($2,000,000)
	resp = app.HandleCommand(chatID, username, "setvol", []string{"2000000"})
	assert.Contains(t, resp, "$2000000")
	user, _ = app.userMgr.GetUser(chatID)
	assert.True(t, user.MinVolume.Equal(decimal.RequireFromString("2000000")))

	// Clamping to hard limit: user passes $500,000, hard limit is $1,000,000
	resp = app.HandleCommand(chatID, username, "setvol", []string{"500000"})
	assert.Contains(t, resp, "$1000000")
	user, _ = app.userMgr.GetUser(chatID)
	assert.True(t, user.MinVolume.Equal(hardVol), "should be clamped to hardMinVolume")

	// 6. /settimeframe
	// No args
	resp = app.HandleCommand(chatID, username, "settimeframe", nil)
	assert.Contains(t, resp, "Usage: /settimeframe")

	// Invalid timeframe
	resp = app.HandleCommand(chatID, username, "settimeframe", []string{"10m"})
	assert.Contains(t, resp, "Неверный таймфрейм")

	// Valid timeframe
	validTFs := []string{"1m", "5m", "15m", "30m", "1h", "4h", "24h"}
	for _, tf := range validTFs {
		resp = app.HandleCommand(chatID, username, "settimeframe", []string{tf})
		assert.Contains(t, resp, tf)
		user, _ = app.userMgr.GetUser(chatID)
		assert.Equal(t, domain.Timeframe(tf), user.Timeframe)
	}

	// 7. /addex and /rmex
	// Административные команды: делаем текущего пользователя админом.
	app.SetAdminIDs([]int64{chatID})

	// No args
	assert.Contains(t, app.HandleCommand(chatID, username, "addex", nil), "Usage: /addex")
	assert.Contains(t, app.HandleCommand(chatID, username, "rmex", nil), "Usage: /rmex")

	// Unsupported exchange
	assert.Contains(t, app.HandleCommand(chatID, username, "addex", []string{"UNSUPPORTED_EX"}), "not supported")

	// Supported exchange: BINANCE
	resp = app.HandleCommand(chatID, username, "addex", []string{"BINANCE"})
	assert.Contains(t, resp, "Hot-swapped IN: BINANCE")

	resp = app.HandleCommand(chatID, username, "rmex", []string{"BINANCE"})
	assert.Contains(t, resp, "Hot-swapped OUT: BINANCE")

	// 8. Unknown command
	resp = app.HandleCommand(chatID, username, "unknown_cmd", nil)
	assert.Contains(t, resp, "Неизвестная команда")

	// 9. /stop: Unsubscribe
	resp = app.HandleCommand(chatID, username, "stop", nil)
	assert.Contains(t, resp, "отписались")
	_, exists = app.userMgr.GetUser(chatID)
	assert.False(t, exists, "user should be removed from userMgr after /stop")

	// Give async DB writes time to settle
	time.Sleep(50 * time.Millisecond)
}

func TestApplication_Run_PreloadsUsersAndGracefulShutdown(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	mockSignalRepo := mocks.NewMockSignalRepository(ctrl)
	mockUserRepo := mocks.NewMockUserRepository(ctrl)

	// Preload 2 existing users from DB
	mockUserRepo.EXPECT().
		GetAllUsers(gomock.Any()).
		Return([]*domain.User{
			{ChatID: 100, Username: "user1", MinSpread: decimal.RequireFromString("0.02"), MinVolume: decimal.RequireFromString("500"), Timeframe: domain.TF_15m},
			{ChatID: 200, Username: "user2", MinSpread: decimal.RequireFromString("0.03"), MinVolume: decimal.RequireFromString("1000"), Timeframe: domain.TF_1h},
		}, nil).
		Times(1)

	app := NewApplication(cfg, mockSignalRepo, mockUserRepo)

	mockTg := mocks.NewMockTelegramSender(ctrl)
	app.SetTelegramSender(mockTg)
	assert.Equal(t, mockTg, app.router.telegram)

	assert.NotNil(t, app.GetConnectorManager())

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context quickly to test startup + shutdown lifecycle
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := app.Run(ctx, mockSignalRepo)
	require.NoError(t, err)

	// Assert users were preloaded into userMgr
	u1, exists1 := app.userMgr.GetUser(100)
	assert.True(t, exists1)
	assert.Equal(t, "user1", u1.Username)

	u2, exists2 := app.userMgr.GetUser(200)
	assert.True(t, exists2)
	assert.Equal(t, "user2", u2.Username)
}
