package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestNotificationRouter_FilteringAndCloseRecipients(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 1, MinSpread: decimal.RequireFromString("0.02"), MinVolume: decimal.RequireFromString("1000"), Timeframe: domain.TF_24h})
	um.SetUser(&domain.User{ChatID: 2, MinSpread: decimal.RequireFromString("0.04"), MinVolume: decimal.RequireFromString("1000"), Timeframe: domain.TF_24h})
	tg := mocks.NewMockTelegramSender(ctrl)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{1}).Times(1)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{1}).Times(1)
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	s := &domain.ArbitrageSignal{Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.03"), InitialSpread: decimal.RequireFromString("0.03"), QuoteVolume: decimal.RequireFromString("5000"), OpenedAt: time.Now()}
	ids := r.ProcessSignal(s, true)
	require.Equal(t, []int64{1}, ids)
	s.NotifiedChatIDs = ids
	r.ProcessSignal(s, false)
}

func TestNotificationRouter_NilTelegramSafe(t *testing.T) {
	um := domain.NewUserManager()
	r := NewNotificationRouter(um, nil)
	defer r.Close()
	r.ProcessSignal(&domain.ArbitrageSignal{Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.01"), OpenedAt: time.Now()}, true)
}
