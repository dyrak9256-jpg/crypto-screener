package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"
	"crypto-screener/internal/observability"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// Стаб репозитория с историей сигналов (domain.SignalReader).
type signalsReaderStub struct {
	domain.SignalRepository
	rows []domain.SignalSummary
	err  error
}

func (s *signalsReaderStub) RecentClosedSignals(_ context.Context, limit int) ([]domain.SignalSummary, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := s.rows
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func TestApplication_SignalsCommand(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	base := mocks.NewMockSignalRepository(ctrl)
	userRepo := mocks.NewMockUserRepository(ctrl)
	userRepo.EXPECT().SaveUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	repo := &signalsReaderStub{SignalRepository: base, rows: []domain.SignalSummary{
		{Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, BuyExchange: "BITGET", SellExchange: "OKX",
			PeakSpread: decimal.RequireFromString("0.0123"), Duration: 14 * time.Minute},
		{Symbol: "ETHUSDT", SpreadType: domain.IntraExchange, BuyExchange: "GATEIO", SellExchange: "GATEIO",
			PeakSpread: decimal.RequireFromString("0.0098"), Duration: 3 * time.Minute},
	}}
	a, err := NewApplication(cfg, repo, userRepo)
	require.NoError(t, err)
	defer a.router.Close()
	a.accepting.Store(true)

	require.Contains(t, a.HandleCommand(0, 1, "u", "start", nil), "Добро пожаловать")
	out := a.HandleCommand(0, 1, "u", "signals", nil)
	require.Contains(t, out, "Активных сейчас: 0")
	require.Contains(t, out, "BTCUSDT")
	require.Contains(t, out, "BITGET→OKX")
	require.Contains(t, out, "1.23%")
	require.Contains(t, out, "ETHUSDT")

	// Без SignalReader — понятная деградация.
	userRepo2 := mocks.NewMockUserRepository(ctrl)
	userRepo2.EXPECT().SaveUser(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	plain, _ := NewApplication(cfg, base, userRepo2)
	plain.router.Close()
	plain.accepting.Store(true)
	require.Contains(t, plain.HandleCommand(0, 2, "u", "start", nil), "Добро пожаловать")
	require.Contains(t, plain.HandleCommand(0, 2, "u", "signals", nil), "недоступна")
}

// Алерты мониторинга уходят администраторам в Telegram.
func TestApplication_HandleAlerts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"), decimal.RequireFromString("1000"))
	base := mocks.NewMockSignalRepository(ctrl)
	a, err := NewApplication(cfg, base, nil)
	require.NoError(t, err)
	defer a.router.Close()
	a.SetAdminIDs([]int64{777})

	tg := mocks.NewMockTelegramSender(ctrl)
	var text string
	tg.EXPECT().Broadcast(gomock.Any(), []int64{777}).Times(1).Do(func(txt string, _ []int64) { text = txt })
	a.SetTelegramSender(tg)

	a.HandleAlerts(observability.Alert{Alerts: []observability.AlertmanagerMsg{
		{Status: "firing", Labels: map[string]string{"alertname": "ExchangeSilent", "exchange": "BITGET"},
			Annotations: map[string]string{"summary": "Биржа BITGET молчит", "description": "Ноль тиков 5 минут."}},
	}})
	require.Contains(t, text, "ExchangeSilent")
	require.Contains(t, text, "BITGET")
	require.Contains(t, text, "🔴")
	require.True(t, strings.Contains(text, "Биржа BITGET молчит"))
}
