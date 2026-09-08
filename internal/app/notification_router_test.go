package app

import (
	"strings"
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
	broadcasts := make(chan struct{}, 2)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{1}).Times(2).Do(func(_ string, _ []int64) { broadcasts <- struct{}{} })
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	s := &domain.ArbitrageSignal{Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.03"), InitialSpread: decimal.RequireFromString("0.03"), QuoteVolume: decimal.RequireFromString("5000"), OpenedAt: time.Now()}
	ids := r.ProcessSignal(s, true)
	require.Equal(t, []int64{1}, ids)
	s.NotifiedChatIDs = ids
	require.Equal(t, []int64{1}, r.ProcessSignal(s, false))
	// Delivery happens asynchronously in worker goroutines: wait for both the
	// OPEN and the CLOSE broadcast before Close() can race the delivery.
	for i := 0; i < 2; i++ {
		select {
		case <-broadcasts:
		case <-time.After(2 * time.Second):
			t.Fatalf("broadcast %d was not delivered", i+1)
		}
	}
}

func TestNotificationRouter_NilTelegramSafe(t *testing.T) {
	um := domain.NewUserManager()
	r := NewNotificationRouter(um, nil)
	defer r.Close()
	r.ProcessSignal(&domain.ArbitrageSignal{Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.01"), OpenedAt: time.Now()}, true)
}

func TestNotificationRouter_FundingTimeFilter(t *testing.T) {
	um := domain.NewUserManager()
	tg := mocks.NewMockTelegramSender(gomock.NewController(t))
	um.SetUser(&domain.User{ChatID: 7, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h, MinFundingMinutes: 30})
	broadcast := make(chan []int64, 1)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{7}).Times(1).Do(func(_ string, ids []int64) { broadcast <- ids })
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	now := time.Date(2026, 9, 7, 4, 0, 0, 0, time.UTC)
	r.SetClock(testClock{now: now})
	tooSoon := &domain.ArbitrageSignal{Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"), QuoteVolume: decimal.NewFromInt(10000), OpenedAt: now, BuyMarket: domain.MarketTypeSpot, SellMarket: domain.MarketTypeFutures, SellNextFunding: now.Add(10 * time.Minute)}
	require.Empty(t, r.ProcessSignal(tooSoon, true))
	ok := tooSoon.Snapshot()
	ok.SellNextFunding = now.Add(31 * time.Minute)
	require.Equal(t, []int64{7}, r.ProcessSignal(ok, true))
	// The router delivers asynchronously via worker goroutines: wait for the
	// actual Broadcast before Close() can race the delivery.
	select {
	case ids := <-broadcast:
		require.Equal(t, []int64{7}, ids)
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast was not delivered")
	}
}

func TestNotificationRouter_TargetsAreComputedWithoutTelegramTransport(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 42, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h})
	r := NewNotificationRouter(um, nil)
	defer r.Close()
	now := time.Now()
	s := &domain.ArbitrageSignal{
		Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"),
		QuoteVolume: decimal.NewFromInt(1000), OpenedAt: now,
	}
	require.Equal(t, []int64{42}, r.ProcessSignal(s, true))
}

func TestNotificationRouter_UpdateThresholdUsesPercentagePoints(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 9, MinSpread: decimal.RequireFromString("0.02"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h, UpdateStep: decimal.RequireFromString("0.003")})
	tg := mocks.NewMockTelegramSender(gomock.NewController(t))
	broadcasts := make(chan string, 2)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{9}).Times(2).Do(func(text string, _ []int64) { broadcasts <- text })
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	now := time.Now()
	s := &domain.ArbitrageSignal{ID: "u1", Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, BuyExchange: "A", SellExchange: "B", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"), QuoteVolume: decimal.NewFromInt(1000), OpenedAt: now, IsActive: true}
	r.ProcessSignal(s, true)
	s.PeakSpread = decimal.RequireFromString("0.023")
	r.ProcessSignalUpdate(s)
	s.PeakSpread = decimal.RequireFromString("0.026")
	r.ProcessSignalUpdate(s)
	got := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case text := <-broadcasts:
			got = append(got, text)
		case <-time.After(2 * time.Second):
			t.Fatalf("broadcast %d was not delivered", i+1)
		}
	}
	updates := 0
	for _, text := range got {
		if strings.HasPrefix(text, "📈 SIGNAL UPDATE") {
			updates++
		}
	}
	require.Equal(t, 1, updates)
}

func TestNotificationRouter_DropsUpdateInvalidatedBeforeTransport(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 55, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h, UpdateStep: decimal.RequireFromString("0.001")})
	tg := mocks.NewMockTelegramSender(ctrl)
	tg.EXPECT().Broadcast(gomock.Any(), gomock.Any()).Times(0)
	r := NewNotificationRouter(um, tg)
	defer r.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	r.SetRevalidator(func(s *domain.ArbitrageSignal) (*domain.ArbitrageSignal, bool) {
		close(entered)
		<-release
		return s.Snapshot(), true
	})
	now := time.Now()
	s := &domain.ArbitrageSignal{ID: "race-1", Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, BuyExchange: "A", SellExchange: "B", PeakSpread: decimal.RequireFromString("0.011"), InitialSpread: decimal.RequireFromString("0.010"), QuoteVolume: decimal.NewFromInt(1000), OpenedAt: now, IsActive: true}
	r.ProcessSignalUpdate(s)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("notification worker did not enter revalidation")
	}
	r.invalidateSignal(s.ID)
	close(release)
	time.Sleep(50 * time.Millisecond)
}
