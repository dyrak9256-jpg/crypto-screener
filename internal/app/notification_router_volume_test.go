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

// Минимальный объём юзера (/setvol) — часть маршрутизации сигнала:
// сигнал с объёмом ниже порога не должен доставляться такому юзеру,
// а CLOSE-события по-прежнему уходят всем, кому сигнал открывался.
func TestNotificationRouter_MinVolumeFilter(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 1, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.RequireFromString("10000"), Timeframe: domain.TF_24h})
	um.SetUser(&domain.User{ChatID: 2, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.RequireFromString("500"), Timeframe: domain.TF_24h})
	tg := mocks.NewMockTelegramSender(gomock.NewController(t))
	// Доставка асинхронная: ждём ОБА Broadcast (OPEN и CLOSE) до Close,
	// иначе select-гонка jobs/stop при shutdown может отбросить уведомление.
	broadcasts := make(chan struct{}, 2)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{2}).Times(2).Do(func(_ string, _ []int64) { broadcasts <- struct{}{} })
	r := NewNotificationRouter(um, tg)
	defer r.Close()

	// Объём сигнала 1000: юзеру 1 (порог 10000) не доставляется, юзеру 2 (500) — да.
	s := &domain.ArbitrageSignal{
		Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"),
		QuoteVolume: decimal.NewFromInt(1000), OpenedAt: time.Now(),
	}
	ids := r.ProcessSignal(s, true)
	require.Equal(t, []int64{2}, ids)

	// CLOSE уходит только тем, кому сигнал открывался (NotifiedChatIDs).
	s.NotifiedChatIDs = ids
	require.Equal(t, []int64{2}, r.ProcessSignal(s, false))
	for i := 0; i < 2; i++ {
		select {
		case <-broadcasts:
		case <-time.After(2 * time.Second):
			t.Fatalf("broadcast %d не доставлен до Close", i+1)
		}
	}
}

// Для таймфрейма ≠ 24h объём сигнала оценивается движком свечей по маршруту
// (VolumeEngine), а не берётся из 24h QuoteVolume тика: связка с большим
// 24h-объёмом, но пустой поминутной историей не проходит порог объёма юзера.
func TestNotificationRouter_TimeframeVolumeUsesEngine(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 5, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.RequireFromString("50000"), Timeframe: domain.TF_15m})
	tg := mocks.NewMockTelegramSender(gomock.NewController(t))
	delivered := make(chan struct{}, 1)
	tg.EXPECT().Broadcast(gomock.Any(), []int64{5}).Times(1).Do(func(_ string, _ []int64) { delivered <- struct{}{} })
	r := NewNotificationRouter(um, tg)
	defer r.Close()

	ve := NewVolumeEngine()
	now := time.Now()
	// Две минуты наблюдений по 1000 USDT/мин на ОБЕИХ ногах маршрута
	// (оценка маршрута — min по ногам) → оценка 15m ≈ 15000 < 50000.
	for i := range 2 {
		for _, market := range []domain.MarketType{domain.MarketTypeSpot, domain.MarketTypeFutures} {
			require.NoError(t, ve.UpdateCandle(domain.MarketCandle{
				Exchange: "BINANCE", Symbol: "BTCUSDT", MarketType: market,
				OpenTime: now.Add(time.Duration(-i) * time.Minute), QuoteVolume: decimal.NewFromInt(1000),
			}))
		}
	}
	r.volume = ve

	// 24h-объём тика огромный, но юзер смотрит 15m — должна сработать оценка движка.
	big24h := &domain.ArbitrageSignal{
		Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"),
		QuoteVolume: decimal.NewFromInt(10_000_000), OpenedAt: now,
		BuyExchange: "BINANCE", BuyMarket: domain.MarketTypeSpot,
		SellExchange: "BINANCE", SellMarket: domain.MarketTypeFutures,
		// Фьючерсная нога без времени funding блокируется fail-closed.
		SellNextFunding: now.Add(8 * time.Hour),
	}
	require.Empty(t, r.ProcessSignal(big24h, true), "оценка 15m (~15K) ниже порога 50K — доставки быть не должно")

	// Та же связка при пороге, который оценка проходит (меняем настройки юзера).
	um.SetUser(&domain.User{ChatID: 5, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.RequireFromString("10000"), Timeframe: domain.TF_15m})
	require.Equal(t, []int64{5}, r.ProcessSignal(big24h, true))
	// Дождаться фактической доставки до Close (см. MinVolumeFilter).
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast не доставлен до Close")
	}
}

// fundingAllowed распространяется только на маршруты с фьючерсной ногой:
// чистый спот-маршрут (теоретический) не блокируется funding-окном.
func TestNotificationRouter_SpotOnlyRouteSkipsFundingGate(t *testing.T) {
	um := domain.NewUserManager()
	um.SetUser(&domain.User{ChatID: 9, MinSpread: decimal.RequireFromString("0.01"), MinVolume: decimal.Zero, Timeframe: domain.TF_24h, MinFundingMinutes: 60})
	r := NewNotificationRouter(um, nil)
	defer r.Close()
	now := time.Now()
	// Обе ноги не фьючерсы → funding-фильтр не применяется.
	s := &domain.ArbitrageSignal{
		Symbol: "BTCUSDT", PeakSpread: decimal.RequireFromString("0.02"), InitialSpread: decimal.RequireFromString("0.02"),
		QuoteVolume: decimal.NewFromInt(1000), OpenedAt: now,
		BuyMarket: domain.MarketTypeSpot, SellMarket: domain.MarketTypeSpot,
		SellNextFunding: now.Add(1 * time.Minute),
	}
	require.Equal(t, []int64{9}, r.ProcessSignal(s, true))
}
