package app

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func mustDec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// mutableClock — управляемые часы для тестов, где время должно течь
// (FundingAge/EvictStale отсчитываются от фактического получения записи).
type mutableClock struct{ now time.Time }

func (c *mutableClock) Now() time.Time { return c.now }

// Чистая функция доходности: net = spread + sellRate − buyRate.
// Направления ног важны: покупатель платит ставку покупки, шорт получает ставку продажи.
func TestCalcNetSpread(t *testing.T) {
	cases := []struct {
		name              string
		spread, buy, sell string
		want              string
	}{
		{"нулевой funding", "0.02", "0", "0", "0.02"},
		{"фьючерсная пара: платим buy, получаем sell", "0.02", "0.0001", "0.0003", "0.0202"},
		{"неблагоприятный funding съедает спред", "0.02", "0.0003", "0", "0.0197"},
		{"спот-фьючерс: buy=0", "0.02", "0", "0.0003", "0.0203"},
		{"отрицательный sell-rate (шорт платит)", "0.02", "0", "-0.0003", "0.0197"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.True(t, CalcNetSpread(mustDec(c.spread), mustDec(c.buy), mustDec(c.sell)).Equal(mustDec(c.want)))
		})
	}
}

// Out-of-order funding-события не откатывают более свежую ставку.
func TestFundingManager_OutOfOrderUpdateIgnored(t *testing.T) {
	now := time.Now()
	fm, err := NewFundingManager(DefaultFundingConfig(), testClock{now: now})
	require.NoError(t, err)
	fresh := now.Add(-1 * time.Minute)
	stale := now.Add(-10 * time.Minute)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", mustDec("0.001"), now.Add(time.Hour), fresh))
	// Поздно пришедшее событие со старым EventTime не должно перезаписать ставку.
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", mustDec("0.999"), now.Add(time.Hour), stale))
	r, ok := fm.GetFunding("BINANCE", "BTCUSDT")
	require.True(t, ok)
	require.True(t, r.Rate.Equal(mustDec("0.001")), "новая ставка не должна перезаписываться старым событием")
}

// EvictStale вычищает записи старше MaxDataStaleness — оценка после этого fail-closed.
func TestFundingManager_EvictStale(t *testing.T) {
	now := time.Now()
	cfg := DefaultFundingConfig() // MaxDataStaleness = 60s
	clock := &mutableClock{now: now}
	fm, err := NewFundingManager(cfg, clock)
	require.NoError(t, err)
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", mustDec("0.001"), now.Add(time.Hour), now))
	require.NoError(t, fm.UpdateFunding("OKX", "ETHUSDT", mustDec("0.002"), now.Add(time.Hour), now))
	// Обе записи получены "сейчас"; время идёт на 2 минуты — BINANCE-запись
	// старее бюджета 60с и должна быть вычищена, OKX обновим перед этим.
	clock.now = now.Add(90 * time.Second)
	require.NoError(t, fm.UpdateFunding("OKX", "ETHUSDT", mustDec("0.002"), now.Add(time.Hour), now.Add(90*time.Second)))
	fm.EvictStale()
	_, ok := fm.GetFunding("BINANCE", "BTCUSDT")
	require.False(t, ok, "устаревшая запись должна быть удалена")
	_, ok = fm.GetFunding("OKX", "ETHUSDT")
	require.True(t, ok, "свежая запись остаётся")
}

// FundingAge: нет данных → -1; после апдейта возраст измерим и растёт.
func TestFundingManager_FundingAgeAndKnownExchanges(t *testing.T) {
	now := time.Now()
	clock := &mutableClock{now: now}
	fm, err := NewFundingManager(DefaultFundingConfig(), clock)
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), fm.FundingAge("BINANCE"))
	require.NoError(t, fm.UpdateFunding("BINANCE", "BTCUSDT", mustDec("0.001"), now.Add(time.Hour), now))
	// Регрессия: ПЕРВЫЙ апдейт биржи уже обязан фиксировать время получения
	// (раньше lastUpdate оставался нулевым до второго апдейта, и FundingAge
	// зря репортил "unknown").
	require.InDelta(t, 0.0, fm.FundingAge("BINANCE").Seconds(), 0.001)
	// Часы идут: возраст funding-данных растёт.
	clock.now = now.Add(10 * time.Second)
	require.InDelta(t, 10.0, fm.FundingAge("BINANCE").Seconds(), 0.001)
	require.Equal(t, []string{"BINANCE"}, fm.KnownExchanges())
}

// IsStale: слегка отрицательный возраст (funding пришёл сразу после тика)
// означает свежесть, а не устаревание.
func TestFundingRecord_IsStaleNegativeAgeIsFresh(t *testing.T) {
	now := time.Now()
	r := FundingRecord{LocalReceivedAt: now.Add(500 * time.Millisecond)}
	require.False(t, r.IsStale(now, 60*time.Second))
	r2 := FundingRecord{LocalReceivedAt: now.Add(-61 * time.Second)}
	require.True(t, r2.IsStale(now, 60*time.Second))
	// Нулевое время получения трактуется как отсутствие данных → stale.
	require.True(t, FundingRecord{}.IsStale(now, 60*time.Second))
}

// Нормализация ключей: биржа/символ в любом регистре и с разделителями
// ("-"/"_"/"/") приводятся к канонической форме.
func TestFundingManager_KeyNormalization(t *testing.T) {
	now := time.Now()
	fm, err := NewFundingManager(DefaultFundingConfig(), testClock{now: now})
	require.NoError(t, err)
	require.NoError(t, fm.UpdateFunding("binance", "BTC-USDT", mustDec("0.001"), now.Add(time.Hour), now))
	r, ok := fm.GetFunding("BINANCE", "btc_usdt")
	require.True(t, ok)
	require.True(t, r.Rate.Equal(mustDec("0.001")))
	require.Equal(t, "BINANCE", r.Exchange)
	require.Equal(t, "BTCUSDT", r.Symbol)
	// Пустые ключи отклоняются.
	require.Error(t, fm.UpdateFunding("", "BTCUSDT", decimal.Zero, now, now))
	require.Error(t, fm.UpdateFunding("BINANCE", "", decimal.Zero, now, now))
}

// Конфиг: отрицательный MinSpread и неположительный staleness-бюджет невалидны.
func TestFundingConfig_Validate(t *testing.T) {
	require.Error(t, FundingConfig{MinSpread: mustDec("-0.01"), MaxDataStaleness: time.Minute}.Validate())
	require.Error(t, FundingConfig{MinSpread: decimal.Zero}.Validate()) // 0 staleness → normalize не спасает при Validate напрямую
	require.NoError(t, FundingConfig{MinSpread: decimal.Zero, MaxDataStaleness: time.Minute}.Validate())
}
