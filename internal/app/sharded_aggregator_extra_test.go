package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// Цена старше priceTTL (5с по умолчанию) не должна ни открывать новые связки,
// ни мешать закрытию существующих: устаревший BBO — это не рынок.
func TestShardedAggregator_StalePricesIgnored(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	cfg.SetFee(domain.FeeKeyDefault, decimal.Zero)
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)

	// Живой спред открывает сигнал.
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	require.Len(t, ch, 1)
	<-ch

	// Спустя 6 секунд (TTL=5с) котировка BINANCE устарела: тик BYBIT не должен
	// переоценивать связку против мёртвой цены — вместо этого активный маршрут
	// с устаревшей ногой ЗАКРЫВАЕТСЯ (не висит активным вечно).
	later := now.Add(6 * time.Second)
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", later))
	require.Len(t, ch, 1, "устаревшая нога закрывает активный маршрут")
	ev := <-ch
	require.Equal(t, domain.SignalClosed, ev.Lifecycle)

	// Свежие котировки обеих ног снова образуют связку (переоткрытие).
	fresh := later.Add(time.Second)
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", fresh))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", fresh))
	require.Len(t, ch, 1)
	ev = <-ch
	require.Equal(t, domain.SignalOpened, ev.Lifecycle)
}

// Cross-объём события — минимум по ногам (узкое место маршрута), а не объём
// одной из бирж.
func TestShardedAggregator_CrossVolumeIsMinOfLegs(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	cfg.SetFee(domain.FeeKeyDefault, decimal.Zero)
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)

	lowVol := tick("BINANCE", domain.MarketTypeFutures, "99", "100", now)
	lowVol.QuoteVolume = decimal.NewFromInt(5000)
	highVol := tick("BYBIT", domain.MarketTypeFutures, "102", "103", now)
	highVol.QuoteVolume = decimal.NewFromInt(9000000)
	sa.ProcessTick(lowVol)
	sa.ProcessTick(highVol)
	require.Len(t, ch, 1)
	ev := <-ch
	require.True(t, ev.QuoteVolume.Equal(decimal.NewFromInt(5000)), "объём связки = min по ногам, получено: %s", ev.QuoteVolume)
}

// Одиночные котировки не создают сигналов: cross требует две биржи, intra —
// спот и фьючерс на одной. Перекрёстная котировка (bid>ask; в бою такие
// отбраковывают конвертеры бирж) не должна ломать агрегатор.
func TestShardedAggregator_MalformedQuotesSafe(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	cfg.SetFee(domain.FeeKeyDefault, decimal.Zero)
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)
	// Перекрёстный рынок (bid > ask) на единственной бирже: пары для cross нет,
	// intra без спот-котировки не образуется — агрегатор не падает, сигналов нет.
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "105", "100", now))
	require.Len(t, ch, 0)
	// Фьючерс на отдельной бирже тоже ни с чем не матчится.
	sa.ProcessTick(tick("OKX", domain.MarketTypeFutures, "150", "151", now))
	require.Len(t, ch, 0)
}
