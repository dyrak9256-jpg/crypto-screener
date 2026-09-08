package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// Janitor обязан закрывать active-маршруты, по которым давно не было
// наблюдений: если тики символа прекратились совсем (rmex/делистинг/
// гео-блок), close-логика ProcessTick не срабатывает — маршрут иначе
// остаётся в памяти и в БД (is_active=TRUE) до перезапуска процесса.
func TestShardedAggregator_JanitorClosesStaleActiveRoutes(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	cfg.SetFee(domain.FeeKeyDefault, decimal.Zero)
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 10)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	opened := <-ch
	require.Equal(t, domain.SignalOpened, opened.Lifecycle)

	key := routeKey(opened)
	shard := sa.shards[shardFor(opened.Symbol)]

	// Свежий маршрут janitor не трогает.
	sa.Janitor()
	require.Len(t, ch, 0)
	shard.mu.Lock()
	_, stillActive := shard.active[key]
	shard.mu.Unlock()
	require.True(t, stillActive, "fresh active route must survive janitor")

	// Симулируем прекращение тиков: сдвигаем время последнего наблюдения
	// за границу TTL (janitor использует реальные часы).
	shard.mu.Lock()
	if st := shard.active[key]; st != nil {
		st.last.Timestamp = time.Now().Add(-staleActiveRouteTTL - time.Minute)
		st.openedAt = time.Now().Add(-staleActiveRouteTTL - 2*time.Minute)
	}
	shard.mu.Unlock()

	sa.Janitor()

	select {
	case ev := <-ch:
		require.Equal(t, domain.SignalClosed, ev.Lifecycle)
		require.True(t, ev.Spread.IsZero())
		require.Equal(t, opened.BuyExchange, ev.BuyExchange)
		require.Equal(t, opened.SellExchange, ev.SellExchange)
	case <-time.After(time.Second):
		t.Fatal("janitor did not emit close event for stale active route")
	}

	shard.mu.Lock()
	_, stillActive = shard.active[key]
	shard.mu.Unlock()
	require.False(t, stillActive, "stale route must be removed from active")

	// Повторный janitor не даёт дублей.
	sa.Janitor()
	require.Len(t, ch, 0)
}

// Параллельный вызов Janitor из нескольких горутин не должен приводить к
// двойному закрытию одного маршрута (гонка за удаление из shard.active).
func TestShardedAggregator_JanitorConcurrentNoDoubleClose(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.01"))
	cfg.SetFee(domain.FeeKeyDefault, decimal.Zero)
	fm, _ := NewFundingManager(DefaultFundingConfig(), nil)
	ch := make(chan domain.SpreadEvent, 64)
	sa := NewShardedAggregator(ch, fm, cfg)
	now := time.Now()
	seedFunding(t, fm, now)
	sa.ProcessTick(tick("BINANCE", domain.MarketTypeFutures, "99", "100", now))
	sa.ProcessTick(tick("BYBIT", domain.MarketTypeFutures, "102", "103", now))
	opened := <-ch
	require.Equal(t, domain.SignalOpened, opened.Lifecycle)

	key := routeKey(opened)
	shard := sa.shards[shardFor(opened.Symbol)]
	shard.mu.Lock()
	if st := shard.active[key]; st != nil {
		st.last.Timestamp = time.Now().Add(-staleActiveRouteTTL - time.Minute)
	}
	shard.mu.Unlock()

	const concurrency = 8
	done := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			sa.Janitor()
			done <- struct{}{}
		}()
	}
	for i := 0; i < concurrency; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent janitor run hung")
		}
	}

	closed := 0
	for len(ch) > 0 {
		ev := <-ch
		require.Equal(t, domain.SignalClosed, ev.Lifecycle)
		closed++
	}
	require.Equal(t, 1, closed, "stale route must be closed exactly once")
}
