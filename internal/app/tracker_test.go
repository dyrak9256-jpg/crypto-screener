package app

import (
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestTracker_LifecycleAndPeakPersistence(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.RequireFromString("1000"))
	db := make(chan *domain.ArbitrageSignal, 10)
	tr := NewTracker(cfg, db, nil)
	t0 := time.Now()
	tr.HandleEvent(domain.SpreadEvent{Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, Spread: decimal.RequireFromString("0.03"), BuyExchange: "BINANCE", SellExchange: "BYBIT", BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures, Timestamp: t0, Lifecycle: domain.SignalOpened})
	require.Len(t, db, 1)
	open := <-db
	require.True(t, open.IsActive)
	tr.HandleEvent(domain.SpreadEvent{Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, Spread: decimal.RequireFromString("0.07"), BuyExchange: "BINANCE", SellExchange: "BYBIT", BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures, Timestamp: t0.Add(10 * time.Second), Lifecycle: domain.SignalUpdated})
	require.Len(t, db, 1)
	tr.HandleEvent(domain.SpreadEvent{Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, Spread: decimal.Zero, BuyExchange: "BINANCE", SellExchange: "BYBIT", BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures, Timestamp: t0.Add(45 * time.Second), Lifecycle: domain.SignalClosed})
	require.Len(t, db, 1)
	closed := <-db
	require.False(t, closed.IsActive)
	require.Equal(t, 45*time.Second, closed.Duration)
	require.True(t, closed.PeakSpread.Equal(decimal.RequireFromString("0.07")))
}

func TestTracker_OutOfOrderIgnored(t *testing.T) {
	cfg := domain.NewScreenerConfig(decimal.RequireFromString("0.02"), decimal.Zero)
	tr := NewTracker(cfg, make(chan *domain.ArbitrageSignal, 10), nil)
	t0 := time.Now()
	tr.HandleEvent(domain.SpreadEvent{Symbol: "ETHUSDT", SpreadType: domain.CrossExchange, Spread: decimal.RequireFromString("0.03"), BuyExchange: "A", SellExchange: "B", BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures, Timestamp: t0, Lifecycle: domain.SignalOpened})
	tr.HandleEvent(domain.SpreadEvent{Symbol: "ETHUSDT", SpreadType: domain.CrossExchange, Spread: decimal.Zero, BuyExchange: "A", SellExchange: "B", BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures, Timestamp: t0.Add(-time.Second), Lifecycle: domain.SignalClosed})
	tr.mu.Lock()
	defer tr.mu.Unlock()
	require.Len(t, tr.activeSignals, 1)
}
