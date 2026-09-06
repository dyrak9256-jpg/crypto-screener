package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConnectorManager_AddAndRemove(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tickChan := make(chan domain.MarketTick, 10)
	mockFundingSink := mocks.NewMockFundingSink(ctrl)
	cm := NewConnectorManager(tickChan, mockFundingSink)
	parentCtx := context.Background()

	var wg sync.WaitGroup
	wg.Add(3)

	var capturedCtx context.Context
	var mu sync.Mutex

	// Первый коннектор: блокируется до отмены своего контекста.
	conn1 := mocks.NewMockExchangeConnector(ctrl)
	conn1.EXPECT().
		ConnectSpot(gomock.Any(), tickChan).
		DoAndReturn(func(ctx context.Context, _ chan<- domain.MarketTick) error {
			mu.Lock()
			capturedCtx = ctx
			mu.Unlock()
			wg.Done()
			<-ctx.Done()
			return nil
		}).
		AnyTimes()
	conn1.EXPECT().
		ConnectFutures(gomock.Any(), tickChan).
		DoAndReturn(func(ctx context.Context, _ chan<- domain.MarketTick) error {
			wg.Done()
			<-ctx.Done()
			return nil
		}).
		AnyTimes()
	conn1.EXPECT().
		ConnectFunding(gomock.Any(), mockFundingSink).
		DoAndReturn(func(ctx context.Context, _ domain.FundingSink) error {
			wg.Done()
			<-ctx.Done()
			return nil
		}).
		AnyTimes()

	// 1. AddConnector
	require.NoError(t, cm.AddConnector("BINANCE", conn1, parentCtx))
	wg.Wait() // Дождались запуска 3 потоков (Spot/Futures/Funding)

	cm.mu.Lock()
	_, ok := cm.entries["BINANCE"]
	cm.mu.Unlock()
	require.True(t, ok, "connector should be registered")

	// 2. Hot-Swap: повторный AddConnector с тем же именем заменяет старый entry.
	//   Старый коннектор должен быть остановлен, остаётся ровно один entry.
	var wg2 sync.WaitGroup
	wg2.Add(3)
	conn2 := mocks.NewMockExchangeConnector(ctrl)
	conn2.EXPECT().
		ConnectSpot(gomock.Any(), tickChan).
		DoAndReturn(func(ctx context.Context, _ chan<- domain.MarketTick) error { wg2.Done(); <-ctx.Done(); return nil }).
		AnyTimes()
	conn2.EXPECT().
		ConnectFutures(gomock.Any(), tickChan).
		DoAndReturn(func(ctx context.Context, _ chan<- domain.MarketTick) error { wg2.Done(); <-ctx.Done(); return nil }).
		AnyTimes()
	conn2.EXPECT().
		ConnectFunding(gomock.Any(), mockFundingSink).
		DoAndReturn(func(ctx context.Context, _ domain.FundingSink) error { wg2.Done(); <-ctx.Done(); return nil }).
		AnyTimes()

	require.NoError(t, cm.AddConnector("BINANCE", conn2, parentCtx))
	wg2.Wait()

	// Контекст старого коннектора должен быть отменён при hot-swap.
	mu.Lock()
	ctx1 := capturedCtx
	mu.Unlock()
	select {
	case <-ctx1.Done():
		// Отлично: hot-swap отменил контекст старого коннектора.
	case <-time.After(2 * time.Second):
		t.Fatal("old connector context should have been cancelled by hot-swap")
	}

	cm.mu.Lock()
	_, ok = cm.entries["BINANCE"]
	cm.mu.Unlock()
	require.True(t, ok, "new connector should be registered after hot-swap")

	// 3. RemoveConnector
	require.NoError(t, cm.RemoveConnector("BINANCE"))
	cm.mu.Lock()
	_, ok = cm.entries["BINANCE"]
	cm.mu.Unlock()
	assert.False(t, ok, "connector should be removed")

	// 4. Removing non-existent connector returns error
	require.Error(t, cm.RemoveConnector("NON_EXISTENT"))
}
