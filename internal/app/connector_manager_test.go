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

	mockConn := mocks.NewMockExchangeConnector(ctrl)

	var wg sync.WaitGroup
	wg.Add(3)

	var capturedCtx context.Context
	var mu sync.Mutex

	mockConn.EXPECT().
		ConnectSpot(gomock.Any(), tickChan).
		Do(func(ctx context.Context, ch chan<- domain.MarketTick) {
			mu.Lock()
			capturedCtx = ctx
			mu.Unlock()
			wg.Done()
			<-ctx.Done()
		}).
		Times(1)

	mockConn.EXPECT().
		ConnectFutures(gomock.Any(), tickChan).
		Do(func(ctx context.Context, ch chan<- domain.MarketTick) {
			wg.Done()
			<-ctx.Done()
		}).
		Times(1)

	mockConn.EXPECT().
		ConnectFunding(gomock.Any(), mockFundingSink).
		Do(func(ctx context.Context, sink domain.FundingSink) {
			wg.Done()
			<-ctx.Done()
		}).
		Times(1)

	parentCtx := context.Background()

	// 1. AddConnector
	err := cm.AddConnector("BINANCE", mockConn, parentCtx)
	require.NoError(t, err)

	wg.Wait() // Wait for all 3 goroutines to launch

	cm.mu.Lock()
	assert.Contains(t, cm.connectors, "BINANCE")
	cm.mu.Unlock()

	// 2. Duplicate AddConnector should be a no-op (no extra calls)
	err = cm.AddConnector("BINANCE", mockConn, parentCtx)
	require.NoError(t, err)

	// 3. RemoveConnector
	cm.RemoveConnector("BINANCE")

	cm.mu.Lock()
	assert.NotContains(t, cm.connectors, "BINANCE")
	cm.mu.Unlock()

	// Verify child context was cancelled
	select {
	case <-capturedCtx.Done():
		// Success! Context was cancelled by RemoveConnector
	case <-time.After(1 * time.Second):
		t.Fatal("child context should have been cancelled by RemoveConnector")
	}

	// 4. Removing non-existent connector is safe
	cm.RemoveConnector("NON_EXISTENT")
}
