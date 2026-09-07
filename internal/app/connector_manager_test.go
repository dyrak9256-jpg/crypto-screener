package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConnectorManager_AddRemoveAndStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tickChan := make(chan domain.MarketTick, 10)
	sink := mocks.NewMockFundingSink(ctrl)
	sink.EXPECT().SetStreamHealth(gomock.Any(), gomock.Any()).AnyTimes()
	cm := NewConnectorManager(tickChan, sink, WithStopTimeout(time.Second))
	conn := mocks.NewMockExchangeConnector(ctrl)
	var calls sync.WaitGroup
	calls.Add(2)
	conn.EXPECT().ConnectSpot(gomock.Any(), tickChan).AnyTimes().Do(func(ctx context.Context, _ chan<- domain.MarketTick) { calls.Done(); <-ctx.Done() })
	conn.EXPECT().ConnectFutures(gomock.Any(), tickChan).AnyTimes().Do(func(ctx context.Context, _ chan<- domain.MarketTick) { calls.Done(); <-ctx.Done() })
	conn.EXPECT().ConnectFunding(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, _ domain.FundingSink) { <-ctx.Done() })
	parent := context.Background()
	require.NoError(t, cm.AddConnector(parent, "BINANCE", conn))
	calls.Wait()
	cm.mu.Lock()
	_, ok := cm.entries["BINANCE"]
	cm.mu.Unlock()
	require.True(t, ok)
	require.NoError(t, cm.RemoveConnector("BINANCE"))
	cm.mu.Lock()
	_, ok = cm.entries["BINANCE"]
	cm.mu.Unlock()
	require.False(t, ok)
	require.Error(t, cm.RemoveConnector("BINANCE"))
	require.NoError(t, cm.StopAll())
	require.Error(t, cm.AddConnector(parent, "BINANCE", conn))
}
