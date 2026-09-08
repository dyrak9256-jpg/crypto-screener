package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func testSignal(id string, peak string) *domain.ArbitrageSignal {
	return &domain.ArbitrageSignal{ID: id, Symbol: "BTCUSDT", SpreadType: domain.CrossExchange, BuyExchange: "A", SellExchange: "B", BuyMarket: domain.MarketTypeFutures, SellMarket: domain.MarketTypeFutures, OpenedAt: time.Now(), IsActive: true, InitialSpread: decimal.RequireFromString("0.02"), PeakSpread: decimal.RequireFromString(peak)}
}

func TestSignalPersistenceBufferDeduplicatesSnapshots(t *testing.T) {
	b := NewSignalPersistenceBuffer()
	require.NoError(t, b.Put(testSignal("s1", "0.02")))
	require.NoError(t, b.Put(testSignal("s1", "0.05")))
	require.Equal(t, 1, b.Len())
	batch := b.SnapshotBatch(10)
	require.Len(t, batch, 1)
	require.Equal(t, "0.05", batch[0].signal.PeakSpread.String())
	b.Ack(batch)
	require.Zero(t, b.Len())
}

func TestSignalPersistenceBufferKeepsNewerSnapshotAfterAck(t *testing.T) {
	b := NewSignalPersistenceBuffer()
	require.NoError(t, b.Put(testSignal("s1", "0.02")))
	batch := b.SnapshotBatch(10)
	require.NoError(t, b.Put(testSignal("s1", "0.07")))
	b.Ack(batch)
	require.Equal(t, 1, b.Len())
	batch2 := b.SnapshotBatch(10)
	require.Equal(t, "0.07", batch2[0].signal.PeakSpread.String())
}

func TestBufferedPersistenceWorkerFlushes(t *testing.T) {
	b := NewSignalPersistenceBuffer()
	repo := &testSignalRepo{}
	require.NoError(t, b.Put(testSignal("s1", "0.02")))
	w := NewBufferedPersistenceWorker(b, repo)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go w.Start(ctx, &wg)
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()
	require.Equal(t, 1, repo.count)
	require.Zero(t, b.Len())
}

type testSignalRepo struct{ count int }

func (r *testSignalRepo) SaveSignal(context.Context, *domain.ArbitrageSignal) error {
	r.count++
	return nil
}
