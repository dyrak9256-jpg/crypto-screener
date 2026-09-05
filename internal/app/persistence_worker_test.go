package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestPersistenceWorker_NormalOperation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSignalRepository(ctrl)
	dbChan := make(chan *domain.ArbitrageSignal, 10)
	pw := NewPersistenceWorker(dbChan, mockRepo)

	sig1 := &domain.ArbitrageSignal{ID: uuid.NewString(), Symbol: "BTCUSDT"}
	sig2 := &domain.ArbitrageSignal{ID: uuid.NewString(), Symbol: "ETHUSDT"}
	sig3 := &domain.ArbitrageSignal{ID: uuid.NewString(), Symbol: "SOLUSDT"}

	savedSignals := make(map[string]bool)
	var mu sync.Mutex

	mockRepo.EXPECT().
		SaveSignal(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, s *domain.ArbitrageSignal) {
			mu.Lock()
			savedSignals[s.ID] = true
			mu.Unlock()
		}).
		Times(3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go pw.Start(ctx, &wg)

	dbChan <- sig1
	dbChan <- sig2
	dbChan <- sig3

	// Give worker time to process, then close channel
	time.Sleep(50 * time.Millisecond)
	close(dbChan)

	wg.Wait()

	mu.Lock()
	assert.True(t, savedSignals[sig1.ID])
	assert.True(t, savedSignals[sig2.ID])
	assert.True(t, savedSignals[sig3.ID])
	mu.Unlock()
}

func TestPersistenceWorker_GracefulDrainOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := mocks.NewMockSignalRepository(ctrl)
	dbChan := make(chan *domain.ArbitrageSignal, 10)
	pw := NewPersistenceWorker(dbChan, mockRepo)

	signals := []*domain.ArbitrageSignal{
		{ID: uuid.NewString(), Symbol: "BTCUSDT"},
		{ID: uuid.NewString(), Symbol: "ETHUSDT"},
		{ID: uuid.NewString(), Symbol: "XRPUSDT"},
		{ID: uuid.NewString(), Symbol: "DOGEUSDT"},
	}

	savedCount := 0
	var mu sync.Mutex

	mockRepo.EXPECT().
		SaveSignal(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, s *domain.ArbitrageSignal) {
			mu.Lock()
			savedCount++
			mu.Unlock()
		}).
		Times(len(signals))

	// Pre-fill dbChan
	for _, s := range signals {
		dbChan <- s
	}
	close(dbChan)

	// Context already cancelled to trigger drain branch
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	pw.Start(ctx, &wg)
	wg.Wait()

	mu.Lock()
	assert.Equal(t, len(signals), savedCount, "all buffered signals should be drained and saved to DB upon context cancellation")
	mu.Unlock()
}
