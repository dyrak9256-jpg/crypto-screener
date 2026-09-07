package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/domain/mocks"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

func TestPersistenceWorker_DrainsClosedQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	repo := mocks.NewMockSignalRepository(ctrl)
	ch := make(chan *domain.ArbitrageSignal, 3)
	pw := NewPersistenceWorker(ch, repo)
	ids := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}
	repo.EXPECT().SaveSignal(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(func(context.Context, *domain.ArbitrageSignal) error { return nil })
	for _, id := range ids {
		ch <- &domain.ArbitrageSignal{ID: id}
	}
	close(ch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go pw.Start(ctx, &wg)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("persistence worker did not stop")
	}
}
