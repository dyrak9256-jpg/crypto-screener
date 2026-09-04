package app

import (
	"context"
	"crypto-screener/internal/domain"
	"log"
	"sync"
)

type PersistenceWorker struct {
	dbChan <-chan *domain.ArbitrageSignal
	repo   domain.SignalRepository
}

func NewPersistenceWorker(dbChan <-chan *domain.ArbitrageSignal, repo domain.SignalRepository) *PersistenceWorker {
	return &PersistenceWorker{dbChan: dbChan, repo: repo}
}

func (pw *PersistenceWorker) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case signal, ok := <-pw.dbChan:
			if !ok {
				return
			}
			pw.repo.SaveSignal(context.Background(), signal)
		case <-ctx.Done():
			log.Println("🛑 Draining remaining signals to DB...")
			for signal := range pw.dbChan {
				pw.repo.SaveSignal(context.Background(), signal)
			}
			return
		}
	}
}
