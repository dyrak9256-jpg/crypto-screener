package app

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"crypto-screener/internal/domain"
)

const persistenceTimeout = 5 * time.Second

type PersistenceWorker struct {
	dbChan     chan *domain.ArbitrageSignal
	signalRepo domain.SignalRepository
	shutdown   context.Context
}

func NewPersistenceWorker(dbChan chan *domain.ArbitrageSignal, repo domain.SignalRepository) *PersistenceWorker {
	return &PersistenceWorker{dbChan: dbChan, signalRepo: repo}
}

func (w *PersistenceWorker) Start(shutdown context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	if shutdown == nil {
		shutdown = context.Background()
	}
	if w.dbChan == nil || w.signalRepo == nil {
		log.Println("💾 Persistence worker stopped: queue or repository is nil")
		return
	}
	const workerCount = 4
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer workers.Done()
			for signal := range w.dbChan {
				if signal == nil {
					continue
				}
				if err := w.persist(shutdown, signal); err != nil {
					log.Printf("❌ Failed to persist signal %s after retries: %v", signal.ID, err)
				}
			}
		}()
	}
	workers.Wait()
	log.Println("💾 Persistence worker stopped")
}

func (w *PersistenceWorker) persist(shutdown context.Context, signal *domain.ArbitrageSignal) error {
	if shutdown == nil {
		shutdown = context.Background()
	}
	backoff := 200 * time.Millisecond
	for {
		saveCtx, cancel := context.WithTimeout(shutdown, persistenceTimeout)
		err := w.signalRepo.SaveSignal(saveCtx, signal)
		cancel()
		if err == nil {
			return nil
		}
		lastErr := fmt.Errorf("save signal %s: %w", signal.ID, err)

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-shutdown.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return fmt.Errorf("persist signal %s interrupted after database error: %w: %v", signal.ID, shutdown.Err(), lastErr)
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}
