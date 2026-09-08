package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"crypto-screener/internal/domain"
	"crypto-screener/internal/observability"
)

// SignalPersistenceBuffer keeps only the newest snapshot of each signal in RAM.
// It is deliberately process-local: market/signal history is not a durability
// requirement, but a database outage must not block or lose the live signal engine.
type SignalPersistenceBuffer struct {
	mu     sync.Mutex
	items  map[string]bufferedSignal
	wake   chan struct{}
	closed bool
}

type bufferedSignal struct {
	version uint64
	signal  *domain.ArbitrageSignal
}

func NewSignalPersistenceBuffer() *SignalPersistenceBuffer {
	return &SignalPersistenceBuffer{items: make(map[string]bufferedSignal), wake: make(chan struct{}, 1)}
}

func (b *SignalPersistenceBuffer) Put(signal *domain.ArbitrageSignal) error {
	if b == nil || signal == nil || signal.ID == "" {
		return nil
	}
	copy := signal.Snapshot()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("persistence buffer is closed")
	}
	item := b.items[copy.ID]
	item.version++
	item.signal = copy
	b.items[copy.ID] = item
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
	return nil
}

func (b *SignalPersistenceBuffer) SnapshotBatch(limit int) []bufferedSignal {
	if b == nil || limit <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]bufferedSignal, 0, minInt(limit, len(b.items)))
	for _, item := range b.items {
		if item.signal == nil {
			continue
		}
		out = append(out, bufferedSignal{version: item.version, signal: item.signal.Snapshot()})
		if len(out) == limit {
			break
		}
	}
	return out
}

func (b *SignalPersistenceBuffer) Ack(batch []bufferedSignal) {
	if b == nil || len(batch) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, item := range batch {
		if current, ok := b.items[item.signal.ID]; ok && current.version == item.version {
			delete(b.items, item.signal.ID)
		}
	}
}

func (b *SignalPersistenceBuffer) Len() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	n := len(b.items)
	b.mu.Unlock()
	return n
}

func (b *SignalPersistenceBuffer) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
}

// BufferedPersistenceWorker flushes compact signal snapshots in batches. A DB
// outage therefore grows memory by the number of active/changed signals, not by
// the number of ticks or lifecycle updates.
type BufferedPersistenceWorker struct {
	buffer *SignalPersistenceBuffer
	repo   domain.SignalRepository
}

func NewBufferedPersistenceWorker(buffer *SignalPersistenceBuffer, repo domain.SignalRepository) *BufferedPersistenceWorker {
	return &BufferedPersistenceWorker{buffer: buffer, repo: repo}
}

func (w *BufferedPersistenceWorker) Start(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	if w == nil || w.buffer == nil || w.repo == nil {
		return
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flush(ctx, 500)
		case <-w.buffer.wake:
			w.flush(ctx, 500)
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			w.flush(finalCtx, 500)
			cancel()
			if w.buffer.Len() > 0 {
				slog.Warn("persistence worker stopped with pending in-memory signals", "pending", w.buffer.Len())
			}
			return
		}
	}
}

func (w *BufferedPersistenceWorker) flush(parent context.Context, limit int) {
	if parent == nil {
		parent = context.Background()
	}
	for {
		batch := w.buffer.SnapshotBatch(limit)
		if len(batch) == 0 {
			return
		}
		for _, item := range batch {
			ctx, cancel := context.WithTimeout(parent, 5*time.Second)
			err := w.repo.SaveSignal(ctx, item.signal)
			cancel()
			if err != nil {
				observability.DBError()
				slog.Warn("signal persistence batch flush failed", "signal_id", item.signal.ID, "pending", w.buffer.Len(), "error", err)
				return
			}
		}
		w.buffer.Ack(batch)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
