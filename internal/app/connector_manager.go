package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"crypto-screener/internal/domain"
)

type connectorEntry struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped chan struct{}
}

func newConnectorEntry(ctx context.Context, name string, conn domain.ExchangeConnector, tickChan chan<- domain.MarketTick, fundingSink domain.FundingSink, candleSink domain.CandleSink) *connectorEntry {
	if ctx == nil {
		ctx = context.Background()
	}
	entryCtx, cancel := context.WithCancel(ctx)
	e := &connectorEntry{cancel: cancel, stopped: make(chan struct{})}

	start := func(label string, fn func(context.Context) error) {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if err := fn(entryCtx); err != nil && entryCtx.Err() == nil {
				wrapped := fmt.Errorf("connector %s/%s: %w", name, label, err)
				log.Printf("⚠️ [%s/%s] stopped with error: %v", name, label, wrapped)
			}
		}()
	}
	start("Spot", func(ctx context.Context) error { return conn.ConnectSpot(ctx, tickChan) })
	start("Futures", func(ctx context.Context) error { return conn.ConnectFutures(ctx, tickChan) })
	if cc, ok := conn.(domain.CandleConnector); ok && candleSink != nil {
		start("Candles", func(ctx context.Context) error { return cc.ConnectCandles(ctx, candleSink) })
	}
	if fc, ok := conn.(domain.FundingConnector); ok && fundingSink != nil {
		start("Funding", func(ctx context.Context) error {
			err := fc.ConnectFunding(ctx, fundingSink)
			// A connector shutdown/error must immediately invalidate funding.
			// The FundingManager also has TTL-based staleness, but an explicit false
			// transition improves diagnostics and closes the failure window.
			fundingSink.SetStreamHealth(name, false)
			if err != nil {
				return fmt.Errorf("funding stream: %w", err)
			}
			return nil
		})
	}
	go func() { e.wg.Wait(); close(e.stopped) }()
	return e
}

func (e *connectorEntry) stop(timeout time.Duration) error {
	e.cancel()
	if timeout <= 0 {
		<-e.stopped
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.stopped:
		return nil
	case <-timer.C:
		return fmt.Errorf("stop timed out after %v", timeout)
	}
}

type ConnectorManager struct {
	lifecycleMu sync.Mutex
	mu          sync.RWMutex
	entries     map[string]*connectorEntry
	stopped     bool
	stopOnce    sync.Once
	stopDone    chan struct{}
	stopErr     error
	stopTimeout time.Duration
	tickChan    chan<- domain.MarketTick
	fundingSink domain.FundingSink
	candleSink  domain.CandleSink
}

type ConnectorManagerOption func(*ConnectorManager)

func WithStopTimeout(d time.Duration) ConnectorManagerOption {
	return func(cm *ConnectorManager) {
		if d > 0 {
			cm.stopTimeout = d
		}
	}
}

func NewConnectorManager(tickChan chan<- domain.MarketTick, fundingSink domain.FundingSink, extras ...any) *ConnectorManager {
	var candleSink domain.CandleSink
	cm := &ConnectorManager{entries: make(map[string]*connectorEntry), stopDone: make(chan struct{}), stopTimeout: 15 * time.Second, tickChan: tickChan, fundingSink: fundingSink}
	for _, extra := range extras {
		switch v := extra.(type) {
		case domain.CandleSink:
			if candleSink == nil {
				candleSink = v
			}
		case []domain.CandleSink:
			if len(v) > 0 && candleSink == nil {
				candleSink = v[0]
			}
		case ConnectorManagerOption:
			if v != nil {
				v(cm)
			}
		}
	}
	cm.candleSink = candleSink
	return cm
}

var ErrManagerStopped = errors.New("connector manager is stopped")

func (cm *ConnectorManager) AddConnector(parentCtx context.Context, name string, conn domain.ExchangeConnector) error {
	cm.lifecycleMu.Lock()
	defer cm.lifecycleMu.Unlock()
	name = strings.ToUpper(strings.TrimSpace(name))
	if name == "" {
		return errors.New("connector name is empty")
	}
	if conn == nil {
		return errors.New("connector is nil")
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return ErrManagerStopped
	}
	old := cm.entries[name]
	cm.mu.Unlock()
	if old != nil {
		if err := old.stop(cm.stopTimeout); err != nil {
			return fmt.Errorf("stop old connector %q: %w", name, err)
		}
		cm.mu.Lock()
		if cm.entries[name] == old {
			delete(cm.entries, name)
		}
		cm.mu.Unlock()
	}
	entry := newConnectorEntry(parentCtx, name, conn, cm.tickChan, cm.fundingSink, cm.candleSink)
	cm.mu.Lock()
	cm.entries[name] = entry
	cm.mu.Unlock()
	log.Printf("✅ [%s] connector started", name)
	return nil
}

func (cm *ConnectorManager) RemoveConnector(name string) error {
	cm.lifecycleMu.Lock()
	defer cm.lifecycleMu.Unlock()
	name = strings.ToUpper(strings.TrimSpace(name))
	if name == "" {
		return errors.New("connector name is empty")
	}
	cm.mu.Lock()
	if cm.stopped {
		cm.mu.Unlock()
		return ErrManagerStopped
	}
	entry, ok := cm.entries[name]
	if !ok {
		cm.mu.Unlock()
		return fmt.Errorf("connector %q not found", name)
	}
	cm.mu.Unlock()
	if err := entry.stop(cm.stopTimeout); err != nil {
		// Keep the entry registered while the old connector may still be alive.
		// Otherwise a later AddConnector could start a second connector for the
		// same exchange while the first one is still producing market data.
		return fmt.Errorf("stop connector %q: %w", name, err)
	}
	cm.mu.Lock()
	if cm.entries[name] == entry {
		delete(cm.entries, name)
	}
	cm.mu.Unlock()
	log.Printf("🛑 [%s] connector stopped cleanly", name)
	return nil
}

func (cm *ConnectorManager) StopAll() error {
	cm.stopOnce.Do(func() {
		defer close(cm.stopDone)
		cm.lifecycleMu.Lock()
		cm.mu.Lock()
		cm.stopped = true
		entries := cm.entries
		cm.entries = make(map[string]*connectorEntry)
		cm.mu.Unlock()
		cm.lifecycleMu.Unlock()

		var wg sync.WaitGroup
		errs := make(chan error, len(entries))
		for name, entry := range entries {
			name, entry := name, entry
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := entry.stop(cm.stopTimeout); err != nil {
					errs <- fmt.Errorf("%s: %w", name, err)
				}
			}()
		}
		wg.Wait()
		close(errs)
		var all []string
		for err := range errs {
			all = append(all, err.Error())
		}
		if len(all) > 0 {
			cm.stopErr = fmt.Errorf("connector shutdown incomplete: %s", strings.Join(all, "; "))
		} else {
			log.Println("✅ ConnectorManager: all connectors stopped")
		}
	})
	<-cm.stopDone
	return cm.stopErr
}
