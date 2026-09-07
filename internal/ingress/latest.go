package ingress

import (
	"crypto-screener/internal/domain"
	"sync"
	"sync/atomic"
)

type key struct {
	exchange, symbol string
	market           domain.MarketType
}

type state struct {
	mu      sync.Mutex
	latest  map[key]domain.MarketTick
	notify  chan struct{}
	out     chan<- domain.MarketTick
	done    chan struct{}
	runDone chan struct{}
	stopped atomic.Bool
	once    sync.Once
}

// The registry preserves the existing adapter interface. State is explicitly
// stopped during Application shutdown; a late producer then gets ignored rather
// than writing into a closed channel. This is important when a misbehaving
// connector survives its cancellation deadline.
var registry sync.Map

func Submit(out chan<- domain.MarketTick, t domain.MarketTick) {
	if out == nil {
		return
	}
	candidate := newState(out)
	v, loaded := registry.LoadOrStore(out, candidate)
	s := v.(*state)
	if !loaded {
		go s.run()
	}
	if s.stopped.Load() {
		return
	}
	k := key{exchange: t.Exchange, symbol: t.Symbol, market: t.MarketType}
	s.mu.Lock()
	if s.stopped.Load() {
		s.mu.Unlock()
		return
	}
	s.latest[k] = t
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func newState(out chan<- domain.MarketTick) *state {
	return &state{
		latest:  make(map[key]domain.MarketTick),
		notify:  make(chan struct{}, 1),
		out:     out,
		done:    make(chan struct{}),
		runDone: make(chan struct{}),
	}
}

func (s *state) run() {
	defer close(s.runDone)
	for {
		select {
		case <-s.notify:
			s.flush()
		case <-s.done:
			return
		}
	}
}

func (s *state) next() (domain.MarketTick, key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.latest {
		delete(s.latest, k)
		return v, k, true
	}
	return domain.MarketTick{}, key{}, false
}

func (s *state) put(k key, t domain.MarketTick) {
	s.mu.Lock()
	if !s.stopped.Load() {
		s.latest[k] = t
	}
	s.mu.Unlock()
}

func (s *state) flush() {
	for {
		t, k, ok := s.next()
		if !ok {
			return
		}
		select {
		case s.out <- t:
		case <-s.done:
			s.put(k, t)
			return
		}
	}
}

// DrainAndStop is called only after all producers have been asked to stop.
// It marks the state stopped before terminating the coalescer so any connector
// that outlives its shutdown deadline cannot recreate a writer or panic by
// sending into a channel that the owner is about to close.
//
// Pending market ticks are intentionally discarded during shutdown. They are
// transient observations, while active arbitrage state is explicitly flushed
// by the aggregator. Trying to synchronously flush the ingress buffer here can
// deadlock shutdown behind downstream backpressure (ingress -> tick queue ->
// shard queue -> tracker queue -> persistence).
func DrainAndStop(out chan<- domain.MarketTick) {
	if out == nil {
		return
	}
	candidate := newState(out)
	v, loaded := registry.LoadOrStore(out, candidate)
	s := v.(*state)
	if !loaded {
		go s.run()
	}
	s.stopped.Store(true)
	s.mu.Lock()
	clear(s.latest)
	s.mu.Unlock()
	s.once.Do(func() { close(s.done) })
	<-s.runDone
}

// Forget removes a stopped state from the registry. It must be called only when
// the owner has positively confirmed that no producer can call Submit again.
func Forget(out chan<- domain.MarketTick) {
	if out == nil {
		return
	}
	registry.Delete(out)
}
