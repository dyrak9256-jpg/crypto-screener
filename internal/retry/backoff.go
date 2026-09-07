package retry

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// Backoff is a connection-local exponential backoff with bounded jitter.
// Each connection owns its state, so one exchange outage cannot affect another
// connection's retry schedule.
type Backoff struct {
	base    time.Duration
	max     time.Duration
	attempt atomic.Uint32
}

func New(base, max time.Duration) *Backoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = 30 * time.Second
	}
	return &Backoff{base: base, max: max}
}

func (b *Backoff) Reset() {
	if b == nil {
		return
	}
	b.attempt.Store(0)
}

func (b *Backoff) Wait(ctxDone <-chan struct{}) bool {
	if b == nil {
		return false
	}
	attempt := b.attempt.Add(1) - 1
	delay := b.base
	for i := uint32(0); i < attempt && delay < b.max; i++ {
		delay *= 2
		if delay >= b.max || delay <= 0 {
			delay = b.max
			break
		}
	}
	// Full jitter in [50%, 100%] avoids synchronized reconnect storms.
	delay = time.Duration(float64(delay) * (0.5 + rand.Float64()*0.5))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctxDone:
		return false
	}
}

// After is retained for small one-shot retry sites. Long-lived reconnect loops
// should use Backoff so failures do not synchronize at a fixed interval.
func After(base time.Duration) <-chan time.Time {
	if base <= 0 {
		base = time.Second
	}
	jitter := 0.75 + rand.Float64()*0.5
	return time.After(time.Duration(float64(base) * jitter))
}
