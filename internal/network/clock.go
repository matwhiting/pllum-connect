package network

import (
	"context"
	"sync"
	"time"
)

// Clock is the time a network's timers, its pace and its changed events run
// on; tests move a fake one (testkit.Clock).
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) (stop func() bool)
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) AfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

// Sleep waits d, or until ctx ends (its error).
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ChangedEvery is the least time between two changed events.
const ChangedEvery = 500 * time.Millisecond

// Changed is a network's changed event: Pllum is told there is something to
// re-read at once, or when ChangedEvery has passed since the last time, so
// a burst of writes is one re-read. The zero value is ready to use.
type Changed struct {
	mu      sync.Mutex
	last    time.Time
	pending bool
}

// Tell sends the changed event with emit, on clock's time: now, unless one
// went less than ChangedEvery ago; then once, when ChangedEvery has passed
// since it.
func (c *Changed) Tell(clock Clock, emit Emit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending {
		return
	}
	now := clock.Now()
	if wait := c.last.Add(ChangedEvery).Sub(now); wait > 0 && !c.last.IsZero() {
		c.pending = true
		clock.AfterFunc(wait, func() {
			c.mu.Lock()
			c.pending = false
			c.last = clock.Now()
			c.mu.Unlock()
			emit(map[string]any{"event": "changed"})
		})
		return
	}
	c.last = now
	emit(map[string]any{"event": "changed"})
}
