package network

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Traffic is a network's requests as the queue sees them: how many are in
// flight, when the last one that was not the queue's ended, and how many
// the queue has made. The zero value is ready to use.
//
// Each network counts its requests in it where its pace lets them go (its
// `request`), so only a request that reaches the network is counted: one
// refused by the gate or the pace never begins.
type Traffic struct {
	mu       sync.Mutex
	inFlight int       // every request, the queue's included
	lastEnd  time.Time // the last end of a request that was not the queue's

	backlog atomic.Int64 // requests begun under WithBacklog
	ended   atomic.Int64 // requests ended, the queue's included
}

// Begin counts a request that goes now; end, called once it has its
// answer (or its error), stops counting it. A request made under a context
// from WithBacklog is the queue's: it is in flight like any other, but its
// end does not break the network's quiet. Calling end again does nothing.
func (t *Traffic) Begin(ctx context.Context, now time.Time) (end func(now time.Time)) {
	queue := IsBacklog(ctx)
	t.mu.Lock()
	t.inFlight++
	t.mu.Unlock()
	if queue {
		t.backlog.Add(1)
	}
	var once sync.Once
	return func(now time.Time) {
		once.Do(func() {
			t.mu.Lock()
			t.inFlight--
			if !queue && now.After(t.lastEnd) {
				t.lastEnd = now
			}
			t.mu.Unlock()
			t.ended.Add(1)
		})
	}
}

// Quiet reports whether nothing is in flight and no request that was not
// the queue's ended within d of now.
func (t *Traffic) Quiet(now time.Time, d time.Duration) bool {
	at, ok := t.quietAt(d)
	return ok && !now.Before(at)
}

// quietAt is when the network is quiet for d, as things stand: ok is false
// while a request is in flight (the time is then unknown).
func (t *Traffic) quietAt(d time.Duration) (at time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlight > 0 {
		return time.Time{}, false
	}
	if t.lastEnd.IsZero() {
		return time.Time{}, true
	}
	return t.lastEnd.Add(d), true
}

// BacklogAsked is how many requests were made under WithBacklog.
func (t *Traffic) BacklogAsked() int64 { return t.backlog.Load() }

// Ended is how many requests have ended, the queue's included.
func (t *Traffic) Ended() int64 { return t.ended.Load() }

type backlogKey struct{}

// WithBacklog is ctx marked as the history queue's: a request made under
// it (or a context made from it) is counted as the queue's.
func WithBacklog(ctx context.Context) context.Context {
	return context.WithValue(ctx, backlogKey{}, true)
}

// IsBacklog reports whether ctx is the history queue's (WithBacklog).
func IsBacklog(ctx context.Context) bool {
	q, _ := ctx.Value(backlogKey{}).(bool)
	return q
}
