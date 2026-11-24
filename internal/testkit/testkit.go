// Package testkit is what the network packages' tests share: a fake clock
// that moves only when told, a wait for a condition, and a log of the
// events a network emits. Only tests import it.
package testkit

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// Clock is a fake clock: it moves only when something sleeps on it or a
// test advances it, and a timer runs when the clock reaches it.
//
// By default Advance moves the clock to its end first and then runs every
// timer due by then, once, outside the clock's lock (a timer set by one of
// them waits for the next Advance). An ordered clock (NewOrderedClock)
// instead runs the due timers one at a time, earliest first, with the clock
// at each one's time, including any a timer sets that falls due before the
// end.
type Clock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*timer
	ordered bool
}

type timer struct {
	at      time.Time
	f       func()
	stopped bool
}

// NewClock is a clock at start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// NewOrderedClock is a clock at start whose Advance runs timers in order.
func NewOrderedClock(start time.Time) *Clock { return &Clock{now: start, ordered: true} }

// Now is the clock's time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc runs f once the clock has moved d on; stop cancels it, and
// reports whether it had not run or been stopped yet.
func (c *Clock) AfterFunc(d time.Duration, f func()) (stop func() bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &timer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		was := !t.stopped
		t.stopped = true
		return was
	}
}

// Sleep moves the clock on by d at once (a d of zero or less moves
// nothing), and returns ctx's error.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return ctx.Err()
}

// Pending is how many timers are set and have neither run nor been stopped.
func (c *Clock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped {
			n++
		}
	}
	return n
}

// Advance moves the clock on by d (none for d of zero or less) and runs the
// timers due by then.
func (c *Clock) Advance(d time.Duration) {
	if c.ordered {
		c.advanceInOrder(d)
		return
	}
	c.mu.Lock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
	var due []*timer
	kept := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.stopped:
		case !t.at.After(c.now):
			t.stopped = true
			due = append(due, t)
		default:
			kept = append(kept, t)
		}
	}
	c.timers = kept
	c.mu.Unlock()
	for _, t := range due {
		t.f()
	}
}

func (c *Clock) advanceInOrder(d time.Duration) {
	c.mu.Lock()
	end := c.now
	if d > 0 {
		end = c.now.Add(d)
	}
	c.mu.Unlock()
	for {
		c.mu.Lock()
		sort.SliceStable(c.timers, func(i, j int) bool { return c.timers[i].at.Before(c.timers[j].at) })
		var due *timer
		for _, t := range c.timers {
			if !t.stopped && !t.at.After(end) {
				due = t
				break
			}
		}
		if due == nil {
			c.now = end
			c.mu.Unlock()
			return
		}
		due.stopped = true
		if due.at.After(c.now) {
			c.now = due.at
		}
		c.mu.Unlock()
		due.f()
	}
}

// timeout is how long Eventually waits before it fails the test: only a
// failure waits this long (the race detector under load is slow).
const timeout = 60 * time.Second

// Eventually waits for cond, polling it: first by yielding to the other
// goroutines, then sleeping a little longer each time, up to a millisecond.
// It fails the test if cond is still false after a minute.
func Eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	if !Wait(cond) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// Wait is Eventually without a test: whether cond came true within the
// time Eventually allows.
func Wait(cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	pause := time.Duration(0)
	for i := 0; !cond(); i++ {
		if time.Now().After(deadline) {
			return false
		}
		if i < 50 {
			runtime.Gosched()
			continue
		}
		pause = min(max(pause*2, 20*time.Microsecond), time.Millisecond)
		time.Sleep(pause)
	}
	return true
}

// Never fails the test if cond comes true within a short real time: the
// one place a test must wait on the wall clock, where nothing happening is
// the point.
func Never(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("%s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Events keeps every event a network emits, in order. Emit is safe from
// any goroutine.
type Events struct {
	mu  sync.Mutex
	all []map[string]any
}

// Emit records one event; it is a network.Emit.
func (e *Events) Emit(ev map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.all = append(e.all, ev)
}

// All is every event so far.
func (e *Events) All() []map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]map[string]any(nil), e.all...)
}

// Named is every event called name so far.
func (e *Events) Named(name string) []map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []map[string]any
	for _, ev := range e.all {
		if ev["event"] == name {
			out = append(out, ev)
		}
	}
	return out
}

// Count is how many events called name have come.
func (e *Events) Count(name string) int { return len(e.Named(name)) }

// LastState is the last state event, or nil before any.
func (e *Events) LastState() map[string]any {
	s := e.Named("state")
	if len(s) == 0 {
		return nil
	}
	return s[len(s)-1]
}

// States lists the state events as "state" or "state/kind".
func (e *Events) States() []string {
	var out []string
	for _, ev := range e.Named("state") {
		s := fmt.Sprint(ev["state"])
		if k, ok := ev["kind"]; ok {
			s += "/" + fmt.Sprint(k)
		}
		out = append(out, s)
	}
	return out
}
