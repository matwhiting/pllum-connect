package network

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/testkit"
)

func TestChangedSendsAtOnceThenOncePerHalfSecond(t *testing.T) {
	clock := testkit.NewClock(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))
	log := &testkit.Events{}
	var c Changed
	c.Tell(clock, log.Emit)
	if n := log.Count("changed"); n != 1 {
		t.Fatalf("%d changed events at first, want 1 at once", n)
	}
	for range 5 {
		c.Tell(clock, log.Emit)
	}
	if n := log.Count("changed"); n != 1 {
		t.Fatalf("%d changed events within the half second, want still 1", n)
	}
	clock.Advance(ChangedEvery - time.Millisecond)
	if n := log.Count("changed"); n != 1 {
		t.Fatalf("one went before the half second was up")
	}
	clock.Advance(time.Millisecond)
	if n := log.Count("changed"); n != 2 {
		t.Fatalf("%d changed events after the half second, want 2: the burst as one", n)
	}
	clock.Advance(time.Second)
	c.Tell(clock, log.Emit)
	if n := log.Count("changed"); n != 3 {
		t.Fatalf("a quiet second on, it did not go at once")
	}
}

func TestRunRetireEndsItsContextAndMovesGenOn(t *testing.T) {
	var r Run
	ctx, gen := r.Begin(context.Background())
	if r.Ctx() != ctx || gen != r.Gen {
		t.Fatalf("Begin did not make the run in force")
	}
	r.Retire()
	if ctx.Err() == nil || r.Ctx() != nil || r.Gen == gen {
		t.Fatalf("Retire left the run: err %v, ctx %v, gen %d", ctx.Err(), r.Ctx(), r.Gen)
	}
	r.Retire() // nothing in force: only the number moves
	if r.Gen != gen+2 {
		t.Fatalf("gen %d, want %d", r.Gen, gen+2)
	}
}

func TestTheOutboxSendsInOrderOneAtATime(t *testing.T) {
	var mu sync.Mutex
	var sent, stopped []string
	var work Work
	var busy int
	running := true
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	o := Outbox{
		Begin: func() (context.Context, bool) {
			mu.Lock()
			defer mu.Unlock()
			if running {
				work.Add(1)
			}
			return ctx, running
		},
		End: work.Done,
		Deliver: func(_ context.Context, id string) {
			if id == "a" {
				<-release
			}
			mu.Lock()
			sent = append(sent, id)
			mu.Unlock()
		},
		Stopped: func(id string) {
			mu.Lock()
			stopped = append(stopped, id)
			mu.Unlock()
		},
		Busy: func(d int) {
			mu.Lock()
			busy += d
			mu.Unlock()
		},
	}
	o.Enqueue("a")
	o.Enqueue("b")
	o.Enqueue("c")
	if work.Running() != 1 {
		t.Fatalf("%d goroutines send, want 1", work.Running())
	}
	close(release)
	testkit.Eventually(t, "the sends", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(sent) == 3 && busy == 0
	})
	work.Wait()
	if sent[0] != "a" || sent[1] != "b" || sent[2] != "c" {
		t.Fatalf("sent %v, want in order", sent)
	}
	// Once stopping, a send never goes.
	cancel()
	mu.Lock()
	running = false
	mu.Unlock()
	o.Enqueue("d")
	if len(stopped) != 1 || stopped[0] != "d" || len(sent) != 3 || busy != 0 {
		t.Fatalf("after the stop: sent %v, stopped %v, busy %d", sent, stopped, busy)
	}
}
