package instagram

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
)

// liveHarness is signed in, its first backfill done, and live.
func liveHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(false, ""), nil
	}
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	_ = h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 101, Timestamp: start})
	if h.lastState() != "live" {
		t.Fatalf("not live: %s", h.lastState())
	}
	return h
}

// drop is the socket failing as the library reports it: not unauthorized,
// so its own loop would dial again.
func drop(h *harness, n int) error {
	return h.m.dispatch(&slidetypes.Disconnected{Error: errors.New("invented dial failure"), FailureCount: n})
}

// stopSocket fails the socket until the adapter stops it.
func stopSocket(t *testing.T, h *harness) {
	t.Helper()
	for i := 0; i < 5; i++ {
		if err := drop(h, i); err != nil {
			t.Fatalf("failure %d stopped the socket: %v", i+1, err)
		}
	}
	if err := drop(h, 5); !errors.Is(err, errStop) {
		t.Fatalf("the 6th failure in a row returned %v, want errStop", err)
	}
}

func TestTheSocketStopsAfterSixFailuresInARow(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	if got := h.lastState(); got != "offline" {
		t.Errorf("a stopped socket is %s, want offline", got)
	}
}

func TestAConnectResetsTheFailureCount(t *testing.T) {
	h := liveHarness(t)
	for i := 0; i < 5; i++ {
		if err := drop(h, i); err != nil {
			t.Fatalf("failure %d stopped the socket: %v", i+1, err)
		}
	}
	_ = h.m.dispatch(&slidetypes.Connected{})
	for i := 0; i < 5; i++ {
		if err := drop(h, i); err != nil {
			t.Fatalf("failure %d after a connect stopped the socket: %v", i+1, err)
		}
	}
	if err := drop(h, 5); !errors.Is(err, errStop) {
		t.Fatalf("the 6th failure after the connect returned %v, want errStop", err)
	}
}

func TestTheDelayedRetryIsOnePacedInboxLoad(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	indexes, connects := h.m.count("LoadIndex"), h.m.count("Connect")

	// Nothing for 15 minutes.
	h.clock.Advance(14 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 0 {
		t.Fatalf("%d inbox loads before the retry was due", n)
	}
	// Then one paced inbox load, and the socket connects again.
	h.clock.Advance(2 * time.Minute)
	h.settle()
	eventually(t, "the socket to connect again", func() bool { return h.m.count("Connect") > connects })
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Errorf("the retry made %d inbox loads, want 1", n)
	}
	if n := h.m.count("Connect") - connects; n != 1 {
		t.Errorf("the retry connected %d times, want 1", n)
	}
	gaps(t, h.m.requests())
	_ = h.m.dispatch(&slidetypes.Connected{})
	if got := h.lastState(); got != "catchingUp" {
		t.Errorf("after the retry the state is %s", got)
	}

	// It connected: nothing more is scheduled.
	h.clock.Advance(10 * time.Hour)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Errorf("%d inbox loads after a retry that worked, want 1", n)
	}
}

func TestAFailedRetryWaitsTwiceAsLong(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	indexes := h.m.count("LoadIndex")
	h.m.indexErr = fmt.Errorf("%w: invented", httpclient.ErrRequestFailed)

	h.clock.Advance(16 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Fatalf("%d inbox loads at the first retry, want 1", n)
	}
	if got := h.lastState(); got != "offline" {
		t.Errorf("a failed retry is %s, want offline", got)
	}
	// The next is 30 minutes on, not 15.
	h.clock.Advance(28 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Fatalf("%d inbox loads 28 minutes after a failed retry, want 1", n)
	}
	h.clock.Advance(3 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 2 {
		t.Fatalf("%d inbox loads 31 minutes after a failed retry, want 2", n)
	}
	// And the wait never grows past the pace's longest, 4 hours.
	// (60 + 120 + 240 + 240 minutes and one over, a minute at a time: the
	// fake clock runs a timer only at the end of each step.)
	for range 661 {
		h.clock.Advance(time.Minute)
		h.settle()
	}
	if n := h.m.count("LoadIndex") - indexes; n != 6 {
		t.Errorf("%d inbox loads over the doubling, want 6", n)
	}
	gaps(t, h.m.requests())
}

func TestADelayedRetryAsksNothingWhileABlockIsInForce(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	indexes := h.m.count("LoadIndex")
	// A rate limit holds every request for the next hour.
	h.g.pace.RestoreRateLimit(h.clock.Now().Add(time.Hour), 1)

	h.clock.Advance(59 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 0 {
		t.Fatalf("%d inbox loads during the block, want 0", n)
	}
	// After the block, the next retry goes, once.
	h.clock.Advance(60 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Errorf("%d inbox loads after the block, want 1", n)
	}
}

func TestACheckpointFoundByTheRetryStopsEverything(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	indexes := h.m.count("LoadIndex")
	h.m.indexErr = httpclient.ErrCheckpointRequired

	h.clock.Advance(16 * time.Minute)
	h.settle()
	if got := h.lastState(); got != "reconnectNeeded/checkpoint" {
		t.Fatalf("a checkpoint at the retry is %s", got)
	}
	h.g.Nudge()
	h.clock.Advance(10 * time.Hour)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Errorf("%d inbox loads after a checkpoint, want 1", n)
	}
}

func TestANudgeBringsTheRetryForward(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	indexes, connects := h.m.count("LoadIndex"), h.m.count("Connect")

	// The Mac woke: the retry goes now, once, instead of in 15 minutes.
	h.g.Nudge()
	h.settle()
	eventually(t, "the socket to connect again", func() bool { return h.m.count("Connect") > connects })
	h.clock.Advance(20 * time.Minute)
	h.settle()
	if n := h.m.count("LoadIndex") - indexes; n != 1 {
		t.Errorf("a nudged retry made %d inbox loads, want 1", n)
	}
}
