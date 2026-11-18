package messenger

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Final review I1 and I2: Messenger's main socket, as Instagram's (e0a1d84).
// Every value is invented.

// liveHarness is signed in, both sockets up: live.
func liveHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.signIn(t)
	eventually(t, "live", func() bool { s, _ := h.state(); return s == network.Live })
	h.settle()
	return h
}

// drop is the main socket failing as messagix reports it: not
// unauthorized, so its own loop would dial again.
func drop(h *harness, n int) {
	h.f.last().dispatch(&messagix.TransientDisconnectEvent{Err: errors.New("invented dial failure"), ConnectionAttempts: n})
}

// running reports whether a run is in force (a client held).
func (h *harness) running() bool {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	return h.m.cli != nil
}

// redialWaiting reports whether a redial is waiting for its time.
func (h *harness) redialWaiting() bool {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	return h.m.redialDue
}

// stopSocket fails the main socket until the network stops it.
func stopSocket(t *testing.T, h *harness) {
	t.Helper()
	for i := 1; i <= 5; i++ {
		drop(h, i)
		if !h.running() {
			t.Fatalf("failure %d stopped the socket", i)
		}
	}
	drop(h, 6)
	if h.running() {
		t.Fatalf("the 6th failure in a row left the socket running")
	}
}

func TestTheMainSocketStopsAfterSixFailuresInARow(t *testing.T) {
	h := liveHarness(t)
	loads, connects := h.f.count("LoadMessagesPage"), h.f.count("Connect")
	stopSocket(t, h)
	if s, k := h.state(); s != network.Offline || k != "" {
		t.Errorf("a stopped socket is %s/%s, want offline", s, k)
	}
	eventually(t, "the library's loop to be stopped", func() bool {
		h.f.mu.Lock()
		defer h.f.mu.Unlock()
		return h.f.disconnects > 0
	})
	h.f.clock.Advance(14 * time.Minute)
	h.settle()
	if h.f.count("LoadMessagesPage") != loads || h.f.count("Connect") != connects {
		t.Errorf("the stopped socket was dialled again before its redial")
	}
}

func TestAConnectResetsTheFailureCount(t *testing.T) {
	h := liveHarness(t)
	for i := 1; i <= 5; i++ {
		drop(h, i)
	}
	h.f.last().dispatch(&messagix.ReconnectedEvent{})
	for i := 1; i <= 5; i++ {
		drop(h, i)
		if !h.running() {
			t.Fatalf("failure %d after a connect stopped the socket", i)
		}
	}
	drop(h, 6)
	if h.running() {
		t.Fatalf("the 6th failure after the connect left the socket running")
	}
}

func TestTheRedialIsOnePacedMessagesPageThenAConnect(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	loads, connects := h.f.count("LoadMessagesPage"), h.f.count("Connect")

	h.f.clock.Advance(14 * time.Minute)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 0 {
		t.Fatalf("%d messages pages before the redial was due", n)
	}
	h.f.clock.Advance(2 * time.Minute)
	eventually(t, "live again", func() bool { s, _ := h.state(); return s == network.Live })
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 1 {
		t.Errorf("the redial loaded %d messages pages, want 1", n)
	}
	if n := h.f.count("Connect") - connects; n != 1 {
		t.Errorf("the redial connected %d times, want 1", n)
	}
	gaps(t, h.f.requests())

	// It connected: nothing more is scheduled.
	h.f.clock.Advance(10 * time.Hour)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 1 {
		t.Errorf("%d messages pages after a redial that worked, want 1", n)
	}
}

func TestAFailedRedialWaitsTwiceAsLong(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	loads := h.f.count("LoadMessagesPage")
	h.f.mu.Lock()
	h.f.loadErr = fmt.Errorf("%w: invented", httpclient.ErrRequestFailed)
	h.f.mu.Unlock()

	h.f.clock.Advance(16 * time.Minute)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 1 {
		t.Fatalf("%d messages pages at the first redial, want 1", n)
	}
	if s, _ := h.state(); s != network.Offline {
		t.Errorf("a failed redial is %s, want offline", s)
	}
	h.f.clock.Advance(28 * time.Minute)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 1 {
		t.Fatalf("%d messages pages 28 minutes after a failed redial, want 1", n)
	}
	h.f.clock.Advance(3 * time.Minute)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 2 {
		t.Fatalf("%d messages pages 31 minutes after a failed redial, want 2", n)
	}
	// The wait never grows past 4 hours: 60 + 120 + 240 + 240 minutes and
	// one over, a minute at a time.
	for range 661 {
		h.f.clock.Advance(time.Minute)
		// a redial that ran has failed and the next is waiting
		eventually(t, "the next redial to be due", h.redialWaiting)
	}
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 6 {
		t.Errorf("%d messages pages over the doubling, want 6", n)
	}
	gaps(t, h.f.requests())
}

func TestARedialAsksNothingWhileABlockIsInForce(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	loads := h.f.count("LoadMessagesPage")
	h.m.pace.RestoreRateLimit(h.f.clock.Now().Add(time.Hour), 1)

	h.f.clock.Advance(59 * time.Minute)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 0 {
		t.Fatalf("%d messages pages during the block, want 0", n)
	}
	h.f.clock.Advance(60 * time.Minute)
	eventually(t, "the redial after the block", func() bool { return h.f.count("LoadMessagesPage")-loads == 1 })
}

func TestACheckpointFoundByTheRedialStopsEverything(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	loads := h.f.count("LoadMessagesPage")
	h.f.mu.Lock()
	h.f.loadErr = httpclient.ErrCheckpointRequired
	h.f.mu.Unlock()

	h.f.clock.Advance(16 * time.Minute)
	eventually(t, "the checkpoint", func() bool {
		s, k := h.state()
		return s == network.ReconnectNeeded && k == string(wire.Checkpoint)
	})
	h.m.Nudge()
	h.f.clock.Advance(10 * time.Hour)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 1 {
		t.Errorf("%d messages pages after a checkpoint, want 1", n)
	}
}

func TestANudgeBringsTheRedialForward(t *testing.T) {
	h := liveHarness(t)
	stopSocket(t, h)
	loads := h.f.count("LoadMessagesPage")
	h.m.Nudge() // the Mac woke: the redial goes now, once
	eventually(t, "live again", func() bool { s, _ := h.state(); return s == network.Live })
	h.f.clock.Advance(20 * time.Minute)
	h.settle()
	if n := h.f.count("LoadMessagesPage") - loads; n != 1 {
		t.Errorf("a nudged redial loaded %d messages pages, want 1", n)
	}
}

// I2: a launch that finds no connection (Pllum starts before the network
// is up) is notConnected offline, and the next nudge signs in again with
// the session held, once per nudge, with no new sign-in in the window.
func TestANudgeRetriesALaunchThatFoundNoConnection(t *testing.T) {
	h := newHarness(t)
	h.f.mu.Lock()
	h.f.loadErr = fmt.Errorf("%w: invented", httpclient.ErrRequestFailed)
	h.f.mu.Unlock()
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "offline", func() bool {
		s, k := h.state()
		return s == network.NotConnected && k == string(wire.Offline)
	})
	h.f.clock.Advance(10 * time.Hour)
	h.settle()
	if n := h.f.count("LoadMessagesPage"); n != 1 {
		t.Fatalf("%d messages pages with no nudge, want 1: nothing retries by itself", n)
	}

	h.m.Nudge() // still no network: one more try
	eventually(t, "the second try", func() bool { return h.f.count("LoadMessagesPage") == 2 })
	h.settle()
	h.f.mu.Lock()
	h.f.loadErr = nil
	h.f.mu.Unlock()
	h.m.Nudge()
	eventually(t, "live", func() bool { s, _ := h.state(); return s == network.Live })
	h.m.Nudge() // live: nothing
	h.settle()
	if n := h.f.count("LoadMessagesPage"); n != 3 {
		t.Errorf("%d messages pages, want 3 (the launch and two nudges)", n)
	}
}

// I2: an encrypted connect that found no connection is retried by a nudge
// too; a failure that is not passing, or a block, is not.
func TestANudgeRetriesAnEncryptedConnectThatFoundNoConnection(t *testing.T) {
	h := newHarness(t)
	h.f.e2ee.mu.Lock()
	h.f.e2ee.connectErr = &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("invented: no route")}
	h.f.e2ee.mu.Unlock()
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "offline", func() bool {
		s, k := h.state()
		return s == network.NotConnected && k == string(wire.Offline)
	})
	h.m.Nudge()
	eventually(t, "live", func() bool { s, _ := h.state(); return s == network.Live })

	// Not passing: an internal failure waits for a new sign-in.
	h2 := newHarness(t)
	h2.f.mu.Lock()
	h2.f.loadErr = errors.New("invented: not a network failure")
	h2.f.mu.Unlock()
	if err := h2.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the failure", func() bool { s, _ := h2.state(); return s == network.NotConnected })
	h2.settle()
	h2.m.Nudge()
	h2.settle()
	if n := h2.f.count("LoadMessagesPage"); n != 1 {
		t.Errorf("a nudge after an internal failure loaded %d pages, want 1 in all", n)
	}
}
