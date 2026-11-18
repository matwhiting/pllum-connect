package messenger

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Pllum registers one Messenger device, ever: a second sign-in in the same
// helper, and a new helper over the same folder, use the device saved the
// first time.
func TestTheDeviceIsRegisteredOnce(t *testing.T) {
	f := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)

	h.signIn(t)
	if c := h.m.GateCounts().(GateCounts); c.RegisterMs < 0 || !c.Registered {
		t.Fatalf("the first sign-in did not register (%+v)", c)
	}
	h.signIn(t) // he signs in again, same helper
	if c := h.m.GateCounts().(GateCounts); c.RegisterMs != 0 {
		t.Fatalf("the second sign-in timed a registration (%d ms)", c.RegisterMs)
	}
	h.close()

	later := newHarnessOver(t, dir, f) // a new helper over the same folder
	later.signIn(t)
	if c := later.m.GateCounts().(GateCounts); !c.Registered || c.RegisterMs != 0 {
		t.Fatalf("the new helper's counts are %+v, want registered before", c)
	}
	f.mu.Lock()
	n := f.registers
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("Meta was asked to register %d devices, want 1", n)
	}
}

// A registration whose answer was lost may have made a device: it is
// reconnectNeeded with kind registrationUnclear (he checks Facebook's
// devices before signing in again), and a new sign-in in the same helper
// registers the same keys and Facebook device ID, so Meta sees the same
// device, not a second one.
func TestAFailedRegistrationReusesItsKeys(t *testing.T) {
	h := newHarness(t)
	h.f.registerErrs = []error{errors.New("invented: the answer was lost")}
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the failed registration", func() bool {
		s, k := h.state()
		return s == network.ReconnectNeeded && k == string(wire.RegistrationUnclear)
	})
	if c := h.m.GateCounts().(GateCounts); c.Registered {
		t.Fatalf("a failed registration counts as registered")
	}

	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: newSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the encrypted connection", func() bool {
		c := h.m.GateCounts().(GateCounts)
		return c.Registered && c.E2EEConnected
	})
	h.f.mu.Lock()
	keys, ids, n := h.f.regKeys, h.f.regUUIDs, h.f.registers
	h.f.mu.Unlock()
	if n != 2 {
		t.Fatalf("Meta was asked %d times, want 2 (the lost one and the retry)", n)
	}
	if keys[0] != keys[1] || ids[0] != ids[1] {
		t.Fatalf("the retry registered new keys or a new device ID")
	}
}

// newSession is an invented session of a new sign-in: other cookies.
func newSession() network.Session {
	return network.Session{"xs": "xs-invented-new", "c_user": "2000001", "datr": "datr-invented-new"}
}

// B4 review I2: a registration whose answer was lost is kept as a block. A
// restart, and the same session sent again (Pllum sends the saved session
// at every start), ask Meta nothing, so no second device is registered on
// their own. A new sign-in (another session) registers exactly once, and
// that clears it.
func TestAnUnclearRegistrationIsKeptUntilANewSignIn(t *testing.T) {
	f := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)
	f.registerErrs = []error{errors.New("invented: the answer was lost")}
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the failed registration", func() bool {
		s, k := h.state()
		return s == network.ReconnectNeeded && k == string(wire.RegistrationUnclear)
	})
	if k, blocked := h.m.pace.Blocked(); !blocked || k != meta.ReconnectNeeded {
		t.Errorf("the pace is %s, %v; want blocked", k, blocked)
	}
	h.close()

	from := len(f.requests())
	h = newHarnessOver(t, dir, f)
	if s, k := h.state(); s != network.ReconnectNeeded || k != string(wire.RegistrationUnclear) {
		t.Errorf("after a restart the state is %s/%s, want reconnectNeeded/registrationUnclear", s, k)
	}
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	h.settle()
	if n := len(f.requests()) - from; n != 0 {
		t.Errorf("the same session after a restart made %d requests, want none", n)
	}
	f.mu.Lock()
	n := f.registers
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("Meta was asked to register %d times, want 1", n)
	}
	if s, k := h.state(); s != network.ReconnectNeeded || k != string(wire.RegistrationUnclear) {
		t.Errorf("after the same session the state is %s/%s", s, k)
	}

	// He signs in again in the window: one registration, and it clears.
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: newSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the encrypted connection", func() bool {
		c := h.m.GateCounts().(GateCounts)
		return c.Registered && c.E2EEConnected
	})
	h.close()
	h = newHarnessOver(t, dir, f)
	if s, _ := h.state(); s == network.ReconnectNeeded {
		t.Errorf("after a registration that went, a restart is still %s", s)
	}
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: newSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the encrypted connection", func() bool {
		c := h.m.GateCounts().(GateCounts)
		return c.Registered && c.E2EEConnected
	})
	f.mu.Lock()
	n = f.registers
	f.mu.Unlock()
	if n != 2 {
		t.Errorf("Meta was asked to register %d times, want 2 (the lost one, then the new sign-in's)", n)
	}
}

// The encrypted side waits for the main socket, as mautrix-meta's connector
// does: nothing is registered before the main socket says it connected, a
// reconnect does not set it up twice, and a socket Meta refuses as
// unauthorized stops everything until he signs in again.
func TestTheEncryptedSocketWaitsForTheMainSocket(t *testing.T) {
	h := newHarness(t)
	h.f.holdConnect = true
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the main socket to be asked", func() bool {
		h.f.mu.Lock()
		defer h.f.mu.Unlock()
		return h.f.connects == 1
	})
	h.f.mu.Lock()
	early := h.f.registers
	h.f.mu.Unlock()
	if early != 0 || h.f.e2ee.connects != 0 {
		t.Fatalf("the encrypted side was set up before the main socket connected")
	}

	cli := h.f.last()
	cli.dispatch(&messagix.ConnectedEvent{})
	eventually(t, "the encrypted connection and the history check", func() bool {
		c := h.m.GateCounts().(GateCounts)
		return c.Registered && c.E2EEConnected && c.HistoryState != ""
	})
	if s, _ := h.state(); s != network.Live {
		t.Fatalf("state with both sockets up = %s, want live", s)
	}
	cli.dispatch(&messagix.ReconnectedEvent{})
	cli.dispatch(&messagix.ConnectedEvent{})
	h.f.e2ee.mu.Lock()
	connects := h.f.e2ee.connects
	h.f.e2ee.mu.Unlock()
	if connects != 1 {
		t.Fatalf("the encrypted socket was connected %d times, want once per run", connects)
	}

	cli.dispatch(&messagix.PermanentErrorEvent{Err: dgw.ErrUnauthorized})
	eventually(t, "reconnect needed", func() bool {
		s, _ := h.state()
		return s == network.ReconnectNeeded
	})
	if k, blocked := h.m.pace.Blocked(); !blocked || k != meta.ReconnectNeeded {
		t.Fatalf("the pace is not blocked after Meta refused the socket (%s, %v)", k, blocked)
	}
}
