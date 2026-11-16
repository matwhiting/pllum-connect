package instagram

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// stopOn signs in on a fresh store, has Meta stop the session with err (on
// the socket, as an AuthError, or at LoadIndex), and closes. It is the
// store's folder.
func stopOn(t *testing.T, session network.Session, err error, atIndex bool) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "connect")
	h := openHarness(t, dir, nil)
	if atIndex {
		h.m.indexErr = err
		h.login(session)
	} else {
		h.login(session)
		_ = h.m.dispatch(&slidetypes.AuthError{Error: err})
	}
	h.settle()
	h.close()
	return dir
}

// restart is a new Instagram on the store in dir, over a Meta that has seen
// nothing.
func restart(t *testing.T, dir string) *harness {
	t.Helper()
	return openHarness(t, dir, nil)
}

func TestACheckpointSurvivesARestartAndTheSameSession(t *testing.T) {
	dir := stopOn(t, validSession(), httpclient.ErrCheckpointRequired, false)

	h := restart(t, dir)
	if got := h.lastState(); got != "reconnectNeeded/checkpoint" {
		t.Errorf("at Start the state is %q, want reconnectNeeded/checkpoint", got)
	}
	if err := h.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Errorf("Login with the stopped session = %v", err)
	}
	h.settle()
	if len(h.m.clients) != 0 || len(h.m.calls) != 0 {
		t.Errorf("the stopped session reached Meta: %d clients, calls %v", len(h.m.clients), h.m.calls)
	}
	if got := h.lastState(); got != "reconnectNeeded/checkpoint" {
		t.Errorf("after the login the state is %q, want reconnectNeeded/checkpoint", got)
	}
	if k, blocked := h.g.pace.Blocked(); !blocked || k != wire.Checkpoint {
		t.Errorf("the pace is not blocked by the checkpoint: %q, %v", k, blocked)
	}
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); kindErr(err) != wire.Checkpoint {
		t.Errorf("FetchOlder = %v, want checkpoint", err)
	}
	h.g.Nudge()
	h.settle()
	if len(h.m.calls) != 0 {
		t.Errorf("a nudge reached Meta: %v", h.m.calls)
	}
	// Only the session's own cookies count: a rotated rur is the same
	// session.
	s := validSession()
	s["rur"] = "rur-invented-9"
	_ = h.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: s})
	h.settle()
	if len(h.m.calls) != 0 {
		t.Errorf("the same session with another rur reached Meta: %v", h.m.calls)
	}
}

func TestANewSessionClearsAPersistedBlock(t *testing.T) {
	dir := stopOn(t, validSession(), httpclient.ErrCheckpointRequired, false)

	h := restart(t, dir)
	fresh := validSession()
	fresh["sessionid"] = "sess-invented-2" // he signed in again in the window
	h.login(fresh)
	if h.m.count("LoadIndex") != 1 || h.m.count("Connect") != 1 {
		t.Errorf("a new session did not sign in: %v", h.m.calls)
	}
	if _, blocked := h.g.pace.Blocked(); blocked {
		t.Errorf("the pace is still blocked after a new session")
	}
	if v, ok, _ := h.st.NetworkValue(Name, "block"); ok {
		t.Errorf("the block is still kept: %q", v)
	}
	h.close()

	// And it stays cleared across a restart.
	again := restart(t, dir)
	again.login(validSession())
	if again.m.count("LoadIndex") != 1 {
		t.Errorf("after the block was cleared, the old session was refused: %v", again.m.calls)
	}
}

func TestBannedAndEndedSessionsPersistToo(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		atIndex bool
		state   string
	}{
		{"suspended at sign-in", httpclient.RedirectedError{Type: httpclient.ErrAccountSuspended, URL: "https://invalid.invalid/"}, true, "banned"},
		{"session ended on the socket", httpclient.ErrTokenInvalidated, false, "reconnectNeeded"},
		{"session ended at sign-in", httpclient.ErrTokenInvalidated, true, "reconnectNeeded"},
		{"consent at sign-in", httpclient.ErrConsentRequired, true, "reconnectNeeded/checkpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := stopOn(t, validSession(), tc.err, tc.atIndex)
			h := restart(t, dir)
			_ = h.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()})
			h.settle()
			if len(h.m.calls) != 0 {
				t.Errorf("the stopped session reached Meta: %v", h.m.calls)
			}
			if got := h.lastState(); got != tc.state {
				t.Errorf("state %q, want %q", got, tc.state)
			}
		})
	}

	// A rate limit is not terminal: it outlives a restart only until its
	// block ends, and then the same session signs in.
	dir := stopOn(t, validSession(), httpclient.ErrRateLimited, true)
	h := restart(t, dir)
	h.clock.Advance(16 * mins)
	h.login(validSession())
	if h.m.count("LoadIndex") != 1 {
		t.Errorf("after its block, a rate limit still stopped the session: %v", h.m.calls)
	}
}

func TestTheBlockStoresNoCookieValue(t *testing.T) {
	s := validSession()
	s["sessionid"] = canaryCookie
	s["csrftoken"] = canaryCookie + "-csrf"
	dir := stopOn(t, s, httpclient.ErrCheckpointRequired, false)
	h := restart(t, dir)
	v, ok, err := h.st.NetworkValue(Name, "block")
	if err != nil || !ok {
		t.Fatalf("no block kept: %v", err)
	}
	if strings.Contains(v, canaryCookie) || strings.Contains(v, "csrf") || strings.Contains(v, "4000000099") {
		t.Errorf("the block holds a cookie value: %q", v)
	}
	if !strings.Contains(v, "checkpoint") {
		t.Errorf("the block does not name its kind: %q", v)
	}
}

const mins = time.Minute
