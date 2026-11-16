package instagram

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/network"
)

func TestAFailedResnapshotIsOfflineAndANudgeRecoversIt(t *testing.T) {
	h := newHarness(t)
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	_ = h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 101, Timestamp: start})
	if h.lastState() != "live" {
		t.Fatalf("not live: %s", h.lastState())
	}

	// Meta asks for a resnapshot (the library has stopped the socket), and
	// reloading the inbox fails for want of a connection.
	h.m.indexErr = fmt.Errorf("%w: invented", httpclient.ErrRequestFailed)
	disconnects := h.m.count("Disconnect")
	_ = h.m.dispatch(&slidetypes.ResnapshotRequired{})
	h.settle()
	if got := h.lastState(); got == "live" || got == "catchingUp" {
		t.Fatalf("a failed resnapshot left the state %s", got)
	}
	if got := h.lastState(); got != "offline" {
		t.Errorf("a failed resnapshot is %s, want offline", got)
	}
	eventually(t, "the old client to disconnect", func() bool { return h.m.count("Disconnect") > disconnects })
	if v := h.g.value("seq"); v != "" {
		t.Errorf("the seq Meta refused is still saved: %s", v)
	}

	// A nudge (the Mac woke, the network came back) reconnects, through
	// the inbox, since the old seq is no good.
	h.m.indexErr = nil
	indexes, connects := h.m.count("LoadIndex"), h.m.count("Connect")
	h.g.Nudge()
	h.settle()
	if h.m.count("LoadIndex") != indexes+1 || h.m.count("Connect") != connects+1 {
		t.Errorf("a nudge did not reconnect: %v", h.m.calls)
	}
	_ = h.m.dispatch(&slidetypes.Connected{})
	_ = h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 102, Timestamp: start})
	if h.lastState() != "live" {
		t.Errorf("after the nudge the state is %s", h.lastState())
	}

	// Under a rate limit: offline with that kind, no request until the
	// block ends, then it reconnects by itself.
	h.m.indexErr = httpclient.ErrRateLimited
	_ = h.m.dispatch(&slidetypes.ResnapshotRequired{})
	h.settle()
	if got := h.lastState(); got != "offline/rateLimited" {
		t.Errorf("a rate-limited resnapshot is %s, want offline/rateLimited", got)
	}
	h.m.indexErr = nil
	indexes = h.m.count("LoadIndex")
	h.g.Nudge()
	h.clock.Advance(14 * time.Minute)
	h.settle()
	if h.m.count("LoadIndex") != indexes {
		t.Errorf("a request went during the rate limit")
	}
	h.clock.Advance(2 * time.Minute)
	h.settle()
	if h.m.count("LoadIndex") != indexes+1 {
		t.Errorf("the end of the block did not reconnect")
	}
}

func TestARotatedSessionStaysBlockedAfterACheckpoint(t *testing.T) {
	dir := t.TempDir() + "/connect"
	h := openHarness(t, dir, nil)
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(true, "older-x", textMsg("mid.$invented-rot", aliceFBI, start.Add(-time.Hour), "x")), nil
	}
	h.login(validSession())

	// Meta rotates sessionid; Pllum is told, and keeps the new set.
	h.m.last().params.Cookies.Set("sessionid", "sess-invented-rotated")
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); err != nil {
		t.Fatal(err)
	}
	if n := len(h.named("session")); n != 1 {
		t.Fatalf("%d session events after the rotation, want 1", n)
	}
	_ = h.m.dispatch(&slidetypes.AuthError{Error: httpclient.ErrCheckpointRequired})
	h.settle()
	h.close()

	rotated := validSession()
	rotated["sessionid"] = "sess-invented-rotated"
	for name, s := range map[string]network.Session{"the rotated set": rotated, "the set it signed in with": validSession()} {
		again := restart(t, dir)
		_ = again.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: s})
		again.settle()
		if len(again.m.calls) != 0 {
			t.Errorf("%s reached Meta after the checkpoint: %v", name, again.m.calls)
		}
		if got := again.lastState(); got != "reconnectNeeded/checkpoint" {
			t.Errorf("%s: state %s", name, got)
		}
		again.close()
	}
}

func TestARateLimitSurvivesARestartAndKeepsDoubling(t *testing.T) {
	dir := t.TempDir() + "/connect"
	h := openHarness(t, dir, nil)
	h.m.indexErr = httpclient.ErrRateLimited
	h.login(validSession())
	if h.lastState() != "notConnected/rateLimited" {
		t.Fatalf("state %s", h.lastState())
	}
	h.close()

	// Restarted five minutes into the 15-minute block: nothing until it ends.
	again := restart(t, dir)
	again.clock.Advance(5 * time.Minute)
	again.m.indexErr = httpclient.ErrRateLimited
	_ = again.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()})
	again.settle()
	if len(again.m.calls) != 0 {
		t.Errorf("a request went during the block, after a restart: %v", again.m.calls)
	}
	if got := again.lastState(); got != "notConnected/rateLimited" {
		t.Errorf("state %s, want notConnected/rateLimited", got)
	}
	again.clock.Advance(9 * time.Minute)
	again.settle()
	if len(again.m.calls) != 0 {
		t.Errorf("a request went before the block ended: %v", again.m.calls)
	}

	// At its end it signs in again, is limited again, and this block is
	// the second: 30 minutes, not 15.
	again.clock.Advance(2 * time.Minute)
	again.settle()
	if n := again.m.count("LoadIndex"); n != 1 {
		t.Fatalf("%d sign-ins at the end of the block, want 1", n)
	}
	again.m.indexErr = nil
	again.clock.Advance(16 * time.Minute)
	again.settle()
	if n := again.m.count("LoadIndex"); n != 1 {
		t.Errorf("the second block was not 30 minutes: %d sign-ins after 16", n)
	}
	again.clock.Advance(15 * time.Minute)
	again.settle()
	if n := again.m.count("LoadIndex"); n != 2 {
		t.Errorf("%d sign-ins after the second block, want 2", n)
	}
}

func TestNudgeRetriesOnlyPassingFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		state   string
		retried bool
	}{
		{"no connection", fmt.Errorf("%w: invented", httpclient.ErrRequestFailed), "notConnected/offline", true},
		{"timed out", context.DeadlineExceeded, "notConnected/timeout", true},
		{"unrecognised", errors.New("invented"), "notConnected/internal", false},
		{"Meta error 1357053", &types.ErrorResponse{ErrorCode: 1357053}, "reconnectNeeded", false},
		{"too many redirects", httpclient.ErrTooManyRedirects, "reconnectNeeded", false},
		{"rate limited", httpclient.ErrRateLimited, "notConnected/rateLimited", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir() + "/connect"
			h := openHarness(t, dir, nil)
			h.m.indexErr = tc.err
			h.login(validSession())
			if got := h.lastState(); got != tc.state {
				t.Errorf("state %s, want %s", got, tc.state)
			}
			h.m.indexErr = nil
			h.g.Nudge()
			h.settle()
			if retried := h.m.count("LoadIndex") == 2; retried != tc.retried {
				t.Errorf("retried by a nudge: %v, want %v", retried, tc.retried)
			}
			if tc.state != "reconnectNeeded" {
				return
			}
			// Terminal, so kept: a restart with the same cookies asks nothing.
			h.close()
			again := restart(t, dir)
			_ = again.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()})
			again.settle()
			if len(again.m.calls) != 0 {
				t.Errorf("a restart reached Meta: %v", again.m.calls)
			}
		})
	}
}
