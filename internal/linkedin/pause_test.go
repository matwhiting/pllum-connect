package linkedin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

func TestPausesDoubleToSixHours(t *testing.T) {
	clock := newFakeClock()
	p := &pause{now: clock.Now}
	want := []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour, 6 * time.Hour, 6 * time.Hour}
	for i, d := range want {
		kind, paused := p.check(statusErr(429))
		if kind != wire.RateLimited || !paused {
			t.Fatalf("429 #%d: %s, %v", i, kind, paused)
		}
		until, blocked := p.blocked()
		if !blocked || until.Sub(clock.Now()) != d {
			t.Fatalf("pause #%d lasts %v, want %v", i, until.Sub(clock.Now()), d)
		}
		// A 429 for a request sent before the pause began does not lengthen it.
		p.check(statusErr(429))
		if again, _ := p.blocked(); !again.Equal(until) {
			t.Fatalf("a second 429 during pause #%d moved its end", i)
		}
		clock.Advance(d)
		if _, blocked := p.blocked(); blocked {
			t.Fatalf("pause #%d still holds at its end", i)
		}
	}
	// A day with none starts the count over.
	clock.Advance(24 * time.Hour)
	p.check(statusErr(403))
	if until, _ := p.blocked(); until.Sub(clock.Now()) != 15*time.Minute {
		t.Fatalf("after a day with none the pause lasts %v, want 15 minutes", until.Sub(clock.Now()))
	}
}

func TestTheRuleReadsEachAnswer(t *testing.T) {
	clock := newFakeClock()
	for _, c := range []struct {
		err    error
		kind   wire.Kind
		paused bool
	}{
		{statusErr(403), wire.RateLimited, true},
		{statusErr(429), wire.RateLimited, true},
		{statusErr(999), wire.RateLimited, true},
		{statusErr(401), reconnectNeeded, false},
		{statusErr(302), reconnectNeeded, false},
		{linkedingo.ErrTokenInvalidated, reconnectNeeded, false},
		{statusErr(503), wire.Offline, false},
		{statusErr(404), wire.Internal, false},
		{context.DeadlineExceeded, wire.Timeout, false},
		{nil, "", false},
	} {
		p := &pause{now: clock.Now}
		kind, paused := p.check(c.err)
		if kind != c.kind || paused != c.paused {
			t.Errorf("%v: %s, %v; want %s, %v", c.err, kind, paused, c.kind, c.paused)
		}
		if _, blocked := p.blocked(); blocked != c.paused {
			t.Errorf("%v: blocked %v, want %v", c.err, blocked, c.paused)
		}
	}
}

// pauseAfterLive signs in, then makes the next request answer code: a
// conversations event with nothing in it asks for a catch-up by the sync
// token.
func pauseAfterLive(t *testing.T, code int) (*rig, *fakeClient) {
	t.Helper()
	r := newRig(t)
	c := r.live()
	r.f.setErr("GetConversationsBySyncToken", statusErr(code))
	c.dispatch(convDeleteless())
	r.waitState(network.Offline, wire.RateLimited)
	r.idle()
	return r, c
}

// checkNothingDuringThePause: from now until the pause ends (15 minutes after
// it began, at began) no call of any kind reaches LinkedIn, whatever happens
// meanwhile, and a call goes once it has ended.
func checkNothingDuringThePause(t *testing.T, r *rig, c *fakeClient, began time.Time) {
	t.Helper()
	n := len(r.f.callsNow())
	r.l.Nudge()
	c.dispatch(heartbeat{})
	c.dispatch(convDeleteless())
	c.dispatch(&linkedingo.ClientConnection{})
	_ = r.login(validSession())
	if kind := r.l.mediaKind(context.Background(), c, "https://media.invented.example/one"); kind != wire.RateLimited {
		t.Errorf("a media fetch during the pause is %s, want rateLimited", kind)
	}
	r.clock.Advance(began.Add(15*time.Minute - time.Second).Sub(r.clock.Now()))
	r.l.Nudge()
	r.idle()
	if names := r.f.namesSince(n); len(names) != 0 {
		t.Fatalf("calls during the pause: %v", names)
	}
	if st, kind := r.l.stateNow(); st != network.Offline || kind != wire.RateLimited {
		t.Fatalf("state during the pause = %s/%s", st, kind)
	}
	r.f.setErr("GetConversationsBySyncToken", nil)
	r.f.setErr("GetCurrentUserProfile", nil)
	r.clock.Advance(time.Second)
	r.waitState(network.Live, "")
	if len(r.f.callsNow()) == n {
		t.Fatalf("nothing was asked once the pause ended")
	}
}

func TestA429PausesEveryRequestForFifteenMinutes(t *testing.T) {
	r, c := pauseAfterLive(t, 429)
	began := r.clock.Now()
	// The live stream is stopped with the pause.
	if r.f.count("RealtimeDisconnect") == 0 {
		t.Fatalf("the realtime stream was not stopped for the pause")
	}
	until, _ := r.l.pause.blocked()
	if d := until.Sub(began); d > 15*time.Minute || d < 15*time.Minute-time.Minute {
		t.Fatalf("the pause lasts %v, want 15 minutes", d)
	}
	checkNothingDuringThePause(t, r, c, until.Add(-15*time.Minute))
}

func TestA999IsAPauseToo(t *testing.T) {
	r := newRig(t)
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-0")
	r.keep("listed", "1")
	r.keep("listedFolders", "2")
	r.f.setErr("GetCurrentUserProfile", statusErr(999))
	if err := r.login(validSession()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	r.waitState(network.Offline, wire.RateLimited)
	r.idle()
	until, blocked := r.l.pause.blocked()
	if !blocked {
		t.Fatalf("a 999 did not pause")
	}
	c := r.f.last()
	checkNothingDuringThePause(t, r, c, until.Add(-15*time.Minute))
}

func TestAPauseOutlastsARestart(t *testing.T) {
	r, _ := pauseAfterLive(t, 403)
	r.restart()
	n := len(r.f.callsNow())
	if err := r.login(validSession()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	r.waitState(network.Offline, wire.RateLimited)
	never(t, "a request went during a pause kept from before a restart", func() bool { return len(r.f.callsNow()) > n })
}

func TestAMediaForbiddenIsNotAPause(t *testing.T) {
	for _, code := range []int{403, 404, 429, 999} {
		r := newRig(t)
		c := r.live()
		r.f.setErr("DownloadHTTP", statusErr(code))
		kind := r.l.mediaKind(context.Background(), c, "https://media.invented.example/gone")
		want := wire.MediaGone
		if code == 429 || code == 999 {
			want = wire.RateLimited
		}
		if kind != want {
			t.Errorf("media %d: %s, want %s", code, kind, want)
		}
		if _, blocked := r.l.pause.blocked(); blocked {
			t.Errorf("media %d paused LinkedIn", code)
		}
		if st, k := r.l.stateNow(); st != network.Live || k != "" {
			t.Errorf("media %d: state %s/%s, want live", code, st, k)
		}
		n := len(r.f.callsNow())
		c.dispatch(convDeleteless())
		r.idle()
		if names := r.f.namesSince(n); !slices.Contains(names, "GetConversationsBySyncToken") {
			t.Errorf("media %d: the next request did not go: %v", code, names)
		}
	}
}

func TestBadCredentialsIsReconnectNeededAndNothingRetries(t *testing.T) {
	for name, stop := range map[string]func(r *rig, c *fakeClient){
		"BadCredentials": func(r *rig, c *fakeClient) {
			c.dispatch(badCredentials{err: errors.New("invented: rejected")})
		},
		"401": func(r *rig, c *fakeClient) {
			r.f.setErr("GetConversationsBySyncToken", statusErr(401))
			c.dispatch(convDeleteless())
		},
		"302": func(r *rig, c *fakeClient) {
			r.f.setErr("GetConversationsBySyncToken", statusErr(302))
			c.dispatch(convDeleteless())
		},
		"li_at deleted": func(r *rig, c *fakeClient) {
			r.f.setErr("GetConversationsBySyncToken", linkedingo.ErrTokenInvalidated)
			c.dispatch(convDeleteless())
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			c := r.live()
			stop(r, c)
			r.waitState(network.ReconnectNeeded, "")
			r.idle()
			if r.f.count("RealtimeDisconnect") == 0 {
				t.Fatalf("the client was not stopped")
			}
			n := len(r.f.callsNow())
			r.l.Nudge()
			c.dispatch(heartbeat{})
			c.dispatch(&linkedingo.ClientConnection{})
			c.dispatch(convDeleteless())
			r.clock.Advance(48 * time.Hour)
			_ = r.login(validSession()) // the same session again, from the Keychain
			r.l.Nudge()
			r.idle()
			if names := r.f.namesSince(n); len(names) != 0 {
				t.Fatalf("calls after the session ended: %v", names)
			}
			if st, _ := r.l.stateNow(); st != network.ReconnectNeeded {
				t.Fatalf("state = %s, want reconnectNeeded", st)
			}
			// A new sign-in in the window is a new session: it goes.
			r.f.setErr("GetConversationsBySyncToken", nil)
			fresh := validSession()
			fresh["li_at"] = "li-at-invented-2"
			if err := r.login(fresh); err != nil {
				t.Fatal(err)
			}
			r.waitState(network.Live, "")
		})
	}
}

func TestADeletedLiAtCookieIsReconnectNeeded(t *testing.T) {
	r := newRig(t)
	c := r.live()
	r.f.rotate = func(jar *linkedingo.StringCookieJar, n int) {
		removeCookie(jar, "li_at")
	}
	c.dispatch(convDeleteless())
	r.waitState(network.ReconnectNeeded, "")
}

func TestATransientDisconnectIsOfflineUntilItConnects(t *testing.T) {
	r := newRig(t)
	c := r.live()
	c.dispatch(transient{err: errors.New("invented: stream ended")})
	r.waitState(network.Offline, "")
	c.dispatch(&linkedingo.ClientConnection{})
	r.waitState(network.Live, "")
	c.dispatch(transient{err: errors.New("invented: stream ended")})
	r.waitState(network.Offline, "")
	c.dispatch(heartbeat{})
	r.waitState(network.Live, "")
	// A realtime connect LinkedIn pushed back on is a pause.
	c.dispatch(transient{err: statusErr(429)})
	r.waitState(network.Offline, wire.RateLimited)
	if got := r.ev.states(); !slices.Contains(got, "offline/rateLimited") {
		t.Fatalf("states = %v", got)
	}
}

// The library's stream drops with a heartbeat's answer wrapped as it wraps
// it (third_party PLLUM-CHANGES.md, change 3): pushback pauses, a redirect
// ends the session.
func TestAHeartbeatPushedBackPausesOrEnds(t *testing.T) {
	for status, want := range map[int]network.State{403: network.Offline, 429: network.Offline, 999: network.Offline, 302: network.ReconnectNeeded} {
		r := newRig(t)
		c := r.live()
		c.dispatch(transient{err: fmt.Errorf("heartbeat: %w", statusErr(status))})
		if want == network.Offline {
			r.waitState(network.Offline, wire.RateLimited)
			if _, blocked := r.l.pause.blocked(); !blocked {
				t.Errorf("a heartbeat answered %d did not pause", status)
			}
		} else {
			r.waitState(network.ReconnectNeeded, "")
		}
		eventually(t, fmt.Sprintf("the stream stopped after a heartbeat answered %d", status), func() bool {
			return r.f.count("RealtimeDisconnect") > 0
		})
	}
}
