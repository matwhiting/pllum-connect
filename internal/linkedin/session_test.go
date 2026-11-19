package linkedin

import (
	"errors"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

func TestTheSignInPageNeedsLiAtAndJSESSIONID(t *testing.T) {
	p := SignInPage()
	if p.URL != "https://www.linkedin.com/login" {
		t.Errorf("url = %q", p.URL)
	}
	if p.CookieDomain != "linkedin.com" {
		t.Errorf("cookieDomain = %q", p.CookieDomain)
	}
	if !slices.Equal(p.Required, []string{"li_at", "JSESSIONID"}) {
		t.Errorf("required = %v, want li_at and JSESSIONID", p.Required)
	}
	wantOptional := []string{"bcookie", "bscookie", "lidc", "liap", "lang", "timezone", "__cf_bm", "dfpfpt",
		"fptctx2", "sdui_ver", "aam_uuid", "AMCV_14215E3D5995C57C0A495C55%40AdobeOrg", "AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg"}
	if !slices.Equal(p.Optional, wantOptional) {
		t.Errorf("optional = %v", p.Optional)
	}
	if p.UserAgent != "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.2 Safari/605.1.15" {
		t.Errorf("userAgent = %q", p.UserAgent)
	}
	done := regexp.MustCompile(p.DonePattern)
	for _, u := range []string{"https://www.linkedin.com/feed/", "https://www.linkedin.com/feed", "https://www.linkedin.com/messaging/thread/2-invented/", "https://www.linkedin.com/feed/?trk=invented"} {
		if !done.MatchString(u) {
			t.Errorf("donePattern does not match %s", u)
		}
	}
	for _, u := range []string{"https://www.linkedin.com/login", "https://www.linkedin.com/checkpoint/challenge/invented", "https://www.linkedin.com.example/feed/"} {
		if done.MatchString(u) {
			t.Errorf("donePattern matches %s", u)
		}
	}
	r := newRig(t)
	if !reflect.DeepEqual(r.l.SignIn(), p) {
		t.Errorf("SignIn() is not SignInPage()")
	}
}

func TestASessionWithoutLiAtIsBadRequest(t *testing.T) {
	for _, missing := range []string{"li_at", "JSESSIONID"} {
		r := newRig(t)
		s := validSession()
		delete(s, missing)
		err := r.login(s)
		if wire.KindOf(err) != wire.BadRequest {
			t.Errorf("without %s: Login = %v, want badRequest", missing, err)
		}
		if got := r.ev.states(); !slices.Equal(got, []string{"notConnected/badRequest"}) {
			t.Errorf("without %s: states = %v", missing, got)
		}
		if n := len(r.f.callsNow()); n != 0 {
			t.Errorf("without %s: %d calls reached LinkedIn", missing, n)
		}
	}
}

func TestAnotherAccountOnReconnectIsRefused(t *testing.T) {
	r := newRig(t)
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-0")
	r.f.profileID = otherID
	if err := r.login(validSession()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	r.waitState(network.NotConnected, wire.Refused)
	r.idle()
	if got := r.value("self"); got != selfID {
		t.Errorf("self = %q after another account signed in, want his", got)
	}
	if got := r.value("syncToken"); got != "sync-token-invented-0" {
		t.Errorf("the sync token changed: %q", got)
	}
	if names := r.f.namesSince(0); !slices.Equal(names, []string{"GetCurrentUserProfile"}) {
		t.Errorf("calls = %v, want only the profile read", names)
	}
	if n := len(r.f.clients); n != 1 {
		t.Errorf("%d clients made, want only the one that read the profile", n)
	}
}

func TestThePagesUserAgentIsUsedAndNoSecChHeaders(t *testing.T) {
	r := newRig(t)
	r.live()
	if len(r.f.clients) < 2 {
		t.Fatalf("%d clients, want the profile reader and the signed-in one", len(r.f.clients))
	}
	for i, c := range r.f.clients {
		if c.ua != SignInPage().UserAgent {
			t.Errorf("client %d user agent = %q, want the sign-in page's", i, c.ua)
		}
	}
	h := browserHeaders(SignInPage().UserAgent)
	if len(h) != 1 || h.Get("User-Agent") != SignInPage().UserAgent {
		t.Errorf("browser headers = %v, want the User-Agent alone", h)
	}
	for name := range h {
		if len(name) >= 7 && http.CanonicalHeaderKey(name)[:7] == "Sec-Ch-" {
			t.Errorf("browser headers carry %s", name)
		}
	}
	// The signed-in client is his profile's, and the profile reader no one's.
	if id := r.f.clients[0].self.ID(); id != "" {
		t.Errorf("the profile reader is signed in as %q", id)
	}
	if id := r.f.last().self.ID(); id != selfID {
		t.Errorf("the signed-in client is %q, want his profile", id)
	}
}

func TestTheJarHoldsTheSessionAsTheWindowGaveIt(t *testing.T) {
	jar := jarOf(validSession())
	if got := jarValue(jar, "JSESSIONID"); got != `"ajax:0000000000000000001"` {
		t.Errorf("JSESSIONID reads %q, want it with its quotes", got)
	}
	if got := jar.GetCookie("JSESSIONID"); got != "ajax:0000000000000000001" {
		t.Errorf("the CSRF token is %q, want the value inside the quotes", got)
	}
	if got := jarValue(jar, "li_at"); got != "li-at-invented-1" {
		t.Errorf("li_at reads %q", got)
	}
}

func TestRefreshedCookiesEmitASessionEventAtMostOncePerMinute(t *testing.T) {
	r := newRig(t)
	r.f.rotate = func(jar *linkedingo.StringCookieJar, n int) {
		jar.SetCookies(linkedingo.CookieBaseURL, []*http.Cookie{
			{Name: "lidc", Value: "b=rotated-" + string(rune('a'+n%26)), Domain: ".linkedin.com", Path: "/"},
			{Name: "not_named_invented", Value: "x" + string(rune('a'+n%26)), Domain: ".linkedin.com", Path: "/"},
		})
	}
	r.live()
	sessions := r.ev.named("session")
	if len(sessions) != 1 {
		t.Fatalf("%d session events during sign-in and catch-up (many requests within a minute), want 1", len(sessions))
	}
	s := sessions[0]["session"].(map[string]string)
	if s["li_at"] != "li-at-invented-1" || s["JSESSIONID"] != `"ajax:0000000000000000001"` {
		t.Errorf("the session event lost the required cookies or their quotes")
	}
	if _, ok := s["not_named_invented"]; ok {
		t.Errorf("the session event carries a cookie the page does not name")
	}
	if s["lidc"] == `"b=invented-1"` {
		t.Errorf("the session event carries the old lidc")
	}

	// Within the minute, more requests change the cookie again: nothing.
	c := r.f.last()
	c.dispatch(convDeleteless())
	r.idle()
	if n := len(r.ev.named("session")); n != 1 {
		t.Fatalf("%d session events within a minute, want 1", n)
	}
	// A minute on, the next request's change goes out.
	r.clock.Advance(time.Minute)
	c.dispatch(convDeleteless())
	r.idle()
	if n := len(r.ev.named("session")); n != 2 {
		t.Fatalf("%d session events a minute on, want 2", n)
	}
}

// convDeleteless is a conversations event with no conversation, which asks
// for a catch-up by the sync token: one request.
func convDeleteless() *linkedingo.DecoratedEvent {
	return &linkedingo.DecoratedEvent{Topic: topic(linkedingo.RealtimeEventTopicConversations)}
}

func TestOnlyTheAllowedCallsExist(t *testing.T) {
	want := []string{
		// reads
		"GetCurrentUserProfile", "GetConversationsBySyncToken", "GetConversationsUpdatedBefore",
		"GetConversationsWithNextCursor", "GetMessagesBefore", "GetMessagesWithPrevCursor",
		"DownloadHTTP", "RealtimeConnect", "RealtimeDisconnect", "IsLoggedIn",
		// writes
		"SendMessage", "UploadMedia", "SendReaction", "RemoveReaction",
		"MarkConversationRead", "MarkConversationUnread", "Logout",
	}
	typ := reflect.TypeOf((*liClient)(nil)).Elem()
	var got []string
	for i := range typ.NumMethod() {
		got = append(got, typ.Method(i).Name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("liClient = %v\nwant      %v", got, want)
	}
	if len(got) != 17 {
		t.Fatalf("liClient has %d calls, want 17", len(got))
	}
}

func TestANewLoginWithAKeptBlockIsRefusedWithNoRequest(t *testing.T) {
	r := newRig(t)
	c := r.live()
	c.dispatch(badCredentials{err: errors.New("invented: session rejected")})
	r.waitState(network.ReconnectNeeded, "")
	r.restart()
	n := len(r.f.callsNow())
	if err := r.login(validSession()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	r.waitState(network.ReconnectNeeded, "")
	never(t, "the stopped session reached LinkedIn after a restart", func() bool { return len(r.f.callsNow()) > n })
}

// otherSession is an invented session of another LinkedIn account, freshly
// signed in in the window.
func otherSession() network.Session {
	s := validSession()
	s["li_at"] = "li-at-invented-other"
	s["JSESSIONID"] = `"ajax:0000000000000000099"`
	return s
}

func TestAnotherAccountAfterAnEndedSessionKeepsItsBlock(t *testing.T) {
	r := newRig(t)
	c := r.live()
	c.dispatch(badCredentials{err: errors.New("invented: rejected")})
	r.waitState(network.ReconnectNeeded, "")
	r.idle()
	block := r.value("block")
	if block == "" {
		t.Fatalf("no block kept for the ended session")
	}

	// He signs in in the window, but as another account: refused.
	r.f.profileID = otherID
	if err := r.login(otherSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.NotConnected, wire.Refused)
	r.idle()
	if got := r.value("block"); got != block {
		t.Fatalf("the refusal changed the kept block: %q, want %q", got, block)
	}
	if got := r.value("self"); got != selfID {
		t.Fatalf("self = %q", got)
	}

	// A restart still knows his session ended, and the ended session sent
	// again from the Keychain asks LinkedIn nothing.
	r.f.profileID = selfID
	r.restart()
	if st, _ := r.l.stateNow(); st != network.ReconnectNeeded {
		t.Fatalf("state after a restart = %s, want reconnectNeeded", st)
	}
	n := len(r.f.callsNow())
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.ReconnectNeeded, "")
	never(t, "the ended session reached LinkedIn after the refusal", func() bool { return len(r.f.callsNow()) > n })
}

func TestAnotherAccountLeavesALiveSessionRunning(t *testing.T) {
	r := newRig(t)
	r.live()
	r.f.profileID = otherID
	if err := r.login(otherSession()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the refusal", func() bool { return slices.Contains(r.ev.states(), "notConnected/refused") })
	r.waitState(network.Live, "")
	r.idle()
	if n := r.f.count("RealtimeDisconnect"); n != 0 {
		t.Errorf("his live session was stopped for another account's sign-in (%d disconnects)", n)
	}
	// His run still works: a catch-up it is asked for still goes.
	n := r.f.count("GetConversationsBySyncToken")
	r.f.clients[1].dispatch(convDeleteless())
	r.idle()
	if r.f.count("GetConversationsBySyncToken") == n {
		t.Errorf("his run no longer answers its stream")
	}
}

// Pllum clears the row's state as it sends the saved session at a start, so
// the login's outcome is always said, even when it is the state already in
// force: after a restart with a kept block, the row reads Reconnect needed,
// not Catching up.
func TestALoginAlwaysSaysItsOutcome(t *testing.T) {
	r := newRig(t)
	c := r.live()
	c.dispatch(badCredentials{err: errors.New("invented: session rejected")})
	r.waitState(network.ReconnectNeeded, "")
	r.idle()
	r.restart()
	if got := r.ev.states(); !slices.Equal(got, []string{"reconnectNeeded"}) {
		t.Fatalf("states at the start = %v, want reconnectNeeded", got)
	}
	if err := r.login(validSession()); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if got := r.ev.states(); !slices.Equal(got, []string{"reconnectNeeded", "reconnectNeeded"}) {
		t.Errorf("states after the saved session's login = %v, want reconnectNeeded said again", got)
	}

	// The same for a login during a pause, and a badRequest twice.
	r2, _ := pauseAfterLive(t, 429)
	k := len(r2.ev.states())
	if err := r2.login(validSession()); err != nil {
		t.Fatal(err)
	}
	if err := r2.login(validSession()); err != nil {
		t.Fatal(err)
	}
	if got := r2.ev.states()[k:]; !slices.Equal(got, []string{"offline/rateLimited", "offline/rateLimited"}) {
		t.Errorf("states for two logins during a pause = %v, want offline/rateLimited said each time", got)
	}
	r3 := newRig(t)
	bad := network.Session{"li_at": "li-at-invented-1"}
	_ = r3.login(bad)
	_ = r3.login(bad)
	if got := r3.ev.states(); !slices.Equal(got, []string{"notConnected/badRequest", "notConnected/badRequest"}) {
		t.Errorf("states for two bad sessions = %v, want notConnected/badRequest said each time", got)
	}
}
