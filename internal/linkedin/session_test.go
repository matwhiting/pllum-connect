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
