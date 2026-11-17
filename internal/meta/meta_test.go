package meta

import (
	"context"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every cookie value here is invented.

func asStrings(list []cookies.MetaCookieName) []string {
	out := []string{}
	for _, n := range list {
		out = append(out, string(n))
	}
	return out
}

func TestSignInPagesMatchTheLibrarysCookieLists(t *testing.T) {
	if !slices.Equal(InstagramPage.Required, asStrings(cookies.IGRequiredCookies)) {
		t.Errorf("Instagram required = %v, want the library's %v", InstagramPage.Required, cookies.IGRequiredCookies)
	}
	if !slices.Equal(InstagramPage.Optional, asStrings(cookies.IGOptionalCookies)) {
		t.Errorf("Instagram optional = %v, want the library's %v", InstagramPage.Optional, cookies.IGOptionalCookies)
	}
	if !slices.Equal(FacebookPage.Required, asStrings(cookies.FBRequiredCookies)) {
		t.Errorf("Facebook required = %v, want the library's %v", FacebookPage.Required, cookies.FBRequiredCookies)
	}
	if FacebookPage.Optional == nil || len(FacebookPage.Optional) != 0 {
		t.Errorf("Facebook optional = %#v, want an empty list", FacebookPage.Optional)
	}
	for name, p := range map[string]SignInPage{"Instagram": InstagramPage, "Facebook": FacebookPage} {
		if p.UserAgent != useragent.UserAgent {
			t.Errorf("%s user agent = %q, want the library's", name, p.UserAgent)
		}
		if _, err := regexp.Compile(p.DonePattern); err != nil {
			t.Errorf("%s done pattern does not compile: %v", name, err)
		}
	}
	if InstagramPage.URL != "https://www.instagram.com/accounts/login/" || InstagramPage.CookieDomain != "instagram.com" {
		t.Errorf("Instagram page = %+v", InstagramPage)
	}
	if FacebookPage.URL != "https://www.facebook.com/" || FacebookPage.CookieDomain != "facebook.com" {
		t.Errorf("Facebook page = %+v", FacebookPage)
	}
}

func TestDonePatternsMatchOnlySignedInPages(t *testing.T) {
	cases := []struct {
		page SignInPage
		urls map[string]bool
	}{
		{InstagramPage, map[string]bool{
			"https://www.instagram.com/":                    true,
			"https://www.instagram.com/?next=%2F":           true,
			"https://www.instagram.com/direct/inbox/":       true,
			"https://www.instagram.com/direct/t/123/":       true,
			"https://www.instagram.com/accounts/login/":     false,
			"https://www.instagram.com/challenge/abc/def/":  false,
			"https://www.instagram.com/accounts/suspended/": false,
			"https://www.instagram.com.example.net/":        false,
			"http://www.instagram.com/":                     false,
		}},
		{FacebookPage, map[string]bool{
			"https://www.facebook.com/":                         true,
			"https://www.facebook.com/messages/t/1/":            true,
			"https://www.facebook.com/messages/e2ee/t/1":        true,
			"https://www.facebook.com/login/":                   false,
			"https://www.facebook.com/checkpoint/1/":            false,
			"https://www.facebook.com/two_step_verification/x/": false,
			"https://www.facebook.com.example.net/":             false,
		}},
	}
	for _, c := range cases {
		re := regexp.MustCompile(c.page.DonePattern)
		for url, want := range c.urls {
			if got := re.MatchString(url); got != want {
				t.Errorf("%s: %q matches = %v, want %v", c.page.CookieDomain, url, got, want)
			}
		}
	}
}

func jar(values map[string]string) *cookies.Cookies {
	m := map[cookies.MetaCookieName]string{}
	for k, v := range values {
		m[cookies.MetaCookieName(k)] = v
	}
	c := &cookies.Cookies{}
	c.UpdateValues(m)
	return c
}

func igJar(rur string) *cookies.Cookies {
	return jar(map[string]string{
		"sessionid": "sess-a", "csrftoken": "csrf-a", "ds_user_id": "1001", "rur": rur,
		"dpr": "2", "wd": "1x1", // not part of the session
	})
}

func TestTheSessionEventCarriesOnlyChangedSessionsAtMostOncePerMinute(t *testing.T) {
	d := NewSessionDiff(InstagramPage)
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// The session Pllum signed in with is not news.
	d.Seed(map[string]string{"sessionid": "sess-a", "csrftoken": "csrf-a", "ds_user_id": "1001", "rur": "rur-1"})
	if got, ok := d.Changed(igJar("rur-1"), t0); ok {
		t.Fatalf("an unchanged session was reported: %v", got)
	}

	// rur rotates: one event, with the full named set and nothing else.
	got, ok := d.Changed(igJar("rur-2"), t0.Add(5*time.Second))
	if !ok {
		t.Fatalf("a changed rur was not reported")
	}
	want := map[string]string{"sessionid": "sess-a", "csrftoken": "csrf-a", "ds_user_id": "1001", "rur": "rur-2"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	for _, outside := range []string{"dpr", "wd"} {
		if _, in := got[outside]; in {
			t.Errorf("cookie %q is not part of the session but was sent", outside)
		}
	}
	got["rur"] = "tampered"

	// The same set again: nothing.
	if _, ok := d.Changed(igJar("rur-2"), t0.Add(10*time.Minute)); ok {
		t.Errorf("a set already sent was sent again")
	}

	// A second change inside the minute waits, then goes once the minute is up.
	if _, ok := d.Changed(igJar("rur-3"), t0.Add(30*time.Second)); ok {
		t.Errorf("a second change within a minute was reported")
	}
	if _, ok := d.Changed(igJar("rur-3"), t0.Add(64*time.Second)); ok {
		t.Errorf("reported before a minute had passed since the last event (at 5 s)")
	}
	got, ok = d.Changed(igJar("rur-3"), t0.Add(66*time.Second))
	if !ok || got["rur"] != "rur-3" {
		t.Errorf("the change was not reported once the minute passed: %v, %v", got, ok)
	}
}

func TestASessionMissingARequiredCookieIsNeverReported(t *testing.T) {
	d := NewSessionDiff(InstagramPage)
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	gone := jar(map[string]string{"csrftoken": "csrf-a", "ds_user_id": "1001", "rur": "rur-1"})
	if got, ok := d.Changed(gone, t0); ok {
		t.Errorf("a session with no sessionid was reported: %v", got)
	}
	if _, ok := d.Changed(nil, t0); ok {
		t.Errorf("no cookies were reported")
	}
}

// testClock is a clock that only moves when something sleeps.
type testClock struct {
	now    time.Time
	slept  []time.Duration
	random float64
}

func (c *testClock) Now() time.Time { return c.now }
func (c *testClock) Sleep(_ context.Context, d time.Duration) error {
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
	return nil
}
func (c *testClock) pace() *Pace {
	return &Pace{Now: c.Now, Sleep: c.Sleep, Rand: func() float64 { return c.random }}
}

func newClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func TestRateLimitedBacksOffAndDoubles(t *testing.T) {
	c := newClock()
	p := c.pace()
	if k, blocked := p.Blocked(); blocked {
		t.Fatalf("a new Pace is blocked by %s", k)
	}

	for _, wait := range []time.Duration{15 * time.Minute, 30 * time.Minute, 60 * time.Minute, 120 * time.Minute, 240 * time.Minute, 240 * time.Minute} {
		p.Fail(wire.RateLimited)
		if k, blocked := p.Blocked(); !blocked || k != wire.RateLimited {
			t.Fatalf("after a rateLimited Blocked = %q, %v", k, blocked)
		}
		c.now = c.now.Add(wait - time.Second)
		if _, blocked := p.Blocked(); !blocked {
			t.Fatalf("a %s block ended early", wait)
		}
		c.now = c.now.Add(2 * time.Second)
		if _, blocked := p.Blocked(); blocked {
			t.Fatalf("a %s block did not end", wait)
		}
	}

	// A success starts over at 15 minutes.
	p.Succeed()
	p.Fail(wire.RateLimited)
	c.now = c.now.Add(16 * time.Minute)
	if _, blocked := p.Blocked(); blocked {
		t.Errorf("after a success the first block was longer than 15 minutes")
	}
}

func TestBackoffDoublesFromFifteenMinutesToFourHours(t *testing.T) {
	want := []time.Duration{15 * time.Minute, 15 * time.Minute, 30 * time.Minute, 60 * time.Minute, 120 * time.Minute, 240 * time.Minute, 240 * time.Minute}
	for i, step := range []int{-1, 0, 1, 2, 3, 4, 50} {
		if got := Backoff(step); got != want[i] {
			t.Errorf("Backoff(%d) = %v, want %v", step, got, want[i])
		}
	}
}

func TestARateLimitedDuringABlockDoesNotLengthenIt(t *testing.T) {
	c := newClock()
	p := c.pace()
	p.Fail(wire.RateLimited)
	p.Fail(wire.RateLimited) // a request sent before the block, failing after it
	c.now = c.now.Add(16 * time.Minute)
	if _, blocked := p.Blocked(); blocked {
		t.Errorf("two failures in one block made it 30 minutes")
	}
	p.Fail(wire.RateLimited)
	c.now = c.now.Add(16 * time.Minute)
	if _, blocked := p.Blocked(); !blocked {
		t.Errorf("the next block, after the first ended, was not 30 minutes")
	}
}

func TestACheckpointBlocksUntilClear(t *testing.T) {
	for _, kind := range []wire.Kind{wire.Checkpoint, ReconnectNeeded, Banned} {
		c := newClock()
		p := c.pace()
		p.Fail(kind)
		c.now = c.now.Add(1000 * time.Hour)
		if k, blocked := p.Blocked(); !blocked || k != kind {
			t.Fatalf("%s: Blocked = %q, %v after 1000 hours", kind, k, blocked)
		}
		p.Succeed()
		if _, blocked := p.Blocked(); !blocked {
			t.Errorf("%s: a success lifted it", kind)
		}
		p.Clear()
		if _, blocked := p.Blocked(); blocked {
			t.Errorf("%s: Clear did not lift it", kind)
		}
	}

	// It outranks a rate limit, and a rate limit is not Clear's to lift.
	c := newClock()
	p := c.pace()
	p.Fail(wire.RateLimited)
	p.Fail(wire.Checkpoint)
	if k, _ := p.Blocked(); k != wire.Checkpoint {
		t.Errorf("Blocked = %q, want the checkpoint first", k)
	}
	p.Clear()
	if k, blocked := p.Blocked(); !blocked || k != wire.RateLimited {
		t.Errorf("after Clear Blocked = %q, %v, want the rate limit still", k, blocked)
	}

	// Kinds that are not Meta pushing back change nothing.
	q := newClock().pace()
	q.Fail(wire.Offline)
	q.Fail(wire.Timeout)
	if _, blocked := q.Blocked(); blocked {
		t.Errorf("an ordinary failure blocked")
	}
}

func TestPaceWaitsTwoToFourSeconds(t *testing.T) {
	for _, tc := range []struct {
		random float64
		want   time.Duration
	}{{0, 2 * time.Second}, {0.5, 3 * time.Second}, {0.999999, 4*time.Second - 2*time.Microsecond}} {
		c := newClock()
		c.random = tc.random
		if err := c.pace().Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(c.slept) != 1 {
			t.Fatalf("slept %d times, want 1", len(c.slept))
		}
		if d := c.slept[0]; d < MinGap || d >= MaxGap || (tc.random != 0.999999 && d != tc.want) {
			t.Errorf("random %v slept %v, want %v within [2s, 4s)", tc.random, d, tc.want)
		}
	}

	// The default Rand stays in the range too, without sleeping for real.
	c := newClock()
	p := &Pace{Now: c.Now, Sleep: c.Sleep}
	for range 200 {
		if err := p.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range c.slept {
		if d < MinGap || d >= MaxGap {
			t.Fatalf("a wait of %v is outside 2 to 4 s", d)
		}
	}
	if slices.Min(c.slept) == slices.Max(c.slept) {
		t.Errorf("200 waits were all %v: not random", c.slept[0])
	}
}

func TestPaceWaitStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := &Pace{Sleep: func(context.Context, time.Duration) error { t.Fatal("slept on a cancelled context"); return nil }}
	if err := p.Wait(ctx); err == nil {
		t.Errorf("Wait on a cancelled context returned nil")
	}
	// The real sleep, cut short.
	ctx, cancel = context.WithCancel(context.Background())
	go cancel()
	start := time.Now()
	if err := (&Pace{}).Wait(ctx); err == nil || time.Since(start) > time.Second {
		t.Errorf("the real Wait did not stop when its context ended (%v)", err)
	}
}

func TestTwoWaitsAtOnceGoAtLeastTwoSecondsApart(t *testing.T) {
	// A clock that does not move while they sleep: each Wait's sleep is the
	// time from now its request goes.
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var goes []time.Time
	p := &Pace{
		Now:  func() time.Time { return start },
		Rand: func() float64 { return 0 }, // the shortest gap, 2 s
		Sleep: func(_ context.Context, d time.Duration) error {
			mu.Lock()
			goes = append(goes, start.Add(d))
			mu.Unlock()
			return nil
		},
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Wait(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(goes) != 2 {
		t.Fatalf("%d waits ended, want 2", len(goes))
	}
	slices.SortFunc(goes, func(a, b time.Time) int { return a.Compare(b) })
	if gap := goes[1].Sub(goes[0]); gap < MinGap {
		t.Errorf("two requests %v apart, want at least %v", gap, MinGap)
	}
	if goes[0].Sub(start) < MinGap {
		t.Errorf("the first request went %v after the call, want at least %v", goes[0].Sub(start), MinGap)
	}

	// A third, after both, goes 2 s after the second; one long after them
	// goes 2 s after its own call.
	c := newClock()
	q := c.pace()
	for range 3 {
		if err := q.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range c.slept {
		if d != MinGap {
			t.Errorf("sequential wait %d slept %v, want %v", i, d, MinGap)
		}
	}
	c.now = c.now.Add(time.Hour)
	c.slept = nil
	if err := q.Wait(context.Background()); err != nil || len(c.slept) != 1 || c.slept[0] != MinGap {
		t.Errorf("a wait after an hour slept %v (%v), want %v", c.slept, err, MinGap)
	}
}

func TestASuccessDuringARateLimitDoesNotLiftIt(t *testing.T) {
	c := newClock()
	p := c.pace()
	p.Fail(wire.RateLimited)
	c.now = c.now.Add(time.Minute)
	p.Succeed() // a request sent before the block, answered during it
	if k, blocked := p.Blocked(); !blocked || k != wire.RateLimited {
		t.Fatalf("a success lifted the block: Blocked = %q, %v", k, blocked)
	}
	c.now = c.now.Add(15 * time.Minute)
	if _, blocked := p.Blocked(); blocked {
		t.Fatalf("the block did not end at its own time")
	}
	// Nor did it count as the success that starts the backoff over: the next
	// block is 30 minutes.
	p.Fail(wire.RateLimited)
	c.now = c.now.Add(16 * time.Minute)
	if _, blocked := p.Blocked(); !blocked {
		t.Errorf("a success during the block started the backoff over")
	}
	// After the block, a success does start it over.
	c.now = c.now.Add(15 * time.Minute)
	p.Succeed()
	p.Fail(wire.RateLimited)
	c.now = c.now.Add(16 * time.Minute)
	if _, blocked := p.Blocked(); blocked {
		t.Errorf("a success after the block did not start the backoff over")
	}
}

func TestARateLimitCarriesOverToANewPace(t *testing.T) {
	c := newClock()
	p := c.pace()
	p.Fail(wire.RateLimited)
	until, step := p.RateLimit()
	if step != 1 || !until.Equal(c.now.Add(15*time.Minute)) {
		t.Fatalf("RateLimit = %v, %d", until, step)
	}
	q := c.pace() // a restart
	q.RestoreRateLimit(until, step)
	c.now = c.now.Add(14 * time.Minute)
	if _, blocked := q.Blocked(); !blocked {
		t.Errorf("the restored block ended early")
	}
	c.now = c.now.Add(2 * time.Minute)
	q.Fail(wire.RateLimited)
	c.now = c.now.Add(16 * time.Minute)
	if _, blocked := q.Blocked(); !blocked {
		t.Errorf("the next block after a restore was not 30 minutes")
	}
}
