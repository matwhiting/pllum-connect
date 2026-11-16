package meta

import (
	"maps"
	"sync"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
)

// SessionEvery is the least time between two `session` events.
const SessionEvery = time.Minute

// SessionDiff decides when the cookie session has changed enough to tell
// Pllum (the `session` event): the named cookies differ from the set last
// returned, and SessionEvery has passed since then. Meta rotates cookies
// (rur above all) as it goes, and Pllum keeps the copy it signs in again
// from, so that copy has to follow, but not on every response.
//
// A set that lacks a required cookie is never returned: it would replace a
// session that works with one that does not.
type SessionDiff struct {
	// Names are the cookies that make the session; others never leave.
	Names []string
	// Required are the Names without which there is no session.
	Required []string

	mu     sync.Mutex
	last   map[string]string
	lastAt time.Time
	sent   bool // a set has been returned, so lastAt means something
}

// NewSessionDiff watches the cookies a sign-in page names.
func NewSessionDiff(p SignInPage) *SessionDiff {
	return &SessionDiff{Names: p.Names(), Required: p.Required}
}

// Seed records the session Pllum already holds (the one it signed in with),
// so Changed does not hand it back. It does not start the minute.
func (d *SessionDiff) Seed(session map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.last = d.pick(func(name string) string { return session[name] })
}

// Changed is the named cookies of c, in full, when they differ from the set
// last returned (or seeded) and a minute has passed since a set was last
// returned; otherwise false.
func (d *SessionDiff) Changed(c *cookies.Cookies, now time.Time) (map[string]string, bool) {
	if c == nil {
		return nil, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	cur := d.pick(func(name string) string { return c.Get(cookies.MetaCookieName(name)) })
	for _, name := range d.Required {
		if cur[name] == "" {
			return nil, false
		}
	}
	if maps.Equal(cur, d.last) {
		return nil, false
	}
	if d.sent && now.Sub(d.lastAt) < SessionEvery {
		return nil, false
	}
	d.last, d.lastAt, d.sent = cur, now, true
	return maps.Clone(cur), true
}

// pick is the named cookies that have a value.
func (d *SessionDiff) pick(get func(name string) string) map[string]string {
	out := map[string]string{}
	for _, name := range d.Names {
		if v := get(name); v != "" {
			out[name] = v
		}
	}
	return out
}
