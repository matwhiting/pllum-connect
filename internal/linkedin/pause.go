package linkedin

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The back-off when LinkedIn pushes back: a 403, 429 or 999 on any request
// but a media download pauses every LinkedIn request, the live stream
// included, for 15 minutes, then twice as long each time, up to 6 hours; a
// day with no pause starts the count over.
const (
	firstPause     = 15 * time.Minute
	longestPause   = 6 * time.Hour
	pauseForgotten = 24 * time.Hour
)

// reconnectNeeded is what check gives for a session LinkedIn has ended (a
// 401, a 302 to the sign-in page, li_at deleted). It is a state's name
// (network.ReconnectNeeded), never sent as an error kind.
const reconnectNeeded wire.Kind = "reconnectNeeded"

// pause keeps the back-off. Its methods are safe from any goroutine.
type pause struct {
	now func() time.Time
	// pushedBack, when set, runs for every pause status LinkedIn answers
	// (whether or not it starts a new pause), outside the pause's lock.
	pushedBack func()

	mu      sync.Mutex
	until   time.Time // no request before this
	step    int       // pauses since the count last started over
	lastEnd time.Time // when the last pause ended (or ends)
}

// pauseFor is how long the step-th pause in a row lasts, counted from 0.
func pauseFor(step int) time.Duration {
	d := firstPause
	for range step {
		d *= 2
		if d >= longestPause {
			return longestPause
		}
	}
	return d
}

// check reads a request's error by the rule. A 403, 429 or 999 starts a
// pause (one that comes while a pause runs, for a request sent before it
// began, does not lengthen it) and is rateLimited with paused true; a 401, a
// 302 or li_at deleted is reconnectNeeded; a 502 to 504 the library gave up
// on, or the network failing, is offline; a timeout is timeout; anything else
// is internal. A nil error is no kind.
func (p *pause) check(err error) (kind wire.Kind, paused bool) {
	if err == nil {
		return "", false
	}
	var re *linkedingo.ResponseError
	if errors.As(err, &re) && re != nil {
		switch re.StatusCode {
		case 403, 429, 999:
			p.start()
			if p.pushedBack != nil {
				p.pushedBack()
			}
			return wire.RateLimited, true
		case 401, 302:
			return reconnectNeeded, false
		case 502, 503, 504:
			return wire.Offline, false
		}
		return wire.Internal, false
	}
	return kindOf(err), false
}

// checkMedia reads a media download's error: a CDN refusing or no longer
// holding a file (401, 403, 404, 410, a redirect) is mediaGone, and a 429 or
// 999 is rateLimited, but neither ever pauses LinkedIn.
func (p *pause) checkMedia(err error) wire.Kind {
	var re *linkedingo.ResponseError
	if errors.As(err, &re) && re != nil {
		switch re.StatusCode {
		case 401, 403, 404, 410, 301, 302, 303, 307, 308:
			return wire.MediaGone
		case 429, 999:
			return wire.RateLimited
		case 502, 503, 504:
			return wire.Offline
		}
		return wire.Internal
	}
	if err == nil {
		return ""
	}
	return kindOf(err)
}

// gone reports whether err is LinkedIn's answer that the thing asked about
// is not to be had: a 4xx that is neither a pause (403, 429) nor an ended
// session (401), or a GraphQL answer with no data. Only such an answer
// settles background work for good (a chat's fresh mark, its history, the
// sync token). Anything else passes: a 5xx, an answer the helper can't read,
// a timeout or no connection says nothing about the thing asked, and the
// work is asked again later.
func gone(err error) bool {
	var re *linkedingo.ResponseError
	if errors.As(err, &re) && re != nil {
		switch re.StatusCode {
		case 401, 403, 429:
			return false
		}
		return re.StatusCode >= 400 && re.StatusCode < 500
	}
	return errors.Is(err, linkedingo.ErrMissingGraphQLData)
}

// kindOf is the kind of an error that is not LinkedIn's answer.
func kindOf(err error) wire.Kind {
	var we *wire.Error
	var ue *url.Error
	switch {
	case errors.Is(err, linkedingo.ErrTokenInvalidated):
		return reconnectNeeded
	case errors.Is(err, context.DeadlineExceeded):
		return wire.Timeout
	case errors.As(err, &we) && we != nil:
		return we.Kind
	case errors.As(err, &ue):
		return wire.Offline
	}
	return wire.Internal
}

// start begins the next pause, unless one is running.
func (p *pause) start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if now.Before(p.until) {
		return
	}
	if !p.lastEnd.IsZero() && now.Sub(p.lastEnd) >= pauseForgotten {
		p.step = 0
	}
	p.until = now.Add(pauseFor(p.step))
	p.lastEnd = p.until
	p.step++
}

// blocked is the end of the pause in force, and whether one is.
func (p *pause) blocked() (until time.Time, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.now().Before(p.until) {
		return p.until, true
	}
	return time.Time{}, false
}

// String is the pause as kept in the store: its end, its step and the last
// pause's end, in milliseconds. Empty when there has never been one.
func (p *pause) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.step == 0 && p.until.IsZero() {
		return ""
	}
	return strconv.FormatInt(p.until.UnixMilli(), 10) + "\n" + strconv.Itoa(p.step) + "\n" + strconv.FormatInt(p.lastEnd.UnixMilli(), 10)
}

// restore takes up a pause an earlier run kept (String's form).
func (p *pause) restore(kept string) {
	parts := strings.Split(kept, "\n")
	if len(parts) != 3 {
		return
	}
	until, err1 := strconv.ParseInt(parts[0], 10, 64)
	step, err2 := strconv.Atoi(parts[1])
	last, err3 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.until, p.step, p.lastEnd = time.UnixMilli(until), step, time.UnixMilli(last)
}
