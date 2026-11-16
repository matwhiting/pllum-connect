package meta

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The gap Wait leaves between requests: a random time from MinGap up to MaxGap.
const (
	MinGap = 2 * time.Second
	MaxGap = 4 * time.Second
)

// ReconnectNeeded is the other thing, besides a checkpoint, that stops Pace
// until he signs in again: the session has gone. It is the name of a state
// (network.ReconnectNeeded), not one of protocol 1's error kinds, so it is
// only ever given to Fail and never sent as an error.
const ReconnectNeeded wire.Kind = "reconnectNeeded"

// Banned is the third: Meta has suspended the account. Like ReconnectNeeded
// it is a state's name (network.Banned), never sent as an error kind.
const Banned wire.Kind = "banned"

// rateBackoff is how long a rateLimited blocks, by how many have come in a
// row; the last holds from then on.
var rateBackoff = []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour}

// Pace keeps the helper's requests to Meta slow and quiet, and stops them
// when Meta pushes back. Meta bans accounts that look like a script, and a
// ban costs him the account, so after a rateLimited the helper waits (15
// minutes, then twice as long each time, up to 4 hours; a success starts
// over), and after a checkpoint, reconnectNeeded or banned it does not ask
// again until he has signed in again (Clear).
//
// The zero value is ready to use. Now, Sleep and Rand may be set before first
// use, so a test runs on its own clock and never sleeps.
type Pace struct {
	Now   func() time.Time                                 // default time.Now
	Sleep func(ctx context.Context, d time.Duration) error // default: wait d or until ctx ends
	Rand  func() float64                                   // in [0, 1); default math/rand/v2

	mu           sync.Mutex
	rateLimits   int       // rateLimited seen since the last success
	blockedUntil time.Time // a rateLimited block, until this time
	until        wire.Kind // checkpoint, reconnectNeeded or banned: blocked until Clear
	next         time.Time // the time the last Wait let its request go
}

func (p *Pace) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Pace) rand() float64 {
	if p.Rand != nil {
		return p.Rand()
	}
	return rand.Float64()
}

// Wait sleeps until the caller's request may go, and returns ctx's error if
// ctx ends first. Requests go in the order Wait is called, each a random 2 to
// 4 seconds after the one before (or after now, when the last was longer ago
// than that), so callers on several goroutines never fire together.
func (p *Pace) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	now := p.now()
	from := now
	if p.next.After(from) {
		from = p.next
	}
	at := from.Add(MinGap + time.Duration(p.rand()*float64(MaxGap-MinGap)))
	prev := p.next
	p.next = at
	p.mu.Unlock()

	err := p.sleep(ctx, at.Sub(now))
	if err != nil {
		// The request will not go: the slot is given back, unless a later
		// caller has already been given the one after it.
		p.mu.Lock()
		if p.next.Equal(at) {
			p.next = prev
		}
		p.mu.Unlock()
	}
	return err
}

func (p *Pace) sleep(ctx context.Context, d time.Duration) error {
	if p.Sleep != nil {
		return p.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Fail records that a request failed with kind. Only four kinds matter:
// rateLimited starts or lengthens a timed block; checkpoint,
// reconnectNeeded and banned block until Clear. A rateLimited that comes while a block
// is running (a request sent before it began, failing after) does not
// lengthen it. Any other kind is not Meta pushing back, and changes nothing.
func (p *Pace) Fail(kind wire.Kind) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch kind {
	case wire.Checkpoint, ReconnectNeeded, Banned:
		if p.until == "" {
			p.until = kind
		}
	case wire.RateLimited:
		now := p.now()
		if now.Before(p.blockedUntil) {
			return
		}
		i := min(p.rateLimits, len(rateBackoff)-1)
		p.rateLimits++
		p.blockedUntil = now.Add(rateBackoff[i])
	}
}

// Succeed records a request that Meta answered: the rateLimited backoff
// starts over. A success while a rate-limit block runs (a request sent
// before it began) changes nothing: only the block's own end lifts it. A
// checkpoint is not lifted by one either.
func (p *Pace) Succeed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.now().Before(p.blockedUntil) {
		return
	}
	p.rateLimits = 0
}

// Blocked is the kind that stops requests now, and whether anything does. A
// checkpoint, reconnectNeeded or banned outranks a rateLimited.
func (p *Pace) Blocked() (wire.Kind, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.until != "" {
		return p.until, true
	}
	if p.now().Before(p.blockedUntil) {
		return wire.RateLimited, true
	}
	return "", false
}

// Clear lifts a checkpoint, reconnectNeeded or banned. Only a new login
// calls it: he has signed in again, which is what they ask for. A rateLimited block is
// Meta's and stays until its time is up.
func (p *Pace) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.until = ""
}

// RateLimit is the rate-limit block in force (its end, zero for none) and
// how many rateLimited have come since the last success, so a restart can
// carry them on (RestoreRateLimit).
func (p *Pace) RateLimit() (until time.Time, step int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blockedUntil, p.rateLimits
}

// RestoreRateLimit takes up a block and count RateLimit gave in an earlier
// run: the block runs to its end, and the next rateLimited doubles from the
// count.
func (p *Pace) RestoreRateLimit(until time.Time, step int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if until.After(p.blockedUntil) {
		p.blockedUntil = until
	}
	p.rateLimits = max(p.rateLimits, step)
}
