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

// rateBackoff is how long a rateLimited blocks, by how many have come in a
// row; the last holds from then on.
var rateBackoff = []time.Duration{15 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour}

// Pace keeps the helper's requests to Meta slow and quiet, and stops them
// when Meta pushes back. Meta bans accounts that look like a script, and a
// ban costs him the account, so after a rateLimited the helper waits (15
// minutes, then twice as long each time, up to 4 hours; a success starts
// over), and after a checkpoint or reconnectNeeded it does not ask again
// until he has signed in again (Clear).
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
	until        wire.Kind // checkpoint or reconnectNeeded: blocked until Clear
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

// Wait sleeps a random 2 to 4 seconds, the gap between one request and the
// next, and returns ctx's error if ctx ends first.
func (p *Pace) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d := MinGap + time.Duration(p.rand()*float64(MaxGap-MinGap))
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

// Fail records that a request failed with kind. Only three kinds matter:
// rateLimited starts or lengthens a timed block, checkpoint and
// reconnectNeeded block until Clear. A rateLimited that comes while a block
// is running (a request sent before it began, failing after) does not
// lengthen it. Any other kind is not Meta pushing back, and changes nothing.
func (p *Pace) Fail(kind wire.Kind) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch kind {
	case wire.Checkpoint, ReconnectNeeded:
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
// starts over. A checkpoint is not lifted by one.
func (p *Pace) Succeed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rateLimits = 0
	p.blockedUntil = time.Time{}
}

// Blocked is the kind that stops requests now, and whether anything does. A
// checkpoint or reconnectNeeded outranks a rateLimited.
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

// Clear lifts a checkpoint or reconnectNeeded. Only a new login calls it: he
// has signed in again, which is what they ask for. A rateLimited block is
// Meta's and stays until its time is up.
func (p *Pace) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.until = ""
}
