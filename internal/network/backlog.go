package network

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue's pace. It asks one page at a time, slowly, and only
// when nothing else of the network is going on; any pushback holds it for
// a day, and anything it does not know stops it until the helper is next
// started. Getting an account locked costs far more than slow history.
const (
	BacklogMinGap = 15 * time.Second // after the queue's last request ended, at least
	BacklogMaxGap = 25 * time.Second // and less than this, at random
	BacklogQuiet  = 10 * time.Second // no other request of the network for this long
	BacklogYield  = 60 * time.Second // after an older read
	BacklogLook   = 30 * time.Second // how often it looks again while it may not ask
	BacklogEmpty  = 10 * time.Minute // how long it waits when no chat has more
	BacklogHold   = 24 * time.Hour   // after any pushback on the network
	BacklogDayCap = 1500             // queue requests in a local day
	BacklogStalls = 3                // passed-over chats in a row with an error that stop it
)

// StepOutcome is what one step of the queue did.
type StepOutcome int

// The outcomes of a step.
const (
	StepNothing StepOutcome = iota // no chat has more to page now; nothing asked
	StepMoved                      // a page reached further back
	StepStart                      // the chat (or one side of it) reached its start
	StepPassed                     // the chat can't be paged now; passed over for this run
)

// Step is what one call of Backlog.Next did.
type Step struct {
	Outcome StepOutcome
	Chat    string // in memory only, for the run's passed-over set
	Err     error  // the request's error; nil when the network answered
}

// The queue's own values, kept in the network's Values (never inside a
// Batch).
const (
	holdKey = "hq.hold" // <until, Unix ms>\n<kind>
)

// Backlog is a network's history queue: it pages its chats back to their
// start in the background, one request at a time, through the network's
// own request function (so its pace, its pause and its kept block apply).
// It owns only the pacing, the waiting and the stopping; Next does the
// paging and keeps each chat's progress.
//
// A network makes one Backlog for the helper's life and calls Run once per
// run. Fill in the fields before the first Run; Rand may be nil.
type Backlog struct {
	Name    string // the network, for the diagnostics file
	Dir     string // the data folder (diag.For)
	Clock   Clock
	Values  *Values
	Traffic *Traffic
	Rand    func() float64 // in [0, 1); default math/rand/v2

	Live    func() bool              // the network's state is live
	Ready   func() bool              // the first sign-in's own history is done
	Blocked func() (wire.Kind, bool) // the network's pace or pause
	Kind    func(error) wire.Kind    // the network's kindOf
	// Next pages one chat by one page, under ctx (the queue's: every
	// request it makes is counted as the queue's), passing over the chats
	// passed reports. It makes at most one request.
	Next func(ctx context.Context, passed func(chat string) bool) Step
	Left func() (int, bool) // chats whose history is open, for the day line

	running atomic.Bool

	mu      sync.Mutex
	yieldAt time.Time       // the last older read
	passed  map[string]bool // chats passed over, until no chat has more

	// The rest is Run's alone: one Run at a time touches it.
	holdUntil  time.Time // no request before this (a pushback's day)
	holdKind   wire.Kind
	stopped    wire.Kind // why it stopped for the helper's life; "" while it goes
	stalls     int       // chats passed over with an error, in a row
	emptyUntil time.Time // no chat had more: it looks again then
	today      queueDay
}

// queueDay is the queue's counts for one local day.
type queueDay struct {
	date            string // YYYY-MM-DD, local
	pages, finished int
	paused, stopped wire.Kind
}

// Run runs the queue until ctx ends: one call per network run. A second
// Run while one runs returns at once.
func (b *Backlog) Run(ctx context.Context) {
	if b.Clock == nil || b.Values == nil || b.Traffic == nil || b.Next == nil {
		return
	}
	if !b.running.CompareAndSwap(false, true) {
		return
	}
	defer b.running.Store(false)

	now := b.Clock.Now()
	b.roll(now)
	b.begin(now)
	// from is what the gap counts from: the end of the queue's last
	// request, or the time it was last allowed to ask again (the run's
	// start, a hold's end, the network going live), so time spent not
	// asking is never made up with a burst.
	from, gap, allowed := now, b.gap(), false
	for ctx.Err() == nil {
		now = b.Clock.Now()
		b.roll(now)
		if wait, ok := b.mayAsk(now); !ok {
			allowed = false
			if !b.sleep(ctx, now, wait) {
				return
			}
			continue
		}
		if !allowed {
			allowed, from = true, now
		}
		at, quiet := b.askAt(from.Add(gap))
		if !quiet {
			if !b.sleep(ctx, now, BacklogQuiet) {
				return
			}
			continue
		}
		if now.Before(at) {
			if !b.sleep(ctx, now, at.Sub(now)) {
				return
			}
			continue
		}
		b.step(ctx)
		from, gap = b.Clock.Now(), b.gap()
	}
}

// Yield is an older read: the queue asks nothing for BacklogYield.
func (b *Backlog) Yield() {
	if b.Clock == nil {
		return
	}
	now := b.Clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.After(b.yieldAt) {
		b.yieldAt = now
	}
}

// begin takes up a hold kept by this or an earlier helper, before the
// run's first request.
func (b *Backlog) begin(now time.Time) {
	v, err := b.Values.Lookup(holdKey)
	if err != nil {
		b.stop(wire.Internal)
		return
	}
	if v == "" {
		return
	}
	ms, kind, ok := strings.Cut(v, "\n")
	n, err := strconv.ParseInt(ms, 10, 64)
	if !ok || err != nil || kind == "" {
		// Unreadable: hold a day from now rather than guess.
		b.hold(now, wire.Internal)
		return
	}
	if until := time.UnixMilli(n); until.After(b.holdUntil) {
		b.holdUntil, b.holdKind = until, wire.Kind(kind)
	}
	if now.Before(b.holdUntil) && b.today.paused == "" {
		b.today.paused = b.holdKind
	}
}

// mayAsk reports whether the queue may ask now, and if not, how long to
// wait before it looks again. A block on the network keeps a hold.
func (b *Backlog) mayAsk(now time.Time) (wait time.Duration, ok bool) {
	switch {
	case b.stopped != "":
		return BacklogHold, false
	case now.Before(b.holdUntil):
		return b.holdUntil.Sub(now), false
	case b.today.pages >= BacklogDayCap:
		return BacklogHold, false // until the local midnight (sleep)
	}
	if b.Blocked != nil {
		if kind, blocked := b.Blocked(); blocked {
			b.hold(now, kind)
			return b.holdUntil.Sub(now), false
		}
	}
	if b.Live == nil || !b.Live() || b.Ready == nil || !b.Ready() {
		return BacklogLook, false
	}
	return 0, true
}

// askAt is the earliest time the queue may ask, at or after at: after the
// last older read's yield, the wait when nothing was left, and the
// network's quiet. quiet is false while another request is in flight.
func (b *Backlog) askAt(at time.Time) (time.Time, bool) {
	b.mu.Lock()
	y := b.yieldAt
	b.mu.Unlock()
	if !y.IsZero() {
		at = later(at, y.Add(BacklogYield))
	}
	at = later(at, b.emptyUntil)
	q, quiet := b.Traffic.quietAt(BacklogQuiet)
	if !quiet {
		return at, false
	}
	return later(at, q), true
}

// step makes one call of Next and reads what came back. It never calls
// Next again itself: the next call waits for Run's gap.
func (b *Backlog) step(ctx context.Context) {
	before := b.Traffic.BacklogAsked()
	s := b.Next(WithBacklog(ctx), b.isPassed)
	now := b.Clock.Now()
	n := b.Traffic.BacklogAsked() - before
	b.roll(now)
	b.today.pages += int(n)
	if n > 1 {
		// A step makes one request at most: this one is a bug, and
		// may be a burst.
		b.stop(wire.Internal)
		return
	}

	var kind wire.Kind
	if s.Err != nil {
		kind = b.kindOf(s.Err)
	}
	if kind == wire.RateLimited || kind == wire.Checkpoint {
		b.hold(now, kind)
		b.pass(s.Chat)
		return
	}
	if b.Blocked != nil {
		if k, blocked := b.Blocked(); blocked {
			// Live work was pushed back meanwhile, or this request was
			// under another kind.
			b.hold(now, k)
		}
	}
	if kind != "" && ctx.Err() != nil {
		return // the run ended under the request: its error says nothing
	}
	switch kind {
	case "":
		switch s.Outcome {
		case StepMoved:
			b.stalls = 0
		case StepStart:
			b.stalls = 0
			b.today.finished++
		case StepPassed:
			b.pass(s.Chat)
		case StepNothing:
			b.emptyUntil = now.Add(BacklogEmpty)
			b.mu.Lock()
			b.passed = nil
			b.mu.Unlock()
		}
	case wire.NotConnected, wire.Offline:
		// Not the chat's doing: it waits for the network to be live.
	case wire.Timeout:
		b.pass(s.Chat)
		if b.stalls++; b.stalls >= BacklogStalls {
			b.stop(wire.Timeout)
		}
	default:
		b.stop(kind)
	}
}

// hold keeps the queue from asking for BacklogHold from now, in memory and
// in the network's values (so a relaunch keeps it too).
func (b *Backlog) hold(now time.Time, kind wire.Kind) {
	if until := now.Add(BacklogHold); until.After(b.holdUntil) {
		b.holdUntil = until
	}
	b.holdKind = kind
	if b.today.paused == "" {
		b.today.paused = kind
	}
	v := strconv.FormatInt(b.holdUntil.UnixMilli(), 10) + "\n" + string(kind)
	if err := b.Values.Keep(holdKey, v); err != nil {
		// A hold a relaunch would not see: stop for this helper's life
		// as well.
		b.stop(wire.Internal)
	}
}

// stop stops the queue for the helper's life.
func (b *Backlog) stop(kind wire.Kind) {
	if b.stopped == "" {
		b.stopped = kind
	}
	if b.today.stopped == "" {
		b.today.stopped = kind
	}
}

// roll starts a new day's counts when now's local day is not the one held.
func (b *Backlog) roll(now time.Time) {
	date := now.Local().Format(time.DateOnly)
	if b.today.date == date {
		return
	}
	b.today = queueDay{date: date, stopped: b.stopped}
	if now.Before(b.holdUntil) {
		b.today.paused = b.holdKind
	}
}

// sleep waits d, but no later than the next local midnight (so a day's
// counts close on time); false when ctx has ended.
func (b *Backlog) sleep(ctx context.Context, now time.Time, d time.Duration) bool {
	if m := nextMidnight(now).Sub(now); d > m {
		d = m
	}
	return b.Clock.Sleep(ctx, d) == nil
}

func (b *Backlog) pass(chat string) {
	if chat == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.passed == nil {
		b.passed = map[string]bool{}
	}
	b.passed[chat] = true
}

func (b *Backlog) isPassed(chat string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.passed[chat]
}

func (b *Backlog) kindOf(err error) wire.Kind {
	if b.Kind != nil {
		return b.Kind(err)
	}
	return wire.KindOf(err)
}

// gap is a random time from BacklogMinGap up to BacklogMaxGap.
func (b *Backlog) gap() time.Duration {
	r := rand.Float64()
	if b.Rand != nil {
		r = b.Rand()
	}
	if r < 0 || r >= 1 {
		r = 0
	}
	return BacklogMinGap + time.Duration(r*float64(BacklogMaxGap-BacklogMinGap))
}

// nextMidnight is the first local midnight after now.
func nextMidnight(now time.Time) time.Time {
	y, m, d := now.Local().Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, time.Local)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
