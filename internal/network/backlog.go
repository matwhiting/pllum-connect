package network

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matwhiting/pllum-connect/internal/diag"
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
	dayKey  = "hq.day"  // <YYYY-MM-DD>\n<pages>\n<finished>\n<minutes>\n<paused kind>\n<stopped kind>
)

// Backlog is a network's history queue: it pages its chats back to their
// start in the background, one request at a time, through the network's
// own request function (so its pace, its pause and its kept block apply).
// It owns only the pacing, the waiting and the stopping; Next does the
// paging and keeps each chat's progress.
//
// A network makes one Backlog for the helper's life and calls Run once per
// run, and PushedBack whenever the network is pushed back (a rate limit, a
// pause, a checkpoint, an ended session), whether or not Run is running.
// Fill in the fields before the first Run or PushedBack; Rand may be nil.
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

	turnOnce sync.Once
	turn     chan struct{} // held by the Run running: one at a time

	mu        sync.Mutex
	yieldAt   time.Time       // the last older read
	passed    map[string]bool // chats passed over, until no chat has more
	holdUntil time.Time       // no request before this (a pushback's day)
	holdKind  wire.Kind
	stopped   wire.Kind // why it stopped for the helper's life; "" while it goes

	// The rest is Run's alone: one Run at a time touches it.
	stalls     int       // chats passed over with an error, in a row
	emptyUntil time.Time // no chat had more: it looks again then
	today      queueDay
	since      time.Time // allowed to ask since then, not yet counted in today; zero while not
}

// queueDay is the queue's counts for one local day, kept in dayKey: once
// the day has passed, its line goes to the diagnostics file.
type queueDay struct {
	date            string // YYYY-MM-DD, local
	pages, finished int
	running         time.Duration // allowed to ask (kept in whole minutes)
	paused, stopped wire.Kind
}

// Run runs the queue until ctx ends: one call per network run. A Run that
// starts while the one before it is still returning (its step still out)
// waits for it.
func (b *Backlog) Run(ctx context.Context) {
	if b.Clock == nil || b.Values == nil || b.Traffic == nil || b.Next == nil {
		return
	}
	b.turnOnce.Do(func() { b.turn = make(chan struct{}, 1) })
	select {
	case b.turn <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-b.turn }()

	now := b.Clock.Now()
	b.since = time.Time{}
	b.roll(now)
	b.begin(now)
	b.keepDay()
	// from is what the gap counts from: the end of the queue's last
	// request, or the time it was last allowed to ask again (the run's
	// start, a hold's end, the network going live), so time spent not
	// asking is never made up with a burst.
	from, gap, allowed := now, b.gap(), false
	for ctx.Err() == nil {
		now = b.Clock.Now()
		b.account(now)
		b.roll(now)
		wait, ok := b.mayAsk(now)
		if !ok {
			allowed, b.since = false, time.Time{}
		} else if !allowed {
			allowed, from, b.since = true, now, now
		}
		b.keepDay()
		if !ok {
			if !b.sleep(ctx, now, wait) {
				return
			}
			continue
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

// MayAsk reports whether, as far as the queue's own state goes, a request
// of its may go now: it is not stopped, not held, and not yielding to an
// older read. A network checks it again once the queue's request has its
// turn in the pace, so a scroll-back that began meanwhile goes first.
func (b *Backlog) MayAsk() bool {
	clock := b.Clock
	if clock == nil {
		return false
	}
	now := clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped != "" || now.Before(b.holdUntil) {
		return false
	}
	return b.yieldAt.IsZero() || !now.Before(b.yieldAt.Add(BacklogYield))
}

// PushedBack is any pushback on the network (kind is why: a rate limit,
// a pause, a checkpoint, an ended session): the queue asks nothing for
// BacklogHold. The hold is kept at once, in memory and in the network's
// values, so it holds whether or not Run is running, and across a
// relaunch. A hold that can't be kept stops the queue for the helper's
// life.
func (b *Backlog) PushedBack(kind wire.Kind) {
	clock := b.Clock
	if clock == nil {
		clock = RealClock{}
	}
	if kind == "" {
		kind = wire.Internal
	}
	b.hold(clock.Now(), kind)
}

// begin takes up a hold kept by this or an earlier helper, before the
// run's first request. A kept end more than BacklogHold ahead (a clock set
// back) holds BacklogHold from now.
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
	until := time.UnixMilli(n)
	if most := now.Add(BacklogHold); until.After(most) {
		until = most
	}
	b.mu.Lock()
	if until.After(b.holdUntil) {
		b.holdUntil, b.holdKind = until, wire.Kind(kind)
	}
	b.mu.Unlock()
}

// state is the hold in force and the stop, as PushedBack may have changed
// them; today's counts are marked with them.
func (b *Backlog) state(now time.Time) (holdUntil time.Time, stopped wire.Kind) {
	b.mu.Lock()
	holdUntil, kind, stopped := b.holdUntil, b.holdKind, b.stopped
	b.mu.Unlock()
	if now.Before(holdUntil) && b.today.paused == "" {
		b.today.paused = kind
	}
	if stopped != "" && b.today.stopped == "" {
		b.today.stopped = stopped
	}
	return holdUntil, stopped
}

// mayAsk reports whether the queue may ask now, and if not, how long to
// wait before it looks again. A block on the network keeps a hold.
func (b *Backlog) mayAsk(now time.Time) (wait time.Duration, ok bool) {
	holdUntil, stopped := b.state(now)
	switch {
	case stopped != "":
		return BacklogHold, false
	case now.Before(holdUntil):
		return holdUntil.Sub(now), false
	case b.today.pages >= BacklogDayCap:
		return BacklogHold, false // until the local midnight (sleep)
	}
	if b.Blocked != nil {
		if kind, blocked := b.Blocked(); blocked {
			b.hold(now, kind)
			return b.mayAsk(now)
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
	b.account(now)
	b.roll(now)
	b.today.pages += int(n)
	defer b.keepDay()
	defer b.state(now)
	if n > 1 {
		// A step makes one request at most: this one is a bug, and
		// may be a burst.
		b.stop(wire.Internal)
		return
	}
	if n == 0 && s.Err == nil && (s.Outcome == StepMoved || s.Outcome == StepStart) {
		// A page came back and no queue request was counted: Next asked
		// outside the queue's context, so the counts, the cap, the guard
		// above and the network's quiet are all blind.
		b.stop(wire.Internal)
		return
	}

	var kind wire.Kind
	if s.Err != nil {
		if kind = b.kindOf(s.Err); kind == "" {
			kind = wire.Internal
		}
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
// Safe from any goroutine: today's counts are marked by Run (state).
func (b *Backlog) hold(now time.Time, kind wire.Kind) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if until := now.Add(BacklogHold); until.After(b.holdUntil) {
		b.holdUntil = until
	}
	b.holdKind = kind
	if b.Values == nil {
		b.stopLocked(wire.Internal)
		return
	}
	v := strconv.FormatInt(b.holdUntil.UnixMilli(), 10) + "\n" + string(kind)
	if err := b.Values.Keep(holdKey, v); err != nil {
		// A hold a relaunch would not see: stop for this helper's life
		// as well.
		b.stopLocked(wire.Internal)
	}
}

// stop stops the queue for the helper's life (Run's: it marks today too).
func (b *Backlog) stop(kind wire.Kind) {
	b.mu.Lock()
	b.stopLocked(kind)
	b.mu.Unlock()
	if b.today.stopped == "" {
		b.today.stopped = kind
	}
}

func (b *Backlog) stopLocked(kind wire.Kind) {
	if b.stopped == "" {
		b.stopped = kind
	}
}

// roll starts a new day's counts when now's local day is not the one held,
// writing the day before's line. The first time, it takes up the day kept
// by this or an earlier helper: today's counts go on; an earlier day's line
// is written. A day the helper never ran has no line.
func (b *Backlog) roll(now time.Time) {
	date := now.Local().Format(time.DateOnly)
	if b.today.date == date || (b.today.date != "" && date < b.today.date) {
		// A date that went back (the clock, or the zone) goes on counting
		// in the later day, so the cap never starts over.
		return
	}
	prev := b.today
	if prev.date == "" {
		if kept, ok := b.keptDay(); ok {
			if kept.date >= date {
				b.today = kept
				return
			}
			prev = kept
		}
	}
	if prev.date != "" {
		b.writeLine(now, prev)
	}
	b.today = queueDay{date: date}
	b.state(now)
}

// account counts the time since b.since, while it was allowed to ask, in
// today's minutes.
func (b *Backlog) account(now time.Time) {
	if b.since.IsZero() {
		return
	}
	if d := now.Sub(b.since); d > 0 {
		b.today.running += d
	}
	b.since = now
}

// writeLine writes d's line to the network's diagnostics file: counts only.
func (b *Backlog) writeLine(now time.Time, d queueDay) {
	if b.Dir == "" || b.Name == "" {
		return
	}
	left := "unknown"
	if b.Left != nil {
		if n, ok := b.Left(); ok {
			left = strconv.Itoa(n)
		}
	}
	diag.For(b.Dir, b.Name).Line(now, fmt.Sprintf(diag.HistoryLine, d.date, d.pages, d.finished, left,
		int(d.running/time.Minute), yesKind(d.paused), yesKind(d.stopped)))
}

// keepDay keeps today's counts (written only when they change). Counts
// that can't be kept stop the queue: a relaunch would start the day cap
// over.
func (b *Backlog) keepDay() {
	d := b.today
	v := strings.Join([]string{d.date, strconv.Itoa(d.pages), strconv.Itoa(d.finished),
		strconv.Itoa(int(d.running / time.Minute)), string(d.paused), string(d.stopped)}, "\n")
	if err := b.Values.Keep(dayKey, v); err != nil {
		b.stop(wire.Internal)
	}
}

// keptDay is the day kept in dayKey; ok is false when none can be read.
func (b *Backlog) keptDay() (queueDay, bool) {
	v, err := b.Values.Lookup(dayKey)
	if err != nil || v == "" {
		return queueDay{}, false
	}
	f := strings.Split(v, "\n")
	if len(f) != 6 {
		return queueDay{}, false
	}
	if _, err := time.Parse(time.DateOnly, f[0]); err != nil {
		return queueDay{}, false
	}
	var n [3]int
	for i := range n {
		x, err := strconv.Atoi(f[i+1])
		if err != nil || x < 0 {
			return queueDay{}, false
		}
		n[i] = x
	}
	return queueDay{date: f[0], pages: n[0], finished: n[1], running: time.Duration(n[2]) * time.Minute,
		paused: wire.Kind(f[4]), stopped: wire.Kind(f[5])}, true
}

func yesKind(k wire.Kind) string {
	if k == "" {
		return "no"
	}
	return "yes " + string(k)
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
