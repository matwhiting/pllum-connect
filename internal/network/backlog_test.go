package network

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/diag"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/testkit"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every chat ID here is invented.

// queueStart is a local 01:00, so a day's worth of steps fits before the
// local midnight.
var queueStart = time.Date(2026, 6, 10, 1, 0, 0, 0, time.Local)

// asked is one Next call: when it began and ended, the chat it paged and
// which chats passed reported passed over at the time.
type asked struct {
	at, end time.Time
	chat    string
	passed  []string
}

// queue is a Backlog over a real store, an ordered fake clock and a
// scripted Next. Its network is live and ready, and nothing blocks it,
// until a test says otherwise.
type queue struct {
	t       *testing.T
	st      *store.Store
	dir     string
	clock   *testkit.Clock
	traffic *Traffic
	b       *Backlog

	mu       sync.Mutex
	live     bool
	ready    bool
	blocked  wire.Kind
	chats    []string // the chats Next pages, first not passed over first
	took     time.Duration
	blind    bool   // Next pages without the queue's context: no request is counted
	script   []Step // what each call returns (Chat filled in when empty); then StepMoved
	calls    []asked
	busy     int
	stopAt   int  // end the run once this many calls have been made (0: never)
	stopping bool // stopAt is reached: the run ends at its next sleep
	cancel   context.CancelFunc
	onNext   func(ctx context.Context, n int) // runs inside the n-th call (from 1)
}

var queueChats = []string{"chat-invented-1", "chat-invented-2", "chat-invented-3"}

func newQueue(t *testing.T) *queue {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "connect")
	st, err := store.Open(dir, [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	q := &queue{t: t, st: st, dir: dir, clock: testkit.NewOrderedClock(queueStart), live: true, ready: true,
		chats: queueChats, took: 2 * time.Second}
	q.relaunch()
	return q
}

// relaunch is a new Backlog (and new Values and Traffic) over the same
// store, as a new helper process makes.
func (q *queue) relaunch() {
	q.traffic = &Traffic{}
	q.b = &Backlog{
		Name:    "instagram",
		Dir:     q.dir,
		Clock:   queueClock{q.clock, q},
		Values:  NewValues(q.st, "instagram"),
		Traffic: q.traffic,
		Rand:    func() float64 { return 0 },
		Live:    func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.live },
		Ready:   func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.ready },
		Blocked: func() (wire.Kind, bool) {
			q.mu.Lock()
			defer q.mu.Unlock()
			return q.blocked, q.blocked != ""
		},
		Kind: wire.KindOf,
		Next: q.next,
		Left: func() (int, bool) { return 7, true },
	}
}

func (q *queue) next(ctx context.Context, passed func(chat string) bool) Step {
	q.mu.Lock()
	q.busy++
	if q.busy > 1 {
		q.t.Errorf("two Next calls at once")
	}
	n := len(q.calls) + 1
	a := asked{at: q.clock.Now()}
	for _, c := range q.chats {
		if passed(c) {
			a.passed = append(a.passed, c)
		} else if a.chat == "" {
			a.chat = c
		}
	}
	step := Step{Outcome: StepMoved}
	if len(q.script) > 0 {
		step, q.script = q.script[0], q.script[1:]
	}
	if step.Chat == "" && step.Outcome != StepNothing {
		step.Chat = a.chat
	}
	a.chat = step.Chat
	took, onNext, blind := q.took, q.onNext, q.blind
	q.mu.Unlock()

	// One request through the network's request, as a real step makes,
	// unless the step never reached it (nothing to page, no connection).
	k := wire.KindOf(step.Err)
	if step.Outcome != StepNothing && k != wire.NotConnected && k != wire.Offline {
		if blind {
			ctx = context.Background()
		}
		end := q.traffic.Begin(ctx, q.clock.Now())
		q.clock.Advance(took)
		end(q.clock.Now())
	}
	if onNext != nil {
		onNext(ctx, n)
	}

	q.mu.Lock()
	a.end = q.clock.Now()
	q.calls = append(q.calls, a)
	q.busy--
	if q.stopAt > 0 && len(q.calls) >= q.stopAt {
		q.stopping = true
	}
	q.mu.Unlock()
	return step
}

// run runs the queue until it has made stopAt calls (0: no limit), or the
// clock has moved d on, whichever comes first.
func (q *queue) run(d time.Duration, stopAt int) {
	q.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.mu.Lock()
	q.stopAt, q.stopping, q.cancel = stopAt, false, cancel
	q.mu.Unlock()
	stop := q.clock.AfterFunc(d, cancel)
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.b.Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		q.t.Fatalf("Run did not return")
	}
}

// queueClock is the queue's fake clock, which ends the run at its first
// sleep once stopAt is reached: the step is read as in any run, and the
// clock does not move on.
type queueClock struct {
	*testkit.Clock
	q *queue
}

func (c queueClock) Sleep(ctx context.Context, d time.Duration) error {
	c.q.mu.Lock()
	stopping, cancel := c.q.stopping, c.q.cancel
	c.q.mu.Unlock()
	if stopping {
		cancel()
		return context.Canceled
	}
	return c.Clock.Sleep(ctx, d)
}

func (q *queue) set(f func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	f()
}

func (q *queue) asked() []asked {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]asked(nil), q.calls...)
}

func errOf(kind wire.Kind) error { return &wire.Error{Kind: kind} }

// keptHold is hq.hold as kept in the store: its end and kind.
func (q *queue) keptHold() (time.Time, wire.Kind) {
	q.t.Helper()
	v, _, err := q.st.NetworkValue("instagram", "hq.hold")
	if err != nil {
		q.t.Fatal(err)
	}
	ms, kind, ok := strings.Cut(v, "\n")
	n, err := strconv.ParseInt(ms, 10, 64)
	if !ok || err != nil {
		q.t.Fatalf("hq.hold is %q, want <ms>\\n<kind>", v)
	}
	return time.UnixMilli(n), wire.Kind(kind)
}

func TestBacklogSpacesItsRequests(t *testing.T) {
	q := newQueue(t)
	q.run(24*time.Hour, 50)
	calls := q.asked()
	if len(calls) != 50 {
		t.Fatalf("%d calls, want 50", len(calls))
	}
	if d := calls[0].at.Sub(queueStart); d != BacklogMinGap {
		t.Errorf("the first call came %v after the run began, want %v", d, BacklogMinGap)
	}
	for i := 1; i < len(calls); i++ {
		if d := calls[i].at.Sub(calls[i-1].end); d != BacklogMinGap {
			t.Fatalf("call %d came %v after the last ended, want exactly %v", i, d, BacklogMinGap)
		}
	}

	q = newQueue(t)
	q.b.Rand = func() float64 { return 0.999 }
	q.run(24*time.Hour, 50)
	calls = q.asked()
	for i := 1; i < len(calls); i++ {
		if d := calls[i].at.Sub(calls[i-1].end); d < BacklogMinGap || d >= BacklogMaxGap {
			t.Fatalf("call %d came %v after the last ended, want from %v and under %v", i, d, BacklogMinGap, BacklogMaxGap)
		}
	}
}

func TestBacklogAsksNothingUntilLiveAndReady(t *testing.T) {
	for _, which := range []string{"live", "ready"} {
		t.Run(which, func(t *testing.T) {
			q := newQueue(t)
			flip := func(v bool) {
				q.set(func() {
					if which == "live" {
						q.live = v
					} else {
						q.ready = v
					}
				})
			}
			flip(false)
			q.clock.AfterFunc(10*time.Minute, func() { flip(true) })
			q.run(time.Hour, 1)
			calls := q.asked()
			if len(calls) != 1 {
				t.Fatalf("%d calls, want 1 once %s", len(calls), which)
			}
			on := queueStart.Add(10 * time.Minute)
			if d := calls[0].at.Sub(on); d < 15*time.Second || d > 45*time.Second {
				t.Errorf("the first call came %v after %s, want 15 to 45 s", d, which)
			}
		})
	}
}

func TestBacklogWaitsForQuiet(t *testing.T) {
	// Another request ends 3 s before the queue's gap is up: it waits 10 s
	// from that end.
	q := newQueue(t)
	var end func(time.Time)
	q.clock.AfterFunc(5*time.Second, func() { end = q.traffic.Begin(context.Background(), q.clock.Now()) })
	q.clock.AfterFunc(12*time.Second, func() { end(q.clock.Now()) })
	q.run(time.Hour, 1)
	calls := q.asked()
	if len(calls) != 1 || !calls[0].at.Equal(queueStart.Add(22*time.Second)) {
		t.Fatalf("calls %v, want one at 22 s", calls)
	}

	// One in flight: no call until 10 s after it ends.
	q = newQueue(t)
	q.clock.AfterFunc(time.Second, func() { end = q.traffic.Begin(context.Background(), q.clock.Now()) })
	q.clock.AfterFunc(100*time.Second, func() { end(q.clock.Now()) })
	q.run(time.Hour, 1)
	calls = q.asked()
	if len(calls) != 1 || !calls[0].at.Equal(queueStart.Add(110*time.Second)) {
		t.Fatalf("calls %v, want one at 110 s", calls)
	}
}

func TestBacklogYieldsAfterAnOlderRead(t *testing.T) {
	q := newQueue(t)
	at := queueStart.Add(20 * time.Second)
	q.clock.AfterFunc(20*time.Second, q.b.Yield)
	q.run(time.Hour, 2)
	calls := q.asked()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2", len(calls))
	}
	if !calls[1].at.Equal(at.Add(BacklogYield)) {
		t.Errorf("the call after the older read came at +%v, want +%v", calls[1].at.Sub(at), BacklogYield)
	}
}

func TestBacklogHoldsADayAfterPushback(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepPassed, Err: errOf(wire.RateLimited)}}
	q.run(48*time.Hour, 2)
	calls := q.asked()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2", len(calls))
	}
	until, kind := q.keptHold()
	if want := calls[0].end.Add(BacklogHold); !until.Equal(want) || kind != wire.RateLimited {
		t.Errorf("hq.hold is %v %s, want %v rateLimited", until, kind, want)
	}
	if calls[1].at.Before(until) {
		t.Errorf("a call came %v into the hold", calls[1].at.Sub(calls[0].end))
	}

	// A relaunch inside the hold waits for its end too.
	q = newQueue(t)
	q.script = []Step{{Outcome: StepPassed, Err: errOf(wire.RateLimited)}}
	q.run(48*time.Hour, 1)
	until, _ = q.keptHold()
	q.clock.Advance(time.Hour)
	q.relaunch()
	q.run(48*time.Hour, 2)
	calls = q.asked()
	if len(calls) != 2 || calls[1].at.Before(until.Add(BacklogMinGap)) {
		t.Fatalf("after a relaunch the next call came at %v, want from %v", calls[len(calls)-1].at, until.Add(BacklogMinGap))
	}
}

func TestBacklogHoldsADayWhenLiveWorkWasPushedBack(t *testing.T) {
	q := newQueue(t)
	seen := queueStart.Add(20 * time.Second)
	q.clock.AfterFunc(20*time.Second, func() { q.set(func() { q.blocked = wire.Checkpoint }) })
	q.clock.AfterFunc(time.Hour, func() { q.set(func() { q.blocked = "" }) })
	q.run(48*time.Hour, 2)
	calls := q.asked()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2", len(calls))
	}
	until, kind := q.keptHold()
	if kind != wire.Checkpoint || until.Before(seen.Add(BacklogHold)) {
		t.Errorf("hq.hold is %v %s, want a day from %v, checkpoint", until, kind, seen)
	}
	if calls[1].at.Before(until) {
		t.Errorf("a call came %v after the block, inside the hold", calls[1].at.Sub(seen))
	}
}

func TestBacklogStopsOnAnUnknownError(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepPassed, Err: errOf(wire.Internal)}}
	q.run(7*24*time.Hour, 0)
	if n := len(q.asked()); n != 1 {
		t.Fatalf("%d calls in 7 days after an unknown error, want 1", n)
	}
	// The same helper's next run stays stopped.
	q.run(24*time.Hour, 0)
	if n := len(q.asked()); n != 1 {
		t.Fatalf("a new run of the same helper asked again: %d calls", n)
	}
	// A relaunch starts again.
	q.relaunch()
	q.run(time.Hour, 2)
	if n := len(q.asked()); n != 2 {
		t.Fatalf("after a relaunch %d calls, want 2", n)
	}
}

func TestBacklogStopsAfterThreeTimeoutsInARow(t *testing.T) {
	timeout := Step{Outcome: StepPassed, Err: errOf(wire.Timeout)}
	q := newQueue(t)
	q.chats = []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"}
	q.script = []Step{timeout, timeout, {Outcome: StepMoved}, timeout, timeout}
	q.run(24*time.Hour, 5)
	if n := len(q.asked()); n != 5 || q.b.stopped != "" {
		t.Fatalf("after %d calls it stopped with %q, want still running", n, q.b.stopped)
	}
	q.set(func() { q.script = []Step{timeout} })
	q.run(7*24*time.Hour, 0)
	if n := len(q.asked()); n != 6 || q.b.stopped != wire.Timeout {
		t.Fatalf("%d calls, stopped with %q; want none after a third timeout in a row, stopped with timeout", n, q.b.stopped)
	}
}

func TestBacklogNeverRetries(t *testing.T) {
	kinds := []wire.Kind{"", wire.RateLimited, wire.Checkpoint, wire.Timeout, wire.Offline, wire.NotConnected,
		wire.Internal, wire.NotFound}
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			q := newQueue(t)
			first := Step{Outcome: StepPassed}
			if kind != "" {
				first.Err = errOf(kind)
			}
			q.script = []Step{first}
			q.run(3*24*time.Hour, 2)
			calls := q.asked()
			if len(calls) < 2 {
				return // stopped: never asked again
			}
			from := calls[0].end.Add(BacklogMinGap)
			if kind == wire.RateLimited || kind == wire.Checkpoint {
				from = calls[0].end.Add(BacklogHold)
			}
			if calls[1].at.Before(from) {
				t.Errorf("asked again %v after the error", calls[1].at.Sub(calls[0].end))
			}
			if kind != wire.Offline && kind != wire.NotConnected && calls[1].chat == calls[0].chat {
				t.Errorf("asked the passed-over chat again")
			}
		})
	}
}

func TestBacklogIdlesWhileOffline(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepPassed, Err: errOf(wire.Offline)}}
	q.onNext = func(_ context.Context, n int) {
		if n == 1 {
			q.set(func() { q.live = false })
			q.clock.AfterFunc(5*time.Minute, func() { q.set(func() { q.live = true }) })
		}
	}
	q.run(time.Hour, 2)
	calls := q.asked()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want a second once live", len(calls))
	}
	if back := calls[0].end.Add(5 * time.Minute); calls[1].at.Before(back) {
		t.Errorf("asked %v before it was live again", back.Sub(calls[1].at))
	}
	if q.b.today.pages != 1 || q.b.stopped != "" {
		t.Errorf("%d pages counted, want only the second call's; stopped %q", q.b.today.pages, q.b.stopped)
	}
}

func TestBacklogStopsWhenAStepAsksTwice(t *testing.T) {
	q := newQueue(t)
	q.onNext = func(ctx context.Context, n int) {
		if n == 1 {
			q.traffic.Begin(ctx, q.clock.Now())(q.clock.Now())
		}
	}
	q.run(7*24*time.Hour, 0)
	if n := len(q.asked()); n != 1 {
		t.Fatalf("%d calls after a step asked twice, want 1", n)
	}
	if q.b.stopped != wire.Internal {
		t.Errorf("stopped with %q, want internal", q.b.stopped)
	}
}

func TestBacklogStopsAtTheDayCap(t *testing.T) {
	q := newQueue(t)
	q.took = 0
	q.run(48*time.Hour, BacklogDayCap+1)
	calls := q.asked()
	if len(calls) != BacklogDayCap+1 {
		t.Fatalf("%d calls, want %d", len(calls), BacklogDayCap+1)
	}
	day := queueStart.Format("2006-01-02")
	for _, c := range calls[:BacklogDayCap] {
		if c.at.Local().Format("2006-01-02") != day {
			t.Fatalf("a capped call came on %v", c.at)
		}
	}
	midnight := time.Date(2026, 6, 11, 0, 0, 0, 0, time.Local)
	if last := calls[BacklogDayCap]; last.at.Before(midnight) {
		t.Errorf("call %d came at %v, before the local midnight", BacklogDayCap+1, last.at)
	}
}

func TestBacklogPassesOverAChatForTheRun(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepPassed}, {Outcome: StepMoved}, {Outcome: StepMoved}, {Outcome: StepNothing}}
	q.run(24*time.Hour, 5)
	calls := q.asked()
	if len(calls) != 5 {
		t.Fatalf("%d calls, want 5", len(calls))
	}
	gone := calls[0].chat
	for i, c := range calls[1:4] {
		if len(c.passed) != 1 || c.passed[0] != gone {
			t.Errorf("call %d sees passed over %v, want %s", i+2, c.passed, gone)
		}
	}
	if len(calls[4].passed) != 0 {
		t.Errorf("after nothing was left, %v is still passed over", calls[4].passed)
	}

	q = newQueue(t)
	q.script = []Step{{Outcome: StepPassed}}
	q.run(24*time.Hour, 1)
	q.relaunch()
	q.run(24*time.Hour, 2)
	if p := q.asked()[1].passed; len(p) != 0 {
		t.Errorf("a relaunch sees %v passed over", p)
	}
}

func TestBacklogWaitsWhenNothingIsLeft(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepNothing}}
	q.run(time.Hour, 2)
	calls := q.asked()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2", len(calls))
	}
	if d := calls[1].at.Sub(calls[0].end); d != BacklogEmpty {
		t.Errorf("the call after nothing was left came %v later, want %v", d, BacklogEmpty)
	}
}

func TestBacklogEndsWithItsRun(t *testing.T) {
	q := newQueue(t)
	q.b.Clock = RealClock{}
	q.set(func() { q.live = false })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		q.b.Run(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Run went on after its context ended")
	}
}

// dayLines are the history queue's lines in the network's diagnostics
// file, as written.
func (q *queue) dayLines() []string {
	q.t.Helper()
	b, err := os.ReadFile(filepath.Join(q.dir, "instagram", diag.FileName))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		q.t.Fatal(err)
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.Contains(l, "history queue") {
			out = append(out, l)
		}
	}
	return out
}

var dayLineShape = regexp.MustCompile(`^\S+ history queue on \d{4}-\d{2}-\d{2}: pages \d+, chats finished \d+, chats left (\d+|unknown), minutes running \d+, paused (no|yes \w+), stopped (no|yes \w+)$`)

const day1 = "2026-06-10"

func TestBacklogWritesOneDayLinePerLocalDay(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepMoved}, {Outcome: StepMoved}, {Outcome: StepMoved}, {Outcome: StepStart}}
	q.onNext = func(_ context.Context, n int) {
		if n == 4 {
			q.set(func() { q.live = false })
		}
	}
	q.run(24*time.Hour, 0) // to 01:00 the next day
	lines := q.dayLines()
	if len(lines) != 1 {
		t.Fatalf("lines %q, want one", lines)
	}
	want := "history queue on " + day1 + ": pages 4, chats finished 1, chats left 7, minutes running 1, paused no, stopped no"
	if !strings.HasSuffix(lines[0], " "+want) {
		t.Errorf("the line is %q, want it to end %q", lines[0], want)
	}
}

func TestBacklogDayLineSurvivesARelaunch(t *testing.T) {
	q := newQueue(t)
	q.run(time.Hour, 2)
	if lines := q.dayLines(); len(lines) != 0 {
		t.Fatalf("a line before the day ended: %q", lines)
	}

	// The next day, a new helper writes yesterday's line once.
	q.clock.Advance(time.Date(2026, 6, 11, 9, 0, 0, 0, time.Local).Sub(q.clock.Now()))
	q.relaunch()
	q.run(time.Hour, 1)
	lines := q.dayLines()
	if len(lines) != 1 || !strings.Contains(lines[0], "history queue on "+day1+": pages 2,") {
		t.Fatalf("lines %q, want one for %s with 2 pages", lines, day1)
	}

	// A third start that day writes nothing more, and goes on from the
	// day's counts.
	q.clock.Advance(time.Hour)
	q.relaunch()
	q.set(func() { q.live = false })
	q.run(time.Hour, 0)
	if again := q.dayLines(); len(again) != 1 {
		t.Errorf("lines %q after a third start, want still one", again)
	}
	if q.b.today.pages != 1 {
		t.Errorf("the third start holds %d pages for the day, want the 1 kept", q.b.today.pages)
	}
}

func TestBacklogDayLineSaysPausedAndStopped(t *testing.T) {
	q := newQueue(t)
	// The step fails with an error the queue does not know, and a send was
	// rate-limited meanwhile.
	q.script = []Step{{Outcome: StepPassed, Err: errOf(wire.Internal)}}
	q.onNext = func(_ context.Context, n int) {
		q.set(func() { q.blocked = wire.RateLimited })
	}
	q.run(24*time.Hour, 0)
	lines := q.dayLines()
	if len(lines) != 1 || !strings.HasSuffix(lines[0], "paused yes rateLimited, stopped yes internal") {
		t.Fatalf("lines %q, want one ending paused yes rateLimited, stopped yes internal", lines)
	}
}

func TestBacklogDayLineHoldsOnlyCounts(t *testing.T) {
	q := newQueue(t)
	q.chats = []string{"chat-invented-41", "chat-invented-42", "chat-invented-43", "chat-invented-44"}
	q.script = []Step{{Outcome: StepMoved}, {Outcome: StepStart}, {Outcome: StepPassed},
		{Outcome: StepPassed, Err: errOf(wire.Timeout)}, {Outcome: StepNothing}, {Outcome: StepMoved},
		{Outcome: StepPassed, Err: errOf(wire.Checkpoint)}}
	q.b.Left = func() (int, bool) { return 0, false }
	q.run(3*24*time.Hour, 0)
	lines := q.dayLines()
	if len(lines) < 2 {
		t.Fatalf("lines %q, want one a day", lines)
	}
	for _, l := range lines {
		if !dayLineShape.MatchString(l) {
			t.Errorf("%q is not a counts-only line", l)
		}
		for _, c := range q.chats {
			if strings.Contains(l, c) || strings.Contains(l, strings.TrimPrefix(c, "chat-invented-")+",") {
				t.Errorf("%q names a chat", l)
			}
		}
	}
}

func TestBacklogWritesADayLineWhenItAskedNothing(t *testing.T) {
	q := newQueue(t)
	q.set(func() { q.live = false })
	q.run(24*time.Hour, 0)
	lines := q.dayLines()
	if len(lines) != 1 || !strings.HasSuffix(lines[0], "history queue on "+day1+": pages 0, chats finished 0, chats left 7, minutes running 0, paused no, stopped no") {
		t.Fatalf("lines %q, want one for %s with nothing asked", lines, day1)
	}
}

func TestBacklogHoldsADayAfterPushbackItDidNotSee(t *testing.T) {
	// Pushed back with no Run (the run had ended, as a LinkedIn pause ends
	// it): kept at once, and the next Run waits it out.
	q := newQueue(t)
	q.b.PushedBack(wire.RateLimited)
	until, kind := q.keptHold()
	if !until.Equal(queueStart.Add(BacklogHold)) || kind != wire.RateLimited {
		t.Fatalf("hq.hold is %v %s, want a day from now, rateLimited", until, kind)
	}
	q.run(48*time.Hour, 1)
	if calls := q.asked(); len(calls) != 1 || !calls[0].at.Equal(until.Add(BacklogMinGap)) {
		t.Fatalf("calls %v, want the first %v after the hold", calls, BacklogMinGap)
	}

	// A relaunch inside it waits too.
	q = newQueue(t)
	q.b.PushedBack(wire.Checkpoint)
	q.relaunch()
	q.run(48*time.Hour, 1)
	if calls := q.asked(); len(calls) != 1 || calls[0].at.Before(queueStart.Add(BacklogHold)) {
		t.Fatalf("after a relaunch calls %v, want none inside the hold", calls)
	}

	// Pushed back while the queue sleeps out a hold: the hold runs a day
	// from the new pushback.
	q = newQueue(t)
	q.script = []Step{{Outcome: StepPassed, Err: errOf(wire.RateLimited)}}
	again := queueStart.Add(23 * time.Hour)
	q.clock.AfterFunc(23*time.Hour, func() { q.b.PushedBack(wire.Checkpoint) })
	q.run(72*time.Hour, 2)
	calls := q.asked()
	if len(calls) != 2 || calls[1].at.Before(again.Add(BacklogHold)) {
		t.Fatalf("calls %v, want the second a day after the later pushback", calls)
	}
	if until, kind := q.keptHold(); !until.Equal(again.Add(BacklogHold)) || kind != wire.Checkpoint {
		t.Errorf("hq.hold is %v %s, want %v checkpoint", until, kind, again.Add(BacklogHold))
	}
}

func TestBacklogStopsWhenAPushbackCannotBeKept(t *testing.T) {
	q := newQueue(t)
	q.b.Values.Get("hq.hold")
	q.st.Close()
	q.b.PushedBack(wire.RateLimited)
	if q.b.stopped != wire.Internal {
		t.Fatalf("a pushback that could not be kept left the queue %q, want stopped internal", q.b.stopped)
	}
	q.run(7*24*time.Hour, 0)
	if n := len(q.asked()); n != 0 || q.b.stopped != wire.Internal {
		t.Fatalf("%d calls, stopped %q; want none, stopped internal", n, q.b.stopped)
	}
}

func TestBacklogStopsWhenItCannotKeepItsDay(t *testing.T) {
	q := newQueue(t)
	q.onNext = func(_ context.Context, n int) {
		if n == 1 {
			q.st.Close()
		}
	}
	q.run(7*24*time.Hour, 0)
	if n := len(q.asked()); n != 1 || q.b.stopped != wire.Internal {
		t.Fatalf("%d calls, stopped %q; want 1, stopped internal", n, q.b.stopped)
	}
}

func TestBacklogStopsWhenAStepWasNotCounted(t *testing.T) {
	for _, out := range []StepOutcome{StepMoved, StepStart} {
		q := newQueue(t)
		q.blind = true
		q.script = []Step{{Outcome: out}}
		q.run(7*24*time.Hour, 0)
		if n := len(q.asked()); n != 1 || q.b.stopped != wire.Internal {
			t.Fatalf("outcome %d with no request counted: %d calls, stopped %q; want 1, internal", out, n, q.b.stopped)
		}
	}
}

func TestBacklogStopsOnAnErrorWithNoKind(t *testing.T) {
	q := newQueue(t)
	q.b.Kind = func(error) wire.Kind { return "" }
	q.script = []Step{{Outcome: StepPassed, Err: errors.New("invented failure")}}
	q.run(7*24*time.Hour, 0)
	if n := len(q.asked()); n != 1 || q.b.stopped != wire.Internal {
		t.Fatalf("%d calls, stopped %q; want 1, internal", n, q.b.stopped)
	}
}

func TestBacklogWaitsForTheRunBeforeIt(t *testing.T) {
	q := newQueue(t)
	entered, release := make(chan struct{}), make(chan struct{})
	q.onNext = func(_ context.Context, n int) {
		if n == 1 {
			close(entered)
			<-release
		}
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	first := make(chan struct{})
	go func() {
		defer close(first)
		q.b.Run(ctx1)
	}()
	<-entered
	cancel1() // the run ends while its step is still out

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	q.set(func() { q.stopAt, q.stopping, q.cancel = 2, false, cancel2 })
	second := make(chan struct{})
	go func() {
		defer close(second)
		q.b.Run(ctx2)
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	for _, done := range []chan struct{}{first, second} {
		select {
		case <-done:
		case <-time.After(time.Minute):
			t.Fatalf("a Run did not return")
		}
	}
	if n := len(q.asked()); n != 2 {
		t.Fatalf("%d calls, want the next run to go on once the one before it returned", n)
	}
}

func TestBacklogKeepsCountingWhenTheDateGoesBack(t *testing.T) {
	q := newQueue(t)
	if err := q.st.PutNetworkValue("instagram", "hq.day", "2026-06-11\n5\n0\n3\n\n"); err != nil {
		t.Fatal(err)
	}
	q.run(time.Hour, 1)
	if q.b.today.date != "2026-06-11" || q.b.today.pages != 6 {
		t.Errorf("the day is %s with %d pages, want 2026-06-11 with 6", q.b.today.date, q.b.today.pages)
	}
	if lines := q.dayLines(); len(lines) != 0 {
		t.Errorf("lines %q, want none", lines)
	}
}

func TestBacklogHoldsNoMoreThanADay(t *testing.T) {
	q := newQueue(t)
	far := queueStart.Add(30 * 24 * time.Hour).UnixMilli()
	if err := q.st.PutNetworkValue("instagram", "hq.hold", strconv.FormatInt(far, 10)+"\nrateLimited"); err != nil {
		t.Fatal(err)
	}
	q.run(48*time.Hour, 1)
	if calls := q.asked(); len(calls) != 1 || !calls[0].at.Equal(queueStart.Add(BacklogHold+BacklogMinGap)) {
		t.Fatalf("calls %v, want one a day and %v on", calls, BacklogMinGap)
	}
}

func TestBacklogMayAskOnlyWhenNotHeldStoppedOrYielding(t *testing.T) {
	q := newQueue(t)
	if !q.b.MayAsk() {
		t.Fatalf("a new queue may not ask")
	}
	q.b.Yield()
	if q.b.MayAsk() {
		t.Errorf("it may ask just after an older read")
	}
	q.clock.Advance(BacklogYield)
	if !q.b.MayAsk() {
		t.Errorf("it may not ask once the yield is over")
	}
	q.b.PushedBack(wire.RateLimited)
	if q.b.MayAsk() {
		t.Errorf("it may ask while held")
	}
	q = newQueue(t)
	q.b.stop(wire.Internal)
	if q.b.MayAsk() {
		t.Errorf("it may ask once stopped")
	}
}

// withList gives the queue a listing of older chats: listed reports it done
// after done list steps (0: never). It records which calls were list steps.
func (q *queue) withList(done int) *[]string {
	var mu sync.Mutex
	var order []string
	lists := 0
	next := q.b.Next
	q.b.Next = func(ctx context.Context, passed func(string) bool) Step {
		mu.Lock()
		order = append(order, "chat")
		mu.Unlock()
		return next(ctx, passed)
	}
	q.b.List = func(ctx context.Context) Step {
		mu.Lock()
		order = append(order, "list")
		lists++
		mu.Unlock()
		end := q.traffic.Begin(ctx, q.clock.Now())
		q.clock.Advance(q.took)
		end(q.clock.Now())
		q.mu.Lock()
		q.calls = append(q.calls, asked{at: q.clock.Now(), end: q.clock.Now(), chat: "list"})
		if q.stopAt > 0 && len(q.calls) >= q.stopAt {
			q.stopping = true
		}
		q.mu.Unlock()
		return Step{Outcome: StepMoved}
	}
	q.b.Listed = func() bool {
		mu.Lock()
		defer mu.Unlock()
		return done > 0 && lists >= done
	}
	return &order
}

func TestBacklogListsAfterEveryFiveChatSteps(t *testing.T) {
	q := newQueue(t)
	order := q.withList(0)
	q.run(24*time.Hour, 12)
	got := strings.Join(*order, " ")
	want := "chat chat chat chat chat list chat chat chat chat chat list"
	if got != want {
		t.Errorf("the steps were %q, want %q", got, want)
	}
	calls := q.asked()
	for i := 1; i < len(calls); i++ {
		if d := calls[i].at.Sub(calls[i-1].end); d < BacklogMinGap {
			t.Fatalf("step %d came %v after the last, want the gap", i, d)
		}
	}
}

func TestBacklogListsWhenNoChatIsLeft(t *testing.T) {
	q := newQueue(t)
	q.script = []Step{{Outcome: StepNothing}}
	order := q.withList(0)
	q.run(24*time.Hour, 2)
	if got := strings.Join(*order, " "); got != "chat list" {
		t.Errorf("the steps were %q, want a list step straight after nothing was left", got)
	}
	if calls := q.asked(); len(calls) != 2 || calls[1].at.Sub(calls[0].end) >= BacklogEmpty {
		t.Errorf("the list step waited the empty wait")
	}
}

func TestBacklogStopsListingOnceListed(t *testing.T) {
	q := newQueue(t)
	order := q.withList(1)
	q.run(24*time.Hour, 13)
	if n := strings.Count(strings.Join(*order, " "), "list"); n != 1 {
		t.Errorf("%d list steps, want 1 then none once listed: %v", n, *order)
	}
}
