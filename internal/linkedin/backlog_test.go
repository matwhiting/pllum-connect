package linkedin

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue's tests. Every ID and text here is invented.

// queueClock is the history queue's clock in these tests: the rig's fake
// clock, but a sleep waits for something to move it.
type queueClock struct {
	*fakeClock
	mu     sync.Mutex
	sleeps int
	woke   int
}

func (c *queueClock) Sleep(ctx context.Context, d time.Duration) error {
	ch := make(chan struct{})
	c.mu.Lock()
	c.sleeps++
	c.mu.Unlock()
	stop := c.fakeClock.AfterFunc(d, func() {
		c.mu.Lock()
		c.woke++
		c.mu.Unlock()
		close(ch)
	})
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		stop()
		return ctx.Err()
	}
}

func (c *queueClock) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sleeps, c.woke
}

// newQueueRig is a rig whose network has a history queue.
func newQueueRig(t *testing.T) *rig {
	t.Helper()
	r := newRig(t)
	r.queue = true
	r.restart()
	return r
}

// startQueue lets the queue be ready and waits for it to park.
func (r *rig) startQueue() {
	r.t.Helper()
	r.queueOn.Store(true)
	eventually(r.t, "the queue to start", func() bool { s, _ := r.qc.counts(); return s > 0 })
	r.parked()
}

func (r *rig) parked() {
	r.t.Helper()
	eventually(r.t, "the queue to park", func() bool {
		s, w := r.qc.counts()
		return s > w || r.l.bqRuns.Load() == 0
	})
}

// advance moves the clock on by total, step at a time, letting the queue
// (and the network's own work) go on after each.
func (r *rig) advance(total, step time.Duration) {
	r.t.Helper()
	for d := time.Duration(0); d < total; d += step {
		r.clock.Advance(step)
		r.parked()
		r.idle()
	}
}

// pagesOf are the history pages asked of conversation n since the i-th call.
func (r *rig) pagesOf(n, i int) int {
	c := 0
	for _, call := range r.f.callsNow()[i:] {
		if (call.name == "GetMessagesWithPrevCursor" || call.name == "GetMessagesBefore") && strings.HasPrefix(call.arg, convURN(n)) {
			c++
		}
	}
	return c
}

func TestTheQueuePagesPastTwoYears(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 800)
	r.live()
	if got := r.value("histDone." + convURN(1)); got != histReach {
		t.Fatalf("the worker stopped with %q, want %q", got, histReach)
	}
	i := len(r.f.callsNow())
	r.startQueue()
	r.advance(10*time.Minute, time.Second)
	if !r.chat(convURN(1)).HistoryComplete {
		t.Fatalf("the chat is not historyComplete after the queue")
	}
	pages := r.pagesOf(1, i)
	if pages == 0 || r.l.traffic.BacklogAsked() != int64(pages) {
		t.Errorf("the queue asked %d pages and counted %d", pages, r.l.traffic.BacklogAsked())
	}
	if ch := r.chat(convURN(1)); ch.HistoryFromMs > start.Add(-790*24*time.Hour).UnixMilli() {
		t.Errorf("history reaches back to %v, want to its start", time.UnixMilli(ch.HistoryFromMs))
	}
}

func TestTheQueueWaitsForTheWorker(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 800)
	r.live()
	r.queueOn.Store(true)
	if !r.l.bq.Ready() {
		t.Fatalf("not ready with the worker done")
	}
	for _, tc := range []struct{ name, value string }{{"histDone.", ""}, {"fresh.", "1"}} {
		was := r.value(tc.name + convURN(1))
		_ = r.l.keep(tc.name+convURN(1), tc.value)
		if r.l.bq.Ready() {
			t.Errorf("ready with %s%s = %q", tc.name, convURN(1), tc.value)
		}
		_ = r.l.keep(tc.name+convURN(1), was)
	}
	_ = r.l.keep("listed", "")
	if r.l.bq.Ready() {
		t.Errorf("ready with the listing owed")
	}
	_ = r.l.keep("listed", "1")

	// A chat within two years the worker can't page now: nothing from the
	// queue for an hour.
	_ = r.l.keep("histDone."+convURN(1), "")
	r.f.setErr("GetMessagesWithPrevCursor", statusErr(500))
	i := len(r.f.callsNow())
	asked := r.l.traffic.BacklogAsked()
	eventually(t, "the queue to start", func() bool { s, _ := r.qc.counts(); return s > 0 })
	r.advance(time.Hour, 30*time.Second)
	if r.l.traffic.BacklogAsked() != asked {
		t.Errorf("the queue asked while the worker had history left (%d pages in all)", r.pagesOf(1, i))
	}
}

func TestA999PausesLinkedInAndHoldsTheQueueADay(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 1500)
	r.live()
	r.f.setErr("GetMessagesWithPrevCursor", statusErr(999))
	r.startQueue()
	r.advance(time.Minute, time.Second)
	r.waitState(network.Offline, wire.RateLimited)
	if r.l.traffic.BacklogAsked() != 1 {
		t.Fatalf("the queue asked %d, want the paused page", r.l.traffic.BacklogAsked())
	}
	v := r.value("hq.hold")
	if _, kind, _ := strings.Cut(v, "\n"); kind != string(wire.RateLimited) {
		t.Fatalf("hq.hold is %q", v)
	}
	// LinkedIn's own pause ends and its run resumes; the queue holds a day.
	r.f.setErr("GetMessagesWithPrevCursor", nil)
	r.advance(16*time.Minute, time.Minute)
	r.waitState(network.Live, "")
	r.advance(23*time.Hour, 10*time.Minute)
	if n := r.l.traffic.BacklogAsked(); n != 1 {
		t.Errorf("the queue asked %d pages within the day", n-1)
	}
	r.advance(2*time.Hour, time.Minute)
	if r.l.traffic.BacklogAsked() < 2 {
		t.Errorf("the queue asked nothing once the day had passed")
	}
}

func TestAnOldMarkIsAskedOnce(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 1500)
	r.live()
	if r.value("cursor."+convURN(1)) == "" {
		t.Fatalf("no cursor kept")
	}
	_ = r.l.keep("histDone."+convURN(1), "1") // as an older version marked it
	r.f.setErr("GetMessagesWithPrevCursor", statusErr(404))
	i := len(r.f.callsNow())
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	if p := r.pagesOf(1, i); p != 1 {
		t.Errorf("the old mark was asked %d times, want once", p)
	}
	if got := r.value("histDone." + convURN(1)); got != histGone {
		t.Errorf("histDone = %q, want %q", got, histGone)
	}
}

func TestALiveWorkPushbackHoldsTheQueueADay(t *testing.T) {
	for _, code := range []int{999, 403, 429} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			r := newQueueRig(t)
			historyFeed(r, 1500)
			r.live()
			chat := r.heldChat(9)
			r.f.setErr("SendMessage", statusErr(code))
			id := r.send(network.Outgoing{ChatID: chat, Text: "invented hello"})
			r.waitSend(id, network.SendFailed, wire.RateLimited)
			pushed := r.clock.Now()
			v := r.value("hq.hold")
			ms, kind, _ := strings.Cut(v, "\n")
			n, _ := strconv.ParseInt(ms, 10, 64)
			until := time.UnixMilli(n)
			if kind != string(wire.RateLimited) || until.Before(pushed.Add(network.BacklogHold-time.Minute)) {
				t.Fatalf("hq.hold is %q, want a day ahead, rateLimited", v)
			}

			// A new helper after LinkedIn's own pause: its queue asks nothing
			// until the day has passed.
			r.f.setErr("SendMessage", nil)
			r.restart()
			r.clock.Advance(16 * time.Minute)
			r.live()
			asked := r.l.traffic.BacklogAsked()
			r.startQueue()
			r.advance(until.Sub(r.clock.Now())-time.Minute, 10*time.Minute)
			if r.l.traffic.BacklogAsked() != asked {
				t.Fatalf("the queue asked inside the hold")
			}
			r.advance(10*time.Minute, time.Second)
			if r.l.traffic.BacklogAsked() == asked {
				t.Fatalf("the queue asked nothing once the hold had passed")
			}
		})
	}
}

func TestAQueuePageLeavesOrderAndUnreadAlone(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 800)
	r.live()
	other := r.heldChat(9)
	_ = r.l.keep("histDone."+other, histGone) // a second chat, settled, so the worker has nothing within reach
	order := func() string {
		p, err := r.st.Chats(Name, 0, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, c := range p.Items {
			out = append(out, c.ID+"/"+strconv.FormatInt(c.LastActivityMs, 10)+"/"+strconv.Itoa(c.Unread))
		}
		return strings.Join(out, " ")
	}
	before := order()
	r.startQueue()
	r.advance(10*time.Minute, time.Second)
	if r.l.traffic.BacklogAsked() == 0 {
		t.Fatalf("the queue asked nothing")
	}
	if after := order(); after != before {
		t.Errorf("the chats were\n%s\nafter the queue\n%s", before, after)
	}
}

// A queue request goes only once quiet (media fetches included), and not
// while the queue yields.
func TestAQueueRequestNeverGoesBesideAnother(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 800)
	c := r.live()
	r.l.mu.Lock()
	gen := r.l.runs.Gen
	r.l.mu.Unlock()
	ask := func() error {
		return r.l.request(network.WithBacklog(context.Background()), gen, true, func(ctx context.Context) error {
			_, err := c.GetMessagesBefore(ctx, linkedingo.NewURN(convURN(1)), r.clock.Now(), 1)
			return err
		})
	}
	r.clock.Advance(network.BacklogQuiet)
	i := len(r.f.callsNow())

	// A media fetch is LinkedIn traffic too: in flight until its body is read.
	var during bool
	r.f.download = func(string) (*http.Response, error) {
		during = !r.l.traffic.Quiet(r.clock.Now(), 0)
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}
	resp, err := r.l.download(context.Background(), c, "https://media.invented.example/one")
	if err != nil {
		t.Fatal(err)
	}
	if !during || r.l.traffic.Quiet(r.clock.Now(), 0) {
		t.Errorf("a media fetch was not in flight (%v), or still is", during)
	}
	_ = resp.Body.Close()
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request just after a media fetch = %v", err)
	}
	r.clock.Advance(network.BacklogQuiet)
	r.l.ScrolledBack()
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request during a scroll-back = %v", err)
	}
	if p := r.pagesOf(1, i); p != 0 {
		t.Fatalf("the queue asked %d pages", p)
	}
	r.clock.Advance(network.BacklogYield)
	if err := ask(); err != nil {
		t.Errorf("a queue request once quiet = %v", err)
	}
}

// A page the queue's counter did not see is reported as the page it was,
// for the engine's guard to judge.
func TestAnUncountedPageIsNotPassedOver(t *testing.T) {
	r := newQueueRig(t)
	historyFeed(r, 1500)
	r.live()
	r.clock.Advance(network.BacklogQuiet)
	asked := r.l.traffic.BacklogAsked()
	s := r.l.backlogNext(context.Background(), func(string) bool { return false })
	if r.l.traffic.BacklogAsked() != asked {
		t.Fatalf("a request outside the queue's context was counted as the queue's")
	}
	if s.Outcome != network.StepMoved || s.Err != nil {
		t.Errorf("the uncounted page is %+v, want StepMoved", s)
	}
}
