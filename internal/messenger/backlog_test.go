package messenger

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue's tests. Every ID and text here is invented.

// queueClock is the history queue's clock in these tests: the fakes'
// clock, but a sleep waits for something to move it, so the queue looks
// again only when the test (or the pace) lets it.
type queueClock struct {
	*fakeClock
	mu     sync.Mutex
	sleeps int // sleeps begun
	woke   int // sleeps that reached their end
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

func (c *queueClock) counts() (sleeps, woke int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sleeps, c.woke
}

// withQueue gives the harness's network a history queue on a queueClock,
// ready only once the test sets queueOn, with no gate. ready keeps the
// listing and the first backfill as done, as an earlier run left them.
func withQueue(ready bool) func(*harness) {
	return func(h *harness) {
		h.m.gate = false
		h.m.WithHistoryQueue()
		h.qc = &queueClock{fakeClock: h.f.clock}
		h.m.bq.Clock = h.qc
		r := h.m.bq.Ready
		h.m.bq.Ready = func() bool { return h.queueOn.Load() && r() }
		if ready {
			for _, name := range []string{"listed", "backfilled"} {
				if err := h.st.PutNetworkValue(Name, name, "1"); err != nil {
					panic(err)
				}
			}
		}
	}
}

func newQueueHarness(t *testing.T, dir string, f *fakeMeta) *harness {
	t.Helper()
	if dir == "" {
		dir = t.TempDir() + "/connect"
	}
	return newHarnessOver(t, dir, f, withQueue(true))
}

// startQueue lets the queue be ready and waits for it to park.
func (h *harness) startQueue(t *testing.T) {
	t.Helper()
	h.queueOn.Store(true)
	eventually(t, "the queue to start", func() bool { s, _ := h.qc.counts(); return s > 0 })
	h.parked(t)
}

// parked waits until the queue is asleep or its Run has returned.
func (h *harness) parked(t *testing.T) {
	t.Helper()
	eventually(t, "the queue to park", func() bool {
		s, w := h.qc.counts()
		return s > w || h.m.bqRuns.Load() == 0
	})
}

// advance moves the clock on by total, step at a time, and lets the queue
// look again (and ask, if it may) after each.
func (h *harness) advance(t *testing.T, total, step time.Duration) {
	t.Helper()
	for d := time.Duration(0); d < total; d += step {
		h.f.clock.Advance(step)
		h.parked(t)
		h.settle()
	}
}

// mark is how many requests have been made.
func (f *fakeMeta) mark() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// since are the names of the requests made since the n-th.
func (f *fakeMeta) since(n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls[n:] {
		out = append(out, c.name)
	}
	return out
}

// pagesOf are the FetchMessagesTasks asked of a thread since the n-th.
func (f *fakeMeta) pagesOf(key int64, n int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := 0
	for _, t := range f.messageTasks[n:] {
		if t.ThreadKey == key {
			c++
		}
	}
	return c
}

func (f *fakeMeta) taskMark() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messageTasks)
}

// withGroupLines is the inbox with a line of each group's thread held, so
// the groups' older pages have something to be asked from.
func withGroupLines(f *fakeMeta) {
	at := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC)
	tbl := inbox()
	for _, g := range []int64{groupOne, groupTwo} {
		h := history(g, true, line(fmt.Sprintf("mid.$invented-q%d-0", g%100), aliceFBID, at, canary))
		tbl.LSUpsertMessage = append(tbl.LSUpsertMessage, h.LSUpsertMessage...)
		tbl.LSInsertNewMessageRange = append(tbl.LSInsertNewMessageRange, h.LSInsertNewMessageRange...)
	}
	f.table = tbl
}

// olderPages scripts n older pages of a group's thread, each a day older,
// the last saying there is nothing older.
func olderPages(f *fakeMeta, key int64, n int) {
	at := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC)
	for i := 1; i <= n; i++ {
		f.older[key] = append(f.older[key], history(key, i < n,
			line(fmt.Sprintf("mid.$invented-q%d-%d", key%100, i), aliceFBID, at.Add(-time.Duration(i)*24*time.Hour), canary)))
	}
}

func TestTheQueuePagesAGroupsThreadToItsStart(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 3)
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	n := f.taskMark()
	h.startQueue(t)
	h.advance(t, 10*time.Minute, time.Second)
	if got := f.pagesOf(groupOne, n); got != 3 {
		t.Errorf("the queue asked %d pages of the group, want 3", got)
	}
	if !h.chat(t, groupChat).HistoryComplete {
		t.Errorf("the group is not historyComplete")
	}
	if asked := h.m.traffic.BacklogAsked(); asked < 3 {
		t.Errorf("the queue counted %d requests", asked)
	}
}

func TestTheQueueReadsTheBackupThenTheThread(t *testing.T) {
	f := newFakeMeta()
	at := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC)
	// A line of Alice's older thread held, and two pages of it older.
	tbl := inbox()
	hl := history(aliceFBID, true, line("mid.$invented-qa-0", aliceFBID, at, canary))
	tbl.LSUpsertMessage, tbl.LSInsertNewMessageRange = hl.LSUpsertMessage, hl.LSInsertNewMessageRange
	f.table = tbl
	olderPages(f, aliceFBID, 2)
	// Her backup: pages enough for the first restore's read and the queue.
	var pages []fakePage
	for i := 1; i <= 8; i++ {
		pages = append(pages, fakePage{more: i < 8, before: at.Add(-time.Duration(i) * time.Hour).UnixMilli(),
			msgs: []*securehistory.DecryptedMessage{archived(fmt.Sprintf("WAINVENTEDQ%d", i), aliceFBID, at.Add(-time.Duration(i)*time.Hour), textContent(canary))}})
	}
	f.history.script(fmt.Sprint(aliceFBID), pages...)
	h := newQueueHarness(t, "", f)
	restored(t, h)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	if h.m.value("encDone."+alice) != "" {
		t.Fatalf("her backup was done before the queue")
	}
	n, tn := f.mark(), f.taskMark()
	h.startQueue(t)
	h.advance(t, 15*time.Minute, time.Second)

	if h.m.value("encDone."+alice) == "" || h.m.value("olderDone."+alice) == "" || !h.chat(t, alice).HistoryComplete {
		t.Fatalf("encDone %q, olderDone %q, complete %v; want both sides read to their start",
			h.m.value("encDone."+alice), h.m.value("olderDone."+alice), h.chat(t, alice).HistoryComplete)
	}
	got := f.since(n)
	lastBackup, firstThread := -1, -1
	for i, name := range got {
		switch name {
		case "FetchMessages":
			lastBackup = i
		case "FetchMessagesTask":
			if firstThread < 0 {
				firstThread = i
			}
		}
	}
	if lastBackup < 0 || firstThread < 0 || firstThread < lastBackup {
		t.Errorf("the queue asked %v, want her backup to its start, then her thread", got)
	}
	if p := f.pagesOf(aliceFBID, tn); p != 2 {
		t.Errorf("the queue asked %d pages of her thread, want 2", p)
	}
}

// endlessRestore is a harness whose network has restored his history from a
// backup that always has an older page.
func endlessRestore(t *testing.T) *harness {
	t.Helper()
	f := newFakeMeta()
	f.history.script("invented-none") // pages set: the fake answers by script
	f.history.endless = true
	h := newQueueHarness(t, "", f)
	restored(t, h)
	return h
}

func TestQueueBackupPagesLeaveTheOnDemandPagesAlone(t *testing.T) {
	h := endlessRestore(t)
	eod := h.m.value("eod")
	before := h.f.history.total()
	h.startQueue(t)
	h.advance(t, 8*time.Minute, time.Second)
	if n := h.f.history.total() - before; n < 10 {
		t.Fatalf("the queue asked %d pages of the backup, want at least 10", n)
	}
	if got := h.m.value("eod"); got != eod {
		t.Errorf("the queue spent FetchOlder's pages: eod %q, was %q", got, eod)
	}
	h.queueOn.Store(false)
	h.advance(t, time.Minute, time.Second)

	carol := fmt.Sprint(carolFBID) + "@msgr"
	before = h.f.history.total()
	for range onDemandPages {
		if _, err := h.m.FetchOlder(context.Background(), carol); err != nil {
			t.Fatalf("FetchOlder: %v", err)
		}
	}
	if n := h.f.history.total() - before; n != onDemandPages {
		t.Errorf("FetchOlder asked %d pages that day, want %d", n, onDemandPages)
	}
}

func TestTheQueueLeavesHeldChatsAlone(t *testing.T) {
	t.Run("stalled", func(t *testing.T) {
		f := newFakeMeta()
		withGroupLines(f)
		olderPages(f, groupOne, 2)
		olderPages(f, groupTwo, 2)
		h := newQueueHarness(t, "", f)
		h.signIn(t)
		until := h.f.clock.Now().Add(stallHold)
		_ = h.m.keep("stalled."+groupChat, strconv.FormatInt(until.UnixMilli(), 10))
		n := f.taskMark()
		h.startQueue(t)
		h.advance(t, 10*time.Minute, time.Second)
		if p := f.pagesOf(groupOne, n); p != 0 {
			t.Errorf("the stalled group was asked %d pages", p)
		}
		if p := f.pagesOf(groupTwo, n); p != 2 {
			t.Errorf("the other group was asked %d pages, want 2", p)
		}
		h.advance(t, stallHold, time.Minute)
		if p := f.pagesOf(groupOne, n); p == 0 {
			t.Errorf("the stalled group was not asked once its hold ended")
		}
	})
	t.Run("estalled", func(t *testing.T) {
		h := endlessRestore(t)
		bob := fmt.Sprint(bobFBID) + "@msgr"
		until := h.f.clock.Now().Add(stallHold)
		_ = h.m.keep("estalled."+bob, strconv.FormatInt(until.UnixMilli(), 10))
		bobAsked := len(h.f.history.asked(fmt.Sprint(bobFBID)))
		aliceAsked := len(h.f.history.asked(fmt.Sprint(aliceFBID)))
		h.startQueue(t)
		h.advance(t, 5*time.Minute, time.Second)
		if n := len(h.f.history.asked(fmt.Sprint(bobFBID))); n != bobAsked {
			t.Errorf("the held backup was asked %d pages", n-bobAsked)
		}
		if n := len(h.f.history.asked(fmt.Sprint(aliceFBID))); n == aliceAsked {
			t.Errorf("the next chat's backup was not asked")
		}
		h.advance(t, stallHold, time.Minute)
		if n := len(h.f.history.asked(fmt.Sprint(bobFBID))); n == bobAsked {
			t.Errorf("the held backup was not asked once its hold ended")
		}
	})
}

func TestTheQueueAsksNoBackupWhileTheBackupIsPaused(t *testing.T) {
	h := endlessRestore(t)
	_ = h.m.keep("epause", strconv.FormatInt(h.f.clock.Now().Add(stallHold).UnixMilli(), 10))
	before := h.f.history.total()
	h.startQueue(t)
	h.advance(t, 10*time.Minute, time.Second)
	if n := h.f.history.total() - before; n != 0 {
		t.Errorf("the queue asked %d pages of a paused backup", n)
	}
}

func TestTheQueueWaitsForTheListingAndTheFirstBackfills(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 3)
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	h.queueOn.Store(true)
	for _, name := range []string{"listed", "backfilled"} {
		_ = h.m.keep(name, "")
		if h.m.bq.Ready() {
			t.Errorf("ready without %s", name)
		}
		_ = h.m.keep(name, "1")
	}
	if !h.m.bq.Ready() {
		t.Fatalf("not ready with the listing and the backfill done")
	}
	prev := h.m.HistoryState()
	h.m.setHistory(historyRestored)
	if h.m.bq.Ready() {
		t.Errorf("ready with his history restored and its first read not done")
	}
	_ = h.m.keep("ebackfilled", "1")
	if !h.m.bq.Ready() {
		t.Errorf("not ready once the first read of the backup is done")
	}
	_ = h.m.keep("ebackfilled", "")
	h.m.setHistory(prev)

	// Not ready for an hour: nothing asked.
	_ = h.m.keep("backfilled", "")
	n := f.taskMark()
	eventually(t, "the queue to start", func() bool { s, _ := h.qc.counts(); return s > 0 })
	h.advance(t, time.Hour, 30*time.Second)
	if p := f.pagesOf(groupOne, n); p != 0 {
		t.Errorf("the queue asked %d pages before the first backfill", p)
	}
}

func TestTheQueueNeverRunsWithTheGate(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 3)
	h := newHarnessOver(t, t.TempDir()+"/connect", f, func(h *harness) {
		h.m.WithHistoryQueue()
		for _, name := range []string{"listed", "backfilled"} {
			_ = h.st.PutNetworkValue(Name, name, "1")
		}
	})
	h.signIn(t)
	n := f.taskMark()
	for range 24 {
		f.clock.Advance(time.Hour)
		h.settle()
	}
	if p := f.pagesOf(groupOne, n); p != 0 || h.m.bqRuns.Load() != 0 {
		t.Errorf("with the gate the queue asked %d pages (%d runs)", p, h.m.bqRuns.Load())
	}
}

func TestAQueueRateLimitHoldsTheQueueADay(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 5)
	limited := true
	f.taskErr = func(task socket.Task) error {
		if _, ok := task.(*socket.FetchMessagesTask); ok && limited {
			limited = false
			return httpclient.ErrRateLimited
		}
		return nil
	}
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	n := f.taskMark()
	h.startQueue(t)
	h.advance(t, time.Minute, time.Second)
	if p := f.pagesOf(groupOne, n); p != 1 {
		t.Fatalf("the queue asked %d pages, want the rate-limited one", p)
	}
	if k, blocked := h.m.pace.Blocked(); !blocked || k != wire.RateLimited {
		t.Fatalf("the pace is not blocked by the rate limit")
	}
	v := h.m.value("hq.hold")
	if _, kind, _ := strings.Cut(v, "\n"); kind != string(wire.RateLimited) {
		t.Fatalf("hq.hold is %q", v)
	}
	// After the pace's 15 minutes a send goes; the queue still asks nothing.
	h.advance(t, 16*time.Minute, time.Minute)
	h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " after"})
	if c := len(tasksOf[*socket.SendMessageTask](h.f)); c != 1 {
		t.Errorf("%d sends went after the rate limit, want 1", c)
	}
	h.advance(t, 23*time.Hour, 10*time.Minute)
	if p := f.pagesOf(groupOne, n); p != 1 {
		t.Errorf("the queue asked %d pages within the day", p)
	}
	h.advance(t, 2*time.Hour, time.Minute)
	if p := f.pagesOf(groupOne, n); p < 2 {
		t.Errorf("the queue asked nothing once the day had passed")
	}
}

func TestALiveWorkPushbackHoldsTheQueueADay(t *testing.T) {
	t.Run("send", func(t *testing.T) {
		dir := t.TempDir() + "/connect"
		f := newFakeMeta()
		withGroupLines(f)
		olderPages(f, groupOne, 5)
		h := newQueueHarness(t, dir, f)
		h.signIn(t)
		f.mu.Lock()
		f.writeErrs["SendMessageTask"] = httpclient.ErrRateLimited
		f.mu.Unlock()
		h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " limited"})
		pushed := f.clock.Now()
		until := keptHold(t, h, wire.RateLimited, pushed)
		h.close()

		// A new helper: its queue asks nothing until the day has passed.
		f2 := newFakeMeta()
		withGroupLines(f2)
		olderPages(f2, groupOne, 5)
		f2.clock.Advance(pushed.Sub(f2.clock.Now()) + 16*time.Minute)
		h2 := newQueueHarness(t, dir, f2)
		h2.signIn(t)
		n := f2.taskMark()
		h2.startQueue(t)
		h2.advance(t, until.Sub(f2.clock.Now())-time.Minute, time.Minute)
		if p := f2.pagesOf(groupOne, n); p != 0 {
			t.Fatalf("the queue asked %d pages inside the hold", p)
		}
		h2.advance(t, 10*time.Minute, time.Second)
		if p := f2.pagesOf(groupOne, n); p == 0 {
			t.Fatalf("the queue asked nothing once the hold had passed")
		}
	})
	t.Run("restore", func(t *testing.T) {
		f := newFakeMeta()
		f.history.recoverErr = httpclient.ErrCheckpointRequired
		h := newQueueHarness(t, "", f)
		h.signIn(t)
		_ = h.m.RestoreHistory(context.Background(), []byte(canaryPIN))
		h.settle()
		keptHold(t, h, wire.Checkpoint, f.clock.Now())
	})
}

// keptHold checks hq.hold holds kind until a day after at (within a
// minute), and is its end.
func keptHold(t *testing.T, h *harness, kind wire.Kind, at time.Time) time.Time {
	t.Helper()
	v, _, _ := h.st.NetworkValue(Name, "hq.hold")
	ms, k, _ := strings.Cut(v, "\n")
	n, _ := strconv.ParseInt(ms, 10, 64)
	until := time.UnixMilli(n)
	if wire.Kind(k) != kind || until.Before(at.Add(network.BacklogHold-time.Minute)) {
		t.Fatalf("hq.hold is %q, want a day from %v, %s", v, at, kind)
	}
	return until
}

func TestAQueuePageTakesOverABeeperCopy(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 1)
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	id := fmt.Sprintf("mid.$invented-q%d-1", groupOne%100)
	at := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC).Add(-24 * time.Hour)
	if _, err := h.st.SeedCopied(Name, groupChat, "bc-invented-1", []store.CopiedLine{
		{BeeperID: "bm-invented-1", NetworkID: id, TimeMs: at.UnixMilli(), Text: canary},
	}); err != nil {
		t.Fatalf("SeedCopied: %v", err)
	}
	h.startQueue(t)
	h.advance(t, time.Minute, time.Second)
	lines := h.lines(t, groupChat)
	if l, ok := lines[id]; !ok || l.Source != store.SourceSync {
		t.Fatalf("the chat holds %v, want the direct line", lines)
	}
	for lid := range lines {
		if strings.HasPrefix(lid, "beeper:") {
			t.Errorf("the copy %s is still beside its line", lid)
		}
	}
}

func TestAQueuePageLeavesOrderAndUnreadAlone(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 3)
	olderPages(f, groupTwo, 3)
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	order := func() string {
		p, err := h.st.Chats(Name, 0, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, c := range p.Items {
			out = append(out, fmt.Sprintf("%s/%d/%d", c.ID, c.LastActivityMs, c.Unread))
		}
		return strings.Join(out, " ")
	}
	before := order()
	n := f.taskMark()
	h.startQueue(t)
	h.advance(t, 10*time.Minute, time.Second)
	if f.pagesOf(groupOne, n)+f.pagesOf(groupTwo, n) == 0 {
		t.Fatalf("the queue asked nothing")
	}
	if after := order(); after != before {
		t.Errorf("the chats were\n%s\nafter the queue\n%s", before, after)
	}
}

// A queue request goes only once quiet, and not while the queue yields.
func TestAQueueRequestNeverGoesBesideAnotherOrAScrollBack(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	f.clock.Advance(network.BacklogQuiet)
	gen, cli, _ := h.m.running()
	ask := func() error {
		return h.m.request(network.WithBacklog(context.Background()), gen, func(ctx context.Context) error {
			_, err := cli.ExecuteTasks(ctx, &socket.FetchThreadsTask{ParentThreadKey: -1, SyncGroup: 1})
			return err
		})
	}
	n := f.mark()
	end := h.m.traffic.Begin(context.Background(), f.clock.Now())
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request beside another = %v", err)
	}
	end(f.clock.Now())
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request just after another = %v", err)
	}
	f.clock.Advance(network.BacklogQuiet)
	h.m.ScrolledBack()
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request during a scroll-back = %v", err)
	}
	if got := f.since(n); len(got) != 0 {
		t.Fatalf("the queue asked %v", got)
	}
	f.clock.Advance(network.BacklogYield)
	if err := ask(); err != nil {
		t.Errorf("a queue request once quiet = %v", err)
	}
}

// A step that finds its chat finished (by a scroll-back, while it waited
// for the chat) asked nothing and says so; a page the queue's counter did
// not see is still reported as the page it was.
func TestAStepReportsWhatItDid(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 3)
	h := newQueueHarness(t, "", f)
	h.signIn(t)
	f.clock.Advance(network.BacklogQuiet)
	none := func(string) bool { return false }

	lock := h.m.chatLock(groupChat)
	lock.Lock()
	got := make(chan network.Step, 1)
	asked := h.m.traffic.BacklogAsked()
	go func() { got <- h.m.backlogNext(network.WithBacklog(context.Background()), none) }()
	time.Sleep(50 * time.Millisecond)
	_ = h.m.keep("olderDone."+groupChat, "1")
	lock.Unlock()
	if s := <-got; s.Err != nil || s.Outcome == network.StepMoved || s.Outcome == network.StepStart || h.m.traffic.BacklogAsked() != asked {
		t.Errorf("a step on a chat finished meanwhile is %+v", s)
	}
	_ = h.m.keep("olderDone."+groupChat, "")

	s := h.m.backlogNext(context.Background(), none) // no WithBacklog
	if h.m.traffic.BacklogAsked() != asked {
		t.Fatalf("a request outside the queue's context was counted as the queue's")
	}
	if s.Outcome != network.StepMoved || s.Err != nil {
		t.Errorf("an uncounted page is %+v, want StepMoved for the engine to judge", s)
	}
}

// Ruling 8: at most queueBackupPages pages of his backup a local day from
// the queue; once they are spent it pages only threads that day, and the
// backup again the next.
func TestTheQueueReadsAtMost300BackupPagesADay(t *testing.T) {
	f := newFakeMeta()
	withGroupLines(f)
	olderPages(f, groupOne, 3)
	f.history.script("invented-none")
	f.history.endless = true
	h := newQueueHarness(t, "", f)
	restored(t, h)
	today := f.clock.Now().Local().Format(time.DateOnly)
	_ = h.m.keep("hq.ebp", today+"\n"+strconv.Itoa(queueBackupPages-1))
	before, n := f.history.total(), f.taskMark()
	h.startQueue(t)
	h.advance(t, 10*time.Minute, time.Second)
	if got := f.history.total() - before; got != 1 {
		t.Errorf("the queue asked %d pages of the backup with 1 left today, want 1", got)
	}
	if v := h.m.value("hq.ebp"); v != today+"\n"+strconv.Itoa(queueBackupPages) {
		t.Errorf("hq.ebp is %q", v)
	}
	if p := f.pagesOf(groupOne, n); p == 0 {
		t.Errorf("with the backup's pages spent the queue paged no thread")
	}

	// The next local day it reads the backup again.
	y, mo, d := f.clock.Now().Local().Date()
	midnight := time.Date(y, mo, d+1, 0, 0, 0, 0, time.Local)
	h.advance(t, midnight.Sub(f.clock.Now()), 10*time.Minute)
	before = f.history.total()
	h.advance(t, 10*time.Minute, time.Second)
	if got := f.history.total() - before; got == 0 {
		t.Errorf("the next day the queue asked no page of the backup")
	}
}

// Listing past the year: the inbox folder's pages, the second reaching past
// a year (the first sign-in's listing stops there), then two older ones.
// Each listed thread carries its newest line and his read marker at it.
const (
	listYear  int64 = 8800000000000102 // 400 days old
	listOld   int64 = 8800000000000103 // 500 days old, with an older page
	listOlder int64 = 8800000000000104 // 600 days old
)

func listingFake() *fakeMeta {
	f := newFakeMeta()
	now := f.clock.Now()
	listed := func(key int64, days int) *table.LSTable {
		at := now.Add(-time.Duration(days) * 24 * time.Hour)
		th := thread(key, table.GROUP_THREAD, at, "inbox")
		th.LastReadWatermarkTimestampMs = at.UnixMilli()
		page := threadsPage(folderKey{1, -1}, true, th)
		h := history(key, true, line(fmt.Sprintf("mid.$invented-l%d", key%1000), aliceFBID, at, canary))
		page.LSUpsertMessage, page.LSInsertNewMessageRange = h.LSUpsertMessage, h.LSInsertNewMessageRange
		return page
	}
	last := listed(listOlder, 600)
	last.LSUpsertSyncGroupThreadsRange[0].HasMoreBefore = false
	f.folders[folderKey{1, -1}] = []*table.LSTable{
		threadsPage(folderKey{1, -1}, true, thread(8800000000000101, table.GROUP_THREAD, now.Add(-30*24*time.Hour), "inbox")),
		listed(listYear, 400), listed(listOld, 500), last,
	}
	at := now.Add(-501 * 24 * time.Hour)
	f.older[listOld] = []*table.LSTable{history(listOld, false, line("mid.$invented-l103-old", aliceFBID, at, canary))}
	return f
}

func newListingHarness(t *testing.T, dir string, f *fakeMeta) *harness {
	t.Helper()
	if dir == "" {
		dir = t.TempDir() + "/connect"
	}
	return newHarnessOver(t, dir, f, withQueue(false))
}

func TestTheQueueListsChatsPastTheYear(t *testing.T) {
	f := listingFake()
	h := newListingHarness(t, "", f)
	h.signIn(t)
	h.waitValue(t, "listed")
	if _, ok, _ := h.st.Chat(Name, keyID(listOld)); ok {
		t.Fatalf("a chat past the year was listed at the first sign-in")
	}
	n, asked := f.mark(), h.m.traffic.BacklogAsked()
	h.startQueue(t)
	h.advance(t, 30*time.Minute, time.Second)
	// Ruling 9: every page asked since is the queue's.
	pages := 0
	for _, name := range f.since(n) {
		if name == "FetchThreadsTask" || name == "FetchMessagesTask" || name == "FetchMessages" {
			pages++
		}
	}
	if int64(pages) != h.m.traffic.BacklogAsked()-asked {
		t.Errorf("%d pages since, %d of them the queue's", pages, h.m.traffic.BacklogAsked()-asked)
	}
	for _, key := range []int64{listOld, listOlder} {
		if _, ok, _ := h.st.Chat(Name, keyID(key)); !ok {
			t.Errorf("chat %d past the year was not listed", key)
		}
	}
	if !h.chat(t, keyID(listOld)).HistoryComplete {
		t.Errorf("a chat listed past the year was not paged to its start")
	}
	if !h.m.olderListed() {
		t.Errorf("the listing is not done")
	}
}

func TestAnOldListedChatLandsInThePastWithNoUnread(t *testing.T) {
	f := listingFake()
	h := newListingHarness(t, "", f)
	h.signIn(t)
	h.startQueue(t)
	h.advance(t, 30*time.Minute, time.Second)
	p, err := h.st.Chats(Name, 0, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, c := range p.Items {
		pos[c.ID] = i
		if c.Unread != 0 && (c.ID == keyID(listOld) || c.ID == keyID(listOlder)) {
			t.Errorf("old chat %s has %d unread", c.ID, c.Unread)
		}
	}
	if pos[keyID(listOlder)] != len(p.Items)-1 || pos[keyID(listOld)] != len(p.Items)-2 {
		t.Errorf("the old chats are at %d and %d of %d, want last", pos[keyID(listOld)], pos[keyID(listOlder)], len(p.Items))
	}
}

func TestListingStopsWhenThereIsNothingOlder(t *testing.T) {
	dir := t.TempDir() + "/connect"
	f := listingFake()
	h := newListingHarness(t, dir, f)
	h.signIn(t)
	h.startQueue(t)
	h.advance(t, 30*time.Minute, time.Second)
	if !h.m.olderListed() {
		t.Fatalf("the listing is not done")
	}
	h.close()

	f2 := listingFake()
	h2 := newListingHarness(t, dir, f2)
	h2.signIn(t)
	n := f2.mark()
	h2.startQueue(t)
	h2.advance(t, 30*time.Minute, time.Second)
	for _, name := range f2.since(n) {
		if name == "FetchThreadsTask" {
			t.Fatalf("after a relaunch the queue listed again")
		}
	}
}

func TestListingObeysPushback(t *testing.T) {
	f := listingFake()
	h := newListingHarness(t, "", f)
	h.signIn(t)
	// The queue's first listing page is rate-limited.
	lists := 0
	f.mu.Lock()
	f.taskErr = func(task socket.Task) error {
		if _, ok := task.(*socket.FetchThreadsTask); ok {
			if lists++; lists == 1 {
				return httpclient.ErrRateLimited
			}
		}
		return nil
	}
	f.mu.Unlock()
	h.startQueue(t)
	h.advance(t, 30*time.Minute, time.Second)
	f.mu.Lock()
	got := lists
	f.mu.Unlock()
	if got != 1 {
		t.Fatalf("%d listing pages from the queue, want the rate-limited one", got)
	}
	keptHold(t, h, wire.RateLimited, f.clock.Now().Add(-30*time.Minute))
	n := f.mark()
	h.advance(t, 23*time.Hour, 10*time.Minute)
	for _, name := range f.since(n) {
		if name == "FetchThreadsTask" || name == "FetchMessagesTask" {
			t.Fatalf("the queue asked %s within the day", name)
		}
	}
}
