package messenger

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Review fix round 1: nothing is ever asked of Meta twice the same way, the
// first sign-in's work resumes rather than restarts, and a one-to-one stays
// one chat whatever order its tables come in.

// changedEvents counts the changed events so far.
func (h *harness) changedEvents() int {
	h.emu.Lock()
	defer h.emu.Unlock()
	n := 0
	for _, e := range h.events {
		if e["event"] == "changed" {
			n++
		}
	}
	return n
}

// messageAsks is how many history pages were asked for a thread.
func (f *fakeMeta) messageAsks(key int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, t := range f.messageTasks {
		if t.ThreadKey == key {
			n++
		}
	}
	return n
}

// Review C1 (the reviewer's probe): an answer that names an existing range
// and holds lines but does not end the history is not asked again, the
// same page every time: the fetch ends, as the library's connector's does.
// A page that brought nothing new tells Pllum nothing.
func TestAnExistingRangeWithLinesIsAskedOnce(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := history(groupOne, true, line("mid.$invented-o2", aliceFBID, at, "newest"))
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at, "inbox")}
	h.f.table = tbl
	h.f.olderFn = func(key int64) *table.LSTable {
		l := line("mid.$invented-o2", aliceFBID, at, "newest")
		l.ThreadKey = key
		return &table.LSTable{
			LSUpsertMessage:              []*table.LSUpsertMessage{l},
			LSUpdateExistingMessageRange: []*table.LSUpdateExistingMessageRange{{ThreadKey: key, TimestampMS: at.UnixMilli(), UnknownBool2: false, UnknownBool3: true}},
		}
	}
	h.signIn(t)
	h.settle()
	time.Sleep(600 * time.Millisecond) // past the changed events' throttle
	changed := h.changedEvents()

	key := fmt.Sprint(groupOne)
	for i := 0; i < 3; i++ {
		if n, err := h.m.FetchOlder(context.Background(), key); n != 0 || err != nil {
			t.Fatalf("FetchOlder = %d, %v", n, err)
		}
	}
	if n := h.f.messageAsks(groupOne); n != 1 {
		t.Errorf("%d history requests for a page that did not move, want 1", n)
	}
	time.Sleep(600 * time.Millisecond)
	if got := h.changedEvents(); got != changed {
		t.Errorf("%d changed events for a page that added nothing", got-changed)
	}
	if h.chat(t, key).HistoryComplete {
		t.Errorf("an existing range that does not end the history completed it")
	}
}

// Review C1: a page that never comes, or comes without moving, is held:
// the next FetchOlder (Pllum asks every 10 s at the oldest line) asks
// nothing, in this helper or the next one over the same folder.
func TestAStalledPageIsNotAskedAgain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(f *fakeMeta, at time.Time)
		err   bool
	}{
		{"never answered", func(f *fakeMeta, at time.Time) { f.silent = true }, true},
		{"did not move", func(f *fakeMeta, at time.Time) {
			f.olderFn = func(key int64) *table.LSTable {
				return history(key, true, line("mid.$invented-m1", aliceFBID, at, "newest"))
			}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMeta()
			dir := filepath.Join(t.TempDir(), "connect")
			h := newHarnessOver(t, dir, f)
			at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
			tbl := history(groupOne, true, line("mid.$invented-m1", aliceFBID, at, "newest"))
			tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at, "inbox")}
			f.table = tbl
			tc.setup(f, at)
			h.signIn(t)
			h.settle()
			key := fmt.Sprint(groupOne)
			if _, err := h.m.FetchOlder(context.Background(), key); (err != nil) != tc.err {
				t.Fatalf("FetchOlder = %v", err)
			}
			f.clock.Advance(time.Minute)
			if _, err := h.m.FetchOlder(context.Background(), key); err != nil {
				t.Errorf("a held chat's FetchOlder = %v, want nothing asked and no error", err)
			}
			if n := f.messageAsks(groupOne); n != 1 {
				t.Errorf("%d requests, want 1", n)
			}
			h.close()

			next := newHarnessOver(t, dir, f)
			next.signIn(t)
			next.settle()
			if _, err := next.m.FetchOlder(context.Background(), key); err != nil {
				t.Errorf("after a restart, FetchOlder = %v", err)
			}
			if n := f.messageAsks(groupOne); n != 1 {
				t.Errorf("after a restart, %d requests, want still 1", n)
			}

			// Hours later the hold ends, and it may ask again.
			f.clock.Advance(stallHold + time.Minute)
			f.mu.Lock()
			f.silent, f.olderFn = false, nil
			f.mu.Unlock()
			if _, err := next.m.FetchOlder(context.Background(), key); err != nil {
				t.Errorf("after the hold, FetchOlder = %v", err)
			}
			if n := f.messageAsks(groupOne); n != 2 {
				t.Errorf("after the hold, %d requests, want 2", n)
			}
		})
	}
}

// Review I1: a thread with no line held is never asked "from now": with
// nothing to anchor on, it is passed over, as the connector does.
func TestAThreadWithNoLineIsNotAsked(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	at := h.f.clock.Now().Add(-time.Hour)
	h.f.table = &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at, "inbox")}}
	h.login(t, validSession())
	h.waitValue(t, "backfilled")
	if n, err := h.m.FetchOlder(context.Background(), fmt.Sprint(groupOne)); n != 0 || err != nil {
		t.Errorf("FetchOlder = %d, %v", n, err)
	}
	if n := h.f.messageAsks(groupOne); n != 0 {
		t.Errorf("%d history requests for a thread with no line held", n)
	}
}

// Review C2: the first sign-in's listing and backfill resume where they
// stopped. A rate limit stops them; the next launch (after it ends) asks
// no folder already listed and gives no thread more than its 5 pages. A
// folder that fails with an error of its own is passed over, not retried
// at every launch.
func TestTheFirstSignInResumesWhereItStopped(t *testing.T) {
	f := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)
	h.m.gate = false
	now := f.clock.Now()
	tbl := inbox()
	for _, k := range []int64{groupOne, groupTwo} {
		x := history(k, true, line(fmt.Sprintf("mid.$invented-r%d", k%10), aliceFBID, now.Add(-time.Duration(k%10)*time.Hour), "x"))
		tbl.LSUpsertMessage = append(tbl.LSUpsertMessage, x.LSUpsertMessage...)
		tbl.LSInsertNewMessageRange = append(tbl.LSInsertNewMessageRange, x.LSInsertNewMessageRange...)
	}
	f.table = tbl
	f.folders[inboxOne] = []*table.LSTable{
		threadsPage(inboxOne, true, thread(8800000000000201, table.GROUP_THREAD, now.Add(-48*time.Hour), "inbox")),
		threadsPage(inboxOne, false, thread(8800000000000202, table.GROUP_THREAD, now.Add(-49*time.Hour), "inbox")),
	}
	pages := map[int64]int{}
	f.olderFn = func(key int64) *table.LSTable {
		pages[key]++
		return history(key, true, line(fmt.Sprintf("mid.$invented-p%d-%d", key%10, pages[key]), aliceFBID, now.Add(-time.Duration(24*pages[key])*time.Hour), "x"))
	}
	// Sync group 95's first page is rate limited.
	limited := true
	f.taskErr = func(task socket.Task) error {
		if t, ok := task.(*socket.FetchThreadsTask); ok && t.SyncGroup == 95 && limited {
			limited = false
			return httpclient.ErrRateLimited
		}
		return nil
	}
	h.login(t, validSession())
	eventually(t, "the rate limit", func() bool { k, _ := h.m.pace.Blocked(); return k == wire.RateLimited })
	h.settle()
	if _, ok, _ := h.st.NetworkValue(Name, "listed"); ok {
		t.Fatalf("a listing stopped by a rate limit was marked done")
	}
	h.close()

	// The next launch, after the rate limit: the inbox is not asked again;
	// group 95 is.
	f.clock.Advance(16 * time.Minute)
	f.mu.Lock()
	before := len(f.threadTasks)
	f.mu.Unlock()
	next := newHarnessOver(t, dir, f)
	next.m.gate = false
	// The second thread's third page is rate limited.
	f.mu.Lock()
	f.taskErr = func(task socket.Task) error {
		if t, ok := task.(*socket.FetchMessagesTask); ok && t.ThreadKey == groupTwo && pages[groupTwo] == 2 && !limited {
			limited = true
			return httpclient.ErrRateLimited
		}
		return nil
	}
	f.mu.Unlock()
	next.login(t, validSession())
	eventually(t, "the second rate limit", func() bool { k, _ := next.m.pace.Blocked(); return k == wire.RateLimited })
	next.settle()
	f.mu.Lock()
	for _, task := range f.threadTasks[before:] {
		if task.SyncGroup == 1 && task.ParentThreadKey == -1 {
			t.Errorf("the inbox, listed in full before, was asked again")
		}
	}
	f.mu.Unlock()
	if _, ok, _ := next.st.NetworkValue(Name, "listed"); !ok {
		t.Errorf("the resumed listing was not marked done")
	}
	if n := f.messageAsks(groupOne); n != threadHistoryPages {
		t.Errorf("the first thread had %d pages, want %d", n, threadHistoryPages)
	}
	next.close()

	// The launch after that gives the first thread nothing more, and the
	// second only its remaining pages.
	f.clock.Advance(16 * time.Minute)
	last := newHarnessOver(t, dir, f)
	last.m.gate = false
	last.login(t, validSession())
	last.waitValue(t, "backfilled")
	if n := f.messageAsks(groupOne); n != threadHistoryPages {
		t.Errorf("the first thread now has %d pages, want still %d", n, threadHistoryPages)
	}
	if n := f.messageAsks(groupTwo); n != threadHistoryPages+1 {
		// 2 given, 1 rate limited (asked, not given), then 3 more
		t.Errorf("the second thread was asked %d times, want %d", n, threadHistoryPages+1)
	}
}

// countThreadTasks is how many of these calls were thread pages.
func countThreadTasks(calls []call) int {
	n := 0
	for _, c := range calls {
		if c.name == "FetchThreadsTask" {
			n++
		}
	}
	return n
}

// Review C2: a folder that fails with an error of its own (a page the
// library cannot parse) is passed over: the listing goes on and ends, and
// the next launch lists nothing.
func TestAFolderThatFailsIsPassedOver(t *testing.T) {
	f := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)
	h.m.gate = false
	f.taskErr = func(task socket.Task) error {
		if t, ok := task.(*socket.FetchThreadsTask); ok && t.SyncGroup == 1 {
			return errors.New("invented: failed to parse response")
		}
		return nil
	}
	h.login(t, validSession())
	h.waitValue(t, "backfilled")
	asked95 := false
	f.mu.Lock()
	for _, task := range f.threadTasks {
		asked95 = asked95 || task.SyncGroup == 95
	}
	f.mu.Unlock()
	if !asked95 {
		t.Errorf("the listing stopped at the failed folder")
	}
	if s := h.lastState(); s != string(network.Live) {
		t.Errorf("a failed folder left the state %s", s)
	}
	h.close()

	before := countThreadTasks(f.requests())
	next := newHarnessOver(t, dir, f)
	next.m.gate = false
	next.login(t, validSession())
	eventually(t, "the encrypted socket", func() bool {
		f.e2ee.mu.Lock()
		defer f.e2ee.mu.Unlock()
		return f.e2ee.connects == 2
	})
	time.Sleep(300 * time.Millisecond)
	next.settle()
	if got := countThreadTasks(f.requests()); got != before {
		t.Errorf("the next launch asked %d thread pages, want none", got-before)
	}
}

// Review I2 (the reviewer's probe): an older one-to-one under another key,
// listed again in a later table without its pointer, stays an alias of the
// encrypted chat: no second chat, and the alias is never re-pointed.
func TestAnOlderKeyListedAgainStaysAnAlias(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)
	const olderCarol int64 = 7700000000000001
	tbl := inbox()
	tbl.LSDeleteThenInsertThread = append(tbl.LSDeleteThenInsertThread, thread(olderCarol, table.ONE_TO_ONE, at, "e2ee_cutover"))
	tbl.LSVerifyHybridThreadExists = append(tbl.LSVerifyHybridThreadExists,
		&table.LSVerifyHybridThreadExists{ThreadKey: olderCarol, ThreadJID: carolFBID, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE})
	h.f.table = tbl
	h.signIn(t)
	h.settle()
	carol := fmt.Sprint(carolFBID) + "@msgr"
	if got, _ := h.st.ChatFor(Name, fmt.Sprint(olderCarol)); got != carol {
		t.Fatalf("before: the older key resolves to %s", got)
	}
	later := &table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{thread(olderCarol, table.ONE_TO_ONE, at, "e2ee_cutover")},
		LSInsertMessage:          []*table.LSInsertMessage{{ThreadKey: olderCarol, MessageId: "mid.$invented-probe", SenderId: carolFBID, TimestampMs: at.UnixMilli(), Text: "p"}},
	}
	h.f.last().dispatch(later)
	eventually(t, "the later line", func() bool {
		_, ok := h.lines(t, carol)["mid.$invented-probe"]
		return ok
	})
	if got, _ := h.st.ChatFor(Name, fmt.Sprint(olderCarol)); got != carol {
		t.Errorf("after: the older key resolves to %s, a second chat", got)
	}
	for _, id := range h.chatIDs(t) {
		if id == fmt.Sprint(olderCarol)+"@msgr" {
			t.Errorf("the older key became chat %s", id)
		}
	}
}

// Review N1: a history request that fails (a page the library cannot
// parse, the library's own timeout) holds the chat too, so FetchOlder at
// the oldest line does not send it again on every read.
func TestAPageThatFailsIsHeld(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"cannot parse", errors.New("invented: failed to parse response")},
		{"library timeout", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
			tbl := history(groupOne, true, line("mid.$invented-f1", aliceFBID, at, "newest"))
			tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at, "inbox")}
			h.f.table = tbl
			h.f.taskErr = func(task socket.Task) error {
				if _, ok := task.(*socket.FetchMessagesTask); ok {
					return tc.err
				}
				return nil
			}
			h.signIn(t)
			h.settle()
			for i := 0; i < 3; i++ {
				_, _ = h.m.FetchOlder(context.Background(), fmt.Sprint(groupOne))
				h.f.clock.Advance(10 * time.Second)
			}
			if n := h.f.messageAsks(groupOne); n != 1 {
				t.Errorf("%d requests for a page that fails, want 1", n)
			}
		})
	}
}

// Review N2: a page counts as moved only when it reaches back past what is
// held, the way the cursor moves: an answer newer than the reference (a
// range that landed for the thread while waiting) holds the chat.
func TestAPageThatDoesNotReachBackIsHeld(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := history(groupOne, true, line("mid.$invented-n1", aliceFBID, at, "newest"))
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at, "inbox")}
	h.f.table = tbl
	h.f.olderFn = func(key int64) *table.LSTable {
		return history(key, true, line("mid.$invented-n2", aliceFBID, at.Add(time.Hour), "later"))
	}
	h.signIn(t)
	h.settle()
	for i := 0; i < 3; i++ {
		_, _ = h.m.FetchOlder(context.Background(), fmt.Sprint(groupOne))
	}
	if n := h.f.messageAsks(groupOne); n != 1 {
		t.Errorf("%d requests for a page that did not reach back, want 1", n)
	}
}

// Review N4: a page whose ingest panics fails its folder, as an error of
// its own does: the listing goes on and ends, and no later launch asks
// that page again.
func TestAPageThatPanicsFailsItsFolder(t *testing.T) {
	f := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)
	h.m.gate = false
	// A nil row: ingesting it panics.
	f.folders[inboxOne] = []*table.LSTable{{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{nil}}}
	h.login(t, validSession())
	h.waitValue(t, "listed")
	asked95 := false
	f.mu.Lock()
	for _, task := range f.threadTasks {
		asked95 = asked95 || task.SyncGroup == 95
	}
	f.mu.Unlock()
	if !asked95 {
		t.Errorf("the listing stopped at the page that panicked")
	}
	if c := h.m.GateCounts().(GateCounts); c.FolderStops[stopFailed] != 1 {
		t.Errorf("folder stops %v, want one failed", c.FolderStops)
	}
	h.close()

	before := countThreadTasks(f.requests())
	next := newHarnessOver(t, dir, f)
	next.m.gate = false
	next.login(t, validSession())
	eventually(t, "the encrypted socket", func() bool {
		f.e2ee.mu.Lock()
		defer f.e2ee.mu.Unlock()
		return f.e2ee.connects == 2
	})
	time.Sleep(300 * time.Millisecond)
	next.settle()
	if got := countThreadTasks(f.requests()); got != before {
		t.Errorf("the next launch asked %d thread pages, want none", got-before)
	}
}
