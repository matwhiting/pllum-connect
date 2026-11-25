package linkedin

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/diag"
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
	pages, lists := r.pagesOf(1, i), 0
	for _, c := range r.f.callsNow()[i:] {
		if c.name == "GetConversationsWithNextCursor" {
			lists++ // the queue's listing of older chats
		}
	}
	if pages == 0 || r.l.traffic.BacklogAsked() != int64(pages+lists) {
		t.Errorf("the queue asked %d pages and %d list pages, and counted %d", pages, lists, r.l.traffic.BacklogAsked())
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

// listingRig lists the inbox past the year: conversation 1 active 10 days
// ago, 2 at 400 days (the first listing stops there), then 3 and 4 older.
func listingRig(t *testing.T) *rig {
	t.Helper()
	r := newQueueRig(t)
	listingFeed(r)
	return r
}

func listingFeed(r *rig) {
	day := func(n int) time.Time { return start.Add(-time.Duration(n) * 24 * time.Hour) }
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, day(10))})
	}
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		switch cursor {
		case "":
			return cursorPageOf("list-invented-1", conv(1, day(10)))
		case "list-invented-1":
			return cursorPageOf("list-invented-2", conv(2, day(400)))
		case "list-invented-2":
			return cursorPageOf("list-invented-3", conv(3, day(500)))
		case "list-invented-3":
			return cursorPageOf("", conv(4, day(600)))
		}
		return cursorPageOf("")
	}
}

// firstSignIn signs in for the first time (the listing owed) and waits
// until live and idle.
func (r *rig) firstSignIn() {
	r.t.Helper()
	r.keep("self", selfID)
	if err := r.login(validSession()); err != nil {
		r.t.Fatalf("Login: %v", err)
	}
	r.waitState(network.Live, "")
	r.idle()
}

func (r *rig) listCalls(i int) []string {
	var out []string
	for _, c := range r.f.callsNow()[i:] {
		if c.name == "GetConversationsWithNextCursor" {
			out = append(out, c.arg)
		}
	}
	return out
}

func TestTheQueueListsChatsPastTheYear(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	if got := r.value("olderCursor.INBOX"); got != "list-invented-2" {
		t.Fatalf("the first listing kept %q, want where it stopped", got)
	}
	if _, ok, _ := r.st.Chat(Name, convURN(3)); ok {
		t.Fatalf("a conversation past the year was listed at the first sign-in")
	}
	i := len(r.f.callsNow())
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	for _, n := range []int{3, 4} {
		if _, ok, _ := r.st.Chat(Name, convURN(n)); !ok {
			t.Errorf("conversation %d past the year was not listed", n)
		}
	}
	if got := r.listCalls(i); len(got) < 2 || !strings.HasSuffix(got[0], "list-invented-2") {
		t.Errorf("the queue listed %v, want on from where the first listing stopped", got)
	}
	if !r.chat(convURN(3)).HistoryComplete {
		t.Errorf("a conversation listed past the year was not paged")
	}
	if !r.l.olderListed() {
		t.Errorf("the listing is not done")
	}
}

func TestAnOldListedChatLandsInThePastWithNoUnread(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	p, err := r.st.Chats(Name, 0, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, c := range p.Items {
		ids = append(ids, c.ID)
		if c.Unread != 0 {
			t.Errorf("conversation %s has %d unread", c.ID, c.Unread)
		}
	}
	if want := []string{convURN(1), convURN(2), convURN(3), convURN(4)}; strings.Join(ids, " ") != strings.Join(want, " ") {
		t.Errorf("the conversations are in the order %v, want %v", ids, want)
	}
}

func TestListingStopsWhenThereIsNothingOlder(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	if !r.l.olderListed() {
		t.Fatalf("the listing is not done")
	}
	i := len(r.f.callsNow())
	r.advance(30*time.Minute, time.Second)
	if got := r.listCalls(i); len(got) != 0 {
		t.Errorf("the queue listed %v once there was nothing older", got)
	}

	r.restart()
	r.firstSignIn()
	i = len(r.f.callsNow())
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	if got := r.listCalls(i); len(got) != 0 {
		t.Errorf("after a relaunch the queue listed %v again", got)
	}
}

func TestListingObeysPushback(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	r.f.foldersErr = map[string]error{"INBOX": statusErr(999)}
	i := len(r.f.callsNow())
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	if got := r.listCalls(i); len(got) != 1 {
		t.Fatalf("the queue listed %v, want the paused page", got)
	}
	v := r.value("hq.hold")
	if _, kind, _ := strings.Cut(v, "\n"); kind != string(wire.RateLimited) {
		t.Fatalf("hq.hold is %q", v)
	}
	r.f.foldersErr = nil
	asked := r.l.traffic.BacklogAsked()
	r.advance(23*time.Hour, 10*time.Minute)
	if r.l.traffic.BacklogAsked() != asked {
		t.Errorf("the queue asked within the day")
	}
}

// Ruling 9: conversations the queue lists past the year are the queue's
// alone: no worker request (its 2 to 4 s pace) is ever made for them, and
// the queue pages them one request per gap.
func TestOldListedConversationsArePagedOnlyByTheQueue(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	i := len(r.f.callsNow())
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	if !r.chat(convURN(3)).HistoryComplete || !r.chat(convURN(4)).HistoryComplete {
		t.Fatalf("the old conversations were not paged")
	}
	var last time.Time
	n := 0
	for _, c := range r.f.callsNow()[i:] {
		switch c.name {
		case "GetMessagesBefore", "GetMessagesWithPrevCursor", "GetConversationsWithNextCursor":
		default:
			continue
		}
		n++
		if !last.IsZero() && c.at.Sub(last) < network.BacklogMinGap {
			t.Errorf("%s %s went %v after the request before: not at the queue's pace", c.name, c.arg, c.at.Sub(last))
		}
		last = c.at
	}
	if int64(n) != r.l.traffic.BacklogAsked() {
		t.Errorf("%d requests, %d of them the queue's: the worker asked for old conversations", n, r.l.traffic.BacklogAsked())
	}
}

// Ruling 10: a plain 500 on a queue page is a wait (the chat passed over
// for now), as a 502 to 504 is; two in a row stop the queue.
func TestAQueue500IsAWaitAndTwoInARowStopIt(t *testing.T) {
	t.Run("one", func(t *testing.T) {
		r := newQueueRig(t)
		historyFeed(r, 800)
		r.live()
		r.f.failOnce("GetMessagesWithPrevCursor", statusErr(500))
		r.startQueue()
		r.advance(30*time.Minute, time.Second)
		if !r.chat(convURN(1)).HistoryComplete {
			t.Errorf("after one 500 the queue did not go on to page the chat")
		}
	})
	t.Run("two", func(t *testing.T) {
		r := newQueueRig(t)
		historyFeed(r, 800)
		r.live()
		r.f.setErr("GetMessagesWithPrevCursor", statusErr(500))
		r.startQueue()
		r.advance(30*time.Minute, time.Second)
		if n := r.l.traffic.BacklogAsked(); n < 2 {
			t.Fatalf("the queue asked %d pages, want a second after the first 500", n)
		}
		asked := r.l.traffic.BacklogAsked()
		r.advance(5*time.Hour, time.Minute)
		if r.l.traffic.BacklogAsked() != asked {
			t.Errorf("the queue went on after two 500s in a row")
		}
		r.clock.Advance(24 * time.Hour)
		r.parked()
		r.advance(time.Minute, time.Minute)
		lines := dayLines(t, r)
		if len(lines) == 0 || !strings.Contains(lines[0], "stopped yes internal") {
			t.Errorf("the day line is %q, want stopped yes internal", lines)
		}
	})
}

func dayLines(t *testing.T, r *rig) []string {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(r.dir, Name, diag.FileName))
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, "history queue") {
			out = append(out, l)
		}
	}
	return out
}

// A listing page never rewrites a conversation the store holds: its
// activity stays what the next catch-up compares the listing with.
func TestAListingPageLeavesHeldChatsAlone(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	before := r.chat(convURN(1)).LastActivityMs
	// An older version's store: the walk starts at the top, where the
	// listing now shows conversation 1 newer than the store holds (a
	// message the stream missed).
	_ = r.l.keep("olderCursor.INBOX", "")
	feed := r.f.cursorPage
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		if cursor == "" {
			return cursorPageOf("list-invented-1", conv(1, start.Add(-24*time.Hour)))
		}
		return feed(cursor)
	}
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	if got := r.chat(convURN(1)).LastActivityMs; got != before {
		t.Errorf("the held conversation's activity moved from %v to %v", time.UnixMilli(before), time.UnixMilli(got))
	}
	if r.value("histDone."+convURN(1)) == histQueue {
		t.Errorf("a held conversation was made the queue's")
	}
}

// The end of the walk: a page past the year that brings no conversation
// the store did not hold ends the folder's listing.
func TestAListingPagePastTheYearWithNothingNewEndsIt(t *testing.T) {
	r := listingRig(t)
	r.firstSignIn()
	held := r.heldChat(3) // conversation 3 is already held
	_ = r.l.keep("histDone."+held, histGone)
	i := len(r.f.callsNow())
	r.startQueue()
	r.advance(30*time.Minute, time.Second)
	for _, c := range r.listCalls(i) {
		if strings.HasSuffix(c, "list-invented-3") {
			t.Errorf("the queue listed past a page with nothing new")
		}
	}
	if _, ok, _ := r.st.Chat(Name, convURN(4)); ok {
		t.Errorf("a page past the end was listed")
	}
	if r.value("listedOlder.INBOX") == "" {
		t.Errorf("the inbox's listing is not done")
	}
}

// Ruling 10's count of 5xx in a row moves on only with LinkedIn's answers:
// a request never made (refused at its turn) or never answered leaves it.
func TestOnlyLinkedInsAnswersStartThe500CountOver(t *testing.T) {
	r := newQueueRig(t)
	for _, tc := range []struct {
		between error
		stop    bool
	}{
		{errNotConnected, true},
		{&wire.Error{Kind: wire.Timeout}, true},
		{&wire.Error{Kind: wire.Offline}, true},
		{nil, false},
		{errGone, false},
	} {
		r.l.bq500s = 0
		if wait, _ := r.l.queueServerError(errServer); !wait {
			t.Fatalf("the first 500 is not a wait")
		}
		r.l.queueServerError(tc.between)
		if _, stop := r.l.queueServerError(errServer); stop != tc.stop {
			t.Errorf("500, %v, 500: stop %v, want %v", tc.between, stop, tc.stop)
		}
	}
}
