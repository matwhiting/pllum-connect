package instagram

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue's tests. Every ID and text here is invented.

// queueClock is the history queue's clock in these tests: the harness's
// fake clock, but a sleep waits for the test to move it (advance), so the
// queue looks again only when the test lets it, one step at a time.
type queueClock struct {
	*fakeClock
	mu     sync.Mutex
	sleeps int // sleeps begun
	woke   int // sleeps that reached their end
}

func (c *queueClock) Sleep(ctx context.Context, d time.Duration) error {
	ch := make(chan struct{})
	// The sleep counts as begun only with its timer set, in one step: a
	// test that sees it asleep (parked) and moves the clock on must find
	// the timer there, or the clock passes it before it is set and the
	// queue sleeps on past the test (the timer is not run under c.mu, so
	// its wake taking c.mu waits for this to end).
	c.mu.Lock()
	stop := c.fakeClock.AfterFunc(d, func() {
		c.mu.Lock()
		c.woke++
		c.mu.Unlock()
		close(ch)
	})
	c.sleeps++
	c.mu.Unlock()
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

// withQueue makes the harness's network with a history queue on a
// queueClock.
func withQueue(h *harness) {
	h.g.WithHistoryQueue()
	h.qc = &queueClock{fakeClock: h.clock}
	h.g.bq.Clock = h.qc
}

// newQueueHarness is a harness whose network has a history queue.
func newQueueHarness(t *testing.T, dir string) *harness {
	t.Helper()
	return openHarness(t, dir, nil, withQueue)
}

// goLive has the socket subscribe and catch up: the run is live, and its
// queue starts. It waits for the queue's first sleep.
func (h *harness) goLive() {
	h.t.Helper()
	before, _ := h.counts()
	_ = h.m.dispatch(&slidetypes.Connected{})
	_ = h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 101, Timestamp: h.clock.Now()})
	if h.lastState() != "live" {
		h.t.Fatalf("the state is %s, want live", h.lastState())
	}
	if h.qc != nil {
		eventually(h.t, "the queue to start", func() bool { s, _ := h.counts(); return s > before })
	}
}

func (h *harness) counts() (int, int) {
	if h.qc == nil {
		return 0, 0
	}
	return h.qc.counts()
}

// advance moves the clock on by total, step at a time, and after each step
// waits until the queue is parked again (asleep, or its Run returned), so
// it looks again (and asks, if it may) at every step it wakes on, whoever
// moved the clock.
func (h *harness) advance(total, step time.Duration) {
	h.t.Helper()
	for d := time.Duration(0); d < total; d += step {
		h.clock.Advance(step)
		h.parked()
	}
}

// parked waits until the queue is asleep or its Run has returned.
func (h *harness) parked() {
	h.t.Helper()
	eventually(h.t, "the queue to park", func() bool {
		s, w := h.counts()
		return s > w || h.g.bqRuns.Load() == 0
	})
}

// queuePages are the PaginateMessages calls since the n-th call, as
// "<thread> <cursor>", with when each went.
func (m *fakeMeta) pagesSince(n int) []call {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []call
	for _, c := range m.calls[n:] {
		if c.name == "PaginateMessages" {
			out = append(out, c)
		}
	}
	return out
}

// oldChat is an invented one-to-one, last active long ago (so the first
// sign-in's backfill fetches none of its pages), whose history is pages
// pages deep.
type oldChat struct {
	key    int64
	thread string
	active time.Time
	pages  int
}

func (c oldChat) chat() string { return strconv.FormatInt(c.key, 10) }

func (c oldChat) line(page int) string { return fmt.Sprintf("mid.$invented-q%d-%d", c.key, page) }

var queueChats = []oldChat{
	{key: 1000301, thread: "340282366841710300000000000000301", active: start.Add(-100 * 24 * time.Hour), pages: 3},
	{key: 1000302, thread: "340282366841710300000000000000302", active: start.Add(-110 * 24 * time.Hour), pages: 2},
	{key: 1000303, thread: "340282366841710300000000000000303", active: start.Add(-120 * 24 * time.Hour), pages: 1},
}

// serve lists chats in the inbox and answers their pages: page k (from 0,
// the newest) has one line, a year older each page, and a next page while
// k+1 < pages.
func serve(m *fakeMeta, chats ...oldChat) {
	var threads []*slidetypes.ThreadInfo
	for _, c := range chats {
		t := threadWith(c.key, c.thread, alice(), c.active)
		// He has read it, as Meta lists a chat he has opened.
		t.SlideReadReceipts = []slidetypes.ReadReceipt{{ParticipantFBID: selfFBID, WatermarkTimestampMS: ms(c.active)}}
		threads = append(threads, t)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mailbox = mailboxOf(false, threads...)
	m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		for _, c := range chats {
			if c.thread != igChatID {
				continue
			}
			k := 0
			if cursor != "" {
				n, err := strconv.Atoi(strings.TrimPrefix(cursor, c.thread+"-p"))
				if err != nil {
					return nil, fmt.Errorf("invented: unknown cursor")
				}
				k = n
			}
			more := k+1 < c.pages
			next := ""
			if more {
				next = fmt.Sprintf("%s-p%d", c.thread, k+1)
			}
			at := c.active.Add(-time.Duration(k+1) * 365 * 24 * time.Hour)
			return messagesPage(more, next, textMsg(c.line(k), aliceFBI, at, "invented old line")), nil
		}
		return nil, fmt.Errorf("invented: unknown thread")
	}
}

func TestTheQueuePagesChatsNewestFirstToTheirStart(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	serve(h.m, queueChats...)
	h.login(validSession())
	if h.g.value("backfilled") == "" {
		t.Fatalf("the first backfill did not finish")
	}
	n := h.m.mark()
	if got := h.m.pagesSince(0); len(got) != 0 {
		t.Fatalf("the backfill paged %v, want none of these old chats", got)
	}
	h.goLive()
	const step = 100 * time.Millisecond
	h.advance(10*time.Minute, step)

	pages := h.m.pagesSince(n)
	var want []string
	for _, c := range queueChats {
		for k := range c.pages {
			cursor := ""
			if k > 0 {
				cursor = fmt.Sprintf("%s-p%d", c.thread, k)
			}
			want = append(want, c.thread+" "+cursor)
		}
	}
	var got []string
	for _, p := range pages {
		got = append(got, p.arg)
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("the queue asked\n%v\nwant\n%v", got, want)
	}
	for i := 1; i < len(pages); i++ {
		if d := pages[i].at.Sub(pages[i-1].at); d < 17*time.Second || d > 29*time.Second+step {
			t.Errorf("page %d went %v after the one before, want 17 to 29 s", i, d)
		}
	}
	for _, c := range queueChats {
		if !h.chat(c.chat()).HistoryComplete {
			t.Errorf("chat %s is not historyComplete", c.chat())
		}
		if _, ok := h.lines(c.chat())[c.line(c.pages-1)]; !ok {
			t.Errorf("chat %s's first line was not stored", c.chat())
		}
	}
	// Each queue page was one request counted as the queue's.
	if asked := h.g.traffic.BacklogAsked(); asked != int64(len(want)) {
		t.Errorf("the queue counted %d requests, want %d", asked, len(want))
	}
}

func TestTheQueueResumesFromTheKeptCursor(t *testing.T) {
	dir := t.TempDir() + "/connect"
	deep := oldChat{key: 1000311, thread: "340282366841710300000000000000311", active: start.Add(-100 * 24 * time.Hour), pages: 5}
	h := newQueueHarness(t, dir)
	serve(h.m, deep)
	// Meta answers nothing for the third page in this run.
	f := h.m.messagePage
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		if cursor == deep.thread+"-p2" {
			return nil, nil
		}
		return f(igChatID, cursor)
	}
	h.login(validSession())
	n := h.m.mark()
	h.goLive()
	h.advance(5*time.Minute, time.Second)
	if pages := h.m.pagesSince(n); len(pages) < 3 {
		t.Fatalf("the queue asked %v, want two pages and the third", pages)
	}
	if got := h.g.value("cursor." + deep.chat()); got != deep.thread+"-p2" {
		t.Fatalf("the kept cursor is %q", got)
	}
	h.close()

	h2 := newQueueHarness(t, dir)
	serve(h2.m, deep)
	h2.login(validSession())
	n = h2.m.mark()
	h2.goLive()
	h2.advance(time.Minute, time.Second)
	pages := h2.m.pagesSince(n)
	if len(pages) == 0 || pages[0].arg != deep.thread+" "+deep.thread+"-p2" {
		t.Fatalf("after a restart the queue asked %v, want from the kept cursor", pages)
	}
}

func TestTheQueueWaitsForTheFirstBackfill(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	serve(h.m, queueChats...)
	// The inbox has a next page, which fails: the first backfill never ends.
	h.m.mailbox.ThreadsByFolder.PageInfo = slidetypes.PageInfo{HasNextPage: true, EndCursor: "inbox-cursor-1"}
	h.login(validSession())
	if h.g.value("backfilled") != "" {
		t.Fatalf("the backfill finished")
	}
	n := h.m.mark()
	h.goLive()
	h.advance(time.Hour, 30*time.Second)
	if pages := h.m.pagesSince(n); len(pages) != 0 {
		t.Fatalf("the queue asked %v before the first backfill", pages)
	}
}

func TestTheQueueStopsAtACheckpoint(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	serve(h.m, queueChats...)
	f := h.m.messagePage
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		if igChatID == queueChats[0].thread && cursor != "" {
			return nil, httpclient.ErrCheckpointRequired
		}
		return f(igChatID, cursor)
	}
	h.login(validSession())
	h.goLive()
	n := h.m.mark()
	h.advance(2*time.Minute, time.Second)
	if got := h.lastState(); got != "reconnectNeeded/checkpoint" {
		t.Fatalf("after the checkpoint the state is %q", got)
	}
	asked := h.m.requests()
	h.clock.Advance(48 * time.Hour)
	h.g.Nudge()
	h.settle()
	if got := h.m.requests(); len(got) != len(asked) {
		t.Errorf("requests after the checkpoint: %v", got[len(asked):])
	}
	if pages := h.m.pagesSince(n); len(pages) != 2 {
		t.Errorf("the queue asked %v, want the page and the checkpointed one", pages)
	}
	v, _, _ := h.st.NetworkValue(Name, "hq.hold")
	if _, kind, _ := strings.Cut(v, "\n"); kind != string(wire.Checkpoint) {
		t.Errorf("hq.hold is %q, want a checkpoint", v)
	}
}

func TestALiveWorkPushbackHoldsTheQueueADay(t *testing.T) {
	dir := t.TempDir() + "/connect"
	h := newQueueHarness(t, dir)
	serve(h.m, append([]oldChat{{key: aliceFBI, thread: aliceThread, active: start.Add(-100 * 24 * time.Hour), pages: 9}}, queueChats...)...)
	h.login(validSession())
	h.goLive()
	// A send, not a queue step, is rate-limited.
	h.m.setErr("SendMessage", httpclient.ErrRateLimited)
	h.send(network.Outgoing{Text: "invented hello"})
	pushed := h.clock.Now()
	v, _, _ := h.st.NetworkValue(Name, "hq.hold")
	ms, kind, _ := strings.Cut(v, "\n")
	until, _ := strconv.ParseInt(ms, 10, 64)
	if kind != string(wire.RateLimited) || time.UnixMilli(until).Before(pushed.Add(network.BacklogHold-time.Second)) {
		t.Fatalf("hq.hold is %q, want a day ahead, rateLimited", v)
	}
	h.close()

	// A new helper: its queue asks nothing until the day has passed.
	h = newQueueHarness(t, dir)
	serve(h.m, append([]oldChat{{key: aliceFBI, thread: aliceThread, active: start.Add(-100 * 24 * time.Hour), pages: 9}}, queueChats...)...)
	h.clock.Advance(pushed.Sub(start) + 16*time.Minute) // past the pace's own 15 minutes
	h.login(validSession())
	n := h.m.mark()
	h.goLive()
	h.advance(time.UnixMilli(until).Sub(h.clock.Now())-time.Minute, time.Minute)
	if pages := h.m.pagesSince(n); len(pages) != 0 {
		t.Fatalf("the queue asked %v inside the hold", pages)
	}
	h.advance(10*time.Minute, time.Second)
	if pages := h.m.pagesSince(n); len(pages) == 0 {
		t.Fatalf("the queue asked nothing once the hold had passed")
	}
}

func TestTheQueueNeverRunsBesideASend(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	serve(h.m, append([]oldChat{{key: aliceFBI, thread: aliceThread, active: start.Add(-100 * 24 * time.Hour), pages: 50}}, queueChats...)...)
	var mu sync.Mutex
	inFlight, most := 0, 0
	sending, release := make(chan struct{}), make(chan struct{})
	var answered time.Time
	h.m.hook = func(name string) {
		switch name {
		case "PaginateMessages", "FetchThreadID", "SendMessage":
		default:
			return
		}
		mu.Lock()
		inFlight++
		most = max(most, inFlight)
		mu.Unlock()
		if name == "SendMessage" {
			close(sending)
			<-release
			mu.Lock()
			answered = h.clock.Now()
			mu.Unlock()
		}
		mu.Lock()
		inFlight--
		mu.Unlock()
	}
	h.login(validSession())
	h.goLive()
	h.advance(time.Minute, time.Second) // the queue is paging
	n := h.m.mark()

	// A send starts while the queue waits, and stays in flight a while.
	if _, err := h.g.Send(context.Background(), network.Outgoing{ChatID: aliceChat, Text: "invented hello"}); err != nil {
		t.Fatal(err)
	}
	<-sending
	h.advance(2*time.Minute, time.Second)
	if pages := h.m.pagesSince(n); len(pages) != 0 {
		t.Fatalf("the queue asked %v while a send was in flight", pages)
	}
	close(release)
	h.settle()
	h.advance(2*time.Minute, time.Second)
	pages := h.m.pagesSince(n)
	if len(pages) == 0 {
		t.Fatalf("the queue asked nothing after the send")
	}
	mu.Lock()
	defer mu.Unlock()
	if d := pages[0].at.Sub(answered); d < network.BacklogQuiet {
		t.Errorf("the queue asked %v after the send's answer, want at least %v", d, network.BacklogQuiet)
	}
	if most != 1 {
		t.Errorf("%d requests were in flight at once, want 1", most)
	}
}

func TestAQueuePageTakesOverABeeperCopy(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	c := oldChat{key: 1000321, thread: "340282366841710300000000000000321", active: start.Add(-100 * 24 * time.Hour), pages: 1}
	serve(h.m, c)
	h.login(validSession())
	at := c.active.Add(-365 * 24 * time.Hour).UnixMilli()
	if _, err := h.st.SeedCopied(Name, c.chat(), "bc-invented-1", []store.CopiedLine{
		{BeeperID: "bm-invented-1", NetworkID: c.line(0), TimeMs: at, Text: "invented old line"},
	}); err != nil {
		t.Fatalf("SeedCopied: %v", err)
	}
	h.goLive()
	h.advance(time.Minute, time.Second)
	lines := h.lines(c.chat())
	if len(lines) != 1 {
		t.Fatalf("the chat holds %d lines, want the one", len(lines))
	}
	if l, ok := lines[c.line(0)]; !ok || l.Source != store.SourceSync {
		t.Errorf("the line is %+v, want the direct one", lines)
	}
}

func TestAQueuePageLeavesOrderAndUnreadAlone(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	serve(h.m, queueChats...)
	h.login(validSession())
	order := func() []string {
		p, err := h.st.Chats(Name, 0, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, c := range p.Items {
			ids = append(ids, fmt.Sprintf("%s/%d/%d", c.ID, c.LastActivityMs, c.Unread))
		}
		return ids
	}
	before := order()
	n := h.m.mark()
	h.goLive()
	h.advance(10*time.Minute, time.Second)
	if pages := h.m.pagesSince(n); len(pages) == 0 {
		t.Fatalf("the queue asked nothing")
	}
	if after := order(); strings.Join(after, " ") != strings.Join(before, " ") {
		t.Errorf("the chats were %v, after the queue %v", before, after)
	}
}

func TestNoQueueWithoutTheFlag(t *testing.T) {
	h := newHarness(t)
	serve(h.m, queueChats...)
	h.login(validSession())
	h.goLive()
	n := h.m.mark()
	h.clock.Advance(24 * time.Hour)
	h.settle()
	if got := h.m.namesSince(n); len(got) != 0 {
		t.Errorf("requests past the backfill without the flag: %v", got)
	}
	if h.g.bq != nil {
		t.Errorf("a network made without WithHistoryQueue has a queue")
	}
}

// The queue's request is checked once its turn in the pace has come: a
// send that began while it waited for that turn goes alone.
func TestAQueueRequestNeverGoesBesideAnother(t *testing.T) {
	h := newHarness(t)
	serve(h.m, queueChats...)
	h.login(validSession())
	h.goLive()
	gen, cli, _ := h.g.running()
	q := network.WithBacklog(context.Background())
	ask := func() error {
		return h.g.request(q, gen, func(ctx context.Context) error {
			_, err := cli.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{ThreadID: queueChats[0].thread})
			return err
		})
	}
	n := h.m.mark()

	end := h.g.traffic.Begin(context.Background(), h.clock.Now()) // a send in flight
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request beside a send = %v, want notConnected", err)
	}
	end(h.clock.Now())
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request just after a send = %v, want notConnected", err)
	}
	if got := h.m.pagesSince(n); len(got) != 0 {
		t.Fatalf("the queue asked Meta %v beside a send", got)
	}
	h.clock.Advance(network.BacklogQuiet)
	if err := ask(); err != nil {
		t.Errorf("a queue request once quiet = %v", err)
	}
	if got := h.m.pagesSince(n); len(got) != 1 {
		t.Fatalf("the queue asked %v once quiet, want one page", got)
	}
}

// A chat Meta lists without his read marker: what it already holds may be
// unread, but the queue's older lines never add to it.
func TestAQueuePageAddsNoUnreadWithoutAReadMarker(t *testing.T) {
	h := newQueueHarness(t, t.TempDir()+"/connect")
	c := oldChat{key: 1000331, thread: "340282366841710300000000000000331", active: start.Add(-100 * 24 * time.Hour), pages: 3}
	serve(h.m, c)
	h.m.mailbox.ThreadsByFolder.Edges[0].Node.AsIGDirectThread.SlideReadReceipts = nil
	h.login(validSession())
	if err := h.m.dispatch(delta(c.thread, &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-q331-new", aliceFBI, start, "invented new")})); err != nil {
		t.Fatal(err)
	}
	if u := h.chat(c.chat()).Unread; u != 1 {
		t.Fatalf("unread = %d before the queue, want the new line", u)
	}
	n := h.m.mark()
	h.goLive()
	h.advance(5*time.Minute, time.Second)
	if pages := h.m.pagesSince(n); len(pages) != c.pages {
		t.Fatalf("the queue asked %v, want the chat's %d pages", pages, c.pages)
	}
	if got := h.chat(c.chat()); got.Unread != 1 || !got.HistoryComplete {
		t.Errorf("after the queue: unread %d, complete %v; want 1, true", got.Unread, got.HistoryComplete)
	}
}

// A scroll-back that reaches a chat's start while a queue step waits for
// the chat is no fault: the step asked nothing, and says so, so the engine
// neither counts a page nor stops.
func TestAChatFinishedByScrollBackIsNoFault(t *testing.T) {
	h := newHarness(t)
	h.g.WithHistoryQueue()
	c := oldChat{key: 1000341, thread: "340282366841710300000000000000341", active: start.Add(-100 * 24 * time.Hour), pages: 3}
	serve(h.m, c)
	h.login(validSession()) // not live: no Run of the queue, the step is called here
	n := h.m.mark()
	asked := h.g.traffic.BacklogAsked()

	lock := h.g.chatLock(c.chat())
	lock.Lock() // a scroll-back holds the chat
	got := make(chan network.Step, 1)
	go func() {
		got <- h.g.backlogNext(network.WithBacklog(context.Background()), func(string) bool { return false })
	}()
	time.Sleep(50 * time.Millisecond)
	complete := true
	if err := h.st.PutChat(Name, store.ChatInfo{ID: c.chat(), HistoryComplete: &complete}); err != nil {
		t.Fatal(err)
	}
	lock.Unlock() // and has reached the start

	s := <-got
	if s.Err != nil || s.Outcome == network.StepMoved || s.Outcome == network.StepStart {
		t.Errorf("the step is %+v, want one that asked nothing", s)
	}
	if h.g.traffic.BacklogAsked() != asked || len(h.m.pagesSince(n)) != 0 {
		t.Errorf("the step asked Meta")
	}
}

// A scroll-back that began while the queue's request waited its turn in
// the pace goes first: the queue's request is not made.
func TestAQueueRequestYieldsAtItsTurn(t *testing.T) {
	h := newHarness(t)
	h.g.WithHistoryQueue()
	serve(h.m, queueChats...)
	h.login(validSession()) // not live: no Run of the queue, the request is made here
	h.clock.Advance(network.BacklogQuiet)
	gen, cli, _ := h.g.running()
	ask := func() error {
		return h.g.request(network.WithBacklog(context.Background()), gen, func(ctx context.Context) error {
			_, err := cli.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{ThreadID: queueChats[0].thread})
			return err
		})
	}
	n := h.m.mark()
	h.g.ScrolledBack()
	if err := ask(); wire.KindOf(err) != wire.NotConnected {
		t.Errorf("a queue request during a scroll-back = %v, want notConnected", err)
	}
	if got := h.m.pagesSince(n); len(got) != 0 {
		t.Fatalf("the queue asked %v during a scroll-back", got)
	}
	h.clock.Advance(network.BacklogYield)
	if err := ask(); err != nil {
		t.Errorf("a queue request after the yield = %v", err)
	}
}

// A page the queue's counter did not see (asked outside the queue's
// context) is still reported as the page it was, so the engine's guard
// sees a page with no counted request and stops the queue. Only a chat
// already finished is passed over without a request.
func TestAnUncountedPageIsNotPassedOver(t *testing.T) {
	h := newHarness(t)
	h.g.WithHistoryQueue()
	c := oldChat{key: 1000351, thread: "340282366841710300000000000000351", active: start.Add(-100 * 24 * time.Hour), pages: 3}
	serve(h.m, c)
	h.login(validSession()) // not live: no Run of the queue, the step is called here
	h.clock.Advance(network.BacklogQuiet)
	asked := h.g.traffic.BacklogAsked()
	s := h.g.backlogNext(context.Background(), func(string) bool { return false }) // no WithBacklog
	if h.g.traffic.BacklogAsked() != asked {
		t.Fatalf("a request outside the queue's context was counted as the queue's")
	}
	if s.Outcome != network.StepMoved || s.Err != nil {
		t.Errorf("the uncounted page is %+v, want StepMoved for the engine to judge", s)
	}
}

// serveListing lists first on the inbox's first page and each later page
// of pages by its cursor ("inbox-<i>", i from 1), every chat with its
// history, and his read marker at its last activity.
func serveListing(m *fakeMeta, first []oldChat, pages ...[]oldChat) {
	all := append([]oldChat{}, first...)
	for _, p := range pages {
		all = append(all, p...)
	}
	serve(m, all...)
	threads := func(cs []oldChat) []*slidetypes.ThreadInfo {
		var out []*slidetypes.ThreadInfo
		for _, c := range cs {
			t := threadWith(c.key, c.thread, alice(), c.active)
			t.SlideReadReceipts = []slidetypes.ReadReceipt{{ParticipantFBID: selfFBID, WatermarkTimestampMS: ms(c.active)}}
			out = append(out, t)
		}
		return out
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mailbox = mailboxOf(len(pages) > 0, threads(first)...)
	m.mailbox.ThreadsByFolder.PageInfo.EndCursor = "inbox-1"
	m.mailboxPage = func(_ int, cursor string) (*slidetypes.PaginateMailboxResponse, error) {
		i, err := strconv.Atoi(strings.TrimPrefix(cursor, "inbox-"))
		if err != nil || i < 1 || i > len(pages) {
			return nil, fmt.Errorf("invented: unknown inbox cursor")
		}
		mb := mailboxOf(i < len(pages), threads(pages[i-1])...)
		if i < len(pages) {
			mb.ThreadsByFolder.PageInfo.EndCursor = fmt.Sprintf("inbox-%d", i+1)
		}
		return &slidetypes.PaginateMailboxResponse{Mailbox: &slidetypes.MailboxPage{ThreadsByFolder: mb.ThreadsByFolder}}, nil
	}
}

// The listing beyond the year: a chat active 10 days ago, one 400 days
// ago (the first listing stops there), then two older pages.
var (
	listNew   = oldChat{key: 1000401, thread: "340282366841710300000000000000401", active: start.Add(-10 * 24 * time.Hour), pages: 1}
	listYear  = oldChat{key: 1000402, thread: "340282366841710300000000000000402", active: start.Add(-400 * 24 * time.Hour), pages: 1}
	listOld   = oldChat{key: 1000403, thread: "340282366841710300000000000000403", active: start.Add(-500 * 24 * time.Hour), pages: 2}
	listOlder = oldChat{key: 1000404, thread: "340282366841710300000000000000404", active: start.Add(-600 * 24 * time.Hour), pages: 1}
)

func listingHarness(t *testing.T, dir string) *harness {
	t.Helper()
	h := newQueueHarness(t, dir)
	serveListing(h.m, []oldChat{listNew}, []oldChat{listYear}, []oldChat{listOld}, []oldChat{listOlder})
	return h
}

func (m *fakeMeta) listPagesSince(n int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, c := range m.calls[n:] {
		if c.name == "PaginateMailbox" {
			out = append(out, c.arg)
		}
	}
	return out
}

func TestTheQueueListsChatsPastTheYear(t *testing.T) {
	h := listingHarness(t, t.TempDir()+"/connect")
	h.login(validSession())
	if got := h.g.value("olderCursor"); got != "inbox-2" {
		t.Fatalf("the first listing kept %q, want where it stopped", got)
	}
	if _, ok, _ := h.st.Chat(Name, listOld.chat()); ok {
		t.Fatalf("a chat past the year was listed at the first sign-in")
	}
	n := h.m.mark()
	asked := h.g.traffic.BacklogAsked()
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	// Ruling 9: every request since is the queue's, at its pace.
	if got := append(h.m.namesSince(n), h.m.listPagesSince(n)...); int64(len(got)) != h.g.traffic.BacklogAsked()-asked {
		t.Errorf("requests %v, of them %d the queue's", got, h.g.traffic.BacklogAsked()-asked)
	}
	if got := h.m.listPagesSince(n); strings.Join(got, " ") != "inbox-2 inbox-3" {
		t.Errorf("the queue listed %v, want the two pages past the first listing", got)
	}
	for _, c := range []oldChat{listOld, listOlder} {
		if !h.chat(c.chat()).HistoryComplete {
			t.Errorf("chat %s listed past the year was not paged to its start", c.chat())
		}
	}
	if !h.g.olderListed() {
		t.Errorf("the listing is not done")
	}
}

func TestAnOldListedChatLandsInThePastWithNoUnread(t *testing.T) {
	h := listingHarness(t, t.TempDir()+"/connect")
	h.login(validSession())
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	p, err := h.st.Chats(Name, 0, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, c := range p.Items {
		ids = append(ids, c.ID)
		if c.Unread != 0 {
			t.Errorf("chat %s has %d unread", c.ID, c.Unread)
		}
	}
	want := []string{listNew.chat(), listYear.chat(), listOld.chat(), listOlder.chat()}
	if strings.Join(ids, " ") != strings.Join(want, " ") {
		t.Errorf("the chats are in the order %v, want %v (older chats in the past)", ids, want)
	}
	if c := h.chat(listOld.chat()); c.LastActivityMs != listOld.active.UnixMilli() {
		t.Errorf("an old chat's activity is %v, want its own", time.UnixMilli(c.LastActivityMs))
	}
}

func TestListingStopsWhenThereIsNothingOlder(t *testing.T) {
	dir := t.TempDir() + "/connect"
	h := listingHarness(t, dir)
	h.login(validSession())
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	if !h.g.olderListed() {
		t.Fatalf("the listing is not done")
	}
	n := h.m.mark()
	h.advance(30*time.Minute, time.Second)
	if got := h.m.listPagesSince(n); len(got) != 0 {
		t.Errorf("the queue listed %v once there was nothing older", got)
	}
	h.close()

	h = listingHarness(t, dir)
	h.login(validSession())
	n = h.m.mark()
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	if got := h.m.listPagesSince(n); len(got) != 0 {
		t.Errorf("after a relaunch the queue listed %v again", got)
	}
}

func TestListingObeysPushback(t *testing.T) {
	h := listingHarness(t, t.TempDir()+"/connect")
	h.login(validSession())
	f := h.m.mailboxPage
	h.m.mailboxPage = func(n int, cursor string) (*slidetypes.PaginateMailboxResponse, error) {
		if cursor == "inbox-2" {
			return nil, httpclient.ErrRateLimited
		}
		return f(n, cursor)
	}
	n := h.m.mark()
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	got := h.m.listPagesSince(n)
	if len(got) != 1 {
		t.Fatalf("the queue listed %v, want the rate-limited page", got)
	}
	v := h.g.value("hq.hold")
	if _, kind, _ := strings.Cut(v, "\n"); kind != string(wire.RateLimited) {
		t.Fatalf("hq.hold is %q", v)
	}
	m := h.m.mark()
	h.advance(23*time.Hour, 10*time.Minute)
	if got := h.m.namesSince(m); len(got) != 0 {
		t.Errorf("the queue asked %v within the day", got)
	}
}

// holdChat puts c in the store as a sync left it: listed, its thread known,
// last active at at.
func (h *harness) holdChat(c oldChat, at time.Time) {
	h.t.Helper()
	if err := h.st.PutChat(Name, store.ChatInfo{ID: c.chat(), Kind: "single", LastActivityMs: at.UnixMilli()}); err != nil {
		h.t.Fatal(err)
	}
	_ = h.g.keep("igid."+c.chat(), c.thread)
}

// A store listed before the queue kept where its listing stopped (his):
// the queue's listing goes on from the first page the sign-in loaded, as
// the first listing paged it, and never asks with an empty cursor.
func TestAnOldStoresListingGoesOnFromTheSignInsFirstPage(t *testing.T) {
	h := listingHarness(t, t.TempDir()+"/connect")
	_ = h.g.keep("backfilled", "1") // listed by an older version: no place kept
	n := h.m.mark()
	h.login(validSession())
	if got := h.g.value("olderCursor"); got != "inbox-1" {
		t.Fatalf("the sign-in kept %q, want the first page's cursor", got)
	}
	h.goLive()
	h.advance(5*time.Minute, time.Second)
	got := h.m.listPagesSince(n)
	if len(got) == 0 || got[0] != "inbox-1" {
		t.Fatalf("the queue listed %v, want the first page's cursor first, as the first listing asks", got)
	}
	for _, c := range got {
		if c == "" {
			t.Fatalf("a listing request went with an empty cursor")
		}
	}

	// With no place known (a run that resumed without loading the inbox),
	// nothing is listed.
	h2 := listingHarness(t, t.TempDir()+"/connect")
	h2.login(validSession())
	_ = h2.g.keep("olderCursor", "")
	m := h2.m.mark()
	h2.goLive()
	h2.advance(10*time.Minute, time.Second)
	if got := h2.m.listPagesSince(m); len(got) != 0 {
		t.Errorf("with no place known the queue listed %v", got)
	}
}

// A listing page never rewrites a chat the store holds: its activity and
// its mark that newer lines are still to fetch stay as they were.
func TestAListingPageLeavesHeldChatsAlone(t *testing.T) {
	mid := oldChat{key: 1000405, thread: "340282366841710300000000000000405", active: start.Add(-200 * 24 * time.Hour), pages: 1}
	h := newQueueHarness(t, t.TempDir()+"/connect")
	serveListing(h.m, []oldChat{listNew}, []oldChat{mid}, []oldChat{listYear}, []oldChat{listOld}, []oldChat{listOlder})
	_ = h.g.keep("backfilled", "1") // an older version's store: the walk starts at the top
	h.login(validSession())
	// mid is held, with a line the socket missed: older activity than the
	// listing shows, and still marked behind.
	held := mid.active.Add(-time.Hour)
	h.holdChat(mid, held)
	_ = h.g.keep("behind."+mid.chat(), "1")
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	if c := h.chat(mid.chat()); c.LastActivityMs != held.UnixMilli() {
		t.Errorf("the held chat's activity moved to %v", time.UnixMilli(c.LastActivityMs))
	}
	if h.g.value("behind."+mid.chat()) == "" {
		t.Errorf("the held chat lost its mark that newer lines are still to fetch")
	}
	if _, ok, _ := h.st.Chat(Name, listOld.chat()); !ok {
		t.Errorf("a new old chat was not stored")
	}
}

// The end of the walk: a page past the year that brings no chat the store
// did not hold ends the listing.
func TestAListingPagePastTheYearWithNothingNewEndsIt(t *testing.T) {
	h := listingHarness(t, t.TempDir()+"/connect")
	h.login(validSession())
	h.holdChat(listOld, listOld.active)
	n := h.m.mark()
	h.goLive()
	h.advance(30*time.Minute, time.Second)
	if got := h.m.listPagesSince(n); strings.Join(got, " ") != "inbox-2" {
		t.Errorf("the queue listed %v, want to stop after the page with nothing new", got)
	}
	if !h.g.olderListed() {
		t.Errorf("the listing is not done")
	}
	if _, ok, _ := h.st.Chat(Name, listOlder.chat()); ok {
		t.Errorf("a page past the end was listed")
	}
}

// holdRecorder is the other Meta network's queue, as far as a hold goes.
type holdRecorder struct {
	mu    sync.Mutex
	kinds []wire.Kind
}

func (r *holdRecorder) HoldQueue(kind wire.Kind) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kinds = append(r.kinds, kind)
}

func (r *holdRecorder) got() []wire.Kind {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]wire.Kind(nil), r.kinds...)
}

// A Meta pushback on Instagram holds Messenger's queue too (one account
// provider, one Mac), and Messenger's holds Instagram's.
func TestAMetaPushbackHoldsTheOtherMetaQueue(t *testing.T) {
	peer := &holdRecorder{}
	h := openHarness(t, t.TempDir()+"/connect", nil, withQueue, func(h *harness) { h.g.HoldWith(peer) })
	serve(h.m, append([]oldChat{{key: aliceFBI, thread: aliceThread, active: start.Add(-100 * 24 * time.Hour), pages: 9}}, queueChats...)...)
	h.login(validSession())
	h.goLive()
	h.m.setErr("SendMessage", httpclient.ErrCheckpointRequired)
	if _, err := h.g.Send(context.Background(), network.Outgoing{ChatID: aliceChat, Text: "invented hello"}); err != nil {
		t.Fatal(err)
	}
	h.settle()
	if got := peer.got(); len(got) == 0 || got[len(got)-1] != wire.Checkpoint {
		t.Errorf("Messenger's queue was held with %v, want a checkpoint", got)
	}

	// The other way: Messenger's pushback holds Instagram's queue a day.
	h2 := newQueueHarness(t, t.TempDir()+"/connect")
	serve(h2.m, queueChats...)
	h2.login(validSession())
	n := h2.m.mark()
	h2.g.HoldQueue(wire.Checkpoint)
	h2.goLive()
	h2.advance(23*time.Hour, 10*time.Minute)
	if got := h2.m.pagesSince(n); len(got) != 0 {
		t.Errorf("Instagram's queue asked %v within the day after Messenger's pushback", got)
	}
	h2.advance(2*time.Hour, time.Minute)
	if got := h2.m.pagesSince(n); len(got) == 0 {
		t.Errorf("Instagram's queue asked nothing once the day had passed")
	}
}
