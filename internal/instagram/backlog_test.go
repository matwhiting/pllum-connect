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
