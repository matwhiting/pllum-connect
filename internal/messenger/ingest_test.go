package messenger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

func containsText(b []byte, s string) bool { return bytes.Contains(b, []byte(s)) }

// kindErr is the kind an error carries, or "".
func kindErr(err error) wire.Kind {
	var e *wire.Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

// states is every state event so far, as "state" or "state/kind".
func (h *harness) states() []string {
	h.emu.Lock()
	defer h.emu.Unlock()
	var out []string
	for _, e := range h.events {
		if e["event"] != "state" {
			continue
		}
		s := string(e["state"].(network.State))
		if k, _ := e["kind"].(string); k != "" {
			s += "/" + k
		}
		out = append(out, s)
	}
	return out
}

func (h *harness) lastState() string {
	s := h.states()
	if len(s) == 0 {
		return ""
	}
	return s[len(s)-1]
}

// login sends a login and waits for its run to settle.
func (h *harness) login(t *testing.T, s network.Session) {
	t.Helper()
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: s}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	h.settle()
}

// settle waits until no request has been made for a moment.
func (h *harness) settle() {
	last := -1
	for i := 0; i < 400; i++ {
		n := len(h.f.requests())
		if n == last {
			time.Sleep(20 * time.Millisecond)
			if len(h.f.requests()) == n {
				return
			}
		}
		last = n
		time.Sleep(10 * time.Millisecond)
	}
}

// lines are a chat's lines by ID.
func (h *harness) lines(t *testing.T, chat string) map[string]store.Message {
	t.Helper()
	page, err := h.st.Messages(Name, chat, "", 200)
	if err != nil {
		t.Fatalf("messages of %s: %v", chat, err)
	}
	out := map[string]store.Message{}
	for _, m := range page.Items {
		out[m.ID] = m
	}
	return out
}

// waitValue waits for the network to keep a value called name.
func (h *harness) waitValue(t *testing.T, name string) {
	t.Helper()
	eventually(t, "the value "+name, func() bool {
		v, _, _ := h.st.NetworkValue(Name, name)
		return v != ""
	})
}

// chat is one chat as the store reads it; it fails when there is none.
func (h *harness) chat(t *testing.T, id string) store.Chat {
	t.Helper()
	c, ok, err := h.st.Chat(Name, id)
	if err != nil || !ok {
		t.Fatalf("chat %s: ok %v, err %v", id, ok, err)
	}
	return c
}

// chatIDs are the IDs of every chat the store lists.
func (h *harness) chatIDs(t *testing.T) []string {
	t.Helper()
	page, err := h.st.Chats(Name, 0, "", 200)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, c := range page.Items {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return ids
}

// A group without encryption is a chat keyed by its thread key, with its
// name, its people (him among them), its lines under Meta's message IDs,
// and the live lines that come on the socket.
func TestAGroupIsKeyedByThreadKey(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := history(groupOne, true,
		line("mid.$invented-g1", aliceFBID, at, canary+" hello group"),
		line("mid.$invented-g2", selfFBID, at.Add(time.Minute), canary+" mine"),
	)
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at.Add(time.Minute), "inbox")}
	tbl.LSAddParticipantIdToGroupThread = []*table.LSAddParticipantIdToGroupThread{
		{ThreadKey: groupOne, ContactId: aliceFBID}, {ThreadKey: groupOne, ContactId: bobFBID}, {ThreadKey: groupOne, ContactId: selfFBID},
	}
	tbl.LSDeleteThenInsertContact = []*table.LSDeleteThenInsertContact{{Id: aliceFBID, Name: "Alice Invented"}, {Id: bobFBID, Name: "Bob Invented"}}
	h.f.table = tbl
	h.signIn(t)

	key := fmt.Sprint(groupOne)
	c := h.chat(t, key)
	if c.Kind != "group" || c.Title != canary+" name" {
		t.Errorf("the group reads as kind %q titled %q", c.Kind, c.Title)
	}
	people := map[string]store.Participant{}
	for _, p := range c.Participants {
		people[p.ID] = p
	}
	if people[fmt.Sprint(aliceFBID)].Name != "Alice Invented" || !people[fmt.Sprint(selfFBID)].IsSelf || len(people) != 3 {
		t.Errorf("participants %+v, want Alice (named), Bob and him", c.Participants)
	}
	got := h.lines(t, key)
	if l := got["mid.$invented-g1"]; l.SenderID != fmt.Sprint(aliceFBID) || l.FromMe || l.Text != canary+" hello group" || l.Kind != "text" {
		t.Errorf("Alice's line reads %+v", l)
	}
	if l := got["mid.$invented-g2"]; !l.FromMe {
		t.Errorf("his own line is not his")
	}
	for _, id := range h.chatIDs(t) {
		if id != key && (id == key+"@msgr" || id == key+"@g.us") {
			t.Errorf("the group is also chat %s", id)
		}
	}

	// A live line on the socket lands in the same chat.
	h.f.last().dispatch(&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{
		ThreadKey: groupOne, MessageId: "mid.$invented-g3", SenderId: bobFBID, TimestampMs: at.Add(time.Hour).UnixMilli(), Text: canary + " live",
	}}})
	eventually(t, "the live line", func() bool {
		_, ok := h.lines(t, key)["mid.$invented-g3"]
		return ok
	})
	if l := h.lines(t, key)["mid.$invented-g3"]; l.Source != store.SourceLive {
		t.Errorf("a live line has source %q", l.Source)
	}
}

// Review Focus 4: a one-to-one shows once. His older, unencrypted thread
// with someone is an alias of their encrypted chat, <FBID>@msgr: its lines,
// its live lines and a pointer from another older thread all land there,
// and no second chat appears. A one-to-one that never became encrypted is
// <FBID>@msgr too, so its ID never changes when it does.
func TestAnOldOneToOneIsAnAliasOfItsEncryptedChat(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 2, 1, 8, 0, 0, 0, time.UTC)
	const olderCarol int64 = 7700000000000001 // an older thread of Carol's under another key
	tbl := inbox()
	old := history(aliceFBID, true, line("mid.$invented-a1", aliceFBID, at, canary+" before encryption"))
	tbl.LSUpsertMessage = old.LSUpsertMessage
	tbl.LSInsertNewMessageRange = old.LSInsertNewMessageRange
	tbl.LSDeleteThenInsertThread = append(tbl.LSDeleteThenInsertThread,
		thread(daveFBID, table.ONE_TO_ONE, at, "inbox"), // never encrypted
		thread(olderCarol, table.ONE_TO_ONE, at, "e2ee_cutover"),
	)
	// Carol's encrypted thread is not on this page: her chat is known only
	// through her older thread's pointer.
	tbl.LSDeleteThenInsertThread = slices.DeleteFunc(tbl.LSDeleteThenInsertThread, func(r *table.LSDeleteThenInsertThread) bool {
		return r.ThreadKey == carolFBID && r.ThreadType.IsWhatsApp()
	})
	tbl.LSVerifyHybridThreadExists = append(tbl.LSVerifyHybridThreadExists,
		&table.LSVerifyHybridThreadExists{ThreadKey: olderCarol, ThreadJID: carolFBID, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE})
	tbl.LSDeleteThenInsertContact = []*table.LSDeleteThenInsertContact{{Id: aliceFBID, Name: "Alice Invented"}}
	h.f.table = tbl
	h.signIn(t)

	alice := fmt.Sprint(aliceFBID) + "@msgr"
	if l, ok := h.lines(t, alice)["mid.$invented-a1"]; !ok || l.Text != canary+" before encryption" {
		t.Fatalf("the older thread's line is not in %s", alice)
	}
	if c := h.chat(t, alice); c.Kind != "single" || c.Title != "Alice Invented" {
		t.Errorf("Alice's chat reads as kind %q titled %q", c.Kind, c.Title)
	}
	if got, ok := h.st.ChatFor(Name, fmt.Sprint(aliceFBID)); !ok || got != alice {
		t.Errorf("her older thread resolves to %q, want %s", got, alice)
	}
	for _, p := range h.chat(t, fmt.Sprint(carolFBID)+"@msgr").Participants {
		if p.ID == fmt.Sprint(olderCarol) {
			t.Errorf("Carol's chat names her older thread's key as a person")
		}
	}
	if got, ok := h.st.ChatFor(Name, fmt.Sprint(daveFBID)); !ok || got != fmt.Sprint(daveFBID)+"@msgr" {
		t.Errorf("a one-to-one never encrypted is %q, want <FBID>@msgr", got)
	}

	// A live line on her older thread, and one on Carol's other older
	// thread, land in their encrypted chats.
	h.f.last().dispatch(&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{
		{ThreadKey: aliceFBID, MessageId: "mid.$invented-a2", SenderId: aliceFBID, TimestampMs: at.Add(time.Hour).UnixMilli(), Text: "x"},
		{ThreadKey: olderCarol, MessageId: "mid.$invented-c1", SenderId: carolFBID, TimestampMs: at.Add(time.Hour).UnixMilli(), Text: "y"},
	}})
	eventually(t, "the live lines", func() bool {
		_, a := h.lines(t, alice)["mid.$invented-a2"]
		_, c := h.lines(t, fmt.Sprint(carolFBID)+"@msgr")["mid.$invented-c1"]
		return a && c
	})

	ids := h.chatIDs(t)
	for _, id := range ids {
		switch id {
		case fmt.Sprint(aliceFBID), fmt.Sprint(carolFBID), fmt.Sprint(olderCarol), fmt.Sprint(daveFBID):
			t.Errorf("an older one-to-one is a second chat: %s", id)
		}
	}
	want := []string{
		fmt.Sprint(groupOne), fmt.Sprint(groupTwo),
		fmt.Sprint(aliceFBID) + "@msgr", fmt.Sprint(bobFBID) + "@msgr", fmt.Sprint(carolFBID) + "@msgr", fmt.Sprint(daveFBID) + "@msgr",
		fmt.Sprint(encGroup) + "@g.us",
	}
	slices.Sort(want)
	if !slices.Equal(ids, want) {
		t.Errorf("chats %v, want %v", ids, want)
	}
	// The gate counts chats after aliasing (review I3): Alice, Bob, Carol
	// and Dave; two groups and the encrypted group.
	if c := h.m.GateCounts().(GateCounts); c.OneToOneChats != 4 || c.GroupChats != 3 {
		t.Errorf("one-to-one chats %d, group chats %d; want 4 and 3", c.OneToOneChats, c.GroupChats)
	}
}

func TestLoadErrorsBecomeKinds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		state   string
		blocked wire.Kind // what the pace now refuses with ("" for nothing)
	}{
		{"challenge", httpclient.ErrChallengeRequired, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"checkpoint", httpclient.ErrCheckpointRequired, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"consent", httpclient.ErrConsentRequired, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"redirected to a checkpoint", httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired, URL: "https://invalid.invalid/"}, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"session ended", fmt.Errorf("%w: invented", httpclient.ErrTokenInvalidated), "reconnectNeeded", meta.ReconnectNeeded},
		{"suspended", httpclient.RedirectedError{Type: httpclient.ErrAccountSuspended, URL: "https://invalid.invalid/"}, "banned", meta.Banned},
		{"rate limited", httpclient.ErrRateLimited, "notConnected/rateLimited", wire.RateLimited},
		{"no connection", fmt.Errorf("%w: invented", httpclient.ErrRequestFailed), "notConnected/offline", ""},
		{"anything else", errors.New("invented"), "notConnected/internal", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.f.loadErr = tc.err
			h.login(t, validSession())
			if got := h.states(); !slices.Equal(got, []string{"connecting", tc.state}) {
				t.Errorf("states %v, want connecting, %s", got, tc.state)
			}
			k, blocked := h.m.pace.Blocked()
			if tc.blocked == "" && blocked || tc.blocked != "" && k != tc.blocked {
				t.Errorf("the pace is blocked by %q (%v), want %q", k, blocked, tc.blocked)
			}
			for _, c := range h.f.requests() {
				if c.name != "LoadMessagesPage" {
					t.Errorf("%s was asked after the messages page failed", c.name)
				}
			}
		})
	}
}

func TestACheckpointStopsEveryRequestUntilANewLogin(t *testing.T) {
	f := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)
	f.taskErr = func(task socket.Task) error {
		if _, ok := task.(*socket.FetchThreadsTask); ok {
			return httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired, URL: "https://invalid.invalid/"}
		}
		return nil
	}
	h.login(t, validSession())
	eventually(t, "the checkpoint", func() bool { return h.lastState() == "reconnectNeeded/checkpoint" })
	h.settle()
	calls := len(f.requests())

	// Nothing more is asked of Meta: not the rest of the listing, not an
	// older page, not after a socket event, not after hours.
	if _, err := h.m.FetchOlder(context.Background(), fmt.Sprint(groupOne)); kindErr(err) != wire.Checkpoint {
		t.Errorf("FetchOlder = %v, want checkpoint", err)
	}
	h.m.Nudge()
	f.last().dispatch(&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: 9100000000000001, MessageId: "mid.$invented-x", SenderId: bobFBID, TimestampMs: 1}}})
	f.clock.Advance(48 * time.Hour)
	h.settle()
	if got := f.requests(); len(got) != calls {
		t.Errorf("%d requests after the checkpoint, want none: %v", len(got)-calls, got[calls:])
	}
	if h.lastState() != "reconnectNeeded/checkpoint" {
		t.Errorf("the state moved without a login: %s", h.lastState())
	}

	// The same cookies sent again (Pllum's start-up login) ask nothing, in
	// this helper or in the next one over the same folder.
	h.login(t, validSession())
	if got := len(f.requests()); got != calls {
		t.Errorf("the stopped session, sent again, made %d requests", got-calls)
	}
	h.close()
	next := newHarnessOver(t, dir, f)
	if next.lastState() != "reconnectNeeded/checkpoint" {
		t.Errorf("after a restart the state is %q, want the checkpoint", next.lastState())
	}
	next.login(t, validSession())
	if got := len(f.requests()); got != calls {
		t.Errorf("after a restart, the stopped session made %d requests", got-calls)
	}
	if b, ok, _ := next.st.NetworkValue(Name, "block"); !ok || containsText([]byte(b), "xs-invented-1") {
		t.Errorf("the block is not kept, or keeps a cookie value")
	}

	// He signs in again in the window (a new session): a new login asks
	// again, and the pace is clear.
	f.mu.Lock()
	f.taskErr = nil
	f.mu.Unlock()
	fresh := validSession()
	fresh["xs"] = "xs-invented-2"
	next.login(t, fresh)
	if got := len(f.requests()); got == calls {
		t.Errorf("a new login made no requests")
	}
	if _, blocked := next.m.pace.Blocked(); blocked {
		t.Errorf("the pace is still blocked after a new login")
	}
}

func TestReactionsReceiptsAndMutes(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := history(groupOne, true,
		line("mid.$invented-r1", aliceFBID, at, "one"),
		line("mid.$invented-r2", selfFBID, at.Add(time.Minute), "two"),
		line("mid.$invented-r3", aliceFBID, at.Add(2*time.Minute), "three"),
	)
	g := thread(groupOne, table.GROUP_THREAD, at.Add(2*time.Minute), "inbox")
	g.LastReadWatermarkTimestampMs = at.UnixMilli() // he has read the first only
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{g}
	tbl.LSUpsertReaction = []*table.LSUpsertReaction{{ThreadKey: groupOne, MessageId: "mid.$invented-r1", ActorId: bobFBID, Reaction: "👍"}}
	h.f.table = tbl
	h.signIn(t)
	key := fmt.Sprint(groupOne)
	if c := h.chat(t, key); c.Unread != 1 {
		t.Errorf("unread %d, want 1 (his read marker is at the first line; the second is his)", c.Unread)
	}
	if r := h.lines(t, key)["mid.$invented-r1"].Reactions; len(r) != 1 || r[0].Emoji != "👍" || r[0].SenderID != fmt.Sprint(bobFBID) {
		t.Errorf("reactions %+v, want Bob's thumbs up", r)
	}

	cli := h.f.last()
	cli.dispatch(&table.LSTable{
		LSUpsertReaction: []*table.LSUpsertReaction{{ThreadKey: groupOne, MessageId: "mid.$invented-r2", ActorId: selfFBID, Reaction: "❤"}},
		LSDeleteReaction: []*table.LSDeleteReaction{{ThreadKey: groupOne, MessageId: "mid.$invented-r1", ActorId: bobFBID}},
		// Alice has read his line; he has read everything, elsewhere.
		LSUpdateReadReceipt: []*table.LSUpdateReadReceipt{
			{ThreadKey: groupOne, ContactId: aliceFBID, ReadWatermarkTimestampMs: at.Add(time.Minute).UnixMilli()},
			{ThreadKey: groupOne, ContactId: selfFBID, ReadWatermarkTimestampMs: at.Add(2 * time.Minute).UnixMilli()},
		},
		LSUpdateThreadMuteSetting: []*table.LSUpdateThreadMuteSetting{{ThreadKey: groupOne, MuteExpireTimeMS: -1}},
	})
	eventually(t, "the socket's changes", func() bool { return h.chat(t, key).Muted })
	got := h.lines(t, key)
	if r := got["mid.$invented-r1"].Reactions; len(r) != 0 {
		t.Errorf("a removed reaction is still there: %+v", r)
	}
	if r := got["mid.$invented-r2"].Reactions; len(r) != 1 || !r[0].FromMe {
		t.Errorf("his own reaction reads %+v", r)
	}
	if got["mid.$invented-r2"].SeenMs == 0 {
		t.Errorf("Alice's receipt did not mark his line seen")
	}
	if c := h.chat(t, key); c.Unread != 0 {
		t.Errorf("unread %d after he read it elsewhere, want 0", c.Unread)
	}

	// A mute with an end, then none; LSMarkThreadRead moves his marker too.
	until := h.f.clock.Now().Add(time.Hour).UnixMilli()
	cli.dispatch(&table.LSTable{LSUpdateThreadMuteSetting: []*table.LSUpdateThreadMuteSetting{{ThreadKey: groupOne, MuteExpireTimeMS: until}}})
	h.f.clock.Advance(2 * time.Hour)
	eventually(t, "the timed mute to end", func() bool { return !h.chat(t, key).Muted })
	cli.dispatch(&table.LSTable{LSUpdateThreadMuteSetting: []*table.LSUpdateThreadMuteSetting{{ThreadKey: groupOne, MuteExpireTimeMS: -1}}})
	eventually(t, "muted again", func() bool { return h.chat(t, key).Muted })
	cli.dispatch(&table.LSTable{LSUpdateThreadMuteSetting: []*table.LSUpdateThreadMuteSetting{{ThreadKey: groupOne, MuteExpireTimeMS: 0}}})
	eventually(t, "unmuted", func() bool { return !h.chat(t, key).Muted })
}

// The history the helper fetches by itself, as Instagram's (A2): every
// request through the pace, 2 s or more apart; every folder up to its page
// cap or a thread over a year old; then, for each thread active in the
// last 90 days, up to 5 pages of its older lines.
func TestBackfillIsPaced(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	now := h.f.clock.Now()
	// An inbox that never ends: one recent group a page.
	h.f.endless = true
	// Each group's history never ends either.
	pages := map[int64]int{}
	h.f.olderFn = func(key int64) *table.LSTable {
		pages[key]++
		at := now.Add(-time.Duration(24*pages[key]) * time.Hour)
		return history(key, true, line(fmt.Sprintf("mid.$invented-%d-%d", key%1000, pages[key]), aliceFBID, at, "x"))
	}
	h.login(t, validSession())
	h.waitValue(t, "backfilled")

	asked := map[int64]int{}
	var threadPages int
	h.f.mu.Lock()
	for _, task := range h.f.messageTasks {
		asked[task.ThreadKey]++
	}
	threadPages = len(h.f.threadTasks)
	h.f.mu.Unlock()
	if threadPages != 2*folderPages {
		t.Errorf("%d thread pages, want %d (each folder's cap)", threadPages, 2*folderPages)
	}
	for key, n := range asked {
		if n != threadHistoryPages {
			t.Errorf("thread …%d: %d history pages, want %d", key%1000, n, threadHistoryPages)
		}
	}
	if len(asked) == 0 {
		t.Errorf("no thread's history was fetched")
	}
	gaps(t, h.f.requests())

	// A folder stops at a thread over a year old, and no history is fetched
	// for threads quiet for more than 90 days.
	h2 := newHarness(t)
	h2.m.gate = false
	now2 := h2.f.clock.Now()
	h2.f.table = &table.LSTable{}
	h2.f.folders[inboxOne] = []*table.LSTable{
		threadsPage(inboxOne, true, thread(8800000000000101, table.GROUP_THREAD, now2.Add(-200*24*time.Hour), "inbox")),
		threadsPage(inboxOne, true, thread(8800000000000102, table.GROUP_THREAD, now2.Add(-400*24*time.Hour), "inbox")),
		threadsPage(inboxOne, true, thread(8800000000000103, table.GROUP_THREAD, now2.Add(-500*24*time.Hour), "inbox")),
	}
	h2.login(t, validSession())
	h2.waitValue(t, "backfilled")
	h2.f.mu.Lock()
	var inboxAsks int
	for _, task := range h2.f.threadTasks {
		if task.SyncGroup == 1 && task.ParentThreadKey == -1 {
			inboxAsks++
		}
	}
	history := len(h2.f.messageTasks)
	h2.f.mu.Unlock()
	if inboxAsks != 2 {
		t.Errorf("%d inbox pages past a year-old thread, want 2", inboxAsks)
	}
	if history != 0 {
		t.Errorf("%d history pages for threads quiet over 90 days, want 0", history)
	}

	// The next sign-in lists and backfills nothing again.
	before := len(h2.f.requests())
	h2.login(t, validSession())
	eventually(t, "the second run's encrypted socket", func() bool {
		h2.f.e2ee.mu.Lock()
		defer h2.f.e2ee.mu.Unlock()
		return h2.f.e2ee.connects == 2
	})
	time.Sleep(300 * time.Millisecond)
	h2.settle()
	for _, c := range h2.f.requests()[before:] {
		if c.name == "FetchThreadsTask" || c.name == "FetchMessagesTask" {
			t.Errorf("a later sign-in asked %s", c.name)
		}
	}
}

// FetchOlder fetches one page of a chat's older thread, older than what the
// store holds, through the pace; when Meta says there is nothing older, a
// group is historyComplete. A chat with no older thread asks nothing.
func TestOlderPagesFetchesTheOlderThread(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := history(groupOne, true, line("mid.$invented-o2", aliceFBID, at, "newest"))
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{
		thread(groupOne, table.GROUP_THREAD, at, "inbox"),
		thread(aliceFBID, table.ONE_TO_ONE, at, "e2ee_cutover"),
		thread(encGroup, table.ENCRYPTED_OVER_WA_GROUP, at, "inbox"),
	}
	// A line of Alice's older thread, from the socket (no range).
	tbl.LSInsertMessage = []*table.LSInsertMessage{{ThreadKey: aliceFBID, MessageId: "mid.$invented-a0", SenderId: aliceFBID, TimestampMs: at.Add(-time.Hour).UnixMilli(), Text: "x"}}
	h.f.table = tbl
	h.f.older[groupOne] = []*table.LSTable{history(groupOne, true, line("mid.$invented-o1", selfFBID, at.Add(-48*time.Hour), "older"))}
	// The first sign-in's backfill is done (an earlier run's), so only
	// FetchOlder asks for history here.
	if err := h.st.PutNetworkValue(Name, "backfilled", "1"); err != nil {
		t.Fatal(err)
	}
	h.signIn(t)
	h.settle()
	key := fmt.Sprint(groupOne)

	added, err := h.m.FetchOlder(context.Background(), key)
	if err != nil || added != 1 {
		t.Fatalf("FetchOlder = %d, %v; want 1 line", added, err)
	}
	h.f.mu.Lock()
	last := h.f.messageTasks[len(h.f.messageTasks)-1]
	h.f.mu.Unlock()
	if last.ThreadKey != groupOne || last.ReferenceMessageId != "mid.$invented-o2" || last.ReferenceTimestampMs != at.UnixMilli() {
		t.Errorf("the page was asked from %+v, want from the oldest line held", last)
	}
	if h.chat(t, key).HistoryComplete {
		t.Fatalf("complete before Meta said so")
	}
	// The next page: nothing older, in the task's own answer this time.
	h.f.mu.Lock()
	h.f.inResponse = true
	h.f.mu.Unlock()
	added, err = h.m.FetchOlder(context.Background(), key)
	if err != nil || added != 0 {
		t.Fatalf("FetchOlder at the start = %d, %v", added, err)
	}
	if !h.chat(t, key).HistoryComplete {
		t.Errorf("no earlier page, and the group is not historyComplete")
	}

	// An encrypted chat with no older thread asks nothing.
	calls := len(h.f.requests())
	if n, err := h.m.FetchOlder(context.Background(), fmt.Sprint(encGroup)+"@g.us"); n != 0 || err != nil {
		t.Errorf("FetchOlder of an encrypted group = %d, %v", n, err)
	}
	if _, err := h.m.FetchOlder(context.Background(), "unplaced:invented"); kindErr(err) != wire.NotFound {
		t.Errorf("FetchOlder of an unknown chat = %v, want notFound", err)
	}
	if len(h.f.requests()) != calls {
		t.Errorf("FetchOlder of a chat with no older thread made a request")
	}

	// A page Meta never answers ends the call with a timeout, not a hang.
	h.f.mu.Lock()
	h.f.silent = true
	h.f.mu.Unlock()
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	if _, err := h.m.FetchOlder(context.Background(), alice); kindErr(err) != wire.Timeout {
		t.Errorf("an unanswered page = %v, want timeout", err)
	}
	// With no range held, the page was anchored on the oldest line held, as
	// the library's connector does (review I1).
	h.f.mu.Lock()
	last = h.f.messageTasks[len(h.f.messageTasks)-1]
	h.f.mu.Unlock()
	if last.ReferenceMessageId != "mid.$invented-a0" {
		t.Errorf("the page was asked from %q, want the oldest line held", last.ReferenceMessageId)
	}
}

// Fix round 3: a Meta line that lands in a row its revoke made first (an
// early row) is a change Pllum must re-read; the same line again is not.
func TestAMetaLineOverItsEarlyRowTellsPllum(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	quiet := func() int {
		time.Sleep(700 * time.Millisecond)
		return h.changedEvents()
	}
	h.f.last().dispatch(&table.LSTable{LSDeleteMessage: []*table.LSDeleteMessage{{ThreadKey: groupOne, MessageId: "mid.$invented-r1"}}})
	before := quiet()
	newLine := func() *table.LSTable {
		return &table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{
			ThreadKey: groupOne, MessageId: "mid.$invented-r1", SenderId: aliceFBID, TimestampMs: at.UnixMilli(), Text: canary + " revoked"}}}
	}
	h.f.last().dispatch(newLine())
	if n := quiet() - before; n != 1 {
		t.Errorf("a line landing in its revoke's row sent %d changed events, want 1", n)
	}
	if l, ok := h.lines(t, fmt.Sprint(groupOne))["mid.$invented-r1"]; !ok || !l.Deleted {
		t.Errorf("the line reads %+v (ok %v), want it held and deleted", l, ok)
	}
	before = h.changedEvents()
	h.f.last().dispatch(newLine())
	if n := quiet() - before; n != 0 {
		t.Errorf("the same line again sent %d changed events, want 0", n)
	}
}
