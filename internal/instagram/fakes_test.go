package instagram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/util/jsontime"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// Every name, ID, cookie and text in these tests is invented.

// testKey is an invented store key.
var testKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(0x50 + i)
	}
	return k
}()

// start is an invented moment: 2026-03-01T09:00:00Z.
var start = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// His account, and two people.
const (
	selfFBID int64 = 1000001
	selfIG         = "17840000000000099"
	aliceFBI int64 = 1000011
	aliceIG        = "17840000000000011"
	bobFBID  int64 = 1000022
	bobIG          = "17840000000000022"

	aliceChat   = "1000011" // the one-to-one's thread key: Alice's FBID
	aliceThread = "340282366841710300000000000000011"
	groupKey    = int64(7700000000000001)
	groupChat   = "7700000000000001"
)

// validSession is an invented web session with every named cookie.
func validSession() network.Session {
	return network.Session{
		"sessionid":  "sess-invented-1",
		"csrftoken":  "csrf-invented-1",
		"ds_user_id": "4000000099",
		"rur":        "rur-invented-1",
		"mid":        "mid-invented-1",
	}
}

// fakeClock moves only when something sleeps on it or a test advances it.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Time
	f       func()
	stopped bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		was := !t.stopped
		t.stopped = true
		return was
	}
}

func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return ctx.Err()
}

// Advance moves the clock and runs every timer now due.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	kept := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.stopped:
		case !t.at.After(c.now):
			t.stopped = true
			due = append(due, t)
		default:
			kept = append(kept, t)
		}
	}
	c.timers = kept
	c.mu.Unlock()
	for _, t := range due {
		t.f()
	}
}

// call is one request a fake client was asked to make.
type call struct {
	name string
	at   time.Time
	arg  string
}

// fakeMeta stands in for Meta: every client the adapter makes is a fakeIG
// over it, scripted by the test, and nothing reaches the network.
type fakeMeta struct {
	mu      sync.Mutex
	clock   *fakeClock
	clients []*fakeIG
	calls   []call

	viewer    *types.PolarisViewer
	mailbox   *slidetypes.Mailbox
	ownFBID   int64
	indexErr  error // LoadIndex fails with it
	stateErr  error // LoadState fails with it (the real one refuses a state over 24 hours old)
	authFirst bool  // a failing request dispatches AuthError first, as the client does

	// mailboxPage answers PaginateMailbox; threadInfo answers GetThread;
	// messagePage answers PaginateMessages. Each may be nil (an error).
	mailboxPage func(n int, cursor string) (*slidetypes.PaginateMailboxResponse, error)
	threadInfo  func(igChatID string) (*slidetypes.ThreadInfoResponse, error)
	messagePage func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error)
	mailboxN    int
}

func newFakeMeta(clock *fakeClock) *fakeMeta {
	return &fakeMeta{
		clock:   clock,
		viewer:  &types.PolarisViewer{ID: selfIG},
		mailbox: mailboxOf(false, threadWith(aliceFBI, aliceThread, alice(), start.Add(-time.Hour))),
		ownFBID: selfFBID,
	}
}

func (m *fakeMeta) record(name, arg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, call{name: name, at: m.clock.Now(), arg: arg})
}

// requests are the calls that reach Meta (not Connect's socket or the
// client's own bookkeeping).
func (m *fakeMeta) requests() []call {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []call
	for _, c := range m.calls {
		switch c.name {
		case "LoadIndex", "PaginateMailbox", "GetThread", "PaginateMessages", "FetchThreadID":
			out = append(out, c)
		}
	}
	return out
}

func (m *fakeMeta) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, c := range m.calls {
		if c.name == name {
			n++
		}
	}
	return n
}

func (m *fakeMeta) last() *fakeIG {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.clients) == 0 {
		return nil
	}
	return m.clients[len(m.clients)-1]
}

// dispatch hands an event to the newest client's handler, as its socket
// would.
func (m *fakeMeta) dispatch(evt slidetypes.ClientEvent) error {
	c := m.last()
	if c == nil {
		return errors.New("no client")
	}
	return c.params.EventHandler(context.Background(), evt)
}

func (m *fakeMeta) newClient(p instameow.ClientParams) igClient {
	c := &fakeIG{m: m, params: p, seq: p.SeqID}
	m.mu.Lock()
	m.clients = append(m.clients, c)
	m.mu.Unlock()
	return c
}

// fail is err, after the AuthError the real client dispatches for it.
func (c *fakeIG) fail(ctx context.Context, err error) error {
	if c.m.authFirst {
		_ = c.params.EventHandler(ctx, &slidetypes.AuthError{Error: err})
	}
	return err
}

// fakeIG is one client over fakeMeta.
type fakeIG struct {
	m      *fakeMeta
	params instameow.ClientParams
	state  json.RawMessage // what LoadState was given
	seq    int64           // where the socket subscribes from, as the real client keeps it
}

func (c *fakeIG) LoadIndex(ctx context.Context) (*types.PolarisViewer, *slidetypes.Mailbox, error) {
	c.m.record("LoadIndex", "")
	c.m.mu.Lock()
	viewer, mailbox, err := c.m.viewer, c.m.mailbox, c.m.indexErr
	c.m.mu.Unlock()
	if err != nil {
		return nil, nil, c.fail(ctx, err)
	}
	_ = c.params.EventHandler(ctx, &slidetypes.ReconnectionStateUpdate{State: json.RawMessage(`{"Configs":{"invented":true}}`)})
	// The real LoadIndex subscribes from the inbox it just loaded, whatever
	// the client was given.
	if mailbox != nil {
		c.seq = mailbox.UQSeqID
	}
	return viewer, mailbox, nil
}

func (c *fakeIG) LoadState(state json.RawMessage) error {
	c.m.record("LoadState", string(state))
	c.m.mu.Lock()
	err := c.m.stateErr
	c.m.mu.Unlock()
	if err != nil {
		return err
	}
	c.state = state
	return nil
}

func (c *fakeIG) DumpState() (json.RawMessage, error) { return c.state, nil }

func (c *fakeIG) Connect(ctx context.Context) { c.m.record("Connect", fmt.Sprint(c.seq)) }

// argsOf are the args of every call called name, in order.
func (m *fakeMeta) argsOf(name string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, c := range m.calls {
		if c.name == name {
			out = append(out, c.arg)
		}
	}
	return out
}

func (c *fakeIG) Disconnect() { c.m.record("Disconnect", "") }

func (c *fakeIG) PaginateMailbox(ctx context.Context, req *slidetypes.PaginateMailboxRequest) (*slidetypes.PaginateMailboxResponse, error) {
	c.m.record("PaginateMailbox", req.Cursor)
	c.m.mu.Lock()
	f := c.m.mailboxPage
	c.m.mailboxN++
	n := c.m.mailboxN
	c.m.mu.Unlock()
	if f == nil {
		return nil, errors.New("no page")
	}
	resp, err := f(n, req.Cursor)
	if err != nil {
		return nil, c.fail(ctx, err)
	}
	return resp, nil
}

func (c *fakeIG) GetThread(ctx context.Context, req *slidetypes.GetThreadInfoRequest) (*slidetypes.ThreadInfoResponse, error) {
	c.m.record("GetThread", req.ThreadFBID)
	c.m.mu.Lock()
	f := c.m.threadInfo
	c.m.mu.Unlock()
	if f == nil {
		return nil, errors.New("no thread")
	}
	resp, err := f(req.ThreadFBID)
	if err != nil {
		return nil, c.fail(ctx, err)
	}
	return resp, nil
}

func (c *fakeIG) PaginateMessages(ctx context.Context, req *slidetypes.PaginateMessagesRequest) (*slidetypes.PaginateMessagesResponse, error) {
	cursor := ""
	if req.AfterCursor != nil {
		cursor = *req.AfterCursor
	}
	c.m.record("PaginateMessages", req.ThreadID+" "+cursor)
	c.m.mu.Lock()
	f := c.m.messagePage
	c.m.mu.Unlock()
	if f == nil {
		return nil, errors.New("no messages")
	}
	resp, err := f(req.ThreadID, cursor)
	if err != nil {
		return nil, c.fail(ctx, err)
	}
	return resp, nil
}

func (c *fakeIG) FetchThreadID(ctx context.Context, threadFBID int64) (*instameow.ThreadIGIDs, error) {
	c.m.record("FetchThreadID", fmt.Sprint(threadFBID))
	return nil, errors.New("not in this task")
}

func (c *fakeIG) GetOwnFBID() int64 {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	return c.m.ownFBID
}

func (c *fakeIG) GetCookies() *cookies.Cookies { return c.params.Cookies }

// harness is one Instagram over a store, a fake Meta and a fake clock.
type harness struct {
	t     *testing.T
	dir   string
	st    *store.Store
	g     *Instagram
	m     *fakeMeta
	clock *fakeClock

	emu    sync.Mutex
	events []map[string]any
}

func newHarness(t *testing.T) *harness {
	return openHarness(t, filepath.Join(t.TempDir(), "connect"), nil)
}

// openHarness opens the store in dir; m, when given, is the fake Meta to
// sign in to again (a restart).
func openHarness(t *testing.T, dir string, m *fakeMeta) *harness {
	t.Helper()
	st, err := store.Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, dir: dir, st: st, clock: newFakeClock()}
	if m == nil {
		m = newFakeMeta(h.clock)
	} else {
		m.clock = h.clock
	}
	h.m = m
	h.g = New(st, dir, testKey, network.Emit(h.collect).For(Name))
	h.g.clock = h.clock
	h.g.newClient = m.newClient
	if err := h.g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.close)
	return h
}

func (h *harness) close() {
	h.g.Stop()
	h.st.Close()
}

func (h *harness) collect(evt map[string]any) {
	h.emu.Lock()
	defer h.emu.Unlock()
	h.events = append(h.events, evt)
}

// named are the events called name.
func (h *harness) named(name string) []map[string]any {
	h.emu.Lock()
	defer h.emu.Unlock()
	var out []map[string]any
	for _, e := range h.events {
		if e["event"] == name {
			out = append(out, e)
		}
	}
	return out
}

// states are the state events, as "state" or "state/kind".
func (h *harness) states() []string {
	var out []string
	for _, e := range h.named("state") {
		s := fmt.Sprint(e["state"])
		if k, ok := e["kind"]; ok {
			s += "/" + fmt.Sprint(k)
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

// login signs in with session and waits for the run to settle: the socket
// connected, or the sign-in failed.
func (h *harness) login(session network.Session) {
	h.t.Helper()
	before := h.m.count("Connect")
	if err := h.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: session}); err != nil {
		h.t.Fatalf("Login: %v", err)
	}
	eventually(h.t, "the sign-in to settle", func() bool {
		return h.m.count("Connect") > before || h.g.Status().State != network.Connecting
	})
	h.settle()
}

// settle waits for the network's background work (the backfill) to end.
func (h *harness) settle() {
	h.t.Helper()
	eventually(h.t, "the background work to end", func() bool { return h.g.busy.Load() == 0 })
}

func (h *harness) chat(id string) store.Chat {
	h.t.Helper()
	c, ok, err := h.st.Chat(Name, id)
	if err != nil || !ok {
		h.t.Fatalf("chat %s: ok %v, err %v", id, ok, err)
	}
	return c
}

func (h *harness) lines(chat string) map[string]store.Message {
	h.t.Helper()
	page, err := h.st.Messages(Name, chat, "", 200)
	if err != nil {
		h.t.Fatal(err)
	}
	out := map[string]store.Message{}
	for _, m := range page.Items {
		out[m.ID] = m
	}
	return out
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Builders of Meta's shapes, with invented values.

func ms(t time.Time) jsontime.UnixMilliString { return jsontime.UnixMilliString{Time: t} }

func alice() *slidetypes.User {
	return &slidetypes.User{InteropMessagingUserFBID: aliceFBI, ID: aliceIG, FullName: "Alice Example", Username: "alice.example"}
}

func bob() *slidetypes.User {
	return &slidetypes.User{InteropMessagingUserFBID: bobFBID, ID: bobIG, Username: "bob.example"}
}

func viewerUser() *slidetypes.User {
	return &slidetypes.User{InteropMessagingUserFBID: selfFBID, ID: selfIG, FullName: "Matt Invented"}
}

// threadWith is a one-to-one thread with u.
func threadWith(key int64, igChatID string, u *slidetypes.User, activity time.Time) *slidetypes.ThreadInfo {
	return &slidetypes.ThreadInfo{
		ID: igChatID, ThreadFBID: igChatID, ThreadKey: key,
		Users: []*slidetypes.User{u}, Viewer: viewerUser(), ViewerID: selfIG,
		LastActivityTimestampMS: ms(activity),
	}
}

func group(activity time.Time) *slidetypes.ThreadInfo {
	return &slidetypes.ThreadInfo{
		ID: groupChat, ThreadFBID: groupChat, ThreadKey: groupKey, IsGroup: true, ThreadTitle: "Invented Group",
		Users: []*slidetypes.User{alice(), bob()}, Viewer: viewerUser(), ViewerID: selfIG,
		LastActivityTimestampMS: ms(activity),
	}
}

func mailboxOf(more bool, threads ...*slidetypes.ThreadInfo) *slidetypes.Mailbox {
	mb := &slidetypes.Mailbox{UQSeqID: 100}
	for _, t := range threads {
		mb.ThreadsByFolder.Edges = append(mb.ThreadsByFolder.Edges, slidetypes.Node[slidetypes.WrappedThreadInfo]{Node: slidetypes.WrappedThreadInfo{AsIGDirectThread: t, ID: t.ID}})
	}
	if more {
		mb.ThreadsByFolder.PageInfo = slidetypes.PageInfo{HasNextPage: true, EndCursor: "inbox-cursor-1"}
	}
	return mb
}

func textMsg(id string, sender int64, at time.Time, text string) *slidetypes.Message {
	return &slidetypes.Message{
		ID: id, MessageID: id, SenderFBID: sender, TimestampMS: ms(at), TextBody: text,
		Content: slidetypes.MessageContentWrapper{TypeName: "SlideMessageText", Content: &slidetypes.MessageContentText{TextBody: text}},
	}
}

func messagesPage(more bool, cursor string, msgs ...*slidetypes.Message) *slidetypes.PaginateMessagesResponse {
	resp := &slidetypes.PaginateMessagesResponse{}
	page := &slidetypes.SlideMessages{PageInfo: slidetypes.PageInfo{HasNextPage: more, EndCursor: cursor}}
	for _, m := range msgs {
		page.Edges = append(page.Edges, slidetypes.Node[*slidetypes.Message]{Node: m})
	}
	resp.ThreadInfo.AsIGDirectThread = &slidetypes.MessagesOnlyThread{Messages: page}
	return resp
}

func delta(threadIGID string, data slidetypes.DeltaEvent) *slidetypes.Delta {
	return &slidetypes.Delta{ThreadIGID: threadIGID, Data: data}
}

// gaps checks no two requests went closer than 2 s.
func gaps(t *testing.T, calls []call) {
	t.Helper()
	sorted := append([]call{}, calls...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].at.Before(sorted[j].at) })
	for i := 1; i < len(sorted); i++ {
		if d := sorted[i].at.Sub(sorted[i-1].at); d < 2*time.Second {
			t.Fatalf("%s went %v after %s", sorted[i].name, d, sorted[i-1].name)
		}
	}
}
