package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every name, ID, PIN and text in these tests is invented.

// metaNetwork is a fake network that has every optional interface.
type metaNetwork struct {
	*fakeNetwork
	page  meta.SignInPage
	state network.SendState

	omu        sync.Mutex
	older      []string      // chat IDs FetchOlder was called with
	olderCh    chan string   // each call, as it comes
	olderBlock chan struct{} // when non-nil, FetchOlder waits for it to close

	histState  string
	restored   []string // the PINs RestoreHistory got
	restoreErr error
}

func (m *metaNetwork) SignIn() meta.SignInPage { return m.page }

func (m *metaNetwork) SendState(string) network.SendState { return m.state }

// SendFor knows the keys "client-N" of the sends "made-N".
func (m *metaNetwork) SendFor(clientID string) (string, error) {
	if id, ok := strings.CutPrefix(clientID, "client-"); ok {
		return "made-" + id, nil
	}
	return "", nil
}

func (m *metaNetwork) FetchOlder(ctx context.Context, chatID string) (int, error) {
	m.omu.Lock()
	m.older = append(m.older, chatID)
	block := m.olderBlock
	m.omu.Unlock()
	if m.olderCh != nil {
		m.olderCh <- chatID
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
		}
	}
	return 0, nil
}

func (m *metaNetwork) olderCalls() []string {
	m.omu.Lock()
	defer m.omu.Unlock()
	return append([]string(nil), m.older...)
}

func (m *metaNetwork) HistoryState() string { return m.histState }

func (m *metaNetwork) RestoreHistory(ctx context.Context, pin []byte) error {
	m.omu.Lock()
	defer m.omu.Unlock()
	m.restored = append(m.restored, string(pin))
	return m.restoreErr
}

// testClock is a server clock that only moves when the test moves it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// startMeta starts the helper over an instagram network with every optional
// interface and a plain whatsapp network with none, and a real store.
func startMeta(t *testing.T, ig *metaNetwork, clock *testClock) (*harness, *store.Store) {
	t.Helper()
	plain := &fakeNetwork{name: "whatsapp"}
	stores := make(chan *store.Store, 1)
	var now func() time.Time
	if clock != nil {
		now = clock.Now
	}
	h := startNets(t, func(emit network.Emit) []network.Network {
		plain.rawEmit, plain.emit = emit, emit.For("whatsapp")
		ig.rawEmit, ig.emit = emit, emit.For("instagram")
		return []network.Network{plain, ig}
	}, func(dir string, key [32]byte) (*store.Store, error) {
		st, err := store.Open(dir, key)
		if err == nil {
			stores <- st
		}
		return st, err
	}, now)
	result(t, h.hello())
	return h, <-stores
}

func newMetaNetwork() *metaNetwork {
	return &metaNetwork{fakeNetwork: &fakeNetwork{name: "instagram"}, page: meta.InstagramPage}
}

func igCall(h *harness, id, method string, params map[string]any) map[string]any {
	p := map[string]any{"network": "instagram"}
	for k, v := range params {
		p[k] = v
	}
	return h.call(id, method, p)
}

func TestSignInAndChatByParticipantOverTheWire(t *testing.T) {
	ig := newMetaNetwork()
	h, st := startMeta(t, ig, nil)

	r := result(t, igCall(h, "s1", "signIn", nil))
	want := map[string]any{
		"url":          "https://www.instagram.com/accounts/login/",
		"donePattern":  meta.InstagramPage.DonePattern,
		"cookieDomain": "instagram.com",
		"userAgent":    meta.InstagramPage.UserAgent,
	}
	for k, v := range want {
		if r[k] != v {
			t.Errorf("signIn %s = %v, want %v", k, r[k], v)
		}
	}
	if len(r) != 6 {
		t.Errorf("signIn has keys %v, want url, donePattern, cookieDomain, required, optional, userAgent", r)
	}
	req, _ := r["required"].([]any)
	opt, _ := r["optional"].([]any)
	if len(req) != 3 || req[0] != "sessionid" || len(opt) != 5 {
		t.Errorf("signIn cookie lists = %v / %v", r["required"], r["optional"])
	}

	// A page with no optional cookies sends an empty list, never null.
	ig.page = meta.SignInPage{URL: "https://example.invalid/", Required: []string{"a"}}
	r = result(t, igCall(h, "s2", "signIn", nil))
	if o, ok := r["optional"].([]any); !ok || len(o) != 0 {
		t.Errorf("optional = %#v, want []", r["optional"])
	}

	// A network with no sign-in page.
	if k := kindOf(t, h.call("s3", "signIn", nil)); k != string(wire.Unsupported) {
		t.Errorf("signIn on a network with no page gave %s, want unsupported", k)
	}

	// chatByParticipant: the one-to-one, null for the rest.
	const chat, group = "ig-chat-1", "ig-chat-2"
	for id, kind := range map[string]string{chat: "single", group: "group"} {
		if _, err := st.PutMessage("instagram", store.Message{ID: "m-" + id, ChatID: id, SenderID: "ig-1001", TimeMs: 1767225600000,
			Text: "hello", Kind: "text", Source: store.SourceLive}); err != nil {
			t.Fatal(err)
		}
		parts := []store.Participant{{ID: "ig-self", IsSelf: true}, {ID: "ig-1001", Name: "Alex Example"}}
		if err := st.PutChat("instagram", store.ChatInfo{ID: id, Kind: kind, Participants: parts}); err != nil {
			t.Fatal(err)
		}
	}
	r = result(t, igCall(h, "p1", "chatByParticipant", map[string]any{"participantID": "ig-1001"}))
	c, _ := r["chat"].(map[string]any)
	if c["id"] != chat || c["kind"] != "single" {
		t.Errorf("chatByParticipant = %v, want the one-to-one", r)
	}
	ps, _ := c["participants"].([]any)
	if len(ps) != 2 {
		t.Errorf("the chat has participants %v", c["participants"])
	}
	for _, id := range []string{"ig-nobody", "ig-self"} {
		r = result(t, igCall(h, "p2", "chatByParticipant", map[string]any{"participantID": id}))
		if v, present := r["chat"]; !present || v != nil {
			t.Errorf("chatByParticipant(%s) = %v, want chat: null", id, r)
		}
	}
	if k := kindOf(t, igCall(h, "p3", "chatByParticipant", nil)); k != string(wire.BadRequest) {
		t.Errorf("chatByParticipant with no participant gave %s, want badRequest", k)
	}
	if k := kindOf(t, igCall(h, "p4", "chatByParticipant", map[string]any{"participantID": 7})); k != string(wire.BadRequest) {
		t.Errorf("chatByParticipant with a number gave %s, want badRequest", k)
	}
	// Another network's chats are not reached.
	r = result(t, h.call("p5", "chatByParticipant", map[string]any{"participantID": "ig-1001"}))
	if r["chat"] != nil {
		t.Errorf("whatsapp answered with instagram's chat: %v", r)
	}
}

func TestLoginBySessionTakesStringsAndNothingElse(t *testing.T) {
	ig := newMetaNetwork()
	h, _ := startMeta(t, ig, nil)
	result(t, igCall(h, "l1", "login", map[string]any{"method": "session", "session": map[string]any{"sessionid": "sess-a", "csrftoken": "csrf-a"}}))
	ig.fakeNetwork.mu.Lock()
	got := ig.login
	ig.fakeNetwork.mu.Unlock()
	if got.Method != "session" || len(got.Session) != 2 || got.Session["sessionid"] != "sess-a" || got.Session["csrftoken"] != "csrf-a" {
		t.Errorf("login passed %+v", got)
	}
	for i, sess := range []any{nil, map[string]any{}, "sessionid=x", map[string]any{"sessionid": 5}, []any{"a"}} {
		params := map[string]any{"method": "session"}
		if sess != nil {
			params["session"] = sess
		}
		if k := kindOf(t, igCall(h, "l2"+string(rune('a'+i)), "login", params)); k != string(wire.BadRequest) {
			t.Errorf("login with session %#v gave %s, want badRequest", sess, k)
		}
	}
}

func TestEncryptedHistoryIsUnsupportedWhereANetworkHasNone(t *testing.T) {
	ig := newMetaNetwork()
	ig.histState = "needsPIN"
	h, _ := startMeta(t, ig, nil)

	for i, call := range []struct {
		method string
		params map[string]any
	}{{"encryptedHistory", nil}, {"restoreHistory", map[string]any{"pin": ""}}} {
		id := string(rune('a' + i))
		if k := kindOf(t, h.call(id, call.method, call.params)); k != string(wire.Unsupported) {
			t.Errorf("%s on a network with none gave %s, want unsupported", call.method, k)
		}
	}

	if r := result(t, igCall(h, "e1", "encryptedHistory", nil)); r["state"] != "needsPIN" {
		t.Errorf("encryptedHistory = %v", r)
	}

	// "" skips, and reaches the network as an empty PIN.
	result(t, igCall(h, "r1", "restoreHistory", map[string]any{"pin": ""}))
	result(t, igCall(h, "r2", "restoreHistory", map[string]any{"pin": "123456"}))
	if got := ig.restored; len(got) != 2 || got[0] != "" || got[1] != "123456" {
		t.Errorf("the network got PINs %q", got)
	}
	for _, params := range []map[string]any{nil, {"pin": nil}, {"pin": 123456}} {
		if k := kindOf(t, igCall(h, "r3", "restoreHistory", params)); k != string(wire.BadRequest) {
			t.Errorf("restoreHistory with %v gave %s, want badRequest", params, k)
		}
	}
}

func TestAttemptsRemainingTravelsOnPinWrong(t *testing.T) {
	ig := newMetaNetwork()
	h, _ := startMeta(t, ig, nil)
	three := 3

	ig.restoreErr = &wire.Error{Kind: wire.PinWrong, AttemptsRemaining: &three}
	m := igCall(h, "w1", "restoreHistory", map[string]any{"pin": "000000"})
	e, _ := m["error"].(map[string]any)
	if e["kind"] != "pinWrong" || e["attemptsRemaining"] != float64(3) {
		t.Errorf("a wrong PIN answered %v, want pinWrong with attemptsRemaining 3", m)
	}

	zero := 0
	ig.restoreErr = &wire.Error{Kind: wire.PinWrong, AttemptsRemaining: &zero}
	e, _ = igCall(h, "w2", "restoreHistory", map[string]any{"pin": "000000"})["error"].(map[string]any)
	if v, ok := e["attemptsRemaining"]; !ok || v != float64(0) {
		t.Errorf("zero attempts left answered %v, want attemptsRemaining 0 present", e)
	}

	// Unknown to the network: the field is left out, not null.
	ig.restoreErr = &wire.Error{Kind: wire.PinWrong}
	e, _ = igCall(h, "w3", "restoreHistory", map[string]any{"pin": "000000"})["error"].(map[string]any)
	if _, ok := e["attemptsRemaining"]; ok || e["kind"] != "pinWrong" {
		t.Errorf("an unknown count answered %v, want pinWrong alone", e)
	}

	// Only a wrong PIN carries it; pinLimit and every other kind do not.
	for _, kind := range []wire.Kind{wire.PinLimit, wire.Checkpoint, wire.Offline} {
		ig.restoreErr = &wire.Error{Kind: kind, AttemptsRemaining: &three}
		e, _ = igCall(h, "w4", "restoreHistory", map[string]any{"pin": "000000"})["error"].(map[string]any)
		if _, ok := e["attemptsRemaining"]; ok || e["kind"] != string(kind) {
			t.Errorf("%s answered %v, want its kind alone", kind, e)
		}
	}
}

func TestSendStatusCarriesTheLineID(t *testing.T) {
	ig := newMetaNetwork()
	h, st := startMeta(t, ig, nil)
	if _, err := st.PutMessage("instagram", store.Message{ID: "line-seed", ChatID: "ig-chat-1", SenderID: "ig-1001",
		TimeMs: 1767225600000, Text: "hi", Kind: "text", Source: store.SourceLive}); err != nil {
		t.Fatal(err)
	}
	for _, o := range []store.Outgoing{
		{ID: "made-1", ChatID: "ig-chat-1", Kind: "message", Client: "client-1", MadeMs: 1},
		{ID: "made-2", ChatID: "ig-chat-1", Kind: "message", Client: "client-2", MadeMs: 2},
		{ID: "made-3", ChatID: "ig-chat-1", Kind: "message", Client: "client-3", MadeMs: 3},
	} {
		if err := st.PutOutgoing("instagram", o); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SetOutgoingLine("instagram", "made-1", "line-9"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetOutgoingLine("instagram", "made-3", "made-3"); err != nil {
		t.Fatal(err)
	}

	ig.state = network.SendState{State: network.SendSent}
	r := result(t, igCall(h, "a", "sendStatus", map[string]any{"messageID": "made-1"}))
	if r["state"] != "sent" || r["lineID"] != "line-9" {
		t.Errorf("sendStatus = %v, want sent with lineID line-9", r)
	}
	if _, ok := result(t, igCall(h, "b", "sendStatus", map[string]any{"messageID": "made-2"}))["lineID"]; ok {
		t.Errorf("a send with no line ID carried one")
	}
	if _, ok := result(t, igCall(h, "c", "sendStatus", map[string]any{"messageID": "made-3"}))["lineID"]; ok {
		t.Errorf("a line ID equal to the sent ID was carried: the field is only for when it differs")
	}
	if _, ok := result(t, igCall(h, "d", "sendStatus", map[string]any{"messageID": "unkept"}))["lineID"]; ok {
		t.Errorf("a send the store does not hold carried a line ID")
	}

	// Asked by the caller's key: lineID comes beside messageID.
	r = result(t, igCall(h, "f", "sendStatus", map[string]any{"clientID": "client-1"}))
	if r["messageID"] != "made-1" || r["lineID"] != "line-9" {
		t.Errorf("sendStatus by key = %v, want made-1 with line-9", r)
	}

	// A send still sending has no line yet, whatever the store says.
	ig.state = network.SendState{State: network.SendSending}
	if _, ok := result(t, igCall(h, "e", "sendStatus", map[string]any{"messageID": "made-1"}))["lineID"]; ok {
		t.Errorf("a send still sending carried a line ID")
	}
}

func TestOlderPagesIsAskedOnceAtTheOldestLine(t *testing.T) {
	ig := newMetaNetwork()
	ig.olderCh = make(chan string, 8)
	clock := &testClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	h, st := startMeta(t, ig, clock)

	const open, done = "ig-chat-open", "ig-chat-done"
	complete := true
	for _, id := range []string{open, done} {
		for i, at := range []int64{1767225600000, 1767225660000, 1767225720000} {
			if _, err := st.PutMessage("instagram", store.Message{ID: id + string(rune('a'+i)), ChatID: id, SenderID: "ig-1001",
				TimeMs: at, Text: "hello", Kind: "text", Source: store.SourceLive}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.PutChat("instagram", store.ChatInfo{ID: done, HistoryComplete: &complete}); err != nil {
		t.Fatal(err)
	}
	messages := func(id, chat string, limit int) map[string]any {
		return result(t, igCall(h, id, "messages", map[string]any{"chatID": chat, "limit": limit}))
	}
	expectCall := func(want string) {
		t.Helper()
		select {
		case got := <-ig.olderCh:
			if got != want {
				t.Fatalf("FetchOlder was called for %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("FetchOlder was not called")
		}
	}
	expectNone := func() {
		t.Helper()
		select {
		case got := <-ig.olderCh:
			t.Fatalf("FetchOlder was called again for %q", got)
		case <-time.After(150 * time.Millisecond):
		}
	}

	// A page that stops short of the oldest line asks for nothing.
	if r := messages("m1", open, 2); r["hasMore"] != true {
		t.Fatalf("hasMore = %v, want true", r["hasMore"])
	}
	expectNone()

	// A page that holds the oldest line of an incomplete chat asks once, and
	// the answer does not wait for it.
	ig.olderBlock = make(chan struct{})
	if r := messages("m2", open, 50); r["hasMore"] != false {
		t.Fatalf("hasMore = %v, want false", r["hasMore"])
	}
	expectCall(open)
	// While it runs, and after, within 10 s: no more.
	messages("m3", open, 50)
	expectNone()
	close(ig.olderBlock)
	time.Sleep(50 * time.Millisecond)
	clock.advance(9 * time.Second)
	messages("m4", open, 50)
	expectNone()

	// After 10 s, again.
	clock.advance(2 * time.Second)
	ig.olderBlock = nil
	deadline := time.After(3 * time.Second)
	for got := false; !got; {
		messages("m5", open, 50) // may race the first call's cleanup; ask until it takes
		select {
		case id := <-ig.olderCh:
			if id != open {
				t.Fatalf("FetchOlder for %q", id)
			}
			got = true
		case <-deadline:
			t.Fatalf("FetchOlder was not asked again 11 s later")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// A complete chat never asks.
	clock.advance(time.Minute)
	messages("m6", done, 50)
	expectNone()
	if calls := ig.olderCalls(); len(calls) != 2 {
		t.Errorf("FetchOlder calls = %v, want 2 in all, none for the complete chat", calls)
	}

	// A network that does not fetch older pages just answers.
	if _, err := st.PutMessage("whatsapp", store.Message{ID: "w1", ChatID: "wa-chat", SenderID: "wa-1", TimeMs: 1767225600000,
		Text: "hi", Kind: "text", Source: store.SourceLive}); err != nil {
		t.Fatal(err)
	}
	if r := result(t, h.call("m7", "messages", map[string]any{"chatID": "wa-chat", "limit": 50})); r["hasMore"] != false {
		t.Errorf("whatsapp messages = %v", r)
	}
}
