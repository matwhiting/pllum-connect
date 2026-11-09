package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// An invented store key: 32 bytes counting up from zero.
const testKeyHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

// fakeNetwork is a network.Network whose behaviour each test sets.
type fakeNetwork struct {
	mu      sync.Mutex
	name    string
	emit    network.Emit // the emit stamped with its name, as main gives it
	rawEmit network.Emit // the server's own emit, unstamped
	onMade  func(emit network.Emit)
	started bool
	stopped bool
	sent    network.Outgoing
	login   network.LoginParams
	reacted []string
	marked  []string
	muted   *bool
	onStart func(emit network.Emit)
	onReact func(ctx context.Context)
	onNudge func()
	sendErr error
}

func (f *fakeNetwork) Name() string { return f.name }

func (f *fakeNetwork) Start(ctx context.Context) error {
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	if f.onStart != nil {
		f.onStart(f.emit)
	}
	return nil
}

func (f *fakeNetwork) Status() network.Status {
	return network.Status{State: network.Live, SelfChatID: "self-chat"}
}

func (f *fakeNetwork) Login(ctx context.Context, p network.LoginParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.login = p
	return nil
}

func (f *fakeNetwork) CancelLogin() {}

func (f *fakeNetwork) Logout(ctx context.Context) error { return nil }

func (f *fakeNetwork) Nudge() {
	if f.onNudge != nil {
		f.onNudge()
	}
}

func (f *fakeNetwork) Send(ctx context.Context, m network.Outgoing) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return "", f.sendErr
	}
	f.sent = m
	return "made-id-1", nil
}

func (f *fakeNetwork) SendState(messageID string) network.SendState {
	return network.SendState{State: network.SendFailed, Kind: wire.Refused}
}

func (f *fakeNetwork) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	if f.onReact != nil {
		f.onReact(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reacted = []string{chatID, messageID, emoji, transactionID}
	return nil
}

func (f *fakeNetwork) MarkRead(ctx context.Context, chatID, through string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = []string{chatID, through}
	return nil
}

func (f *fakeNetwork) MarkUnread(ctx context.Context, chatID string) error {
	return &wire.Error{Kind: wire.NotConnected}
}

func (f *fakeNetwork) SetMuted(ctx context.Context, chatID string, muted bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.muted = &muted
	return nil
}

func (f *fakeNetwork) Media(ctx context.Context, chatID, messageID string) (string, error) {
	return "/invented/media/file", nil
}

func (f *fakeNetwork) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = true
}

// harness runs Serve over pipes and reads its output line by line.
type harness struct {
	t      *testing.T
	in     *io.PipeWriter
	lines  chan map[string]any
	done   chan struct{} // closed when Serve returns
	err    error         // Serve's result, set before done closes
	opened chan [32]byte
}

func start(t *testing.T, fake *fakeNetwork) *harness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	h := &harness{
		t:      t,
		in:     inW,
		lines:  make(chan map[string]any, 64),
		done:   make(chan struct{}),
		opened: make(chan [32]byte, 4),
	}
	deps := Deps{
		Dir: t.TempDir(),
		Open: func(key [32]byte) (*store.Store, error) {
			h.opened <- key
			return &store.Store{}, nil
		},
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			if fake == nil {
				return nil
			}
			fake.rawEmit = emit
			fake.emit = emit.For(fake.name)
			if fake.onMade != nil {
				fake.onMade(fake.emit)
			}
			return []network.Network{fake}
		},
	}
	go func() {
		h.err = Serve(context.Background(), inR, outW, deps)
		close(h.done)
		outW.Close()
	}()
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 1<<20), wire.MaxLine+1)
		for sc.Scan() {
			var m map[string]any
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				m = map[string]any{"unparsable": sc.Text()}
			}
			h.lines <- m
		}
		close(h.lines)
	}()
	t.Cleanup(func() {
		inW.Close()
		select {
		case <-h.done:
		case <-time.After(6 * time.Second):
		}
	})
	return h
}

func (h *harness) send(line string) {
	h.t.Helper()
	if _, err := io.WriteString(h.in, line+"\n"); err != nil {
		h.t.Fatalf("writing a request: %v", err)
	}
}

func (h *harness) next() map[string]any {
	h.t.Helper()
	select {
	case m, ok := <-h.lines:
		if !ok {
			h.t.Fatalf("the output ended")
		}
		return m
	case <-time.After(3 * time.Second):
		h.t.Fatalf("no output within 3 s")
	}
	return nil
}

func (h *harness) hello() map[string]any {
	h.t.Helper()
	h.send(`{"id":"h","method":"hello","params":{"protocol":1,"storeKey":"` + testKeyHex + `","app":"0.0.1-test"}}`)
	return h.next()
}

func kindOf(t *testing.T, m map[string]any) string {
	t.Helper()
	e, ok := m["error"].(map[string]any)
	if !ok {
		t.Fatalf("not an error response: %v", m)
	}
	k, _ := e["kind"].(string)
	return k
}

func result(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	r, ok := m["result"].(map[string]any)
	if !ok {
		t.Fatalf("not a result response: %v", m)
	}
	return r
}

func TestHelloAgreesOnProtocolOne(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp"}
	h := start(t, fake)
	m := h.hello()
	if m["id"] != "h" {
		t.Fatalf("id = %v, want h", m["id"])
	}
	r := result(t, m)
	if r["protocol"] != float64(1) {
		t.Errorf("protocol = %v, want 1", r["protocol"])
	}
	if v, _ := r["version"].(string); v == "" {
		t.Errorf("version is empty")
	}
	if c, _ := r["commit"].(string); c == "" {
		t.Errorf("commit is empty")
	}
	nets, _ := r["networks"].([]any)
	if len(nets) != 1 || nets[0] != "whatsapp" {
		t.Errorf("networks = %v, want [whatsapp]", r["networks"])
	}
	key := <-h.opened
	for i := range key {
		if key[i] != byte(i) {
			t.Fatalf("the store was opened with the wrong key bytes")
		}
	}
	// Start runs on the read loop after hello's answer is written, so an
	// answer to a later request proves it has run.
	h.send(`{"id":"after","method":"status","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "after" {
		t.Fatalf("got %v, want the status answer", m)
	}
	fake.mu.Lock()
	started := fake.started
	fake.mu.Unlock()
	if !started {
		t.Errorf("the network was not started after hello")
	}
}

func TestAnotherProtocolIsOutdated(t *testing.T) {
	h := start(t, &fakeNetwork{name: "whatsapp"})
	h.send(`{"id":"h","method":"hello","params":{"protocol":2,"storeKey":"` + testKeyHex + `","app":"0.0.1-test"}}`)
	m := h.next()
	if m["id"] != "h" || kindOf(t, m) != string(wire.Outdated) {
		t.Fatalf("got %v, want outdated for h", m)
	}
	select {
	case <-h.opened:
		t.Fatalf("the store was opened for an outdated hello")
	default:
	}
	h.send(`{"id":"s","method":"status","params":{"network":"whatsapp"}}`)
	if k := kindOf(t, h.next()); k != string(wire.BadRequest) {
		t.Fatalf("after an outdated hello, status = %v, want badRequest", k)
	}
}

func TestEverythingBeforeHelloIsBadRequest(t *testing.T) {
	h := start(t, &fakeNetwork{name: "whatsapp"})
	for i, line := range []string{
		`{"id":"a","method":"status","params":{"network":"whatsapp"}}`,
		`{"id":"b","method":"send","params":{"network":"whatsapp","chatID":"c","text":"t"}}`,
		`{"id":"c","method":"noSuchMethod","params":{"network":"whatsapp"}}`,
		`{"id":"d","method":"shutdown","params":{}}`,
	} {
		h.send(line)
		m := h.next()
		want := string(rune('a' + i))
		if m["id"] != want || kindOf(t, m) != string(wire.BadRequest) {
			t.Fatalf("got %v, want badRequest for %s", m, want)
		}
	}
	if r := result(t, h.hello()); r["protocol"] != float64(1) {
		t.Fatalf("hello after refused requests failed: %v", r)
	}
}

func TestABadHelloKeyIsStoreKey(t *testing.T) {
	h := start(t, &fakeNetwork{name: "whatsapp"})
	for _, key := range []string{"", "abc", strings.Repeat("zz", 32), testKeyHex + "00"} {
		h.send(`{"id":"h","method":"hello","params":{"protocol":1,"storeKey":"` + key + `","app":"0.0.1-test"}}`)
		if k := kindOf(t, h.next()); k != string(wire.StoreKey) {
			t.Fatalf("a malformed key gave %v, want storeKey", k)
		}
	}
}

func TestAStoreThatWillNotOpenIsItsKind(t *testing.T) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	deps := Deps{
		Dir:  t.TempDir(),
		Open: func(key [32]byte) (*store.Store, error) { return nil, &wire.Error{Kind: wire.StoreKey} },
		Networks: func(*store.Store, string, [32]byte, network.Emit) []network.Network {
			t.Errorf("networks were made without a store")
			return nil
		},
	}
	go Serve(context.Background(), inR, outW, deps)
	defer inW.Close()
	go io.WriteString(inW, `{"id":"h","method":"hello","params":{"protocol":1,"storeKey":"`+testKeyHex+`","app":"x"}}`+"\n")
	line, err := bufio.NewReader(outR).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, `"kind":"storeKey"`) {
		t.Fatalf("got %s, want storeKey", line)
	}
}

func TestUnknownMethodAndUnknownNetworkAreKinds(t *testing.T) {
	h := start(t, &fakeNetwork{name: "whatsapp"})
	h.hello()

	h.send(`{"id":"1","method":"noSuchMethod","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "1" || kindOf(t, m) != string(wire.UnknownMethod) {
		t.Fatalf("got %v, want unknownMethod", m)
	}
	h.send(`{"id":"2","method":"status","params":{"network":"carrierpigeon"}}`)
	if m := h.next(); m["id"] != "2" || kindOf(t, m) != string(wire.UnknownNetwork) {
		t.Fatalf("got %v, want unknownNetwork", m)
	}
	h.send(`{"id":"3","method":"status","params":{}}`)
	if m := h.next(); m["id"] != "3" || kindOf(t, m) != string(wire.BadRequest) {
		t.Fatalf("got %v, want badRequest for a missing network", m)
	}
	h.send(`{"id":"4","method":"hello","params":{"protocol":1,"storeKey":"` + testKeyHex + `","app":"x"}}`)
	if m := h.next(); m["id"] != "4" || kindOf(t, m) != string(wire.BadRequest) {
		t.Fatalf("got %v, want badRequest for a second hello", m)
	}
}

func TestABadLineIsBadRequestAndTheNextLineIsServed(t *testing.T) {
	h := start(t, &fakeNetwork{name: "whatsapp"})
	h.hello()

	h.send(`this is not json`)
	if m := h.next(); m["id"] != "" || kindOf(t, m) != string(wire.BadRequest) {
		t.Fatalf("got %v, want badRequest with an empty id", m)
	}
	h.send(`{"id":"7","method":"setMuted","params":{"network":"whatsapp","chatID":"c","muted":"yes"}}`)
	if m := h.next(); m["id"] != "7" || kindOf(t, m) != string(wire.BadRequest) {
		t.Fatalf("got %v, want badRequest keeping id 7", m)
	}
	h.send(`{"id":"8","method":"send","params":{"network":"whatsapp","text":"no chat"}}`)
	if m := h.next(); m["id"] != "8" || kindOf(t, m) != string(wire.BadRequest) {
		t.Fatalf("got %v, want badRequest for a send with no chatID", m)
	}
	h.send(`{"id":"9","method":"status","params":{"network":"whatsapp"}}`)
	m := h.next()
	if m["id"] != "9" {
		t.Fatalf("got %v, want the status answer", m)
	}
	if r := result(t, m); r["state"] != "live" || r["selfChatID"] != "self-chat" {
		t.Fatalf("status = %v", r)
	}
}

func TestALineOverTheLimitIsBadRequest(t *testing.T) {
	h := start(t, &fakeNetwork{name: "whatsapp"})
	h.hello()

	// A request padded to exactly MaxLine bytes is served.
	head := `{"id":"fits","method":"status","params":{"network":"whatsapp","pad":"`
	tail := `"}}`
	h.send(head + strings.Repeat("x", wire.MaxLine-len(head)-len(tail)) + tail)
	if m := h.next(); m["id"] != "fits" || m["result"] == nil {
		t.Fatalf("a line of exactly MaxLine bytes was not served: %v", m["error"])
	}

	// One byte more is a bad request, and the line after it is served.
	head = `{"id":"over","method":"status","params":{"network":"whatsapp","pad":"`
	h.send(head + strings.Repeat("x", wire.MaxLine+1-len(head)-len(tail)) + tail)
	if m := h.next(); m["id"] != "" || kindOf(t, m) != string(wire.BadRequest) {
		t.Fatalf("got %v, want badRequest", m["error"])
	}
	h.send(`{"id":"after","method":"status","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "after" || m["result"] == nil {
		t.Fatalf("the line after an over-long one was not served: %v", m)
	}
}

func TestResponsesMayComeOutOfOrder(t *testing.T) {
	release := make(chan struct{})
	fake := &fakeNetwork{name: "whatsapp", onReact: func(ctx context.Context) { <-release }}
	h := start(t, fake)
	h.hello()

	h.send(`{"id":"slow","method":"react","params":{"network":"whatsapp","chatID":"c","messageID":"m","emoji":"x","transactionID":"t"}}`)
	h.send(`{"id":"fast","method":"status","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "fast" {
		t.Fatalf("first answer = %v, want fast", m)
	}
	close(release)
	if m := h.next(); m["id"] != "slow" || m["result"] == nil {
		t.Fatalf("second answer = %v, want slow's result", m)
	}
}

func TestAPanicInAHandlerIsInternal(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp", onNudge: func() { panic("an invented failure") }}
	h := start(t, fake)
	h.hello()

	h.send(`{"id":"p","method":"nudge","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "p" || kindOf(t, m) != string(wire.Internal) {
		t.Fatalf("got %v, want internal", m)
	}
	h.send(`{"id":"q","method":"status","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "q" || m["result"] == nil {
		t.Fatalf("the helper did not carry on after a panic: %v", m)
	}
}

func TestEndOfInputStopsWithinFiveSeconds(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)
	// A handler that ignores its context and never returns on its own.
	fake := &fakeNetwork{name: "whatsapp", onReact: func(ctx context.Context) { <-stuck }}
	h := start(t, fake)
	h.hello()
	h.send(`{"id":"stuck","method":"react","params":{"network":"whatsapp","chatID":"c","messageID":"m","emoji":"x","transactionID":"t"}}`)
	time.Sleep(50 * time.Millisecond)

	began := time.Now()
	h.in.Close()
	select {
	case <-h.done:
		if err := h.err; err != nil {
			t.Fatalf("Serve returned %v at end of input, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Serve still running 5 s after end of input")
	}
	if d := time.Since(began); d >= 5*time.Second {
		t.Fatalf("stopping took %v", d)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.stopped {
		t.Fatalf("the network was not stopped")
	}
}

func TestShutdownAnswersThenStops(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp"}
	h := start(t, fake)
	h.hello()
	h.send(`{"id":"bye","method":"shutdown","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "bye" || m["result"] == nil {
		t.Fatalf("got %v, want an empty result", m)
	}
	select {
	case <-h.done:
		if err := h.err; err != nil {
			t.Fatalf("Serve returned %v after shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Serve still running 5 s after shutdown")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.stopped {
		t.Fatalf("the network was not stopped")
	}
}

func TestEventsGoOutAsLinesWithNoID(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp", onStart: func(emit network.Emit) {
		emit(map[string]any{"event": "state", "network": "whatsapp", "state": "connecting"})
	}}
	h := start(t, fake)
	if m := h.hello(); m["id"] != "h" {
		t.Fatalf("hello's answer must come before the network's first event, got %v", m)
	}
	m := h.next()
	if _, has := m["id"]; has {
		t.Fatalf("an event carried an id: %v", m)
	}
	if m["event"] != "state" || m["network"] != "whatsapp" || m["state"] != "connecting" {
		t.Fatalf("event = %v", m)
	}
}

func TestNetworkMethodsReachTheNetwork(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp"}
	h := start(t, fake)
	h.hello()

	call := func(id, method, params string) map[string]any {
		t.Helper()
		h.send(fmt.Sprintf(`{"id":%q,"method":%q,"params":{"network":"whatsapp"%s}}`, id, method, params))
		m := h.next()
		if m["id"] != id {
			t.Fatalf("answer id = %v, want %s", m["id"], id)
		}
		return m
	}

	r := result(t, call("1", "send", `,"chatID":"chat-a","text":"invented words","replyTo":"line-b","file":"/invented/file","fileName":"f.txt"`))
	if r["messageID"] != "made-id-1" {
		t.Errorf("send result = %v", r)
	}
	if want := (network.Outgoing{ChatID: "chat-a", Text: "invented words", ReplyTo: "line-b", File: "/invented/file", FileName: "f.txt"}); fake.sent != want {
		t.Errorf("send passed %+v", fake.sent)
	}

	r = result(t, call("2", "sendStatus", `,"messageID":"made-id-1"`))
	if r["state"] != "failed" || r["kind"] != "refused" {
		t.Errorf("sendStatus = %v", r)
	}

	result(t, call("3", "react", `,"chatID":"chat-a","messageID":"line-b","emoji":"","transactionID":"txn-1"`))
	if got := strings.Join(fake.reacted, "|"); got != "chat-a|line-b||txn-1" {
		t.Errorf("react passed %s", got)
	}

	result(t, call("4", "markRead", `,"chatID":"chat-a","through":null`))
	if got := strings.Join(fake.marked, "|"); got != "chat-a|" {
		t.Errorf("markRead passed %s", got)
	}

	if k := kindOf(t, call("5", "markUnread", `,"chatID":"chat-a"`)); k != string(wire.NotConnected) {
		t.Errorf("markUnread kind = %s, want the network's notConnected", k)
	}

	result(t, call("6", "setMuted", `,"chatID":"chat-a","muted":true`))
	if fake.muted == nil || !*fake.muted {
		t.Errorf("setMuted did not reach the network")
	}

	if r := result(t, call("7", "media", `,"chatID":"chat-a","messageID":"line-b"`)); r["path"] != "/invented/media/file" {
		t.Errorf("media = %v", r)
	}

	result(t, call("8", "login", `,"method":"qr"`))
	if fake.login.Method != "qr" {
		t.Errorf("login passed %+v", fake.login)
	}
	if k := kindOf(t, call("9", "login", `,"method":"carrierpigeon"`)); k != string(wire.BadRequest) {
		t.Errorf("an unknown login method gave %s, want badRequest", k)
	}

	for i, m := range []string{"cancelLogin", "logout", "nudge"} {
		result(t, call(fmt.Sprintf("1%d", i), m, ""))
	}

	fake.mu.Lock()
	fake.sendErr = errors.New("an untyped failure")
	fake.mu.Unlock()
	if k := kindOf(t, call("20", "send", `,"chatID":"chat-a","text":"x"`)); k != string(wire.Internal) {
		t.Errorf("an untyped error gave %s, want internal", k)
	}
}

func TestAnEventCarriesItsNetworksName(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp", onStart: func(emit network.Emit) {
		emit(map[string]any{"event": "changed"})
		emit(map[string]any{"event": "changed", "network": "carrierpigeon"})
	}}
	h := start(t, fake)
	h.hello()
	for i := 0; i < 2; i++ {
		if m := h.next(); m["event"] != "changed" || m["network"] != "whatsapp" {
			t.Fatalf("event %d = %v, want changed for whatsapp", i, m)
		}
	}
}

func TestAnEventNamingNoKnownNetworkIsDropped(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp"}
	h := start(t, fake)
	h.hello()
	fake.rawEmit(map[string]any{"event": "changed"})
	fake.rawEmit(map[string]any{"event": "changed", "network": "carrierpigeon"})
	h.send(`{"id":"s","method":"status","params":{"network":"whatsapp"}}`)
	if m := h.next(); m["id"] != "s" {
		t.Fatalf("got %v, want the status answer with no event before it", m)
	}
}

func TestNoEventComesBeforeHellosAnswer(t *testing.T) {
	fake := &fakeNetwork{name: "whatsapp", onMade: func(emit network.Emit) {
		emit(map[string]any{"event": "state", "state": "connecting"})
	}}
	h := start(t, fake)
	if m := h.hello(); m["id"] != "h" {
		t.Fatalf("first line = %v, want hello's answer", m)
	}
	if m := h.next(); m["event"] != "state" || m["network"] != "whatsapp" || m["state"] != "connecting" {
		t.Fatalf("the event held back during hello = %v", m)
	}
}

func TestAHandlerWithNoResultAnswersEmpty(t *testing.T) {
	methods["invented"] = func(context.Context, network.Network, json.RawMessage) (any, error) { return nil, nil }
	t.Cleanup(func() { delete(methods, "invented") })
	h := start(t, &fakeNetwork{name: "whatsapp"})
	h.hello()
	h.send(`{"id":"n","method":"invented","params":{"network":"whatsapp"}}`)
	m := h.next()
	if m["id"] != "n" {
		t.Fatalf("got %v", m)
	}
	if r, ok := m["result"].(map[string]any); !ok || len(r) != 0 {
		t.Fatalf("got %v, want an empty result", m)
	}
}
