package linkedin

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// The canaries are an invented text, URN and cookie that must never reach
// standard error, and the cookie nowhere but a session event.
const (
	canary       = "canary-li-7e3a-invented"
	canaryCookie = "cookie-canary-li-5d1b"
)

func TestNothingReachesStderr(t *testing.T) {
	quiet.Init()

	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved, err := syscall.Dup(2)
	if err != nil {
		t.Fatal(err)
	}
	savedFile := os.Stderr
	if err := syscall.Dup2(int(pw.Fd()), 2); err != nil {
		t.Fatal(err)
	}
	os.Stderr = pw
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		os.Stderr = savedFile
		_ = syscall.Dup2(saved, 2)
		syscall.Close(saved)
	}
	defer restore()
	captured := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(pr)
		captured <- b
	}()

	clock := newFakeClock()
	f := newFakeLinkedIn(clock)
	canaryConv := "urn:li:msg_conversation:(urn:li:fsd_profile:" + selfID + ",2-" + canary + ")"
	canaryMsg := "urn:li:msg_message:(urn:li:fsd_profile:" + selfID + ",2-" + canary + ")"
	named := person(aliceID, canary, "Name")
	cv := linkedingo.Conversation{
		EntityURN:                linkedingo.NewURN(canaryConv),
		LastActivityAt:           ms(start),
		ConversationParticipants: []linkedingo.MessagingParticipant{me(), named},
	}
	msg := linkedingo.Message{
		EntityURN:               linkedingo.NewURN(canaryMsg),
		Body:                    linkedingo.AttributedText{Text: canary + " text"},
		DeliveredAt:             ms(start),
		Sender:                  named,
		MessageBodyRenderFormat: linkedingo.MessageBodyRenderFormatDefault,
		Conversation:            linkedingo.Conversation{EntityURN: linkedingo.NewURN(canaryConv)},
	}
	f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		if cursor != "" {
			return cursorPageOf("")
		}
		return cursorPageOf("next-"+canary, cv)
	}
	f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{cv})
	}
	f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("prev-"+canary, msg)
	}
	f.prev = func(_, _ string, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("")
	}
	f.newToken = "token-" + canary
	f.rotate = func(jar *linkedingo.StringCookieJar, n int) {
		removeCookie(jar, "lidc")
		jar.SetCookies(linkedingo.CookieBaseURL, nil)
	}

	var lmu sync.Mutex
	var l *LinkedIn
	dir := filepath.Join(t.TempDir(), "connect")
	deps := server.Deps{
		Dir:  dir,
		Open: func(key [32]byte) (*store.Store, error) { return store.Open(dir, key) },
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			lmu.Lock()
			defer lmu.Unlock()
			l = New(st, dir, key, emit.For(Name), Options{NewClient: f.newClient, Clock: clock})
			return []network.Network{l}
		},
		Now: clock.Now,
	}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	served := make(chan error, 1)
	go func() { served <- server.Serve(context.Background(), inR, outW, deps); outW.Close() }()
	var omu sync.Mutex
	var output []string
	lines := make(chan map[string]any, 512)
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			omu.Lock()
			output = append(output, sc.Text())
			omu.Unlock()
			var v map[string]any
			if json.Unmarshal(sc.Bytes(), &v) == nil {
				lines <- v
			}
		}
		close(lines)
	}()
	answers := map[string]map[string]any{}
	n := 0
	request := func(method string, params map[string]any) map[string]any {
		t.Helper()
		n++
		id := "r" + string(rune('a'+n%26)) + strings.Repeat("x", n/26)
		b, _ := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
		if _, err := inW.Write(append(b, '\n')); err != nil {
			t.Fatalf("writing %s: %v", method, err)
		}
		deadline := time.After(10 * time.Second)
		for {
			if a, ok := answers[id]; ok {
				return a
			}
			select {
			case v, ok := <-lines:
				if !ok {
					t.Fatalf("the output ended before %s was answered", method)
				}
				if rid, _ := v["id"].(string); rid != "" {
					answers[rid] = v
				}
			case <-deadline:
				t.Fatalf("no answer to %s", method)
			}
		}
	}
	li := func(params map[string]any) map[string]any {
		params["network"] = Name
		return params
	}

	request("hello", map[string]any{"protocol": 1, "storeKey": hex.EncodeToString(testKey[:]), "app": "0.0.1-test"})
	session := map[string]any{"li_at": canaryCookie, "JSESSIONID": `"ajax:` + canaryCookie + `"`, "lidc": "lidc-" + canaryCookie}
	request("login", li(map[string]any{"method": "session", "session": session}))
	eventually(t, "realtime", func() bool { return f.count("RealtimeConnect") > 0 })
	lmu.Lock()
	ln := l
	lmu.Unlock()
	eventually(t, "the history", func() bool { return ln.idleNow() })

	c := f.last()
	clock.Advance(2 * time.Minute)
	reply := msg
	reply.EntityURN = linkedingo.NewURN(canaryMsg + "-2")
	reply.Body.Text = canary + " live"
	reply.RenderContent = []linkedingo.RenderContent{{RepliedMessageContent: &linkedingo.RepliedMessage{OriginalMessage: msg}}}
	for _, evt := range []any{
		&linkedingo.ClientConnection{},
		heartbeat{},
		messageEvent(reply),
		&linkedingo.DecoratedEvent{Topic: topic(linkedingo.RealtimeEventTopicMessageReactionSummaries), Payload: linkedingo.DecoratedEventPayload{Data: linkedingo.DecoratedEventData{
			DecoratedReactionSummary: &linkedingo.DecoratedReactionSummary{Result: linkedingo.RealtimeReactionSummary{
				ReactionAdded: true, Message: reply, ReactionSummary: linkedingo.ReactionSummary{Emoji: "👍", Count: 1, ViewerReacted: true},
			}},
		}}},
		convEvent(cv),
		&linkedingo.DecoratedEvent{Topic: topic(linkedingo.RealtimeEventTopicTypingIndicators)},
		transient{err: errors.New(canary + " stream")},
		&linkedingo.ClientConnection{SessID: [16]byte{9}},
	} {
		c.dispatch(evt)
	}
	eventually(t, "the history", func() bool { return ln.idleNow() })
	for _, r := range []struct {
		method string
		params map[string]any
	}{
		{"status", map[string]any{}},
		{"signIn", map[string]any{}},
		{"chats", map[string]any{"sinceMs": 0, "limit": 50}},
		{"chat", map[string]any{"chatID": canaryConv}},
		{"messages", map[string]any{"chatID": canaryConv, "limit": 50}},
		{"search", map[string]any{"text": canary, "chatIDs": []string{}, "limit": 50}},
		{"findChats", map[string]any{"query": canary, "limit": 20}},
		{"send", map[string]any{"chatID": canaryConv, "text": canary}},
		{"nudge", map[string]any{}},
		{"messages", map[string]any{"chatID": "unknown-" + canary, "limit": 5}},
		{"login", map[string]any{"method": "session", "session": map[string]any{"li_at": canaryCookie}}},
		{"login", map[string]any{"method": "qr"}},
	} {
		a := request(r.method, li(r.params))
		if e, ok := a["error"].(map[string]any); ok {
			if s, _ := json.Marshal(e); strings.Contains(string(s), canary) || strings.Contains(string(s), canaryCookie) {
				t.Errorf("%s's error carries a canary: %s", r.method, s)
			}
		}
	}
	// LinkedIn pushes back, then the session ends, then a sign-out.
	f.setErr("GetConversationsBySyncToken", statusErr(429))
	c.dispatch(convDeleteless())
	c.dispatch(badCredentials{err: errors.New(canary + " rejected")})
	request("logout", li(map[string]any{}))

	inW.Close()
	select {
	case <-served:
	case <-time.After(6 * time.Second):
		t.Fatalf("Serve did not return at end of input")
	}
	for range lines {
	}
	restore()
	pw.Close()
	got := <-captured
	if len(got) != 0 {
		t.Fatalf("%d bytes reached standard error (canary in them: %v)", len(got),
			strings.Contains(string(got), canary) || strings.Contains(string(got), canaryCookie))
	}

	omu.Lock()
	defer omu.Unlock()
	sessions := 0
	for _, line := range output {
		var v map[string]any
		_ = json.Unmarshal([]byte(line), &v)
		if v["event"] == "session" {
			sessions++
			continue
		}
		if strings.Contains(line, canaryCookie) {
			t.Errorf("the cookie went out in a %v %v line", v["event"], v["id"])
		}
	}
	if sessions == 0 {
		t.Errorf("the changed cookies never went out in a session event")
	}
}
