package instagram

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// The canaries are invented text and an invented cookie that must never
// reach standard error, and the cookie nowhere but a session event.
const (
	canary       = "canary-ig-4b9d-invented"
	canaryCookie = "cookie-canary-ig-8c2f"
)

func TestNothingReachesStderr(t *testing.T) {
	quiet.Init()

	// Everything written to file descriptor 2, by Go or by anything else in
	// the process, goes into the pipe while the session runs.
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
	m := newFakeMeta(clock)
	th := threadWith(aliceFBI, aliceThread, &slidetypes.User{InteropMessagingUserFBID: aliceFBI, ID: aliceIG, FullName: canary + " name"}, start.Add(-time.Hour))
	m.mailbox = mailboxOf(false, th, group(start.Add(-2*time.Hour)))
	m.viewer = &types.PolarisViewer{ID: selfIG}
	m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		if cursor == "" {
			return messagesPage(true, "older-"+canary, textMsg("mid.$invented-s1", aliceFBI, start.Add(-time.Hour), canary+" history")), nil
		}
		return messagesPage(false, "", textMsg("mid.$invented-s0", selfFBID, start.Add(-30*time.Hour), canary+" oldest")), nil
	}
	m.threadInfo = func(igChatID string) (*slidetypes.ThreadInfoResponse, error) {
		t := threadWith(bobFBID, igChatID, &slidetypes.User{InteropMessagingUserFBID: bobFBID, ID: bobIG, Username: canary + ".user"}, start)
		t.SlideMessages = &slidetypes.SlideMessages{Edges: []slidetypes.Node[*slidetypes.Message]{{Node: textMsg("mid.$invented-s4", bobFBID, start, canary+" fetched")}}}
		return &slidetypes.ThreadInfoResponse{ThreadInfo: slidetypes.WrappedThreadInfo{AsIGDirectThread: t}}, nil
	}

	var gmu sync.Mutex
	var g *Instagram
	dir := filepath.Join(t.TempDir(), "connect")
	deps := server.Deps{
		Dir:  dir,
		Open: func(key [32]byte) (*store.Store, error) { return store.Open(dir, key) },
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			gmu.Lock()
			defer gmu.Unlock()
			g = New(st, dir, key, emit.For(Name))
			g.clock = clock
			g.newClient = m.newClient
			return []network.Network{g}
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
	instagram := func(params map[string]any) map[string]any {
		params["network"] = Name
		return params
	}

	request("hello", map[string]any{"protocol": 1, "storeKey": hex.EncodeToString(testKey[:]), "app": "0.0.1-test"})
	session := map[string]any{"sessionid": canaryCookie, "csrftoken": "csrf-invented-1", "ds_user_id": "4000000099", "rur": "rur-invented-1"}
	request("login", instagram(map[string]any{"method": "session", "session": session}))
	eventually(t, "the socket to connect", func() bool { return m.count("Connect") > 0 })
	gmu.Lock()
	eventually(t, "the backfill", func() bool { return g.busy.Load() == 0 })
	gmu.Unlock()

	ck := m.last().params.Cookies
	ck.Set("rur", canaryCookie+"-rotated")
	clock.Advance(2 * time.Minute)
	for _, evt := range []slidetypes.ClientEvent{
		&slidetypes.Connected{},
		delta(aliceThread, &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-s2", aliceFBI, start, canary+" live")}),
		delta(aliceThread, &slidetypes.CreateReactionEvent{MessageID: "mid.$invented-s2", Reaction: slidetypes.Reaction{Reaction: "👍", SenderFBID: selfFBID}}),
		delta(aliceThread, &slidetypes.EditMessageEvent{MessageID: "mid.$invented-s2", TextBody: canary + " edited"}),
		delta(aliceThread, &slidetypes.ReadReceiptEvent{ReadReceipt: slidetypes.ReadReceipt{ParticipantFBID: aliceFBI, WatermarkTimestampMS: ms(start)}}),
		delta("340282366841710300000000000000077", &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-s3", bobFBID, start, canary+" unknown thread")}),
		&slidetypes.SeqIDUpdate{SeqID: 4343, Timestamp: start},
		&slidetypes.ReconnectionStateUpdate{State: json.RawMessage(`{"Configs":{"token":"` + canary + `"}}`)},
		&slidetypes.Disconnected{Error: io.ErrUnexpectedEOF},
	} {
		_ = m.dispatch(evt)
	}
	for _, r := range []struct {
		method string
		params map[string]any
	}{
		{"status", map[string]any{}},
		{"signIn", map[string]any{}},
		{"chats", map[string]any{"sinceMs": 0, "limit": 50}},
		{"chat", map[string]any{"chatID": aliceChat}},
		{"messages", map[string]any{"chatID": aliceChat, "limit": 50}}, // reaches the oldest line: FetchOlder
		{"search", map[string]any{"text": canary, "chatIDs": []string{}, "limit": 50}},
		{"findChats", map[string]any{"query": canary, "limit": 20}},
		{"send", map[string]any{"chatID": aliceChat, "text": canary}},
		{"markRead", map[string]any{"chatID": aliceChat}},
		{"nudge", map[string]any{}},
		{"messages", map[string]any{"chatID": "unknown-" + canary, "limit": 5}},
		{"login", map[string]any{"method": "session", "session": map[string]any{"sessionid": canaryCookie}}},
		{"login", map[string]any{"method": "qr"}},
	} {
		a := request(r.method, instagram(r.params))
		if e, ok := a["error"].(map[string]any); ok {
			if s, _ := json.Marshal(e); strings.Contains(string(s), canary) || strings.Contains(string(s), canaryCookie) {
				t.Errorf("%s's error carries a canary: %s", r.method, s)
			}
		}
	}
	// A checkpoint, then a sign-out.
	_ = m.dispatch(&slidetypes.AuthError{Error: httpclient.ErrCheckpointRequired})
	request("logout", instagram(map[string]any{}))

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

	// The cookie went out in session events and nowhere else.
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
		t.Errorf("the rotated cookie never went out in a session event")
	}
}
