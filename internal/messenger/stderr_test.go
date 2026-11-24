package messenger

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// canaryCookie is an invented cookie that must never reach standard error,
// and nowhere but a session event.
const canaryCookie = "cookie-canary-msgr-3d7a"

// A whole session over the wire, groups, older one-to-ones, the listing,
// the backfill, older pages, the writes, a checkpoint and a sign-out, writes nothing to
// standard error, and the cookie goes out only in session events.
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

	f := newFakeMeta()
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := inbox()
	old := history(groupOne, true, line("mid.$invented-s1", aliceFBID, at, canary+" history"))
	tbl.LSUpsertMessage, tbl.LSInsertNewMessageRange = old.LSUpsertMessage, old.LSInsertNewMessageRange
	tbl.LSDeleteThenInsertContact = []*table.LSDeleteThenInsertContact{{Id: aliceFBID, Name: canary + " Alice"}}
	f.table = tbl
	f.folders[inboxOne] = []*table.LSTable{threadsPage(inboxOne, false, thread(2000088, table.ONE_TO_ONE, at, "pending"))}
	f.writeErrs["MuteThreadTask"] = errors.New(canary + " invented failure")
	photo := writePhoto(t)
	// Every thread's history goes on and on, so groups are never complete.
	pages := 0
	f.olderFn = func(key int64) *table.LSTable {
		pages++
		return history(key, true, line(fmt.Sprintf("mid.$invented-h%d", pages), aliceFBID, at.Add(-time.Duration(pages)*time.Hour), canary+" older"))
	}

	var mmu sync.Mutex
	var m *Messenger
	dir := filepath.Join(t.TempDir(), "connect")
	deps := server.Deps{
		Dir:  dir,
		Open: func(key [32]byte) (*store.Store, error) { return store.Open(dir, key) },
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			mmu.Lock()
			defer mmu.Unlock()
			m = New(st, dir, key, emit.For(Name)).WithGate()
			m.newClient = f.newClient
			m.newWriter = func(msgrClient, e2eeClient) msgrWriter { return fakeWriter{f} }
			m.newDownloader = func(msgrClient) msgrDownloader { return fakeWriter{f} }
			m.clock = f.clock
			m.pageWait = time.Second
			return []network.Network{m}
		},
		Now:  f.clock.Now,
		Gate: true,
	}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	served := make(chan error, 1)
	go func() { served <- server.Serve(context.Background(), inR, outW, deps); outW.Close() }()
	var omu sync.Mutex
	var output []string
	lines := make(chan map[string]any, 1024)
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
		id := fmt.Sprintf("r%d", n)
		if method != "hello" {
			params["network"] = Name
		}
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

	request("hello", map[string]any{"protocol": 1, "storeKey": hex.EncodeToString(testKey[:]), "app": "0.0.1-test"})
	session := map[string]any{"xs": canaryCookie, "c_user": "2000001", "datr": "datr-invented-1"}
	request("login", map[string]any{"method": "session", "session": session})
	mmu.Lock()
	net := m
	mmu.Unlock()
	eventually(t, "the listing", func() bool { return net.GateCounts().(GateCounts).ListComplete })
	settleOn(net, f)

	// Meta rotates the cookie, and the socket brings news.
	cli := f.last()
	cli.ck.Set("xs", canaryCookie+"-rotated")
	f.clock.Advance(2 * time.Minute)
	for _, evt := range []any{
		&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: groupOne, MessageId: "mid.$invented-s2", SenderId: aliceFBID, TimestampMs: at.Add(time.Hour).UnixMilli(), Text: canary + " live"}}},
		&table.LSTable{LSUpsertReaction: []*table.LSUpsertReaction{{ThreadKey: groupOne, MessageId: "mid.$invented-s2", ActorId: selfFBID, Reaction: "👍"}}},
		&table.LSTable{LSEditMessage: []*table.LSEditMessage{{MessageID: "mid.$invented-s2", Text: canary + " edited"}}},
		&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: 9100000000000077, MessageId: "mid.$invented-s3", SenderId: bobFBID, TimestampMs: 1, Text: canary + " unknown thread"}}},
		&messagix.TransientDisconnectEvent{Err: io.ErrUnexpectedEOF},
		&messagix.ReconnectedEvent{},
	} {
		cli.dispatch(evt)
	}
	// A request after the rotation (the older page "messages" asks for)
	// carries the session event out.
	for _, r := range []struct {
		method string
		params map[string]any
	}{
		{"status", map[string]any{}},
		{"signIn", map[string]any{}},
		{"gateCounts", map[string]any{}},
		{"chats", map[string]any{"sinceMs": 0, "limit": 50}},
		{"chat", map[string]any{"chatID": fmt.Sprint(groupOne)}},
		{"messages", map[string]any{"chatID": fmt.Sprint(groupOne), "limit": 50}}, // the oldest line: FetchOlder
		{"messages", map[string]any{"chatID": fmt.Sprint(aliceFBID) + "@msgr", "limit": 50}},
		{"search", map[string]any{"text": canary, "chatIDs": []string{}, "limit": 50}},
		{"findChats", map[string]any{"query": canary, "limit": 20}},
		{"encryptedHistory", map[string]any{}},
		// The writes (Task B3): a group's send, an encrypted send, a
		// reaction, read receipts, mutes (one Meta fails with a canary in
		// its error), and media.
		{"send", map[string]any{"chatID": fmt.Sprint(groupOne), "text": canary + " sent", "clientID": "key-" + canary}},
		{"send", map[string]any{"chatID": fmt.Sprint(aliceFBID) + "@msgr", "text": canary + " sent encrypted", "clientID": "key-e-" + canary}},
		{"send", map[string]any{"chatID": fmt.Sprint(aliceFBID) + "@msgr", "text": canary + " caption", "file": photo, "clientID": "key-p-" + canary}},
		{"sendStatus", map[string]any{"clientID": "key-" + canary}},
		{"react", map[string]any{"chatID": fmt.Sprint(groupOne), "messageID": "mid.$invented-s2", "emoji": "👍", "transactionID": "txn-" + canary}},
		{"markRead", map[string]any{"chatID": fmt.Sprint(groupOne), "through": "mid.$invented-s2"}},
		{"markUnread", map[string]any{"chatID": fmt.Sprint(groupOne)}},
		{"setMuted", map[string]any{"chatID": fmt.Sprint(groupOne), "muted": true}},
		{"setMuted", map[string]any{"chatID": fmt.Sprint(aliceFBID) + "@msgr", "muted": true}},
		{"media", map[string]any{"chatID": fmt.Sprint(groupOne), "messageID": "mid.$invented-s2"}},
		{"media", map[string]any{"chatID": fmt.Sprint(groupOne), "messageID": "mid.$invented-" + canary}},
		{"messages", map[string]any{"chatID": "unknown-" + canary, "limit": 5}},
		{"login", map[string]any{"method": "session", "session": map[string]any{"xs": canaryCookie}}},
	} {
		a := request(r.method, r.params)
		if e, ok := a["error"].(map[string]any); ok {
			if s, _ := json.Marshal(e); strings.Contains(string(s), canary) || strings.Contains(string(s), canaryCookie) {
				t.Errorf("%s's error carries a canary: %s", r.method, s)
			}
		}
	}
	eventually(t, "the writes to reach Meta", func() bool {
		return f.count("SendMessageTask") == 1 && f.count("SendFBMessage") == 3 && f.count("UploadFB") == 1 && f.count("SendReactionTask") == 1 &&
			f.count("ThreadMarkReadTask") == 1 && f.count("MuteThreadTask") == 2
	})
	// A checkpoint on a read, then a sign-out.
	f.mu.Lock()
	f.taskErr = func(socket.Task) error { return httpclient.ErrCheckpointRequired }
	f.mu.Unlock()
	request("login", map[string]any{"method": "session", "session": map[string]any{"xs": canaryCookie + "-2", "c_user": "2000001", "datr": "datr-invented-1"}})
	settleOn(net, f) // the run the login starts meets the checkpoint
	request("logout", map[string]any{})

	inW.Close()
	select {
	case <-served:
	case <-time.After(8 * time.Second):
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
		t.Errorf("the rotated cookie never went out in a session event")
	}
}
