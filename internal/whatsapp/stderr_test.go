package whatsapp

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// canary is invented text that must never reach standard error.
const canary = "canary-7f3e-invented"

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

	dir := filepath.Join(t.TempDir(), "connect")
	pairDevice(t, dir)
	made := make(chan *fakeClient, 4)
	deps := server.Deps{
		Dir:  dir,
		Open: func(key [32]byte) (*store.Store, error) { return store.Open(dir, key) },
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			w := New(st, dir, key, emit.For("whatsapp"))
			w.newClient = func(dev *wastore.Device) waClient {
				fc := newFakeClient(dev)
				fc.history["/v/canary"] = &waHistorySync.HistorySync{
					SyncType: waHistorySync.HistorySync_FULL.Enum(),
					Progress: proto.Uint32(100),
					Pushnames: []*waHistorySync.Pushname{{
						ID: proto.String(aliceJID.String()), Pushname: proto.String(canary + " name"),
					}},
					Conversations: []*waHistorySync.Conversation{{
						ID: proto.String(aliceJID.String()), Name: proto.String(canary + " title"),
						Messages: []*waHistorySync.HistorySyncMsg{
							webMsg(aliceJID, aliceJID, "3EB0C1000000000000001", base+1000, false,
								&waE2E.Message{Conversation: proto.String(canary + " history")}),
						},
					}},
				}
				made <- fc
				return fc
			}
			return []network.Network{w}
		},
	}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	served := make(chan error, 1)
	go func() { served <- server.Serve(context.Background(), inR, outW, deps); outW.Close() }()
	lines := make(chan map[string]any, 256)
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var m map[string]any
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				lines <- m
			}
		}
		close(lines)
	}()
	answers := map[string]map[string]any{}
	request := func(id, method string, params map[string]any) map[string]any {
		t.Helper()
		b, _ := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
		if _, err := inW.Write(append(b, '\n')); err != nil {
			t.Fatalf("writing %s: %v", method, err)
		}
		deadline := time.After(5 * time.Second)
		for {
			if a, ok := answers[id]; ok {
				return a
			}
			select {
			case m, ok := <-lines:
				if !ok {
					t.Fatalf("the output ended before %s was answered", method)
				}
				if rid, _ := m["id"].(string); rid != "" {
					answers[rid] = m
				}
			case <-deadline:
				t.Fatalf("no answer to %s", method)
			}
		}
	}

	request("h", "hello", map[string]any{"protocol": 1, "storeKey": hex.EncodeToString(testKey[:]), "app": "0.0.1-test"})
	var fc *fakeClient
	select {
	case fc = <-made:
	case <-time.After(5 * time.Second):
		t.Fatalf("the adapter made no client")
	}
	ctx := context.Background()
	_, _, _ = fc.dev.Contacts.PutPushName(ctx, aliceJID, canary+" push")
	session := []any{
		&events.Connected{},
		textFrom(aliceJID, aliceJID, "3EB0C1000000000000002", base+2000, canary+" live"),
		msg(aliceJID, aliceJID, "3EB0C1000000000000003", base+3000, false, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			Caption: proto.String(canary + " caption"), DirectPath: proto.String("/v/" + canary)}}),
		editOf(aliceJID, aliceJID, "3EB0C1000000000000004", "3EB0C1000000000000002", base+4000, false, canary+" edited"),
		reactionTo(aliceJID, selfJID, "3EB0C1000000000000005", "3EB0C1000000000000002", base+5000, true, "👍"),
		&events.Receipt{MessageSource: types.MessageSource{Chat: aliceJID, Sender: aliceJID},
			MessageIDs: []string{"3EB0C1000000000000002"}, Timestamp: time.UnixMilli(base + 6000), Type: types.ReceiptTypeRead},
		&events.PushName{JID: aliceJID, NewPushName: canary + " push"},
		textFrom(statusJID, aliceJID, "3EB0C1000000000000006", base+6000, canary+" status"),
		historyNotif("3EB0C1000000000000007", "/v/canary", base+7000),
		historyNotif("3EB0C1000000000000008", "/v/missing-"+canary, base+8000),
		&events.OfflineSyncCompleted{},
		&events.Disconnected{},
		&events.Connected{},
	}
	for _, evt := range session {
		fc.dispatch(evt)
	}
	eventually(t, "the history to be processed", func() bool {
		a := request("c"+time.Now().Format("150405.000000000"), "messages", map[string]any{"network": "whatsapp", "chatID": aliceJID.String(), "limit": 50})
		items, _ := a["result"].(map[string]any)["items"].([]any)
		return len(items) == 3
	})
	for i, r := range []struct {
		method string
		params map[string]any
	}{
		{"status", map[string]any{}},
		{"chats", map[string]any{"sinceMs": 0, "limit": 50}},
		{"chat", map[string]any{"chatID": aliceJID.String()}},
		{"search", map[string]any{"text": canary, "chatIDs": []string{}, "limit": 50}},
		{"findChats", map[string]any{"query": canary, "limit": 20}},
		{"login", map[string]any{"method": "qr"}},
		{"nudge", map[string]any{}},
		{"send", map[string]any{"chatID": aliceJID.String(), "text": canary}},
		{"markRead", map[string]any{"chatID": aliceJID.String()}},
		{"media", map[string]any{"chatID": aliceJID.String(), "messageID": "3EB0C1000000000000003"}},
		{"messages", map[string]any{"chatID": "unknown-" + canary, "limit": 5}},
	} {
		r.params["network"] = "whatsapp"
		a := request("r"+string(rune('a'+i)), r.method, r.params)
		if e, ok := a["error"].(map[string]any); ok {
			if s, _ := json.Marshal(e); strings.Contains(string(s), canary) {
				t.Errorf("%s's error carries the canary: %s", r.method, s)
			}
		}
	}
	fc.dispatch(&events.LoggedOut{Reason: events.ConnectFailureLoggedOut})

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
		t.Fatalf("%d bytes reached standard error (canary in them: %v)", len(got), strings.Contains(string(got), canary))
	}
}
