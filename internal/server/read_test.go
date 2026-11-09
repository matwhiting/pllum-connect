package server

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// call sends one request for the whatsapp network and waits for its answer,
// skipping events.
func (h *harness) call(id, method string, params map[string]any) map[string]any {
	h.t.Helper()
	p := map[string]any{"network": "whatsapp"}
	for k, v := range params {
		p[k] = v
	}
	b, err := json.Marshal(map[string]any{"id": id, "method": method, "params": p})
	if err != nil {
		h.t.Fatal(err)
	}
	h.send(string(b))
	for {
		m := h.next()
		if m["id"] == id {
			return m
		}
	}
}

func TestTheReadMethodsAnswerOverTheWire(t *testing.T) {
	// Every name, number, ID and text here is invented.
	const chat = "10000000001@s.whatsapp.net"
	const base = 1767225600000
	stores := make(chan *store.Store, 1)
	h := startWith(t, &fakeNetwork{name: "whatsapp"}, func(dir string, key [32]byte) (*store.Store, error) {
		st, err := store.Open(dir, key)
		if err == nil {
			stores <- st
		}
		return st, err
	})
	result(t, h.hello())
	st := <-stores
	if _, err := st.PutMessage("whatsapp", store.Message{ID: "3EB0X0001", ChatID: chat, SenderID: chat,
		TimeMs: base, Text: "Lunch on Friday?", Kind: "text", Source: store.SourceLive}); err != nil {
		t.Fatalf("PutMessage: %v", err)
	}

	r := result(t, h.call("i1", "import", map[string]any{
		"beeperChatID": "bc-1",
		"chatID":       chat,
		"chat":         map[string]any{"kind": "single", "title": "Alex Example", "phone": "+10000000001"},
		"messages": []map[string]any{
			{"beeperID": "bm-1", "networkID": nil, "fromMe": false, "senderID": "bs-1", "senderName": "Alex Example",
				"timeMs": base + 900, "text": "Lunch on Friday?", "kind": "text", "hidden": false, "deleted": false,
				"replyToBeeperID": nil, "media": []any{}, "reactions": []any{}},
			{"beeperID": "bm-2", "networkID": nil, "fromMe": true, "senderID": nil, "senderName": nil,
				"timeMs": base + 60_000, "text": "Yes, noon", "kind": "text", "hidden": false, "deleted": false,
				"replyToBeeperID": "bm-1", "media": []any{}, "reactions": []map[string]any{{"emoji": "👍", "senderID": "bs-1", "fromMe": false}}},
		},
	}))
	if r["added"] != float64(1) || r["matched"] != float64(1) || r["skipped"] != float64(0) {
		t.Fatalf("import = %v", r)
	}
	r = result(t, h.call("i2", "import", map[string]any{
		"beeperChatID": "bc-2", "chatID": nil, "chat": map[string]any{"kind": "group"},
		"messages": []map[string]any{{"beeperID": "bm-3", "fromMe": false, "timeMs": base - 1000, "text": "Old news", "kind": "text", "media": []any{}, "reactions": []any{}}},
	}))
	if r["added"] != float64(1) {
		t.Fatalf("unplaced import = %v", r)
	}

	r = result(t, h.call("c1", "chats", map[string]any{"sinceMs": 0, "limit": 50}))
	items, _ := r["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("chats = %v", r)
	}
	first, _ := items[0].(map[string]any)
	wantKeys(t, "a chat", first, "id", "kind", "title", "phone", "lastActivityMs", "lastMessageID", "lastFromMe",
		"unread", "muted", "readOnly", "participants", "historyFromMs", "historyComplete")
	if first["id"] != chat || first["lastMessageID"] != "beeper:bm-2" || first["lastFromMe"] != true || first["title"] != "Alex Example" {
		t.Fatalf("the newest chat is %v", first)
	}
	if _, ok := r["cursor"]; ok && r["cursor"] != nil {
		t.Errorf("one page has a cursor: %v", r["cursor"])
	}

	r = result(t, h.call("c2", "chat", map[string]any{"chatID": "unplaced:bc-2"}))
	if c, _ := r["chat"].(map[string]any); c["readOnly"] != true || c["title"] != nil {
		t.Fatalf("the unplaced chat = %v", r)
	}
	r = result(t, h.call("c3", "chat", map[string]any{"chatID": "10000000099@s.whatsapp.net"}))
	if v, ok := r["chat"]; !ok || v != nil {
		t.Fatalf("an unknown chat = %v, want chat: null", r)
	}

	r = result(t, h.call("m1", "messages", map[string]any{"chatID": chat, "limit": 50}))
	msgs, _ := r["items"].([]any)
	if len(msgs) != 2 || r["hasMore"] != false {
		t.Fatalf("messages = %v", r)
	}
	m0, _ := msgs[0].(map[string]any)
	m1, _ := msgs[1].(map[string]any)
	wantKeys(t, "a message", m0, "id", "chatID", "senderID", "senderName", "fromMe", "timeMs", "text", "kind",
		"hidden", "deleted", "edited", "replyTo", "media", "reactions", "seenMs")
	if m0["id"] != "3EB0X0001" || m1["id"] != "beeper:bm-2" || m1["replyTo"] != "3EB0X0001" || m0["seenMs"] != nil {
		t.Fatalf("messages = %v", msgs)
	}
	if rs, _ := m1["reactions"].([]any); len(rs) != 1 {
		t.Fatalf("the reply's reactions = %v", m1["reactions"])
	}

	r = result(t, h.call("s1", "search", map[string]any{"text": "noon", "chatIDs": []string{}, "limit": 10}))
	if found, _ := r["items"].([]any); len(found) != 1 {
		t.Fatalf("search = %v", r)
	}
	r = result(t, h.call("s2", "search", map[string]any{"text": nil, "chatIDs": []string{chat}, "fromMe": true, "limit": 10}))
	if found, _ := r["items"].([]any); len(found) != 1 {
		t.Fatalf("his lines = %v", r)
	}

	r = result(t, h.call("f1", "findChats", map[string]any{"query": "alex", "limit": 5}))
	if found, _ := r["items"].([]any); len(found) != 1 {
		t.Fatalf("findChats = %v", r)
	}

	r = result(t, h.call("b1", "beeperChats", nil))
	if mp, _ := r["map"].(map[string]any); mp["bc-1"] != chat || mp["bc-2"] != "unplaced:bc-2" {
		t.Fatalf("beeperChats = %v", r)
	}
	r = result(t, h.call("l1", "lineIDs", map[string]any{"beeperIDs": []string{"bm-1", "bm-2", "bm-none"}}))
	if mp, _ := r["map"].(map[string]any); len(mp) != 2 || mp["bm-1"] != "3EB0X0001" || mp["bm-2"] != "beeper:bm-2" {
		t.Fatalf("lineIDs = %v", r)
	}

	r = result(t, h.call("n1", "counts", nil))
	wantKeys(t, "counts", r, "chats", "messages", "imported", "matched", "unplacedChats", "oldestMs")
	for k, v := range r {
		if _, ok := v.(float64); !ok {
			t.Errorf("counts.%s = %v, not a number", k, v)
		}
	}
	if r["chats"] != float64(2) || r["messages"] != float64(3) || r["imported"] != float64(3) ||
		r["matched"] != float64(1) || r["unplacedChats"] != float64(1) || r["oldestMs"] != float64(base-1000) {
		t.Fatalf("counts = %v", r)
	}

	// Malformed reads are badRequest.
	tooMany := make([]string, 1001)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("bm-%d", i)
	}
	for i, bad := range []struct {
		method string
		params map[string]any
	}{
		{"messages", map[string]any{"limit": 10}},
		{"messages", map[string]any{"chatID": chat, "limit": 0}},
		{"messages", map[string]any{"chatID": chat, "limit": 10, "before": "nonsense"}},
		{"chats", map[string]any{"limit": 10, "cursor": "nonsense"}},
		{"chat", map[string]any{}},
		{"findChats", map[string]any{"query": "", "limit": 5}},
		{"lineIDs", map[string]any{"beeperIDs": tooMany}},
		{"import", map[string]any{"chatID": chat, "messages": []any{}}},
		{"import", map[string]any{"beeperChatID": "bc-9", "chatID": "unplaced:bc-9", "messages": []any{}}},
	} {
		if k := kindOf(t, h.call(fmt.Sprintf("bad%d", i), bad.method, bad.params)); k != "badRequest" {
			t.Errorf("%s %v = %s, want badRequest", bad.method, bad.params, k)
		}
	}
	if k := kindOf(t, h.call("nf", "messages", map[string]any{"chatID": "10000000099@s.whatsapp.net", "limit": 10})); k != "notFound" {
		t.Errorf("messages of an unknown chat = %s, want notFound", k)
	}
}

func wantKeys(t *testing.T, what string, m map[string]any, keys ...string) {
	t.Helper()
	var got []string
	for k := range m {
		got = append(got, k)
	}
	sort.Strings(got)
	sort.Strings(keys)
	if strings.Join(got, ",") != strings.Join(keys, ",") {
		t.Errorf("%s has %v, want %v", what, got, keys)
	}
}
