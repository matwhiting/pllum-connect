package store

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// plan is SQLite's query plan for q, one detail a line.
func plan(t *testing.T, s *Store, q string, args ...any) string {
	t.Helper()
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN `+q, args...)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, detail)
	}
	return strings.Join(out, "\n")
}

// ChatOfLine looks the line up by its ID through an index, not by walking
// every line of the network and sorting (review C11).
func TestChatOfLineIsAnIndexedLookup(t *testing.T) {
	s := openTest(t)
	p := plan(t, s, chatOfLineQuery, net, "3EB0L0001")
	if !strings.Contains(p, "messages_id") || strings.Contains(p, "TEMP B-TREE") {
		t.Fatalf("ChatOfLine plan:\n%s", p)
	}
}

// The first line by rid answers when two chats hold the same ID.
func TestChatOfLinePicksTheFirstRow(t *testing.T) {
	s := openTest(t)
	put(t, s, text(otherChat, "3EB0L0001", base, "first"))
	put(t, s, text(phoneChat, "3EB0L0001", base+1, "second"))
	if chat, ok := s.ChatOfLine(net, "3EB0L0001"); !ok || chat != otherChat {
		t.Fatalf("ChatOfLine = %q, %v; want %q", chat, ok, otherChat)
	}
}

// A store from before messages_id gains it on its next open, with its rows
// and schema version as they were.
func TestOldStoreGainsTheLineIDIndex(t *testing.T) {
	dir := t.TempDir() + "/connect"
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, s, text(phoneChat, "3EB0L0001", base, "kept"))
	if _, err := s.db.Exec(`DROP INDEX messages_id`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'messages_id'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("messages_id: %d, %v", n, err)
	}
	var v string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&v); err != nil || v != "10" {
		t.Fatalf("schema = %q, %v; want 10", v, err)
	}
	if chat, ok := s.ChatOfLine(net, "3EB0L0001"); !ok || chat != phoneChat {
		t.Fatalf("ChatOfLine = %q, %v", chat, ok)
	}
}

// The store keeps a small pool of connections (each a SQLite instance of its
// own) and reuses them: a burst of reads beside writes neither opens one per
// request nor waits for ever (review C10).
func TestPoolIsBoundedAndKeptWarm(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0P0001", base, "hello"))
	if got := s.db.Stats().MaxOpenConnections; got != maxConns {
		t.Fatalf("MaxOpenConnections = %d; want %d", got, maxConns)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := range 32 {
			wg.Go(func() {
				if _, ok, err := s.Chat(net, phoneChat); err != nil || !ok {
					t.Errorf("Chat: %v %v", ok, err)
				}
			})
			if i%8 == 0 {
				wg.Go(func() {
					// A write that also reads through the pool while it holds
					// its own connection.
					err := s.write(func(tx *sql.Tx) error {
						var n int
						return s.db.QueryRow(`SELECT count(*) FROM chats`).Scan(&n)
					})
					if err != nil {
						t.Errorf("write: %v", err)
					}
				})
			}
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a burst of reads beside writes never finished")
	}
	st := s.db.Stats()
	if st.OpenConnections > maxConns {
		t.Fatalf("OpenConnections = %d; want at most %d", st.OpenConnections, maxConns)
	}
	if st.MaxIdleClosed > 0 {
		t.Fatalf("closed %d idle connections; want them kept", st.MaxIdleClosed)
	}
}

// A page of chats carries each chat's own participants, in position order,
// at most maxParticipants, and an empty list for a chat with none, however
// they are read (review C2).
func TestChatsPageCarriesEachChatsParticipants(t *testing.T) {
	s := openTest(t)
	var big []Participant
	for i := range maxParticipants + 6 {
		big = append(big, Participant{ID: fmt.Sprintf("3000%08d@s.whatsapp.net", i), Name: fmt.Sprintf("Member %d", i)})
	}
	big[0], big[1] = big[1], big[0] // position, not ID, orders them
	if err := s.PutChat(net, ChatInfo{ID: groupChat, Kind: "group", Participants: big, LastActivityMs: base + 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, LastActivityMs: base + 2,
		Participants: []Participant{{ID: phoneChat, Name: "Ana", Phone: "+10000000001"}, {ID: "self", IsSelf: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(net, ChatInfo{ID: otherChat, LastActivityMs: base + 1}); err != nil {
		t.Fatal(err)
	}
	p, err := s.Chats(net, 0, "", 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 3 {
		t.Fatalf("Chats: %d items", len(p.Items))
	}
	g, one, none := p.Items[0], p.Items[1], p.Items[2]
	if g.ID != groupChat || len(g.Participants) != maxParticipants || g.Participants[0].ID != big[0].ID ||
		g.Participants[maxParticipants-1].ID != big[maxParticipants-1].ID {
		t.Fatalf("group participants: %d, first %+v", len(g.Participants), g.Participants[0])
	}
	if one.ID != phoneChat || len(one.Participants) != 2 || one.Participants[0].Name != "Ana" ||
		one.Participants[0].Phone != "+10000000001" || !one.Participants[1].IsSelf {
		t.Fatalf("single participants: %+v", one.Participants)
	}
	if none.ID != otherChat || none.Participants == nil || len(none.Participants) != 0 {
		t.Fatalf("no participants: %#v", none.Participants)
	}
	// One chat read alone reads the same.
	if c := chatOf(t, s, groupChat); len(c.Participants) != maxParticipants || c.Participants[0].ID != big[0].ID {
		t.Fatalf("Chat participants: %d", len(c.Participants))
	}
}

// A page of lines carries each line's own reactions, ordered by sender, and
// an empty list for a line with none; a search across chats keeps each
// chat's reactions to its own lines (review C2).
func TestPagesCarryEachLinesReactions(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0R0001", base+1, "one"))
	put(t, s, text(phoneChat, "3EB0R0002", base+2, "two"))
	put(t, s, text(otherChat, "3EB0R0001", base+3, "one elsewhere"))
	for _, r := range []struct {
		chat, id, sender, emoji string
	}{
		{phoneChat, "3EB0R0001", "b-sender", "👍"},
		{phoneChat, "3EB0R0001", "a-sender", "❤️"},
		{otherChat, "3EB0R0001", "c-sender", "😂"},
	} {
		if err := s.SetReaction(net, r.chat, r.id, Reaction{Emoji: r.emoji, SenderID: r.sender}); err != nil {
			t.Fatal(err)
		}
	}
	got := all(t, s, phoneChat)
	if len(got) != 2 || len(got[0].Reactions) != 2 || got[0].Reactions[0].SenderID != "a-sender" ||
		got[0].Reactions[1].Emoji != "👍" || got[1].Reactions == nil || len(got[1].Reactions) != 0 {
		t.Fatalf("Messages reactions: %+v", got)
	}
	page, err := s.Search(net, Query{Text: "one", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("Search: %d items", len(page.Items))
	}
	for _, m := range page.Items {
		want := map[string]string{phoneChat: "❤️", otherChat: "😂"}[m.ChatID]
		if len(m.Reactions) == 0 || m.Reactions[0].Emoji != want {
			t.Fatalf("Search reactions for %s: %+v", m.ChatID, m.Reactions)
		}
	}
}

// ChatIDs lists a network's chats newest activity first, page by page,
// narrowed by prefix, writability and open history, and NewestActivity is
// the newest activity among chats under a prefix.
func TestChatIDsAndNewestActivity(t *testing.T) {
	s := openTest(t)
	yes := true
	for i, c := range []ChatInfo{
		{ID: "conv:1", LastActivityMs: base + 5},
		{ID: "conv:2", LastActivityMs: base + 4, ReadOnly: &yes},
		{ID: "other:3", LastActivityMs: base + 9},
		{ID: "conv:4", LastActivityMs: base + 3, HistoryComplete: &yes},
		{ID: "conv:5", LastActivityMs: base + 3},
		{ID: "conv:6"},
	} {
		if err := s.PutChat(net, c); err != nil {
			t.Fatalf("PutChat %d: %v", i, err)
		}
	}
	walk := func(f ChatFilter, limit int) []string {
		var out []string
		cursor := ""
		for range 20 {
			got, next, err := s.ChatIDs(net, f, cursor, limit)
			if err != nil {
				t.Fatalf("ChatIDs: %v", err)
			}
			out = append(out, got...)
			if next == "" {
				return out
			}
			cursor = next
		}
		t.Fatal("ChatIDs never ended")
		return nil
	}
	same := func(got []string, want ...string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v; want %v", got, want)
		}
	}
	// Equal activity: the later chat first, as Chats orders them.
	same(walk(ChatFilter{}, 2), "other:3", "conv:1", "conv:2", "conv:5", "conv:4", "conv:6")
	same(walk(ChatFilter{Prefix: "conv:"}, 1), "conv:1", "conv:2", "conv:5", "conv:4", "conv:6")
	same(walk(ChatFilter{Prefix: "conv:", Writable: true}, 200), "conv:1", "conv:5", "conv:4", "conv:6")
	same(walk(ChatFilter{Prefix: "conv:", Writable: true, HistoryOpen: true}, 3), "conv:1", "conv:5", "conv:6")

	if ms, ok, err := s.NewestActivity(net, "conv:"); err != nil || !ok || ms != base+5 {
		t.Fatalf("NewestActivity = %d, %v, %v", ms, ok, err)
	}
	if _, ok, err := s.NewestActivity(net, "none:"); err != nil || ok {
		t.Fatalf("NewestActivity of nothing = %v, %v", ok, err)
	}
	if _, _, err := s.ChatIDs(net, ChatFilter{}, "", 0); err == nil {
		t.Fatal("ChatIDs took a limit of 0")
	}
}
