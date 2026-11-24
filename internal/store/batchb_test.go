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

// A step of the batch equivalence test: a line, a reaction, a reaction
// count or a settle.
type step struct {
	line    *Message
	react   *[3]string // chat, line, emoji (sender react-sender)
	summary *[3]string // chat, line, emoji
	settle  *[2]string // chat, line
}

func lineStep(m Message) step {
	if m.SenderID == "" && m.Kind != "" {
		m.SenderID = "sender-1"
	}
	return step{line: &m}
}

// batchSetup is what both stores hold before the steps: a read marker, an
// alias, two placeholders for messages that couldn't be read, lines of
// Beeper's copy, and a line held already.
func batchSetup(t *testing.T, s *Store) {
	t.Helper()
	marker := base + 50
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, ReadThroughMs: &marker}); err != nil {
		t.Fatal(err)
	}
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	hidden := false
	for _, m := range []Message{
		{ID: "3EB0U0001", ChatID: phoneChat, SenderID: "sender-1", TimeMs: base + 10, Kind: "undecryptable", Hidden: &hidden},
		{ID: "3EB0U0002", ChatID: phoneChat, SenderID: "sender-1", TimeMs: base + 11, Kind: "unavailable", Hidden: &hidden},
		{ID: "3EB0H0001", ChatID: phoneChat, SenderID: "sender-1", TimeMs: base + 12, Kind: "text", Text: "held already"},
	} {
		if _, err := s.PutMessage(net, m); err != nil {
			t.Fatal(err)
		}
	}
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-1", base+60, "beeper copy one"), beeperMsg("bm-2", base+61, "beeper copy two")}})
}

// batchSteps covers every shape a network writes in one run of history:
// whole lines through an alias, read and unread, a line sent again, news
// before its line (an early row), edits and revokes, receipts held and not,
// a placeholder taken over by its message, notices, a line taking over
// Beeper's copy, a newest line revoked at once (its time still moves the
// chat's activity), reactions, counts, settles, a refused line, and lines
// in other chats.
func batchSteps() []step {
	hidden, shown := true, false
	return []step{
		lineStep(Message{ID: "3EB0B0001", ChatID: lidChat, TimeMs: base + 20, Kind: "text", Text: "read line", Hidden: &shown}),
		lineStep(Message{ID: "3EB0B0002", ChatID: lidChat, TimeMs: base + 70, Kind: "text", Text: "unread line", Hidden: &shown}),
		lineStep(Message{ID: "3EB0B0002", ChatID: lidChat, TimeMs: base + 70, Kind: "text", Text: "unread line", Hidden: &shown}),
		lineStep(Message{ID: "3EB0B0009", ChatID: phoneChat, Deleted: true}),
		lineStep(Message{ID: "3EB0B0010", ChatID: phoneChat, Text: "edited first", Edited: true}),
		lineStep(Message{ID: "3EB0B0009", ChatID: phoneChat, TimeMs: base + 71, Kind: "text", Text: "revoked", Hidden: &shown}),
		lineStep(Message{ID: "3EB0B0010", ChatID: phoneChat, TimeMs: base + 72, Kind: "text", Text: "original", Hidden: &shown}),
		lineStep(Message{ID: "3EB0B0003", ChatID: phoneChat, TimeMs: base + 73, Kind: "text", Text: "to edit", FromMe: true, SenderID: "self"}),
		lineStep(Message{ID: "3EB0B0003", ChatID: phoneChat, Text: "edited", Edited: true}),
		lineStep(Message{ID: "3EB0B0001", ChatID: phoneChat, SeenMs: base + 99}),
		lineStep(Message{ID: "3EB0B0003", ChatID: phoneChat, SeenMs: base + 98}),
		lineStep(Message{ID: "3EB0NONE01", ChatID: phoneChat, SeenMs: base + 97}),
		lineStep(Message{ID: "3EB0U0001", ChatID: phoneChat, TimeMs: base + 10, Kind: "text", Text: "now readable", Hidden: &shown}),
		lineStep(Message{ID: "3EB0N0001", ChatID: phoneChat, TimeMs: base + 74, Kind: "notice", Hidden: &hidden}),
		lineStep(Message{ID: "3EB0B0060", ChatID: phoneChat, TimeMs: base + 60, Kind: "text", Text: "beeper copy one", Hidden: &shown, SenderID: "beeper-sender"}),
		lineStep(Message{ID: "3EB0B0099", ChatID: phoneChat, TimeMs: base + 90, Kind: "text", Text: "newest, then gone", Hidden: &shown}),
		lineStep(Message{ID: "3EB0B0099", ChatID: phoneChat, Deleted: true}),
		{react: &[3]string{lidChat, "3EB0B0002", "👍"}},
		{react: &[3]string{phoneChat, "3EB0B0002", ""}},
		{react: &[3]string{phoneChat, "3EB0B0001", "❤️"}},
		{summary: &[3]string{phoneChat, "3EB0B0001", "😂"}},
		{settle: &[2]string{phoneChat, "3EB0U0002"}},
		{settle: &[2]string{phoneChat, "3EB0B0001"}},
		lineStep(Message{ID: "3EB0BAD001", ChatID: phoneChat, TimeMs: -1, Kind: "text", Text: "refused"}),
		lineStep(Message{ID: "3EB0G0001", ChatID: groupChat, TimeMs: base + 80, Kind: "photo", Media: []Media{{Kind: "photo", Width: 4}}, Hidden: &shown}),
		lineStep(Message{ID: "3EB0O0001", ChatID: otherChat, TimeMs: base + 81, Kind: "text", Text: "other", Hidden: &shown, Source: SourceSync}),
	}
}

// dump is every row of the tables a write touches, as text.
func dump(t *testing.T, s *Store) string {
	t.Helper()
	var out []string
	for _, q := range []string{
		`SELECT * FROM chats ORDER BY rid`,
		`SELECT * FROM messages ORDER BY rid`,
		`SELECT * FROM reactions ORDER BY network, chat_id, msg_id, sender_id`,
		`SELECT * FROM beeper_ids ORDER BY rid, beeper_id`,
		`SELECT * FROM aliases ORDER BY network, alias_jid`,
		`SELECT rowid, text FROM messages_fts ORDER BY rowid`,
	} {
		rows, err := s.db.Query(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprint(vals...))
		}
		rows.Close()
		out = append(out, "--")
	}
	return strings.Join(out, "\n")
}

// A batch leaves the store exactly as the same writes made one at a time
// do: the same rows, the same derived chat values (newest line, unread
// count, oldest line, activity), the same reactions, and the same answer
// for each write (review C1).
func TestBatchMatchesWritesOneAtATime(t *testing.T) {
	one, batched := openTest(t), openTest(t)
	batchSetup(t, one)
	batchSetup(t, batched)

	var wantAdded []bool
	var wantChanged []bool
	var wantErr []bool
	for _, st := range batchSteps() {
		switch {
		case st.line != nil:
			before, held := one.LineState(net, st.line.ChatID, st.line.ID)
			added, err := one.PutMessage(net, *st.line)
			after, _ := one.LineState(net, st.line.ChatID, st.line.ID)
			wantAdded = append(wantAdded, added)
			wantChanged = append(wantChanged, err == nil && (added || !held || after != before))
			wantErr = append(wantErr, err != nil)
		case st.react != nil:
			if err := one.SetReaction(net, st.react[0], st.react[1], Reaction{Emoji: st.react[2], SenderID: "react-sender"}); err != nil {
				t.Fatal(err)
			}
		case st.summary != nil:
			if err := one.SetReactionSummary(net, st.summary[0], st.summary[1], []Reaction{{Emoji: st.summary[2], FromMe: true}}); err != nil {
				t.Fatal(err)
			}
		case st.settle != nil:
			if _, err := one.SettlePlaceholder(net, st.settle[0], st.settle[1]); err != nil {
				t.Fatal(err)
			}
		}
	}

	var gotAdded, gotChanged, gotErr []bool
	err := batched.Batch(net, func(b *Batch) error {
		for _, st := range batchSteps() {
			switch {
			case st.line != nil:
				p, err := b.Put(*st.line)
				gotAdded = append(gotAdded, p.Added)
				gotChanged = append(gotChanged, p.Changed)
				gotErr = append(gotErr, err != nil)
			case st.react != nil:
				if err := b.SetReaction(st.react[0], st.react[1], Reaction{Emoji: st.react[2], SenderID: "react-sender"}); err != nil {
					return err
				}
			case st.summary != nil:
				if err := b.SetReactionSummary(st.summary[0], st.summary[1], []Reaction{{Emoji: st.summary[2], FromMe: true}}); err != nil {
					return err
				}
			case st.settle != nil:
				if _, err := b.SettlePlaceholder(st.settle[0], st.settle[1]); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if fmt.Sprint(gotAdded) != fmt.Sprint(wantAdded) || fmt.Sprint(gotChanged) != fmt.Sprint(wantChanged) ||
		fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
		t.Fatalf("answers differ:\nadded   %v\nwant    %v\nchanged %v\nwant    %v\nerr     %v\nwant    %v",
			gotAdded, wantAdded, gotChanged, wantChanged, gotErr, wantErr)
	}
	if a, b := dump(t, one), dump(t, batched); a != b {
		t.Fatalf("stores differ:\none at a time:\n%s\n\nbatched:\n%s", a, b)
	}
	// The newest line was revoked in the batch, yet its time moved the
	// chat's activity, as it did one at a time.
	if c := chatOf(t, batched, phoneChat); c.LastActivityMs != base+90 || c.LastMessageID == "3EB0B0099" {
		t.Fatalf("activity %d, newest %q", c.LastActivityMs, c.LastMessageID)
	}
	// The steps did what they are there for: a line took over Beeper's
	// copy, a placeholder was taken over by its message, another settled.
	if got := lineIDs(t, batched, []string{"bm-1"}); got["bm-1"] != "3EB0B0060" {
		t.Fatalf("Beeper's copy is held as %v", got)
	}
	if st, _ := batched.LineState(net, phoneChat, "3EB0U0001"); st.Kind != "text" || st.Text != "now readable" {
		t.Fatalf("the placeholder's message: %+v", st)
	}
	if st, _ := batched.LineState(net, phoneChat, "3EB0U0002"); !st.Hidden {
		t.Fatalf("the settled placeholder: %+v", st)
	}
}

// A batch's Put tells what each line did, and the same lines again change
// nothing; a refused line changes nothing and the batch goes on, its other
// lines kept; the chat is refreshed once, at the end, all the same.
func TestBatchPutCountsAndRefusesOneLine(t *testing.T) {
	s := openTest(t)
	ms := []Message{
		{ID: "3EB0P0001", ChatID: phoneChat, SenderID: "sender-1", TimeMs: base + 1, Kind: "text", Text: "one"},
		{ID: "3EB0P0004", ChatID: phoneChat, TimeMs: base + 4, Kind: "nonsense"},
		{ID: "3EB0P0002", ChatID: phoneChat, SenderID: "sender-1", TimeMs: base + 2, Kind: "text", Text: "two"},
		{ID: "3EB0P0002", ChatID: phoneChat, Text: "two, edited", Edited: true},
	}
	run := func() (added, changed, refused int) {
		t.Helper()
		err := s.Batch(net, func(b *Batch) error {
			for _, m := range ms {
				p, err := b.Put(m)
				switch {
				case err != nil:
					refused++
				case p.Added:
					added++
					changed++
				case p.Changed:
					changed++
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Batch: %v", err)
		}
		return
	}
	if a, c, r := run(); a != 2 || c != 3 || r != 1 {
		t.Fatalf("first run: %d added, %d changed, %d refused", a, c, r)
	}
	if a, c, r := run(); a != 0 || c != 0 || r != 1 {
		t.Fatalf("again: %d added, %d changed, %d refused", a, c, r)
	}
	c := chatOf(t, s, phoneChat)
	if c.Unread != 2 || c.LastMessageID != "3EB0P0002" || c.LastActivityMs != base+2 || c.HistoryFromMs != base+1 {
		t.Fatalf("chat after the batch: %+v", c)
	}
	if _, ok := s.LineState(net, phoneChat, "3EB0P0004"); ok {
		t.Fatal("a refused line was kept")
	}
}

// An error from the batch's function, or a panic in it, keeps nothing and
// leaves the store writable.
func TestBatchRollsBackOnErrorAndPanic(t *testing.T) {
	s := openTest(t)
	line := Message{ID: "3EB0R0001", ChatID: phoneChat, SenderID: "sender-1", TimeMs: base + 1, Kind: "text", Text: "one"}
	err := s.Batch(net, func(b *Batch) error {
		if _, err := b.PutMessage(line); err != nil {
			return err
		}
		return errNotFound
	})
	if err != errNotFound {
		t.Fatalf("Batch = %v; want its function's error", err)
	}
	func() {
		defer func() { _ = recover() }()
		_ = s.Batch(net, func(b *Batch) error {
			_, _ = b.PutMessage(line)
			panic("invented failure")
		})
	}()
	if _, ok := s.LineState(net, phoneChat, "3EB0R0001"); ok {
		t.Fatal("a rolled-back batch kept its line")
	}
	var kept *Batch
	if err := s.Batch(net, func(b *Batch) error { kept = b; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := kept.PutMessage(line); err == nil {
		t.Fatal("a batch wrote after its function returned")
	}
	if !put(t, s, line) {
		t.Fatal("the store took no line after a rolled-back batch")
	}
}

// A store write called from inside a batch's function (or any write's)
// would wait for ever on the batch's own lock. It is caught instead: under
// test it panics, naming the mistake, and the store stays writable.
func TestAWriteInsideABatchIsCaughtNotHung(t *testing.T) {
	s := openTest(t)
	for name, inner := range map[string]func() error{
		"PutNetworkValue": func() error { return s.PutNetworkValue(net, "invented", "1") },
		"Batch":           func() error { return s.Batch(net, func(*Batch) error { return nil }) },
		"PutMessage": func() error {
			_, err := s.PutMessage(net, text(phoneChat, "3EB0W0001", base, "inner"))
			return err
		},
	} {
		done := make(chan any, 1)
		go func() {
			defer func() { done <- recover() }()
			_ = s.Batch(net, func(b *Batch) error {
				if _, err := b.PutMessage(text(phoneChat, "3EB0W0002", base+1, "outer")); err != nil {
					return err
				}
				return inner()
			})
		}()
		select {
		case got := <-done:
			if got == nil || !strings.Contains(fmt.Sprint(got), "inside a write") {
				t.Fatalf("%s inside a batch: recovered %v; want the store to name the mistake", name, got)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s inside a batch hung", name)
		}
	}
	if _, ok := s.LineState(net, phoneChat, "3EB0W0002"); ok {
		t.Fatal("a batch that was caught writing inside itself kept its line")
	}
	if !put(t, s, text(phoneChat, "3EB0W0003", base+2, "after")) {
		t.Fatal("the store took no write after a caught batch")
	}
	// Writers on other goroutines still just wait their turn.
	var wg sync.WaitGroup
	err := s.Batch(net, func(b *Batch) error {
		wg.Go(func() {
			if err := s.PutNetworkValue(net, "invented", "2"); err != nil {
				t.Errorf("a concurrent write: %v", err)
			}
		})
		time.Sleep(50 * time.Millisecond)
		_, err := b.PutMessage(text(phoneChat, "3EB0W0004", base+3, "beside"))
		return err
	})
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if v, _, _ := s.NetworkValue(net, "invented"); v != "2" {
		t.Fatalf("the concurrent write kept %q", v)
	}
}
