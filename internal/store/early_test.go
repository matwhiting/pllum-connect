package store

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every ID, number and text here is invented.

// earlyKinds are the updates that can reach the store before the line they
// are about: none is a whole line, so none may show until the line arrives.
var earlyKinds = []struct {
	name   string
	update Message
	check  func(Message) string // the line once its original has arrived
}{
	{"an edit with no time", Message{Text: "fixed", Edited: true}, wantText("fixed", true)},
	{"an edit with a time and no side", Message{TimeMs: base + 90_000, Text: "fixed", Edited: true}, wantText("fixed", true)},
	{"a revoke with no time", Message{Deleted: true}, wantDeleted},
	{"a revoke with a time", Message{TimeMs: base + 90_000, Deleted: true}, wantDeleted},
	{"a placeholder", Message{TimeMs: base + 1000, Hidden: ptr(true)}, wantText("the original", false)},
	{"a seen revoke", Message{Deleted: true, SeenMs: base + 95_000}, wantDeleted},
	{"a line with no time", Message{Text: "the original", Kind: "text"}, wantText("the original", false)},
}

func wantText(text string, edited bool) func(Message) string {
	return func(m Message) string {
		if m.Text != text || m.Edited != edited || m.Deleted || *m.Hidden {
			return fmt.Sprintf("text %q edited %v deleted %v hidden %v", m.Text, m.Edited, m.Deleted, *m.Hidden)
		}
		return ""
	}
}

func wantDeleted(m Message) string {
	if !m.Deleted || m.Text != "" {
		return fmt.Sprintf("deleted %v text %q", m.Deleted, m.Text)
	}
	return ""
}

func TestAnEarlyUpdateNeverShowsBeforeItsLine(t *testing.T) {
	for _, k := range earlyKinds {
		for _, withLines := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, chat with lines %v", k.name, withLines), func(t *testing.T) {
				s := openTest(t)
				if withLines {
					put(t, s, text(phoneChat, "3EB0L0001", base-60_000, "an earlier line"))
				}
				var before Chat
				if withLines {
					before = chatOf(t, s, phoneChat)
				}

				u := k.update
				u.ID, u.ChatID = "3EB0L0002", phoneChat
				if _, err := s.PutMessage(net, u); err != nil {
					t.Fatalf("the early update: %v", err)
				}
				wantIDs := []string{}
				if withLines {
					wantIDs = []string{"3EB0L0001"}
				}
				if got := ids(all(t, s, phoneChat)); !slices.Equal(got, wantIDs) {
					t.Fatalf("listed %v, want %v", got, wantIDs)
				}
				c := chatOf(t, s, phoneChat)
				if c.LastMessageID != before.LastMessageID || c.LastFromMe != before.LastFromMe ||
					c.LastActivityMs != before.LastActivityMs || c.HistoryFromMs != before.HistoryFromMs ||
					c.Unread != before.Unread {
					t.Fatalf("the early update moved the chat from %+v to %+v", before, c)
				}
				n, err := s.Counts(net)
				if err != nil {
					t.Fatal(err)
				}
				if withLines && (n.Messages != 1 || n.OldestMs == nil || *n.OldestMs != base-60_000) ||
					!withLines && (n.Messages != 0 || n.OldestMs != nil) {
					t.Fatalf("counts moved: %+v", n)
				}
				if p, err := s.Search(net, Query{Text: "fixed", Limit: 10}); err != nil || len(p.Items) != 0 {
					t.Fatalf("search found the early update: %v, %v", ids(p.Items), err)
				}

				// The original arrives: one line, at its own time and side.
				orig := mine(phoneChat, "3EB0L0002", base, "the original")
				orig.Source, orig.Hidden = SourceSync, ptr(false)
				if put(t, s, orig) {
					t.Fatalf("the original was added beside its early update")
				}
				got := all(t, s, phoneChat)
				if len(got) != len(wantIDs)+1 {
					t.Fatalf("after the original: %v", ids(got))
				}
				m := got[len(got)-1]
				if m.ID != "3EB0L0002" || m.TimeMs != base || !m.FromMe || m.SenderID != "self" {
					t.Fatalf("the line is %+v", m)
				}
				if bad := k.check(m); bad != "" {
					t.Fatalf("the line: %s", bad)
				}
				c = chatOf(t, s, phoneChat)
				wantLast := "3EB0L0002"
				if m.Deleted {
					wantLast = before.LastMessageID
				}
				if c.LastMessageID != wantLast || c.Unread != before.Unread {
					t.Fatalf("after the original the chat is %+v", c)
				}
			})
		}
	}
}

func TestABeeperCopyFillsAnEarlyRow(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0F0000", base-60_000, "an earlier line"))
	// An edit and a revoke of lines older than WhatsApp's history window.
	if _, err := s.PutMessage(net, Message{ID: "3EB0F0001", ChatID: phoneChat, Text: "fixed", Edited: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutMessage(net, Message{ID: "3EB0F0002", ChatID: phoneChat, Deleted: true}); err != nil {
		t.Fatal(err)
	}
	// Beeper's copy holds both lines, naming their WhatsApp IDs.
	a := beeperMsg("bm-f1", base, "typo")
	a.NetworkID = "3EB0F0001"
	b := beeperMsg("bm-f2", base+1000, "regret this")
	b.NetworkID, b.FromMe = "3EB0F0002", true
	b.Kind, b.Media = "photo", []ImportMedia{{Kind: "photo", Width: 4, Height: 3}}
	if r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{a, b}}); r != (ImportResult{Matched: 2}) {
		t.Fatalf("the import = %+v, want both matched", r)
	}
	got := all(t, s, phoneChat)
	if !slices.Equal(ids(got), []string{"3EB0F0000", "3EB0F0001", "3EB0F0002"}) {
		t.Fatalf("listed %v", ids(got))
	}
	if m := got[1]; m.TimeMs != base || m.FromMe || m.SenderID != "beeper-sender" || m.Text != "fixed" || !m.Edited {
		t.Fatalf("the edited line is %+v", m)
	}
	if m := got[2]; m.TimeMs != base+1000 || !m.FromMe || !m.Deleted || m.Text != "" || m.Kind != "photo" {
		t.Fatalf("the deleted line is %+v", m)
	}
	c := chatOf(t, s, phoneChat)
	if c.LastMessageID != "3EB0F0001" || c.Unread != 1 || c.HistoryFromMs != base-60_000 {
		t.Fatalf("the chat is %+v (only the earlier line is unread: copied history never is)", c)
	}
	want := map[string]string{"bm-f1": "3EB0F0001", "bm-f2": "3EB0F0002"}
	if got := lineIDs(t, s, []string{"bm-f1", "bm-f2"}); !maps.Equal(got, want) {
		t.Fatalf("LineIDs = %v, want %v", got, want)
	}
	// Copying again adds nothing; the original arriving after all sets its own time.
	if r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{a, b}}); r != (ImportResult{Skipped: 2}) {
		t.Fatalf("the second import = %+v", r)
	}
	orig := text(phoneChat, "3EB0F0001", base+300, "typo")
	orig.Source = SourceSync
	if put(t, s, orig) {
		t.Fatalf("the original was added beside its filled row")
	}
	if got := all(t, s, phoneChat); len(got) != 3 || got[1].TimeMs != base+300 || got[1].Text != "fixed" {
		t.Fatalf("after the original: %+v", got)
	}
}

func TestAMediaEntryNeedsAMediaKind(t *testing.T) {
	s := openTest(t)
	for _, k := range []string{"", "text", "notice", "hologram"} {
		m := text(phoneChat, "3EB0M0001", base, "")
		m.Kind, m.Media = "photo", []Media{{Kind: k}}
		if _, err := s.PutMessage(net, m); wire.KindOf(err) != wire.BadRequest {
			t.Errorf("PutMessage with media kind %q gave %v", k, err)
		}
		in := beeperMsg("bm-m1", base, "")
		in.Kind, in.Media = "photo", []ImportMedia{{Kind: k}}
		if _, err := s.Import(net, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat,
			Messages: []ImportMessage{beeperMsg("bm-m0", base-1, "fine"), in}}); wire.KindOf(err) != wire.BadRequest {
			t.Errorf("import with media kind %q gave %v", k, err)
		}
	}
	if _, ok := s.ChatFor(net, phoneChat); ok {
		t.Fatalf("a refused write left a chat behind")
	}
	for _, k := range []string{"photo", "video", "voiceNote", "sticker", "file"} {
		m := text(phoneChat, "3EB0M"+k, base, "")
		m.Kind, m.Media = k, []Media{{Kind: k}}
		if _, err := s.PutMessage(net, m); err != nil {
			t.Errorf("media kind %q was refused: %v", k, err)
		}
		in := beeperMsg("bm-"+k, base+5000, "")
		in.Kind, in.Media = k, []ImportMedia{{Kind: k}}
		if _, err := s.Import(net, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat,
			Messages: []ImportMessage{in}}); err != nil {
			t.Errorf("imported media kind %q was refused: %v", k, err)
		}
	}
}

func TestAPlaceholderNeverHidesReadableText(t *testing.T) {
	s := openTest(t)
	in := beeperMsg("bm-h", base, "readable in Beeper")
	in.NetworkID = "3EB0H0101"
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})
	put(t, s, text(phoneChat, "3EB0H0102", base+1000, "readable here"))

	for _, id := range []string{"3EB0H0101", "3EB0H0102"} {
		if _, err := s.PutMessage(net, Message{ID: id, ChatID: phoneChat, TimeMs: base, Hidden: ptr(true),
			SeenMs: base + 5000}); err != nil {
			t.Fatal(err)
		}
	}
	got := all(t, s, phoneChat)
	if len(got) != 2 {
		t.Fatalf("listed %v", ids(got))
	}
	for _, m := range got {
		if *m.Hidden || m.SeenMs != base+5000 || m.Text == "" {
			t.Fatalf("a placeholder hid a readable line, or did not mark it seen: %+v", m)
		}
	}
	if got[0].ID != "beeper:bm-h" {
		t.Fatalf("the placeholder took the Beeper line over: %v", ids(got))
	}
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0H0102" {
		t.Fatalf("lastMessageID = %q", c.LastMessageID)
	}
}

func TestAStoreFromBeforeEarlyRowsGetsTheColumn(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, text(phoneChat, "3EB0U0001", base, "kept"))
	// A schema-2 store: no early column, and a revoke kept at time 0.
	for _, q := range []string{
		`ALTER TABLE messages DROP COLUMN early`,
		`INSERT INTO messages (network, chat_id, msg_id, ts_ms, deleted, source) VALUES ('whatsapp', '` +
			phoneChat + `', '3EB0U0002', 0, 1, 'live')`,
		`UPDATE meta SET value = '2' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening a schema-2 store: %v", err)
	}
	defer s.Close()
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0U0001"}) {
		t.Fatalf("after the upgrade the chat lists %v", got)
	}
	// The line arriving later still lands in the kept revoke.
	if put(t, s, text(phoneChat, "3EB0U0002", base+1000, "gone")) {
		t.Fatalf("the line was added beside its revoke")
	}
	if got := all(t, s, phoneChat); len(got) != 2 || !got[1].Deleted || got[1].Text != "" {
		t.Fatalf("after the line: %+v", got)
	}
}

// permutations lists every order of 0..n-1.
func permutations(n int) [][]int {
	var out [][]int
	var rec func(p []int, used []bool)
	rec = func(p []int, used []bool) {
		if len(p) == n {
			out = append(out, slices.Clone(p))
			return
		}
		for i := range n {
			if !used[i] {
				used[i] = true
				rec(append(p, i), used)
				used[i] = false
			}
		}
	}
	rec(nil, make([]bool, n))
	return out
}

// The five things that can reach the store about one line of his, in any
// order: the original (from history), an edit and a revoke (live, with no
// time), a read receipt, and Beeper's copy (which saw the edit).
const (
	evOriginal = iota
	evEdit
	evRevoke
	evReceipt
	evBeeper
)

// TestEveryOrderShowsTheLineOnce runs all 120 orders of those five events
// (Review Focus 2 and 3). After every step nothing early shows; at the end
// there is exactly one line, at the original's time and side, edited or
// deleted.
func TestEveryOrderShowsTheLineOnce(t *testing.T) {
	cases := []struct {
		revoke, networkID bool
		strict            bool // false: only the weaker property below holds
	}{
		{revoke: false, networkID: true, strict: true},
		{revoke: true, networkID: true, strict: true},
		{revoke: false, networkID: false, strict: true},
		// Beeper's copy saw the edit but not the revoke, and names no
		// WhatsApp ID: a deleted line holds no text to match it by, so when
		// the revoke comes first the copy stays a line of its own. Still the
		// line itself is held once, deleted, and never shows early.
		{revoke: true, networkID: false, strict: false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("revoke %v, networkID %v", tc.revoke, tc.networkID), func(t *testing.T) {
			s := openTest(t)
			for n, order := range permutations(5) {
				chat := fmt.Sprintf("1%010d@s.whatsapp.net", n)
				if bad := runOrder(t, s, chat, order, tc.revoke, tc.networkID, tc.strict); bad != "" {
					t.Fatalf("order %v: %s", order, bad)
				}
			}
		})
	}
}

func runOrder(t *testing.T, s *Store, chat string, order []int, revoke, networkID, strict bool) string {
	t.Helper()
	const id, prev = "3EB0Q0001", "3EB0Q0000"
	beeperLine := beeperPrefix + "bm-q-" + chat
	put(t, s, text(chat, prev, base-5000, "before"))
	seen := map[int]bool{}
	receiptLands := false // the receipt came when a row held the ID; else it is dropped
	for step, e := range order {
		var err error
		switch e {
		case evOriginal:
			o := mine(chat, id, base, "hello")
			o.Source = SourceSync
			_, err = s.PutMessage(net, o)
		case evEdit:
			_, err = s.PutMessage(net, Message{ID: id, ChatID: chat, Text: "hello2", Edited: true})
		case evRevoke:
			if revoke {
				_, err = s.PutMessage(net, Message{ID: id, ChatID: chat, Deleted: true})
			}
		case evReceipt:
			receiptLands = seen[evOriginal] || seen[evEdit] || (revoke && seen[evRevoke]) || (networkID && seen[evBeeper])
			_, err = s.PutMessage(net, Message{ID: id, ChatID: chat, SeenMs: base + 10})
		case evBeeper:
			in := beeperMsg("bm-q-"+chat, base+300, "hello2")
			in.FromMe = true
			if networkID {
				in.NetworkID = id
			}
			_, err = s.Import(net, ImportBatch{BeeperChatID: "bc-" + chat, ChatID: chat,
				Chat: ImportChat{Kind: "single"}, Messages: []ImportMessage{in}})
		}
		if err != nil {
			return fmt.Sprintf("event %d: %v", e, err)
		}
		seen[e] = true

		// After every step.
		got := all(t, s, chat)
		c := chatOf(t, s, chat)
		for _, m := range got {
			if m.TimeMs <= 0 {
				return fmt.Sprintf("after step %d a line shows at time %d: %+v", step, m.TimeMs, m)
			}
			if m.ID != prev && m.ID != id && m.ID != beeperLine {
				return fmt.Sprintf("after step %d a stranger shows: %v", step, ids(got))
			}
		}
		if c.Unread != 1 || c.HistoryFromMs != base-5000 {
			return fmt.Sprintf("after step %d the chat is %+v", step, c)
		}
		if !seen[evOriginal] && !seen[evBeeper] {
			if !slices.Equal(ids(got), []string{prev}) || c.LastMessageID != prev || c.LastActivityMs != base-5000 {
				return fmt.Sprintf("after step %d, before any copy of the line: %v, %+v", step, ids(got), c)
			}
		}
		if (strict && networkID) && len(got) > 2 {
			return fmt.Sprintf("after step %d the line shows twice: %v", step, ids(got))
		}
	}

	got := all(t, s, chat)
	c := chatOf(t, s, chat)
	var line *Message
	for i := range got {
		if got[i].ID == id {
			if line != nil {
				return fmt.Sprintf("two lines hold the ID: %v", ids(got))
			}
			line = &got[i]
		}
	}
	switch {
	case line == nil:
		return fmt.Sprintf("the line is missing: %v", ids(got))
	case strict && len(got) != 2:
		return fmt.Sprintf("rows %v", ids(got))
	case line.TimeMs != base:
		return fmt.Sprintf("time %+d", line.TimeMs-base)
	case !line.FromMe:
		return "fromMe false"
	case receiptLands && line.SeenMs != base+10:
		return fmt.Sprintf("seenMs %d", line.SeenMs)
	case revoke && (!line.Deleted || line.Text != ""):
		return fmt.Sprintf("not deleted: %+v", *line)
	case !revoke && (line.Text != "hello2" || !line.Edited):
		return fmt.Sprintf("text %q edited %v", line.Text, line.Edited)
	case revoke && c.LastMessageID == id:
		return "lastMessageID is the deleted line"
	case revoke && strict && c.LastMessageID != prev:
		return "lastMessageID " + c.LastMessageID
	case !revoke && c.LastMessageID != id:
		return "lastMessageID " + c.LastMessageID
	}
	return ""
}

func TestAliasingKeepsEarlyRowsEarlyUntilTheirLine(t *testing.T) {
	s := openTest(t)
	edit := func(chat, id string) {
		t.Helper()
		if _, err := s.PutMessage(net, Message{ID: id, ChatID: chat, Text: id + " fixed", Edited: true}); err != nil {
			t.Fatal(err)
		}
	}
	// An early edit on one side, its original on the other: both ways round.
	edit(lidChat, "3EB0A0001")
	put(t, s, mine(phoneChat, "3EB0A0001", base, "one"))
	put(t, s, mine(lidChat, "3EB0A0002", base+1000, "two"))
	edit(phoneChat, "3EB0A0002")
	// An early revoke with nothing on the other side.
	if _, err := s.PutMessage(net, Message{ID: "3EB0A0003", ChatID: lidChat, Deleted: true}); err != nil {
		t.Fatal(err)
	}
	// An early edit on one side, Beeper's copy naming it on the other: both ways round.
	edit(phoneChat, "3EB0A0004")
	in := beeperMsg("bm-a4", base+4000, "four")
	in.NetworkID = "3EB0A0004"
	importOne(t, s, ImportBatch{BeeperChatID: "bc-l", ChatID: lidChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})
	edit(lidChat, "3EB0A0005")
	in = beeperMsg("bm-a5", base+5000, "five")
	in.NetworkID = "3EB0A0005"
	importOne(t, s, ImportBatch{BeeperChatID: "bc-p", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})

	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	got := all(t, s, phoneChat)
	if want := []string{"3EB0A0001", "3EB0A0002", "3EB0A0004", "beeper:bm-a5"}; !slices.Equal(ids(got), want) {
		t.Fatalf("the merged chat lists %v, want %v", ids(got), want)
	}
	for i, want := range []struct {
		ms   int64
		text string
	}{{base, "3EB0A0001 fixed"}, {base + 1000, "3EB0A0002 fixed"}, {base + 4000, "3EB0A0004 fixed"}, {base + 5000, "3EB0A0005 fixed"}} {
		if got[i].TimeMs != want.ms || got[i].Text != want.text || !got[i].Edited {
			t.Errorf("line %d is %+v, want %q at %+d", i, got[i], want.text, want.ms-base)
		}
	}
	wantIDs := map[string]string{"bm-a4": "3EB0A0004", "bm-a5": "beeper:bm-a5"}
	if got := lineIDs(t, s, []string{"bm-a4", "bm-a5"}); !maps.Equal(got, wantIDs) {
		t.Errorf("LineIDs = %v, want %v", got, wantIDs)
	}
	// The early revoke came across and still waits for its line.
	if put(t, s, text(lidChat, "3EB0A0003", base+3000, "three")) {
		t.Fatalf("the line was added beside its revoke")
	}
	if got := all(t, s, phoneChat); len(got) != 5 || got[2].ID != "3EB0A0003" || !got[2].Deleted || got[2].Text != "" {
		t.Fatalf("after the revoked line: %+v", got)
	}
}
