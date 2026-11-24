package store

import (
	"fmt"
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

func TestAFilledRowReadsAsCopiedHistory(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0F0000", base-60_000, "an earlier line"))
	// An edit and a revoke of lines older than WhatsApp's history window.
	if _, err := s.PutMessage(net, Message{ID: "3EB0F0001", ChatID: phoneChat, Text: "fixed", Edited: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutMessage(net, Message{ID: "3EB0F0002", ChatID: phoneChat, Deleted: true}); err != nil {
		t.Fatal(err)
	}
	// Beeper's copy held both lines, naming their WhatsApp IDs, and filled them.
	fillCopy(t, s, phoneChat, "bc-1", "3EB0F0001", copyLine{beeperID: "bm-f1", ms: base, text: "typo"})
	fillCopy(t, s, phoneChat, "bc-1", "3EB0F0002", copyLine{beeperID: "bm-f2", ms: base + 1000, text: "regret this",
		fromMe: true, kind: "photo", media: []Media{{Kind: "photo", Width: 4, Height: 3}}})
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
	for b, want := range map[string]string{"bm-f1": "3EB0F0001", "bm-f2": "3EB0F0002"} {
		if got := carrier(t, s, b); got != want {
			t.Fatalf("%s is carried by %q, want %q", b, got, want)
		}
	}
	// counts: both filled rows are matched lines.
	if n, err := s.Counts(net); err != nil || n.Matched != 2 || n.Imported != 2 || n.Messages != 3 {
		t.Fatalf("Counts = %+v, %v", n, err)
	}
	// The original arriving after all sets its own time.
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
	}
}

func TestAPlaceholderNeverHidesReadableText(t *testing.T) {
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-h", ms: base, text: "readable in Beeper", networkID: "3EB0H0101"})
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

// The five things that can reach the store about one line of his: Beeper's
// copy (which saw the edit), held from before the switch, then in any order
// the original (from history), an edit and a revoke (live, with no time) and
// a read receipt.
const (
	evOriginal = iota
	evEdit
	evRevoke
	evReceipt
	evBeeper
)

// TestEveryOrderShowsTheLineOnce runs every order of those events that a
// store can still see (Review Focus 2 and 3): the copy first, as nothing
// copies any more, then all 24 orders of the other four. After every step
// nothing early shows; at the end there is exactly one line, at the
// original's time and side, edited or deleted.
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
			for n, rest := range permutations(4) {
				order := []int{evBeeper}
				for _, e := range rest {
					order = append(order, e) // evOriginal..evReceipt are 0..3
				}
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
			l := copyLine{beeperID: "bm-q-" + chat, ms: base + 300, text: "hello2", fromMe: true}
			if networkID {
				l.networkID = id
			}
			seedCopies(t, s, chat, "bc-"+chat, l)
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
	seedCopies(t, s, lidChat, "bc-l", copyLine{beeperID: "bm-a4", ms: base + 4000, text: "four", networkID: "3EB0A0004"})
	edit(lidChat, "3EB0A0005")
	seedCopies(t, s, phoneChat, "bc-p", copyLine{beeperID: "bm-a5", ms: base + 5000, text: "five", networkID: "3EB0A0005"})

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
	for b, want := range map[string]string{"bm-a4": "3EB0A0004", "bm-a5": "beeper:bm-a5"} {
		if got := carrier(t, s, b); got != want {
			t.Errorf("%s is carried by %q, want %q", b, got, want)
		}
	}
	// The early revoke came across and still waits for its line.
	if put(t, s, text(lidChat, "3EB0A0003", base+3000, "three")) {
		t.Fatalf("the line was added beside its revoke")
	}
	if got := all(t, s, phoneChat); len(got) != 5 || got[2].ID != "3EB0A0003" || !got[2].Deleted || got[2].Text != "" {
		t.Fatalf("after the revoked line: %+v", got)
	}
}

func TestAHiddenLineCanBeShownAgain(t *testing.T) {
	s := openTest(t)
	placeholder := text(phoneChat, "3EB0H0001", base, "")
	placeholder.Hidden = ptr(true) // could not be decrypted yet
	put(t, s, placeholder)
	put(t, s, text(phoneChat, "3EB0H0000", base-1000, "before"))
	if _, err := s.PutMessage(net, Message{ID: "3EB0H0001", ChatID: phoneChat, SeenMs: base + 1}); err != nil {
		t.Fatal(err)
	}
	if got := all(t, s, phoneChat); !*got[1].Hidden {
		t.Fatalf("an update that says nothing of hidden showed the line")
	}
	decrypted := text(phoneChat, "3EB0H0001", base, "now readable")
	decrypted.Hidden = ptr(false)
	put(t, s, decrypted)
	got := all(t, s, phoneChat)
	if *got[1].Hidden || got[1].Text != "now readable" {
		t.Fatalf("the decrypted line is %+v", got[1])
	}
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0H0001" {
		t.Fatalf("lastMessageID = %q", c.LastMessageID)
	}
}

func TestARevokeBeforeItsLineStillDeletesIt(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0V0000", base-1000, "earlier"))
	// The revoke comes live; the line itself is still in pending history.
	added, err := s.PutMessage(net, Message{ID: "3EB0V0001", ChatID: phoneChat, Deleted: true})
	if err != nil || !added {
		t.Fatalf("the revoke: added %v, %v; want a tombstone kept", added, err)
	}
	if c := chatOf(t, s, phoneChat); c.HistoryFromMs != base-1000 || c.LastMessageID != "3EB0V0000" {
		t.Fatalf("the tombstone moved the chat: %+v", c)
	}
	if put(t, s, text(phoneChat, "3EB0V0001", base, "regret this")) {
		t.Fatalf("the line was added beside its tombstone")
	}
	got := all(t, s, phoneChat)
	if len(got) != 2 || got[1].ID != "3EB0V0001" || !got[1].Deleted || got[1].Text != "" || got[1].TimeMs != base {
		t.Fatalf("the line that arrived after its revoke is %+v", got[len(got)-1])
	}
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0V0000" {
		t.Fatalf("lastMessageID = %q, want the line before the deleted one", c.LastMessageID)
	}
}

func TestAKindlessHiddenPlaceholderIsKeptUntilItDecrypts(t *testing.T) {
	s := openTest(t)
	added, err := s.PutMessage(net, Message{ID: "3EB0P0001", ChatID: phoneChat, TimeMs: base, Hidden: ptr(true)})
	if err != nil || !added {
		t.Fatalf("the placeholder: added %v, %v", added, err)
	}
	// Kept as an early row (fix round 4): it shows nowhere until it decrypts.
	if got := all(t, s, phoneChat); len(got) != 0 {
		t.Fatalf("the placeholder is listed: %+v", got)
	}
	decrypted := text(phoneChat, "3EB0P0001", base, "now readable")
	decrypted.Hidden = ptr(false)
	put(t, s, decrypted)
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0P0001" {
		t.Fatalf("the decrypted line does not show: %+v", c)
	}
}
