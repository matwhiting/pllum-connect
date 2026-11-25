package store

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// Lines an older-history page brings (the history queue, FetchOlder, a
// backfill's older pages) are history: only lines newer than what the chat
// already held can be unread. Every ID and text here is invented.

// page writes lines in one batch, as a network lands one page.
func page(t *testing.T, s *Store, lines ...Message) {
	t.Helper()
	err := s.Batch(net, func(b *Batch) error {
		for _, m := range lines {
			m.Kind, m.Source, m.SenderID = "text", SourceSync, m.ChatID
			if _, err := b.PutMessage(m); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
}

func TestLinesFromAnOlderPageNeverCountAsUnread(t *testing.T) {
	s := openTest(t)
	// A chat with no read marker: its first page counts, as it always has.
	page(t, s, text(phoneChat, "o-5", base+5000, "newest"), text(phoneChat, "o-4", base+4000, "four"),
		text(phoneChat, "o-3", base+3000, "three"))
	if u := chatOf(t, s, phoneChat).Unread; u != 3 {
		t.Fatalf("unread after the first page = %d, want 3", u)
	}

	// An older page (the queue's, or a scroll-back's) adds history only.
	page(t, s, text(phoneChat, "o-2", base+2000, "two"), text(phoneChat, "o-1", base+1000, "one"))
	c := chatOf(t, s, phoneChat)
	if c.Unread != 3 {
		t.Errorf("unread after an older page = %d, want still 3", c.Unread)
	}
	if c.LastActivityMs != base+5000 || c.LastMessageID != "o-5" {
		t.Errorf("an older page moved the chat: activity %d, last %q", c.LastActivityMs, c.LastMessageID)
	}
	if n := len(all(t, s, phoneChat)); n != 5 {
		t.Errorf("the chat holds %d lines, want 5", n)
	}

	// A new line still counts.
	put(t, s, text(phoneChat, "o-6", base+6000, "new"))
	if u := chatOf(t, s, phoneChat).Unread; u != 4 {
		t.Errorf("unread after a new line = %d, want 4", u)
	}
	// So does one inside what the chat holds (a catch-up's line between two
	// held ones).
	put(t, s, text(phoneChat, "o-45", base+4500, "between"))
	if u := chatOf(t, s, phoneChat).Unread; u != 5 {
		t.Errorf("unread after a line between held ones = %d, want 5", u)
	}
}

func TestAnOlderPageTakingOverACopyAddsNoUnread(t *testing.T) {
	s := openTest(t)
	// Beeper's copy holds old history; a live line has come since.
	seedCopies(t, s, phoneChat, "bc-1",
		copyLine{beeperID: "bm-1", ms: base + 1000, text: "old one", networkID: "o-1"},
		copyLine{beeperID: "bm-2", ms: base + 2000, text: "old two", networkID: "o-2"})
	put(t, s, text(phoneChat, "o-9", base+9000, "live"))
	if u := chatOf(t, s, phoneChat).Unread; u != 1 {
		t.Fatalf("unread = %d, want the live line", u)
	}
	page(t, s, text(phoneChat, "o-2", base+2000, "old two"), text(phoneChat, "o-1", base+1000, "old one"))
	got := ids(all(t, s, phoneChat))
	if len(got) != 3 || got[0] != "o-1" || got[1] != "o-2" {
		t.Fatalf("the chat holds %v, want the copies taken over", got)
	}
	if u := chatOf(t, s, phoneChat).Unread; u != 1 {
		t.Errorf("unread after an older page took over its copies = %d, want 1", u)
	}
}

func TestUnreadSurvivesTheUpgradeToOlderLines(t *testing.T) {
	for _, tc := range []struct {
		from string
		drop []string
	}{
		// A schema-10 store gains participants.handle (11), then
		// messages.older (12).
		{"10", []string{`ALTER TABLE messages DROP COLUMN older`, `ALTER TABLE participants DROP COLUMN handle`}},
		// A schema-11 store (participants.handle already there) gains
		// only messages.older.
		{"11", []string{`ALTER TABLE messages DROP COLUMN older`}},
	} {
		t.Run(tc.from, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "connect")
			s, err := Open(dir, testKey)
			if err != nil {
				t.Fatal(err)
			}
			put(t, s, text(phoneChat, "u-1", base+1000, "one"))
			put(t, s, text(phoneChat, "u-2", base+2000, "two"))
			if err := s.PutChat(net, ChatInfo{ID: phoneChat, Participants: []Participant{{ID: otherChat, Name: "Ana", Handle: "ana.invented"}}}); err != nil {
				t.Fatal(err)
			}
			for _, q := range append(tc.drop, `UPDATE meta SET value = '`+tc.from+`' WHERE key = 'schema'`) {
				if _, err := s.db.Exec(q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			s.Close()
			s, err = Open(dir, testKey)
			if err != nil {
				t.Fatalf("reopening a schema-%s store: %v", tc.from, err)
			}
			defer s.Close()
			var version string
			if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&version); err != nil || version != "12" {
				t.Fatalf("schema = %q, %v; want 12", version, err)
			}
			var handle sql.NullString
			if err := s.db.QueryRow(`SELECT handle FROM participants WHERE participant_id = ?`, otherChat).Scan(&handle); err != nil {
				t.Fatalf("participants.handle: %v", err)
			}
			if tc.from == "11" && handle.String != "ana.invented" {
				t.Errorf("a schema-11 store lost its handle: %q", handle.String)
			}
			if u := chatOf(t, s, phoneChat).Unread; u != 2 {
				t.Errorf("after the upgrade unread = %d, want the 2 it had", u)
			}
			// And the rule holds from then on.
			page(t, s, text(phoneChat, "u-0", base, "older"))
			if u := chatOf(t, s, phoneChat).Unread; u != 2 {
				t.Errorf("an older page after the upgrade made unread %d, want 2", u)
			}
		})
	}
}

// Unread (what a MarkRead marks) and the unread count are one rule: an
// older page's lines are in neither.
func TestUnreadListsOnlyWhatTheCountCounts(t *testing.T) {
	s := openTest(t)
	put(t, s, mine(phoneChat, "m-9", base+9000, "his"))
	page(t, s, text(phoneChat, "o-2", base+2000, "two"), text(phoneChat, "o-1", base+1000, "one"))
	if u := chatOf(t, s, phoneChat).Unread; u != 0 {
		t.Fatalf("unread = %d, want 0", u)
	}
	lines, err := s.Unread(net, phoneChat, base+9000)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 0 {
		t.Errorf("Unread lists %v, want none: the count counts none", ids(linesAsMessages(lines)))
	}
	if in, ok, err := s.NewestIncoming(net, phoneChat); err != nil || ok {
		t.Errorf("NewestIncoming = %q, %v, %v; want none that can be unread", in.ID, ok, err)
	}
	put(t, s, text(phoneChat, "n-10", base+10000, "new"))
	lines, _ = s.Unread(net, phoneChat, base+10000)
	if len(lines) != 1 || lines[0].ID != "n-10" || chatOf(t, s, phoneChat).Unread != 1 {
		t.Errorf("after a new line Unread lists %d lines, want the new one", len(lines))
	}
}

func linesAsMessages(ls []Line) []Message {
	var out []Message
	for _, l := range ls {
		out = append(out, Message{ID: l.ID})
	}
	return out
}
