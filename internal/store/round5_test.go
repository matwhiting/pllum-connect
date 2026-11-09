package store

import (
	"slices"
	"testing"
)

// Every ID, number and text here is invented.

// fillEarlyRow makes id an early row in chat through update, then lets
// Beeper's copy (from Beeper chat bc, naming id, his, at base+300) fill it.
func fillEarlyRow(t *testing.T, s *Store, chat, bc, id string, update Message, beeperText string) {
	t.Helper()
	update.ID, update.ChatID = id, chat
	if _, err := s.PutMessage(net, update); err != nil {
		t.Fatal(err)
	}
	in := beeperMsg("bm-"+id, base+300, beeperText)
	in.NetworkID, in.FromMe = id, true
	if r := importOne(t, s, ImportBatch{BeeperChatID: bc, ChatID: chat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}}); r != (ImportResult{Matched: 1}) {
		t.Fatalf("filling %s: %+v", id, r)
	}
}

func TestAFilledRowSurvivesAnAlias(t *testing.T) {
	edit := Message{Text: "fixed", Edited: true}
	revoke := Message{Deleted: true}

	t.Run("a filled edit onto its original", func(t *testing.T) {
		s := openTest(t)
		fillEarlyRow(t, s, lidChat, "bc-l", "3EB0S0001", edit, "typo")
		put(t, s, mine(phoneChat, "3EB0S0001", base, "typo"))
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		got := all(t, s, phoneChat)
		if len(got) != 1 || got[0].ID != "3EB0S0001" || got[0].Text != "fixed" || !got[0].Edited ||
			got[0].TimeMs != base || !got[0].FromMe {
			t.Fatalf("after the alias: %+v", got)
		}
		if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0S0001" {
			t.Fatalf("lastMessageID = %q", c.LastMessageID)
		}
	})

	t.Run("a filled placeholder onto its original", func(t *testing.T) {
		s := openTest(t)
		fillEarlyRow(t, s, lidChat, "bc-l", "3EB0S0006", Message{TimeMs: base, Hidden: ptr(true)}, "readable")
		put(t, s, text(phoneChat, "3EB0S0006", base, "readable"))
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		// The original's time and side stand: Beeper's copy never moves them.
		got := all(t, s, phoneChat)
		if len(got) != 1 || got[0].TimeMs != base || got[0].FromMe || got[0].Text != "readable" || *got[0].Hidden {
			t.Fatalf("after the alias: %+v", got)
		}
		if c := chatOf(t, s, phoneChat); c.Unread != 1 {
			t.Fatalf("the chat is %+v", c)
		}
	})

	t.Run("a filled revoke onto its original", func(t *testing.T) {
		s := openTest(t)
		fillEarlyRow(t, s, lidChat, "bc-l", "3EB0S0002", revoke, "regret this")
		put(t, s, mine(phoneChat, "3EB0S0002", base, "regret this"))
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		got := all(t, s, phoneChat)
		if len(got) != 1 || !got[0].Deleted || got[0].Text != "" || got[0].TimeMs != base {
			t.Fatalf("after the alias the revoked line is %+v", got)
		}
		if p, err := s.Search(net, Query{Text: "regret", Limit: 10}); err != nil || len(p.Items) != 0 {
			t.Fatalf("the revoked text came back: %v, %v", ids(p.Items), err)
		}
	})

	t.Run("a filled edit onto an early placeholder", func(t *testing.T) {
		s := openTest(t)
		fillEarlyRow(t, s, lidChat, "bc-l", "3EB0S0003", edit, "typo")
		put(t, s, text(phoneChat, "3EB0S0000", base-5000, "before"))
		if _, err := s.PutMessage(net, Message{ID: "3EB0S0003", ChatID: phoneChat, TimeMs: base,
			Hidden: ptr(true), SeenMs: base + 5}); err != nil {
			t.Fatal(err)
		}
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		got := all(t, s, phoneChat)
		if !slices.Equal(ids(got), []string{"3EB0S0000", "3EB0S0003"}) {
			t.Fatalf("after the alias: %v", ids(got))
		}
		if m := got[1]; m.Text != "fixed" || !m.Edited || *m.Hidden || m.TimeMs != base+300 || !m.FromMe || m.SeenMs != base+5 {
			t.Fatalf("the line is %+v", m)
		}
		if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0S0003" || c.Unread != 1 {
			t.Fatalf("the chat is %+v", c)
		}
	})

	t.Run("an early revoke onto a filled edit", func(t *testing.T) {
		s := openTest(t)
		fillEarlyRow(t, s, phoneChat, "bc-p", "3EB0S0004", edit, "typo")
		if _, err := s.PutMessage(net, Message{ID: "3EB0S0004", ChatID: lidChat, Deleted: true}); err != nil {
			t.Fatal(err)
		}
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		got := all(t, s, phoneChat)
		if len(got) != 1 || !got[0].Deleted || got[0].Text != "" || got[0].TimeMs != base+300 {
			t.Fatalf("after the alias: %+v", got)
		}
	})

	t.Run("a filled edit onto a filled revoke", func(t *testing.T) {
		s := openTest(t)
		fillEarlyRow(t, s, lidChat, "bc-l", "3EB0S0005", edit, "typo")
		fillEarlyRow(t, s, phoneChat, "bc-p", "3EB0S0005", revoke, "typo")
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		got := all(t, s, phoneChat)
		if len(got) != 1 || !got[0].Deleted || got[0].Text != "" || !got[0].Edited {
			t.Fatalf("after the alias: %+v", got)
		}
		want := map[string]string{"bm-3EB0S0005": "3EB0S0005"}
		if got := lineIDs(t, s, []string{"bm-3EB0S0005"}); got["bm-3EB0S0005"] != want["bm-3EB0S0005"] {
			t.Fatalf("LineIDs = %v", got)
		}
	})
}

func TestAnAliasedOriginalFoldsInItsBeeperLine(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0T0000", base-5000, "before"))
	if _, err := s.PutMessage(net, Message{ID: "3EB0T0001", ChatID: phoneChat, Text: "hello2", Edited: true}); err != nil {
		t.Fatal(err)
	}
	in := beeperMsg("bm-t1", base+300, "hello2") // Beeper names no WhatsApp ID
	in.FromMe = true
	importOne(t, s, ImportBatch{BeeperChatID: "bc-p", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})
	put(t, s, mine(lidChat, "3EB0T0001", base, "hello"))
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	got := all(t, s, phoneChat)
	if !slices.Equal(ids(got), []string{"3EB0T0000", "3EB0T0001"}) || got[1].Text != "hello2" || got[1].TimeMs != base {
		t.Fatalf("after the alias: %+v", got)
	}
	if got := lineIDs(t, s, []string{"bm-t1"}); got["bm-t1"] != "3EB0T0001" {
		t.Fatalf("LineIDs = %v", got)
	}
}

func TestAFilledRowIsNeverAnotherLinesCopy(t *testing.T) {
	s := openTest(t)
	fillEarlyRow(t, s, phoneChat, "bc-p", "3EB0W0001", Message{TimeMs: base, Hidden: ptr(true)}, "ok")
	// Another of his lines, with the same text, a second later: a takeover by content.
	put(t, s, mine(phoneChat, "3EB0W0002", base+1000, "ok"))
	// A third, which an edit makes the same text: an absorb by content.
	put(t, s, mine(phoneChat, "3EB0W0003", base+1500, "ok?"))
	if _, err := s.PutMessage(net, Message{ID: "3EB0W0003", ChatID: phoneChat, Text: "ok", Edited: true}); err != nil {
		t.Fatal(err)
	}
	want := []string{"3EB0W0001", "3EB0W0002", "3EB0W0003"}
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	put(t, s, mine(phoneChat, "3EB0W0001", base, "ok"))
	if got := all(t, s, phoneChat); !slices.Equal(ids(got), want) || got[0].TimeMs != base {
		t.Fatalf("after the first line's original: %+v", got)
	}
	if got := lineIDs(t, s, []string{"bm-3EB0W0001"}); got["bm-3EB0W0001"] != "3EB0W0001" {
		t.Fatalf("LineIDs = %v", got)
	}
}

func TestABeeperCopyWithNoTimeLeavesAnEarlyRowEarly(t *testing.T) {
	s := openTest(t)
	if _, err := s.PutMessage(net, Message{ID: "3EB0N0001", ChatID: phoneChat, Text: "fixed", Edited: true}); err != nil {
		t.Fatal(err)
	}
	in := beeperMsg("bm-n1", 0, "typo")
	in.NetworkID = "3EB0N0001"
	importOne(t, s, ImportBatch{BeeperChatID: "bc-p", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})
	if got := all(t, s, phoneChat); len(got) != 0 {
		t.Fatalf("a copy with no time showed the line: %+v", got)
	}
	put(t, s, text(phoneChat, "3EB0N0001", base, "typo"))
	if got := all(t, s, phoneChat); len(got) != 1 || got[0].TimeMs != base || got[0].Text != "fixed" {
		t.Fatalf("after the original: %+v", got)
	}
	if got := lineIDs(t, s, []string{"bm-n1"}); got["bm-n1"] != "3EB0N0001" {
		t.Fatalf("LineIDs = %v", got)
	}
}

// Ruling 7: a line first seen already edited or revoked is written as the
// whole line, then the edit or revoke.
func TestALineFirstSeenEditedOrRevoked(t *testing.T) {
	s := openTest(t)
	for _, step := range []Message{
		mine(phoneChat, "3EB0R0001", base, "first words"),
		{ID: "3EB0R0001", ChatID: phoneChat, Text: "final words", Edited: true},
		mine(phoneChat, "3EB0R0002", base+1000, "gone soon"),
		{ID: "3EB0R0002", ChatID: phoneChat, Deleted: true},
	} {
		step.Source = SourceSync
		if _, err := s.PutMessage(net, step); err != nil {
			t.Fatal(err)
		}
	}
	got := all(t, s, phoneChat)
	if len(got) != 2 || got[0].Text != "final words" || !got[0].Edited || got[0].TimeMs != base ||
		!got[1].Deleted || got[1].Text != "" || got[1].TimeMs != base+1000 {
		t.Fatalf("listed %+v", got)
	}
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0R0001" {
		t.Fatalf("lastMessageID = %q", c.LastMessageID)
	}
}

func TestTheUpgradeRefreshesChatsAndUnmarksFilledRows(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	// A filled row as 5c9555b kept it: source beeper under its direct ID.
	put(t, s, text(otherChat, "3EB0G0000", base-5000, "before"))
	fillEarlyRow(t, s, otherChat, "bc-o", "3EB0G0001", Message{Text: "fixed", Edited: true}, "typo")
	for _, q := range []string{
		`ALTER TABLE messages DROP COLUMN copied`,
		`UPDATE messages SET source = 'beeper' WHERE msg_id = '3EB0G0001'`,
		// A schema-2 chat whose only row is an edit kept at time 0, shown then.
		`ALTER TABLE messages DROP COLUMN early`,
		`INSERT INTO messages (network, chat_id, msg_id, ts_ms, text, edited, source) VALUES ('whatsapp', '` +
			phoneChat + `', '3EB0G0002', 0, 'fixed', 1, 'live')`,
		`INSERT INTO chats (network, chat_id, last_message_id) VALUES ('whatsapp', '` + phoneChat + `', '3EB0G0002')`,
		`UPDATE meta SET value = '2' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer s.Close()
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "" {
		t.Fatalf("the upgrade left lastMessageID %q on an early row", c.LastMessageID)
	}
	if c := chatOf(t, s, otherChat); c.LastMessageID != "3EB0G0001" || c.Unread != 1 {
		t.Fatalf("the filled row's chat is %+v", c)
	}
	if n, err := s.Counts(net); err != nil || n.Matched != 1 {
		t.Fatalf("Counts = %+v, %v", n, err)
	}
	// It is no longer a Beeper line: another of his lines is never taken for it.
	put(t, s, mine(otherChat, "3EB0G0003", base+1000, "fixed"))
	if got := ids(all(t, s, otherChat)); !slices.Equal(got, []string{"3EB0G0000", "3EB0G0001", "3EB0G0003"}) {
		t.Fatalf("listed %v", got)
	}
}
