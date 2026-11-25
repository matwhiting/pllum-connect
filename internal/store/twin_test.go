package store

import (
	"slices"
	"testing"
)

// An exact-time twin: Beeper's copy and WhatsApp's own line of one message
// whose texts differ (a mention is a name in one, a number in the other),
// told apart only by time and by being the only pair there.

// groupCopies seeds lines of Beeper's copy into the group chat.
func groupCopies(t *testing.T, s *Store, ls ...copyLine) {
	t.Helper()
	seedCopies(t, s, groupChat, "bc-twin", ls...)
}

// groupLine is a direct line in the group chat from someone else.
func groupLine(id string, ms int64, body string) Message {
	m := text(groupChat, id, ms, body)
	m.SenderID = "10000000009@s.whatsapp.net"
	return m
}

func TestATwinOneMsApartIsNotTakenOver(t *testing.T) {
	s := openTest(t)
	groupCopies(t, s, copyLine{beeperID: "bm-tw2", ms: base + 1, text: "Alex hi"})
	if !put(t, s, groupLine("3EB0TW0002", base, "@6421000099 hi")) {
		t.Fatalf("a line 1 ms from a copy with other text took it over (exact time only)")
	}
}

func TestTwoCopiesAtOneTimeHaveNoTwin(t *testing.T) {
	// Two copies held at one time, then one direct line: no takeover.
	s := openTest(t)
	groupCopies(t, s, copyLine{beeperID: "bm-tw6", ms: base, text: "Alex hi"}, copyLine{beeperID: "bm-tw7", ms: base, text: "Alex bye"})
	if !put(t, s, groupLine("3EB0TW0006", base, "@6421000099 hi")) {
		t.Fatalf("the direct line took over one of two copies")
	}
	if n := len(all(t, s, groupChat)); n != 3 {
		t.Fatalf("the chat holds %d lines, want 3", n)
	}
}

func TestADirectTwinTakesItsCopy(t *testing.T) {
	s := openTest(t)
	groupCopies(t, s, copyLine{beeperID: "bm-tw8", ms: base, text: "Alex hi"},
		copyLine{beeperID: "bm-tw9", ms: base + 60_000, text: "Yes", replyTo: "bm-tw8"})
	if put(t, s, groupLine("3EB0TW0008", base, "@6421000099 hi")) {
		t.Fatalf("the direct line was added beside its copied twin")
	}
	got := all(t, s, groupChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0TW0008", "beeper:bm-tw9"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[0].BeeperID != "bm-tw8" {
		t.Fatalf("the line carries Beeper ID %q", got[0].BeeperID)
	}
	if got[1].ReplyTo != "3EB0TW0008" {
		t.Fatalf("the reply points at %q, want the direct line", got[1].ReplyTo)
	}
	if c := carrier(t, s, "bm-tw8"); c != "3EB0TW0008" {
		t.Fatalf("bm-tw8 is carried by %q", c)
	}
}

func TestAnEditedTwinLandingInItsEarlyRowFoldsItsCopy(t *testing.T) {
	// The edit came first (an early row), then Beeper's copy was made, then
	// the line itself lands: absorbBeeperCopy folds the copy in.
	s := openTest(t)
	put(t, s, Message{ID: "3EB0TW0010", ChatID: groupChat, Text: "@6421000099 hi!", Edited: true})
	groupCopies(t, s, copyLine{beeperID: "bm-tw10", ms: base, text: "Alex hi"},
		copyLine{beeperID: "bm-tw11", ms: base + 60_000, text: "Yes", replyTo: "bm-tw10"})
	put(t, s, groupLine("3EB0TW0010", base, "@6421000099 hi"))
	got := all(t, s, groupChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0TW0010", "beeper:bm-tw11"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[1].ReplyTo != "3EB0TW0010" {
		t.Fatalf("the reply points at %q, want the direct line", got[1].ReplyTo)
	}
	if c := carrier(t, s, "bm-tw10"); c != "3EB0TW0010" {
		t.Fatalf("bm-tw10 is carried by %q", c)
	}
}

func TestATwinMustShareTextOrMediaKind(t *testing.T) {
	// A sticker copy against a photo with no text: different kinds, no text.
	s := openTest(t)
	groupCopies(t, s, copyLine{beeperID: "bm-tw12", ms: base, kind: "sticker"})
	photo := groupLine("3EB0TW0012", base, "")
	photo.Kind, photo.Media = "photo", []Media{{Kind: "photo"}}
	if !put(t, s, photo) {
		t.Fatalf("a photo took over a sticker")
	}

	// Text against a photo with a caption: the caption is the text.
	s = openTest(t)
	groupCopies(t, s, copyLine{beeperID: "bm-tw13", ms: base, text: "Alex look"})
	captioned := groupLine("3EB0TW0013", base, "@6421000099 look")
	captioned.Kind, captioned.Media = "photo", []Media{{Kind: "photo"}}
	if put(t, s, captioned) {
		t.Fatalf("a captioned photo did not take over its copied twin")
	}
}

func TestTheUpgradeFoldsStoredTwins(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	// Copied lines first, while nothing is beside them.
	groupCopies(t, s,
		copyLine{beeperID: "bm-up1", ms: base, text: "Alex hi"},           // an exact twin
		copyLine{beeperID: "bm-up2", ms: base + 10_001, text: "Alex two"}, // 1 ms from its direct line
		copyLine{beeperID: "bm-up3", ms: base + 20_000, text: "Alex three"},
		copyLine{beeperID: "bm-up4", ms: base + 60_000, text: "Yes", replyTo: "bm-up1"},
	)
	// The direct lines as an older helper stored them, unmatched.
	for _, d := range []struct {
		id   string
		ms   int64
		body string
	}{
		{"3EB0UP0001", base, "@6421000099 hi"},
		{"3EB0UP0002", base + 10_000, "@6421000099 two"},
		{"3EB0UP0003", base + 20_000, "@6421000099 three"}, // two direct lines here:
		{"3EB0UP0004", base + 20_000, "@6421000099 four"},  // not one-to-one
	} {
		if _, err := s.db.Exec(`INSERT INTO messages (network, chat_id, msg_id, sender_id, from_me, ts_ms, text, kind, source)
				VALUES (?, ?, ?, 'x', 0, ?, ?, 'text', 'live')`, net, groupChat, d.id, d.ms, d.body); err != nil {
			t.Fatal(err)
		}
	}
	before := nearMisses(t, s)
	if before != 3 {
		t.Fatalf("before the upgrade nearMisses = %d, want 3", before)
	}
	if _, err := s.db.Exec(`UPDATE meta SET value = '8' WHERE key = 'schema'`); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening a schema-8 store: %v", err)
	}
	defer s.Close()
	got := all(t, s, groupChat)
	want := []string{"3EB0UP0001", "3EB0UP0002", "beeper:bm-up2", "beeper:bm-up3", "3EB0UP0003", "3EB0UP0004", "beeper:bm-up4"}
	if g := ids(got); !slices.Equal(g, want) {
		t.Fatalf("after the upgrade the chat holds %v, want %v", g, want)
	}
	if got[0].BeeperID != "bm-up1" {
		t.Fatalf("the folded line carries Beeper ID %q", got[0].BeeperID)
	}
	if got[6].ReplyTo != "3EB0UP0001" {
		t.Fatalf("the reply points at %q, want the direct line", got[6].ReplyTo)
	}
	if after := nearMisses(t, s); after != before-1 {
		t.Fatalf("after the upgrade nearMisses = %d, want %d", after, before-1)
	}
	if n := merged(t, s, net); n != 1 {
		t.Fatalf("the upgrade's fold counted %d merges, want 1", n)
	}
}
