package store

import (
	"maps"
	"slices"
	"testing"
)

// An exact-time twin: Beeper's copy and WhatsApp's own line of one message
// whose texts differ (a mention is a name in one, a number in the other),
// told apart only by time and by being the only pair there.

// groupBatch is one Beeper batch for the group chat.
func groupBatch(ms ...ImportMessage) ImportBatch {
	return ImportBatch{BeeperChatID: "bc-twin", ChatID: groupChat, Chat: ImportChat{Kind: "group"}, Messages: ms}
}

// groupLine is a direct line in the group chat from someone else.
func groupLine(id string, ms int64, body string) Message {
	m := text(groupChat, id, ms, body)
	m.SenderID = "10000000009@s.whatsapp.net"
	return m
}

func TestAnExactTimeTwinImportIsMatched(t *testing.T) {
	s := openTest(t)
	put(t, s, groupLine("3EB0TW0001", base, "@6421000099 hi"))
	r := importOne(t, s, groupBatch(beeperMsg("bm-tw1", base, "Alex hi")))
	if r != (ImportResult{Matched: 1}) {
		t.Fatalf("import = %+v, want matched 1, added 0", r)
	}
	got := all(t, s, groupChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0TW0001"}) {
		t.Fatalf("the chat holds %v, want the one direct line", g)
	}
	if got[0].BeeperID != "bm-tw1" {
		t.Fatalf("the line carries Beeper ID %q", got[0].BeeperID)
	}
	if l := lineIDs(t, s, []string{"bm-tw1"}); !maps.Equal(l, map[string]string{"bm-tw1": "3EB0TW0001"}) {
		t.Fatalf("LineIDs = %v", l)
	}
}

func TestATwinOneMsApartIsNotMatched(t *testing.T) {
	s := openTest(t)
	put(t, s, groupLine("3EB0TW0002", base, "@6421000099 hi"))
	if r := importOne(t, s, groupBatch(beeperMsg("bm-tw2", base+1, "Alex hi"))); r != (ImportResult{Added: 1}) {
		t.Fatalf("import = %+v, want added 1 (exact time only)", r)
	}
}

func TestTwoDirectLinesAtOneTimeHaveNoTwin(t *testing.T) {
	s := openTest(t)
	put(t, s, groupLine("3EB0TW0003", base, "@6421000099 hi"))
	put(t, s, groupLine("3EB0TW0004", base, "@6421000099 bye"))
	if r := importOne(t, s, groupBatch(beeperMsg("bm-tw3", base, "Alex hi"))); r != (ImportResult{Added: 1}) {
		t.Fatalf("import = %+v, want added 1 (not one-to-one)", r)
	}
}

func TestTwoBeeperCopiesAtOneTimeHaveNoTwin(t *testing.T) {
	// Two copies in one batch: neither is the only one there.
	s := openTest(t)
	put(t, s, groupLine("3EB0TW0005", base, "@6421000099 hi"))
	if r := importOne(t, s, groupBatch(beeperMsg("bm-tw4", base, "Alex hi"), beeperMsg("bm-tw5", base, "Alex bye"))); r != (ImportResult{Added: 2}) {
		t.Fatalf("import = %+v, want added 2 (not one-to-one)", r)
	}

	// Two Beeper-only lines held, then one direct line: no fold.
	s = openTest(t)
	importOne(t, s, groupBatch(beeperMsg("bm-tw6", base, "Alex hi")))
	importOne(t, s, groupBatch(beeperMsg("bm-tw7", base, "Alex bye")))
	if !put(t, s, groupLine("3EB0TW0006", base, "@6421000099 hi")) {
		t.Fatalf("the direct line was folded into one of two Beeper lines")
	}
	if n := len(all(t, s, groupChat)); n != 3 {
		t.Fatalf("the chat holds %d lines, want 3", n)
	}
}

func TestADirectTwinArrivingSecondTakesItsBeeperLine(t *testing.T) {
	s := openTest(t)
	reply := beeperMsg("bm-tw9", base+60_000, "Yes")
	reply.ReplyToBeeperID = "bm-tw8"
	importOne(t, s, groupBatch(beeperMsg("bm-tw8", base, "Alex hi"), reply))
	if put(t, s, groupLine("3EB0TW0008", base, "@6421000099 hi")) {
		t.Fatalf("the direct line was added beside its Beeper twin")
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
	if l := lineIDs(t, s, []string{"bm-tw8"}); l["bm-tw8"] != "3EB0TW0008" {
		t.Fatalf("LineIDs = %v", l)
	}
}

func TestAnEditedTwinLandingInItsEarlyRowFoldsItsBeeperLine(t *testing.T) {
	// The edit comes first (an early row), then Beeper's copy, then the
	// line itself: absorbBeeperCopy folds the Beeper line in.
	s := openTest(t)
	put(t, s, Message{ID: "3EB0TW0010", ChatID: groupChat, Text: "@6421000099 hi!", Edited: true})
	reply := beeperMsg("bm-tw11", base+60_000, "Yes")
	reply.ReplyToBeeperID = "bm-tw10"
	importOne(t, s, groupBatch(beeperMsg("bm-tw10", base, "Alex hi"), reply))
	put(t, s, groupLine("3EB0TW0010", base, "@6421000099 hi"))
	got := all(t, s, groupChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0TW0010", "beeper:bm-tw11"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[1].ReplyTo != "3EB0TW0010" {
		t.Fatalf("the reply points at %q, want the direct line", got[1].ReplyTo)
	}
	if l := lineIDs(t, s, []string{"bm-tw10"}); l["bm-tw10"] != "3EB0TW0010" {
		t.Fatalf("LineIDs = %v", l)
	}
}

func TestATwinMustShareTextOrMediaKind(t *testing.T) {
	// A photo with no text against a sticker: different kinds, no text.
	s := openTest(t)
	photo := groupLine("3EB0TW0012", base, "")
	photo.Kind, photo.Media = "photo", []Media{{Kind: "photo"}}
	put(t, s, photo)
	sticker := beeperMsg("bm-tw12", base, "")
	sticker.Kind = "sticker"
	if r := importOne(t, s, groupBatch(sticker)); r != (ImportResult{Added: 1}) {
		t.Fatalf("a sticker against a photo = %+v, want added 1", r)
	}

	// A photo with a caption against text: the caption is the text.
	s = openTest(t)
	captioned := groupLine("3EB0TW0013", base, "@6421000099 look")
	captioned.Kind, captioned.Media = "photo", []Media{{Kind: "photo"}}
	put(t, s, captioned)
	if r := importOne(t, s, groupBatch(beeperMsg("bm-tw13", base, "Alex look"))); r != (ImportResult{Matched: 1}) {
		t.Fatalf("text against a captioned photo = %+v, want matched 1", r)
	}
}

func TestTheUpgradeFoldsStoredTwins(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	// Beeper-only lines first, while nothing is beside them.
	reply := beeperMsg("bm-up4", base+60_000, "Yes")
	reply.ReplyToBeeperID = "bm-up1"
	importOne(t, s, groupBatch(
		beeperMsg("bm-up1", base, "Alex hi"),         // an exact twin
		beeperMsg("bm-up2", base+10_001, "Alex two"), // 1 ms from its direct line
		beeperMsg("bm-up3", base+20_000, "Alex three"),
		reply,
	))
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
}
