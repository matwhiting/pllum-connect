package store

import (
	"maps"
	"slices"
	"testing"
)

func TestAnImportMatchedByNetworkIDAddsNothing(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0I0001", base, "the direct copy"))

	// Different text and a time far off: only the ID ties them.
	m := beeperMsg("bm-1", base+60_000, "beeper's own rendering")
	m.NetworkID = "3EB0I0001"
	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{m}})
	if r != (ImportResult{Matched: 1}) {
		t.Fatalf("result = %+v, want one matched", r)
	}
	got := all(t, s, phoneChat)
	if len(got) != 1 || got[0].ID != "3EB0I0001" || got[0].Text != "the direct copy" {
		t.Fatalf("the chat holds %+v", got)
	}
	if lines := lineIDs(t, s, []string{"bm-1"}); lines["bm-1"] != "3EB0I0001" {
		t.Fatalf("LineIDs = %v", lines)
	}
}

func TestAnImportMatchedBySenderTimeAndTextAddsNothing(t *testing.T) {
	s := openTest(t)
	put(t, s, mine(phoneChat, "3EB0T0001", base, "See you at 5 & bring snacks"))
	put(t, s, mine(phoneChat, "3EB0T0002", base+600_000, "Running late"))
	put(t, s, text(phoneChat, "3EB0T0003", base+900_000, "ok"))

	near := beeperMsg("bm-near", base+1500, "<p>See you at 5 &amp; bring\u200b  <b>snacks</b></p>")
	near.FromMe = true
	far := beeperMsg("bm-far", base+600_000+2500, "Running late")
	far.FromMe = true
	// Same time and text, but the other side of the chat: not the same line.
	otherSide := beeperMsg("bm-side", base+900_000, "ok")
	otherSide.FromMe = true

	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{near, far, otherSide}})
	if r != (ImportResult{Added: 2, Matched: 1}) {
		t.Fatalf("result = %+v, want 1 matched and 2 added", r)
	}
	got := ids(all(t, s, phoneChat))
	want := []string{"3EB0T0001", "3EB0T0002", "beeper:bm-far", "3EB0T0003", "beeper:bm-side"}
	if !slices.Equal(got, want) {
		t.Fatalf("the chat holds %v\nwant            %v", got, want)
	}
}

func TestMatchKey(t *testing.T) {
	for _, tc := range []struct{ a, b string }{
		{"<p>Caf\u0065\u0301 &amp; more</p>", "café & more"},
		{"line one<br>line two", "Line one\nline two"},
		{"a\u200bb\u200dc\ufeff", "abc"},
		{"  spaced \t out  ", "spaced out"},
		{"a < b > c", "a &lt; b &gt; c"},
	} {
		if MatchKey(tc.a) != MatchKey(tc.b) {
			t.Errorf("MatchKey(%q) = %q, MatchKey(%q) = %q", tc.a, MatchKey(tc.a), tc.b, MatchKey(tc.b))
		}
	}
	if MatchKey("hello") == MatchKey("hello there") {
		t.Errorf("different texts share a key")
	}
}

func TestADirectMessageArrivingAfterItsImportTakesItsPlace(t *testing.T) {
	s := openTest(t)
	byText := beeperMsg("bm-text", base, "<i>Dinner at 7?</i>")
	byID := beeperMsg("bm-id", base+300_000, "beeper says")
	byID.NetworkID = "3EB0D0002"
	reply := beeperMsg("bm-reply", base+400_000, "Sounds good")
	reply.ReplyToBeeperID = "bm-text"
	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{byText, byID, reply}})
	if r != (ImportResult{Added: 3}) {
		t.Fatalf("import = %+v, want 3 added", r)
	}
	if err := s.SetReaction(net, phoneChat, "beeper:bm-text", Reaction{Emoji: "👍", SenderID: otherChat}); err != nil {
		t.Fatal(err)
	}

	// WhatsApp's own sync brings the same lines under their own IDs.
	direct := text(phoneChat, "3EB0D0001", base+800, "Dinner at 7?")
	direct.SenderID = "beeper-sender"
	direct.Source = SourceSync
	if put(t, s, direct) {
		t.Fatalf("the direct line was added beside its import")
	}
	other := text(phoneChat, "3EB0D0002", base+300_500, "whatsapp says")
	other.Source = SourceSync
	if put(t, s, other) {
		t.Fatalf("the line named by its network ID was added beside its import")
	}

	got := all(t, s, phoneChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0D0001", "3EB0D0002", "beeper:bm-reply"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[0].Text != "Dinner at 7?" || len(got[0].Reactions) != 1 {
		t.Errorf("the taken-over line is %+v", got[0])
	}
	if got[2].ReplyTo != "3EB0D0001" {
		t.Errorf("the reply points at %q, want the direct ID", got[2].ReplyTo)
	}
	lines := lineIDs(t, s, []string{"bm-text", "bm-id", "bm-reply"})
	want := map[string]string{"bm-text": "3EB0D0001", "bm-id": "3EB0D0002", "bm-reply": "beeper:bm-reply"}
	if !maps.Equal(lines, want) {
		t.Fatalf("LineIDs = %v, want %v", lines, want)
	}
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "beeper:bm-reply" {
		t.Errorf("lastMessageID = %q", c.LastMessageID)
	}
}

func TestAnUnmatchedImportIsABeeperLine(t *testing.T) {
	s := openTest(t)
	photo := beeperMsg("bm-photo", base+1000, "")
	photo.Kind, photo.Media = "photo", []ImportMedia{{Kind: "photo", Width: 800, Height: 600}}
	photo.Reactions = []Reaction{{Emoji: "❤️", SenderID: "beeper-sender"}}
	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single", Title: "Alex Example"},
		Messages: []ImportMessage{beeperMsg("bm-1", base, "only in beeper"), photo}})
	if r != (ImportResult{Added: 2}) {
		t.Fatalf("result = %+v", r)
	}
	got := all(t, s, phoneChat)
	if g := ids(got); !slices.Equal(g, []string{"beeper:bm-1", "beeper:bm-photo"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[1].Kind != "photo" || len(got[1].Media) != 1 || got[1].Media[0].HasFile || got[1].Media[0].Width != 800 {
		t.Errorf("the photo line is %+v", got[1])
	}
	if len(got[1].Reactions) != 1 || got[1].Reactions[0].Emoji != "❤️" {
		t.Errorf("the photo's reactions are %+v", got[1].Reactions)
	}
	c := chatOf(t, s, phoneChat)
	if c.LastMessageID != "beeper:bm-photo" || c.Title != "Alex Example" || c.ReadOnly {
		t.Errorf("the chat is %+v", c)
	}
	if c.Unread != 0 {
		t.Errorf("copied history counts %d unread", c.Unread)
	}
}

func TestImportingTwiceAddsNothing(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0W0001", base, "already here"))
	batch := ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{
			beeperMsg("bm-1", base+500, "already here"),
			beeperMsg("bm-2", base+10_000, "new one"),
			beeperMsg("bm-3", base+20_000, "new two"),
		}}
	if r := importOne(t, s, batch); r != (ImportResult{Added: 2, Matched: 1}) {
		t.Fatalf("first import = %+v", r)
	}
	if r := importOne(t, s, batch); r != (ImportResult{Skipped: 3}) {
		t.Fatalf("second import = %+v, want 3 skipped", r)
	}
	unplaced := ImportBatch{BeeperChatID: "bc-2", Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-9", base, "somewhere")}}
	importOne(t, s, unplaced)
	if r := importOne(t, s, unplaced); r != (ImportResult{Skipped: 1}) {
		t.Fatalf("second unplaced import = %+v", r)
	}
	if got := len(all(t, s, phoneChat)); got != 3 {
		t.Fatalf("the chat holds %d lines, want 3", got)
	}
}

func TestAnUnplacedChatIsReadOnlyAndDroppedWhenReplaced(t *testing.T) {
	s := openTest(t)
	unplacedID := "unplaced:bc-1"
	first := []ImportMessage{beeperMsg("bm-1", base, "who is this"), beeperMsg("bm-2", base+1000, "pineapple party")}
	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", Chat: ImportChat{Kind: "single", Title: "Unknown Person"},
		Messages: first})
	if r != (ImportResult{Added: 2}) {
		t.Fatalf("unplaced import = %+v", r)
	}
	c := chatOf(t, s, unplacedID)
	if !c.ReadOnly || c.LastMessageID != "beeper:bm-2" {
		t.Fatalf("the unplaced chat is %+v", c)
	}
	if p, err := s.Search(net, Query{Text: "pineapple", Limit: 10}); err != nil || len(p.Items) != 1 || p.Items[0].ChatID != unplacedID {
		t.Fatalf("search did not find the unplaced line: %+v, %v", p.Items, err)
	}
	if got := beeperChats(t, s); got["bc-1"] != unplacedID {
		t.Fatalf("BeeperChats = %v", got)
	}

	// A later run places it, one page at a time. The page holding only
	// bm-2 drops only bm-2; the unplaced chat keeps bm-1 until it is carried.
	put(t, s, text(phoneChat, "3EB0R0002", base+1200, "pineapple party"))
	r = importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: first[1:], Replaces: unplacedID})
	if r != (ImportResult{Matched: 1}) {
		t.Fatalf("placed import = %+v", r)
	}
	if got := ids(all(t, s, unplacedID)); !slices.Equal(got, []string{"beeper:bm-1"}) {
		t.Fatalf("the unplaced chat holds %v, want only the line not yet carried", got)
	}
	r = importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: first, Replaces: unplacedID})
	if r != (ImportResult{Added: 1, Skipped: 1}) {
		t.Fatalf("second placed import = %+v", r)
	}
	if _, ok, err := s.Chat(net, unplacedID); ok || err != nil {
		t.Fatalf("the unplaced chat is still there (%v)", err)
	}
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"beeper:bm-1", "3EB0R0002"}) {
		t.Fatalf("the placed chat holds %v", got)
	}
	if got := beeperChats(t, s); got["bc-1"] != phoneChat {
		t.Fatalf("BeeperChats after placing = %v", got)
	}
	if c := chatOf(t, s, phoneChat); c.ReadOnly {
		t.Fatalf("the placed chat is read-only")
	}

	// replaces never drops a placed chat.
	importOne(t, s, ImportBatch{BeeperChatID: "bc-2", ChatID: otherChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-1", base, "who is this")}, Replaces: phoneChat})
	if got := len(all(t, s, phoneChat)); got != 2 {
		t.Fatalf("a placed chat lost lines to replaces: %d left", got)
	}
}

func TestBeeperChatsAndLineIDsMapBack(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0B0001", base, "matched line"))
	importOne(t, s, ImportBatch{BeeperChatID: "bc-placed", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-matched", base+900, "matched line"), beeperMsg("bm-only", base+5000, "only beeper")}})
	importOne(t, s, ImportBatch{BeeperChatID: "bc-unplaced", Chat: ImportChat{Kind: "group", Title: "Old Group"},
		Messages: []ImportMessage{beeperMsg("bm-lost", base, "somewhere")}})

	chats := beeperChats(t, s)
	if want := map[string]string{"bc-placed": phoneChat, "bc-unplaced": "unplaced:bc-unplaced"}; !maps.Equal(chats, want) {
		t.Fatalf("BeeperChats = %v, want %v", chats, want)
	}
	lines := lineIDs(t, s, []string{"bm-matched", "bm-only", "bm-lost", "bm-never-copied"})
	want := map[string]string{"bm-matched": "3EB0B0001", "bm-only": "beeper:bm-only", "bm-lost": "beeper:bm-lost"}
	if !maps.Equal(lines, want) {
		t.Fatalf("LineIDs = %v, want %v", lines, want)
	}
}
