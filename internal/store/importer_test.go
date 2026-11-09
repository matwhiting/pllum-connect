package store

import (
	"maps"
	"slices"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/wire"
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

	// A direct line's plain text against Beeper's text.
	for _, tc := range []struct {
		direct, beeper string
		same           bool
	}{
		{"Alex <alex@example.com>", "Alex &lt;alex@example.com&gt;", true},
		{"Alex <alex@example.com>", "<p>Alex &lt;alex@example.com&gt;</p>", true},
		{"Alex <alex@example.com>", "Alex <alex@example.com>", true},
		{"Alex <alex@example.com>", "Alex", false},
		{"Tom & Jerry", "Tom &amp; Jerry", true},
		{"see <b>this</b>", "<p>see &lt;b&gt;this&lt;/b&gt;</p>", true},
		{"", "", true},
	} {
		if got := sameContent(tc.direct, "text", true, tc.beeper, "text", false); got != tc.same {
			t.Errorf("sameContent(%q, %q) = %v, want %v", tc.direct, tc.beeper, got, tc.same)
		}
		if got := sameContent(tc.beeper, "text", false, tc.direct, "text", true); got != tc.same {
			t.Errorf("sameContent the other way (%q, %q) = %v", tc.beeper, tc.direct, got)
		}
	}
	if sameContent("", "photo", true, "", "video", false) {
		t.Errorf("a photo matched a video")
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

func TestAnAddressInAnglesStillMatches(t *testing.T) {
	s := openTest(t)
	// WhatsApp's text is plain; Beeper's is HTML with the brackets escaped.
	put(t, s, mine(phoneChat, "3EB0E0001", base, "Write to Alex <alex@example.com>"))
	in := beeperMsg("bm-addr", base+700, "Write to Alex &lt;alex@example.com&gt;")
	in.FromMe = true
	other := beeperMsg("bm-other", base+800, "Write to Sam")
	other.FromMe = true
	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in, other}})
	if r != (ImportResult{Matched: 1, Added: 1}) {
		t.Fatalf("import = %+v, want the address line matched", r)
	}

	// And the other way round: the import first, the direct line after.
	later := beeperMsg("bm-addr2", base+60_000, "Ask &lt;sam@example.com&gt; too")
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{later}})
	direct := text(phoneChat, "3EB0E0002", base+60_400, "Ask <sam@example.com> too")
	direct.SenderID = "beeper-sender"
	if put(t, s, direct) {
		t.Fatalf("the direct address line was added beside its import")
	}
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0E0001", "beeper:bm-other", "3EB0E0002"}) {
		t.Fatalf("the chat holds %v", got)
	}
}

func TestATakeOverTakesTheDirectSide(t *testing.T) {
	s := openTest(t)
	// Beeper got the side wrong; the network ID still ties the lines.
	in := beeperMsg("bm-side", base, "on my way")
	in.NetworkID = "3EB0G0001"
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})
	if put(t, s, mine(phoneChat, "3EB0G0001", base+100, "on my way")) {
		t.Fatalf("added beside its import")
	}
	got := all(t, s, phoneChat)
	if len(got) != 1 || !got[0].FromMe {
		t.Fatalf("the line is %+v, want his", got)
	}
	if c := chatOf(t, s, phoneChat); !c.LastFromMe || c.Unread != 0 {
		t.Fatalf("the chat is %+v", c)
	}
}

func TestTwoBeeperIDsForOneLineBothMap(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0J0001", base, "held directly"))
	// Beeper split one person into two chats; both place into one.
	for _, b := range []struct{ chat, msg string }{{"bc-lid", "bm-1"}, {"bc-phone", "bm-2"}} {
		m := beeperMsg(b.msg, base+60_000, "held directly")
		m.NetworkID = "3EB0J0001"
		if r := importOne(t, s, ImportBatch{BeeperChatID: b.chat, ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
			Messages: []ImportMessage{m}}); r != (ImportResult{Matched: 1}) {
			t.Fatalf("import from %s = %+v", b.chat, r)
		}
	}
	// A line only Beeper has, copied from both chats, shows once.
	importOne(t, s, ImportBatch{BeeperChatID: "bc-lid", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-3", base+90_000, "only in beeper")}})
	twice := ImportBatch{BeeperChatID: "bc-phone", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-4", base+90_500, "only in beeper")}}
	if r := importOne(t, s, twice); r != (ImportResult{Matched: 1}) {
		t.Fatalf("the second copy = %+v, want matched", r)
	}
	if r := importOne(t, s, twice); r != (ImportResult{Skipped: 1}) {
		t.Fatalf("the second copy again = %+v, want skipped", r)
	}
	// Within one Beeper chat, two alike lines are two lines.
	r := importOne(t, s, ImportBatch{BeeperChatID: "bc-lid", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-5", base+120_000, "ok"), beeperMsg("bm-6", base+120_500, "ok")}})
	if r != (ImportResult{Added: 2}) {
		t.Fatalf("two alike lines = %+v", r)
	}

	lines := lineIDs(t, s, []string{"bm-1", "bm-2", "bm-3", "bm-4"})
	want := map[string]string{"bm-1": "3EB0J0001", "bm-2": "3EB0J0001", "bm-3": "beeper:bm-3", "bm-4": "beeper:bm-3"}
	if !maps.Equal(lines, want) {
		t.Fatalf("LineIDs = %v, want %v", lines, want)
	}
	if chats := beeperChats(t, s); chats["bc-lid"] != phoneChat || chats["bc-phone"] != phoneChat {
		t.Fatalf("BeeperChats = %v", chats)
	}
	if got := len(all(t, s, phoneChat)); got != 4 {
		t.Fatalf("the chat holds %d lines, want 4", got)
	}
}

func TestAnImportWithAnUnknownKindIsRefused(t *testing.T) {
	s := openTest(t)
	bad := beeperMsg("bm-bad", base, "x")
	bad.Kind = "hologram"
	_, err := s.Import(net, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-good", base, "fine"), bad}})
	if wire.KindOf(err) != wire.BadRequest {
		t.Fatalf("an unknown kind gave %v, want badRequest", err)
	}
	if _, ok, _ := s.Chat(net, phoneChat); ok {
		t.Fatalf("a refused batch wrote a chat")
	}
	for _, k := range []string{"", "text", "photo", "video", "voiceNote", "sticker", "file", "notice"} {
		m := beeperMsg("bm-"+k, base, "fine")
		m.Kind = k
		if _, err := s.Import(net, ImportBatch{BeeperChatID: "bc-2", Messages: []ImportMessage{m}}); err != nil {
			t.Errorf("kind %q was refused: %v", k, err)
		}
	}
}

func TestAReceiptForABeeperOnlyLineOnlyMarksItSeen(t *testing.T) {
	s := openTest(t)
	in := beeperMsg("bm-r", base, "sent from my phone")
	in.FromMe = true
	in.NetworkID = "3EB0R0001"
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}})

	// A read receipt names the line by its WhatsApp ID and nothing else.
	added, err := s.PutMessage(net, Message{ID: "3EB0R0001", ChatID: phoneChat, TimeMs: base + 50_000, SeenMs: base + 60_000})
	if err != nil || added {
		t.Fatalf("the receipt: added %v, %v", added, err)
	}
	got := all(t, s, phoneChat)
	if len(got) != 1 || !got[0].FromMe || got[0].TimeMs != base || got[0].Text != "sent from my phone" ||
		got[0].SeenMs != base+60_000 || *got[0].Hidden {
		t.Fatalf("after the receipt the line is %+v", got[0])
	}
	if c := chatOf(t, s, phoneChat); !c.LastFromMe || c.Unread != 0 {
		t.Fatalf("after the receipt the chat is %+v", c)
	}

	// A receipt for a line the store has never seen adds nothing.
	added, err = s.PutMessage(net, Message{ID: "3EB0R0099", ChatID: phoneChat, SeenMs: base + 60_000})
	if err != nil || added {
		t.Fatalf("a receipt for an unknown line: added %v, %v", added, err)
	}
	if got := len(all(t, s, phoneChat)); got != 1 {
		t.Fatalf("a receipt made a line: %d lines", got)
	}
	if c := chatOf(t, s, phoneChat); c.HistoryFromMs != base {
		t.Fatalf("historyFromMs moved to %d", c.HistoryFromMs)
	}

	// The full line, when it comes, still takes the Beeper line over.
	if put(t, s, mine(phoneChat, "3EB0R0001", base+300, "sent from my phone")) {
		t.Fatalf("the direct line was added beside its import")
	}
	if got := all(t, s, phoneChat); len(got) != 1 || got[0].ID != "3EB0R0001" || got[0].SeenMs != base+60_000 {
		t.Fatalf("after the direct line: %+v", got)
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

func TestAStoreFromBeforeBeeperIDsGetsThem(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, text(phoneChat, "3EB0O0001", base, "matched"))
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-1", base+100, "matched"), beeperMsg("bm-2", base+9000, "copied")}})
	// As a store made at schema 1 is once reopened: beeper_ids made, empty.
	if _, err := s.db.Exec(`DELETE FROM beeper_ids`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE meta SET value = '1' WHERE key = 'schema'`); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer s.Close()
	want := map[string]string{"bm-1": "3EB0O0001", "bm-2": "beeper:bm-2"}
	if got := lineIDs(t, s, []string{"bm-1", "bm-2"}); !maps.Equal(got, want) {
		t.Fatalf("LineIDs after the upgrade = %v, want %v", got, want)
	}
}

func TestAnAliasedBeeperLineKeepsEveryBeeperChatApart(t *testing.T) {
	s := openTest(t)
	// The phone chat: a direct "ok" holding a message of Beeper chat bc-b.
	put(t, s, text(phoneChat, "3EB0X0001", base, "ok"))
	importOne(t, s, ImportBatch{BeeperChatID: "bc-b", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-9", base+100, "ok")}})
	// The LID chat: one Beeper line carrying a message of bc-a and another of bc-b.
	importOne(t, s, ImportBatch{BeeperChatID: "bc-a", ChatID: lidChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-1", base+500, "ok")}})
	importOne(t, s, ImportBatch{BeeperChatID: "bc-b", ChatID: lidChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-2", base+600, "ok")}})

	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	// bm-2 and bm-9 are two messages of bc-b, so they stay two lines.
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0X0001", "beeper:bm-1"}) {
		t.Fatalf("the merged chat holds %v", got)
	}
	want := map[string]string{"bm-9": "3EB0X0001", "bm-1": "beeper:bm-1", "bm-2": "beeper:bm-1"}
	if got := lineIDs(t, s, []string{"bm-1", "bm-2", "bm-9"}); !maps.Equal(got, want) {
		t.Fatalf("LineIDs = %v, want %v", got, want)
	}
}

func TestAnEmptyOrDeletedLineIsNeverMatchedByContent(t *testing.T) {
	s := openTest(t)
	gone := beeperMsg("bm-gone", base, "")
	gone.Deleted = true
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{gone}})
	revoked := text(phoneChat, "3EB0Y0001", base+500, "")
	revoked.Deleted = true
	revoked.SenderID = "beeper-sender"
	if !put(t, s, revoked) {
		t.Fatalf("a deleted direct line took over a deleted Beeper line")
	}
	gone2 := beeperMsg("bm-gone2", base+600, "")
	gone2.Deleted = true
	if r := importOne(t, s, ImportBatch{BeeperChatID: "bc-2", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{gone2}}); r != (ImportResult{Added: 1}) {
		t.Fatalf("a deleted import = %+v, want added", r)
	}

	// A photo with no caption still matches a photo with no caption.
	photo := text(phoneChat, "3EB0Y0002", base+60_000, "")
	photo.Kind, photo.Media = "photo", []Media{{Kind: "photo", HasFile: true}}
	put(t, s, photo)
	in := beeperMsg("bm-photo", base+60_400, "")
	in.Kind, in.Media = "photo", []ImportMedia{{Kind: "photo"}}
	if r := importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{in}}); r != (ImportResult{Matched: 1}) {
		t.Fatalf("the captionless photo = %+v, want matched", r)
	}
}
