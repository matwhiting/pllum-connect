package store

import (
	"database/sql"
	"fmt"
	"slices"
	"testing"
)

// Lines of Beeper's copy, as a store from before the switch holds them
// (seedCopies): they read like any other line, and a direct line the
// network delivers later (a history sync, a backfill, an older page) still
// takes its copy over, so it shows once. Every ID and text is invented.

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

func TestACopiedLineReadsAsBefore(t *testing.T) {
	s := openTest(t)
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Title: ptr("Alex Example")}); err != nil {
		t.Fatal(err)
	}
	seedCopies(t, s, phoneChat, "bc-1",
		copyLine{beeperID: "bm-1", ms: base, text: "only in beeper"},
		copyLine{beeperID: "bm-photo", ms: base + 1000, kind: "photo", media: []Media{{Kind: "photo", Width: 800, Height: 600}},
			reactions: []Reaction{{Emoji: "❤️", SenderID: "beeper-sender"}}},
		copyLine{beeperID: "bm-reply", ms: base + 2000, text: "a reply", replyTo: "bm-1"})
	got := all(t, s, phoneChat)
	if g := ids(got); !slices.Equal(g, []string{"beeper:bm-1", "beeper:bm-photo", "beeper:bm-reply"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[1].Kind != "photo" || len(got[1].Media) != 1 || got[1].Media[0].HasFile || got[1].Media[0].Width != 800 {
		t.Errorf("the photo line is %+v", got[1])
	}
	if len(got[1].Reactions) != 1 || got[1].Reactions[0].Emoji != "❤️" {
		t.Errorf("the photo's reactions are %+v", got[1].Reactions)
	}
	if got[2].ReplyTo != "beeper:bm-1" || got[0].Source != SourceBeeper || got[0].BeeperID != "bm-1" {
		t.Errorf("the copied lines read %+v and %+v", got[0], got[2])
	}
	c := chatOf(t, s, phoneChat)
	if c.LastMessageID != "beeper:bm-reply" || c.Title != "Alex Example" || c.ReadOnly || c.HistoryFromMs != base {
		t.Errorf("the chat is %+v", c)
	}
	if c.Unread != 0 {
		t.Errorf("copied history counts %d unread", c.Unread)
	}
	if p, err := s.Search(net, Query{Text: "only in beeper", Limit: 10}); err != nil || len(p.Items) != 1 ||
		p.Items[0].ID != "beeper:bm-1" || p.Items[0].ChatID != phoneChat {
		t.Errorf("search found %v, %v", ids(p.Items), err)
	}
	if p, err := s.Chats(net, 0, "", 50); err != nil || len(p.Items) != 1 || p.Items[0].ID != phoneChat {
		t.Errorf("Chats = %+v, %v", p.Items, err)
	}
}

func TestAnUnplacedChatReadsAsBefore(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0R0001", base+5000, "a placed line"))
	unplaced := seedCopies(t, s, "", "bc-1",
		copyLine{beeperID: "bm-1", ms: base, text: "who is this"},
		copyLine{beeperID: "bm-2", ms: base + 1000, text: "pineapple party", fromMe: true})
	if unplaced != "unplaced:bc-1" {
		t.Fatalf("seeded into %q", unplaced)
	}
	c := chatOf(t, s, unplaced)
	if !c.ReadOnly || c.LastMessageID != "beeper:bm-2" || !c.LastFromMe || c.Unread != 0 {
		t.Fatalf("the unplaced chat is %+v", c)
	}
	if got := ids(all(t, s, unplaced)); !slices.Equal(got, []string{"beeper:bm-1", "beeper:bm-2"}) {
		t.Fatalf("the unplaced chat holds %v", got)
	}
	p, err := s.Chats(net, 0, "", 50)
	if err != nil || !slices.Equal(chatIDs(p.Items), []string{phoneChat, unplaced}) {
		t.Fatalf("Chats = %v, %v", chatIDs(p.Items), err)
	}
	if p, err := s.Search(net, Query{Text: "pineapple", Limit: 10}); err != nil || len(p.Items) != 1 || p.Items[0].ChatID != unplaced {
		t.Fatalf("search did not find the unplaced line: %+v, %v", p.Items, err)
	}
	if p, err := s.Search(net, Query{ChatIDs: []string{unplaced}, Limit: 10}); err != nil || len(p.Items) != 2 {
		t.Fatalf("search in the unplaced chat found %v, %v", ids(p.Items), err)
	}
	// A network never writes into it, nor under a copied line's ID.
	if err := s.PutChat(net, ChatInfo{ID: unplaced, Title: ptr("x")}); err == nil {
		t.Errorf("PutChat into an unplaced chat was accepted")
	}
	if _, err := s.PutMessage(net, text(unplaced, "3EB0R0002", base+9000, "x")); err == nil {
		t.Errorf("a line into an unplaced chat was accepted")
	}
	if _, err := s.PutMessage(net, text(phoneChat, "beeper:bm-9", base+9000, "x")); err == nil {
		t.Errorf("a line under a beeper: ID was accepted")
	}
	if err := s.Alias(net, unplaced, phoneChat); err == nil {
		t.Errorf("an unplaced chat was aliased")
	}
}

// chatIDs lists the chats' IDs, in order.
func chatIDs(cs []Chat) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.ID
	}
	return out
}

func TestADirectLineTakesOverItsCopy(t *testing.T) {
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1",
		copyLine{beeperID: "bm-text", ms: base, text: "<i>Dinner at 7?</i>"},
		copyLine{beeperID: "bm-id", ms: base + 300_000, text: "beeper says", networkID: "3EB0D0002"},
		copyLine{beeperID: "bm-reply", ms: base + 400_000, text: "Sounds good", replyTo: "bm-text"})
	if err := s.SetReaction(net, phoneChat, "beeper:bm-text", Reaction{Emoji: "👍", SenderID: otherChat}); err != nil {
		t.Fatal(err)
	}

	// WhatsApp's own sync brings the same lines under their own IDs.
	direct := text(phoneChat, "3EB0D0001", base+800, "Dinner at 7?")
	direct.SenderID = "beeper-sender"
	direct.Source = SourceSync
	if put(t, s, direct) {
		t.Fatalf("the direct line was added beside its copy")
	}
	other := text(phoneChat, "3EB0D0002", base+300_500, "whatsapp says")
	other.Source = SourceSync
	if put(t, s, other) {
		t.Fatalf("the line named by its network ID was added beside its copy")
	}

	got := all(t, s, phoneChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0D0001", "3EB0D0002", "beeper:bm-reply"}) {
		t.Fatalf("the chat holds %v", g)
	}
	if got[0].Text != "Dinner at 7?" || len(got[0].Reactions) != 1 || got[0].BeeperID != "bm-text" {
		t.Errorf("the taken-over line is %+v", got[0])
	}
	if got[2].ReplyTo != "3EB0D0001" {
		t.Errorf("the reply points at %q, want the direct ID", got[2].ReplyTo)
	}
	for b, want := range map[string]string{"bm-text": "3EB0D0001", "bm-id": "3EB0D0002", "bm-reply": "beeper:bm-reply"} {
		if got := carrier(t, s, b); got != want {
			t.Errorf("%s is carried by %q, want %q", b, got, want)
		}
	}
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "beeper:bm-reply" {
		t.Errorf("lastMessageID = %q", c.LastMessageID)
	}
}

func TestAnAddressInAnglesStillTakesOver(t *testing.T) {
	// WhatsApp's text is plain; Beeper's is HTML with the brackets escaped.
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-addr", ms: base, text: "Ask &lt;sam@example.com&gt; too"})
	direct := text(phoneChat, "3EB0E0002", base+400, "Ask <sam@example.com> too")
	direct.SenderID = "beeper-sender"
	if put(t, s, direct) {
		t.Fatalf("the direct address line was added beside its copy")
	}
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0E0002"}) {
		t.Fatalf("the chat holds %v", got)
	}
}

func TestATakeOverTakesTheDirectSide(t *testing.T) {
	s := openTest(t)
	// Beeper got the side wrong; the network ID still ties the lines.
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-side", ms: base, text: "on my way", networkID: "3EB0G0001"})
	if put(t, s, mine(phoneChat, "3EB0G0001", base+100, "on my way")) {
		t.Fatalf("added beside its copy")
	}
	got := all(t, s, phoneChat)
	if len(got) != 1 || !got[0].FromMe {
		t.Fatalf("the line is %+v, want his", got)
	}
	if c := chatOf(t, s, phoneChat); !c.LastFromMe || c.Unread != 0 {
		t.Fatalf("the chat is %+v", c)
	}
}

func TestAReceiptForACopiedLineOnlyMarksItSeen(t *testing.T) {
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1",
		copyLine{beeperID: "bm-r", ms: base, text: "sent from my phone", fromMe: true, networkID: "3EB0R0001"})

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

	// The full line, when it comes, still takes the copy over.
	if put(t, s, mine(phoneChat, "3EB0R0001", base+300, "sent from my phone")) {
		t.Fatalf("the direct line was added beside its copy")
	}
	if got := all(t, s, phoneChat); len(got) != 1 || got[0].ID != "3EB0R0001" || got[0].SeenMs != base+60_000 {
		t.Fatalf("after the direct line: %+v", got)
	}
}

func TestAStoreFromBeforeBeeperIDsGetsThem(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, text(phoneChat, "3EB0O0001", base, "matched"))
	carry(t, s, phoneChat, "3EB0O0001", "bm-1", "bc-1")
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-2", ms: base + 9000, text: "copied"})
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
	for b, want := range map[string]string{"bm-1": "3EB0O0001", "bm-2": "beeper:bm-2"} {
		if got := carrier(t, s, b); got != want {
			t.Errorf("after the upgrade %s is carried by %q, want %q", b, got, want)
		}
	}
}

func TestAnAliasKeepsEveryBeeperChatApart(t *testing.T) {
	s := openTest(t)
	// The phone chat: a direct "ok" carrying a message of Beeper chat bc-b.
	put(t, s, text(phoneChat, "3EB0X0001", base, "ok"))
	carry(t, s, phoneChat, "3EB0X0001", "bm-9", "bc-b")
	// The LID chat: one copied line carrying a message of bc-a and another of bc-b.
	seedCopies(t, s, lidChat, "bc-a", copyLine{beeperID: "bm-1", ms: base + 500, text: "ok"})
	carry(t, s, lidChat, "beeper:bm-1", "bm-2", "bc-b")

	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	// bm-2 and bm-9 are two messages of bc-b, so they stay two lines.
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0X0001", "beeper:bm-1"}) {
		t.Fatalf("the merged chat holds %v", got)
	}
	for b, want := range map[string]string{"bm-9": "3EB0X0001", "bm-1": "beeper:bm-1", "bm-2": "beeper:bm-1"} {
		if got := carrier(t, s, b); got != want {
			t.Errorf("%s is carried by %q, want %q", b, got, want)
		}
	}
}

func TestAnAliasFoldsACopyIntoItsLine(t *testing.T) {
	// The copy landed in the phone chat, the network's line in the LID chat;
	// the alias brings them together, as one line.
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-p", copyLine{beeperID: "bm-1", ms: base + 400, text: "see you there"},
		copyLine{beeperID: "bm-2", ms: base + 60_000, text: "only copied"})
	put(t, s, text(lidChat, "3EB0K0001", base, "see you there"))
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0K0001", "beeper:bm-2"}) {
		t.Fatalf("the merged chat holds %v", got)
	}
	if got := carrier(t, s, "bm-1"); got != "3EB0K0001" {
		t.Fatalf("bm-1 is carried by %q", got)
	}
}

func TestAnEmptyOrDeletedLineNeverTakesOverByContent(t *testing.T) {
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-gone", ms: base, deleted: true})
	revoked := text(phoneChat, "3EB0Y0001", base+500, "")
	revoked.Deleted = true
	revoked.SenderID = "beeper-sender"
	if !put(t, s, revoked) {
		t.Fatalf("a deleted direct line took over a deleted copy")
	}
}

func TestAnEditOfACopiedLineKeepsItsTimeAndSide(t *testing.T) {
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-e", ms: base, text: "typo", fromMe: true, networkID: "3EB0E0001"})

	// An edit of a line held only in Beeper's copy changes its text alone.
	if added, err := s.PutMessage(net, Message{ID: "3EB0E0001", ChatID: phoneChat, TimeMs: base + 3_600_000,
		Text: "fixed", Edited: true}); err != nil || added {
		t.Fatalf("the edit: added %v, %v", added, err)
	}
	got := all(t, s, phoneChat)
	if len(got) != 1 || got[0].TimeMs != base || !got[0].FromMe || got[0].Text != "fixed" || !got[0].Edited {
		t.Fatalf("after the edit the line is %+v", got[0])
	}
	// The original, when history brings it, takes the line over and keeps the edit.
	orig := mine(phoneChat, "3EB0E0001", base+200, "typo")
	orig.Source = SourceSync
	if put(t, s, orig) {
		t.Fatalf("the original was added beside its edited copy")
	}
	got = all(t, s, phoneChat)
	if len(got) != 1 || got[0].ID != "3EB0E0001" || got[0].Text != "fixed" || !got[0].Edited || got[0].TimeMs != base+200 {
		t.Fatalf("after the original the line is %+v", got[0])
	}
}

func TestAnEditWithNoCopyKeepsTheLinesTimeAndSide(t *testing.T) {
	s := openTest(t)
	// An edit of a line never held is kept with its own time; the original,
	// arriving later under the same ID, is the same line and keeps the edit.
	if added, err := s.PutMessage(net, Message{ID: "3EB0E0002", ChatID: phoneChat, TimeMs: base + 7_200_000,
		Text: "second, fixed", Edited: true}); err != nil || !added {
		t.Fatalf("the early edit: added %v, %v", added, err)
	}
	if put(t, s, text(phoneChat, "3EB0E0002", base+1000, "second, typo")) {
		t.Fatalf("the original duplicated its early edit")
	}
	got := all(t, s, phoneChat)
	if len(got) != 1 || got[0].ID != "3EB0E0002" || got[0].Text != "second, fixed" || got[0].TimeMs != base+1000 {
		t.Fatalf("after the late original: %+v", got)
	}
	// An edit of a line already held directly keeps its time too.
	if _, err := s.PutMessage(net, Message{ID: "3EB0E0002", ChatID: phoneChat, TimeMs: base + 9_000_000,
		Text: "second, fixed again", Edited: true}); err != nil {
		t.Fatal(err)
	}
	if got := all(t, s, phoneChat); got[0].TimeMs != base+1000 || got[0].Text != "second, fixed again" {
		t.Fatalf("a second edit moved the line: %+v", got[0])
	}
}

func TestACaptionlessMediaLineTakesOverByItsKind(t *testing.T) {
	// Beeper's copy names only the kind; the direct video names only its kind.
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-v1", ms: base, kind: "video", media: []Media{{Kind: "video"}}})
	direct := text(phoneChat, "3EB0C0002", base+300, "")
	direct.Kind, direct.SenderID = "video", "beeper-sender"
	if put(t, s, direct) {
		t.Fatalf("a video without media entries was added beside its copy")
	}
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0C0002"}) {
		t.Fatalf("the chat holds %v", got)
	}
	// A media kind with no media entries still shows.
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0C0002" {
		t.Fatalf("lastMessageID = %q", c.LastMessageID)
	}
}

// TestCopyNearSkipsOnlyWhatCouldNotMatch checks copyNear against the
// lookups it lets a write skip: for every line it says has no copy near,
// beeperLineNaming and beeperLineLike find nothing, and it does say so for
// the lines that are near.
func TestCopyNearSkipsOnlyWhatCouldNotMatch(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0CN0000", base-90_000, "a direct line"))
	put(t, s, text(phoneChat, "3EB0CN0099", base+30_000, "hello"))
	seedCopies(t, s, phoneChat, "bc-1",
		copyLine{beeperID: "bm-1", ms: base, text: "hello"},
		copyLine{beeperID: "bm-2", ms: base + 10_000, text: "Alex hi", fromMe: true},
		copyLine{beeperID: "bm-3", ms: base + 20_000, kind: "photo"},
		copyLine{beeperID: "bm-4", ms: base + 40_000, text: "named", networkID: "3EB0CN0004"})
	seedCopies(t, s, otherChat, "bc-2", copyLine{beeperID: "bm-5", ms: base, text: "hello"})

	var skipped, near int
	err := s.write(func(tx *sql.Tx) error {
		for _, chat := range []string{phoneChat, otherChat, groupChat} {
			for _, d := range []int64{-60_000, -2001, -2000, -1, 0, 1, 2000, 2001, 9_999, 10_000, 20_000, 30_000, 40_000, 120_000} {
				for _, fromMe := range []bool{false, true} {
					for _, body := range []string{"hello", "Alex hi", "", "named"} {
						for _, id := range []string{"3EB0CN0001", "3EB0CN0004"} {
							m := text(chat, id, base+d, body)
							m.FromMe = fromMe
							if body == "" {
								m.Kind, m.Media = "photo", []Media{{Kind: "photo"}}
							}
							ok, err := copyNear(tx, net, chat, m.ID, m.TimeMs, isWhole(m))
							if err != nil {
								return err
							}
							if ok {
								near++
								continue
							}
							skipped++
							rid, _, err := beeperLineNaming(tx, net, m)
							if err != nil {
								return err
							}
							like, _, err := beeperLineLike(tx, net, m, 0)
							if err != nil {
								return err
							}
							if rid != 0 || like != 0 {
								return fmt.Errorf("%s at %+d (fromMe %v, %q, %s): skipped, but naming %d, like %d",
									chat, d, fromMe, body, id, rid, like)
							}
						}
					}
				}
			}
		}
		// News about a line (an edit, with no time) is looked up by its ID
		// alone: near when a copy names it, else nothing to find.
		for id, want := range map[string]bool{"3EB0CN0004": true, "3EB0CN0001": false} {
			m := Message{ID: id, ChatID: phoneChat, Text: "renamed", Edited: true}
			ok, err := copyNear(tx, net, phoneChat, id, m.TimeMs, isWhole(m))
			if err != nil {
				return err
			}
			rid, _, err := beeperLineNaming(tx, net, m)
			if err != nil {
				return err
			}
			if ok != want || (rid != 0) != want {
				return fmt.Errorf("an edit of %s: near %v, naming %d; want %v", id, ok, rid, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if skipped == 0 || near == 0 {
		t.Fatalf("skipped %d, near %d: the grid tests nothing", skipped, near)
	}
}
