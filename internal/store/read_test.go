package store

import (
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

func TestChatsAreNewestFirstWithAStableCursor(t *testing.T) {
	s := openTest(t)
	// 250 chats; pairs share a time, so the cursor has ties to get past.
	for i := range 250 {
		chat := fmt.Sprintf("1%010d@s.whatsapp.net", i)
		put(t, s, text(chat, idN("3EB0P", i), base+int64(i/2)*1000, "hello"))
	}

	seen := map[string]bool{}
	var order []Chat
	cursor := ""
	pages := 0
	for {
		p, err := s.Chats(net, 0, cursor, 100)
		if err != nil {
			t.Fatalf("Chats: %v", err)
		}
		pages++
		for _, c := range p.Items {
			if seen[c.ID] {
				t.Fatalf("chat repeated on page %d", pages)
			}
			seen[c.ID] = true
			order = append(order, c)
		}
		if p.Cursor == "" {
			break
		}
		if pages > 3 {
			t.Fatalf("more than three pages")
		}
		cursor = p.Cursor
	}
	if pages != 3 || len(seen) != 250 {
		t.Fatalf("%d pages, %d chats; want 3 pages and 250 chats", pages, len(seen))
	}
	for i := 1; i < len(order); i++ {
		if order[i].LastActivityMs > order[i-1].LastActivityMs {
			t.Fatalf("chat %d is newer than the one before it", i)
		}
	}

	// sinceMs leaves out chats quiet since then.
	p, err := s.Chats(net, base+100*1000, "", 200)
	if err != nil {
		t.Fatalf("Chats since: %v", err)
	}
	if len(p.Items) != 50 {
		t.Fatalf("since gave %d chats, want 50", len(p.Items))
	}
	if _, err := s.Chats(net, 0, "not-a-cursor", 10); err == nil {
		t.Fatalf("a made-up cursor was accepted")
	}
}

func TestMessagesPageBackOldestFirstWithinAPage(t *testing.T) {
	s := openTest(t)
	var want []string
	for i := range 25 {
		id := idN("3EB0M", i)
		want = append(want, id)
		// Two lines share each time, so the order within a time is held too.
		put(t, s, text(phoneChat, id, base+int64(i/2)*1000, "line"))
	}
	var got [][]string
	before := ""
	for {
		p, err := s.Messages(net, phoneChat, before, 10)
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		got = append(got, ids(p.Items))
		for i := 1; i < len(p.Items); i++ {
			if p.Items[i].TimeMs < p.Items[i-1].TimeMs {
				t.Fatalf("a page is not oldest first")
			}
		}
		if !p.HasMore {
			break
		}
		before = p.OldestCursor
	}
	if len(got) != 3 || len(got[0]) != 10 || len(got[1]) != 10 || len(got[2]) != 5 {
		t.Fatalf("pages = %v", got)
	}
	var joined []string
	for i := len(got) - 1; i >= 0; i-- {
		joined = append(joined, got[i]...)
	}
	if !slices.Equal(joined, want) {
		t.Fatalf("paged back = %v\nwant       %v", joined, want)
	}
	if !slices.Equal(got[0], want[15:]) {
		t.Fatalf("the first page is not the newest ten: %v", got[0])
	}
}

func TestHasMoreIsFalseOnlyAtTheOldest(t *testing.T) {
	s := openTest(t)
	for i := range 20 {
		put(t, s, text(phoneChat, idN("3EB0H", i), base+int64(i)*1000, "line"))
	}
	for _, tc := range []struct {
		limit int
		want  []bool
	}{
		{10, []bool{true, false}},
		{7, []bool{true, true, false}},
		{20, []bool{false}},
		{200, []bool{false}},
	} {
		var got []bool
		before := ""
		for range 10 {
			p, err := s.Messages(net, phoneChat, before, tc.limit)
			if err != nil {
				t.Fatalf("Messages: %v", err)
			}
			got = append(got, p.HasMore)
			if !p.HasMore {
				break
			}
			before = p.OldestCursor
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("limit %d: hasMore = %v, want %v", tc.limit, got, tc.want)
		}
	}
	// A chat with nothing in it has no more.
	if err := s.PutChat(net, ChatInfo{ID: otherChat}); err != nil {
		t.Fatal(err)
	}
	p, err := s.Messages(net, otherChat, "", 10)
	if err != nil || p.HasMore || len(p.Items) != 0 {
		t.Fatalf("empty chat: %+v, %v", p, err)
	}
	if _, err := s.Messages(net, "10000000099@s.whatsapp.net", "", 10); err == nil {
		t.Fatalf("an unknown chat gave no error")
	}
}

func TestLastMessageIDSkipsHiddenDeletedAndEmpty(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0L0001", base+1000, "visible"))
	photo := text(phoneChat, "3EB0L0002", base+2000, "")
	photo.Kind = "photo"
	photo.Media = []Media{{Kind: "photo", Width: 640, Height: 480, HasFile: true}}
	put(t, s, photo)
	notice := text(phoneChat, "3EB0L0003", base+3000, "disappearing messages on")
	notice.Kind, notice.Hidden = "notice", true
	put(t, s, notice)
	gone := text(phoneChat, "3EB0L0004", base+4000, "")
	gone.Deleted = true
	put(t, s, gone)
	put(t, s, text(phoneChat, "3EB0L0005", base+5000, ""))

	c := chatOf(t, s, phoneChat)
	if c.LastMessageID != "3EB0L0002" || c.LastFromMe {
		t.Fatalf("lastMessageID = %q (fromMe %v), want the photo", c.LastMessageID, c.LastFromMe)
	}
	if c.LastActivityMs != base+2000 {
		t.Errorf("lastActivityMs = %d, want the photo's time", c.LastActivityMs)
	}

	// A line deleted later stops being the last.
	put(t, s, mine(phoneChat, "3EB0L0006", base+6000, "mine"))
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0L0006" || !c.LastFromMe {
		t.Fatalf("after his line: %q fromMe %v", c.LastMessageID, c.LastFromMe)
	}
	revoked := mine(phoneChat, "3EB0L0006", base+6000, "")
	revoked.Deleted = true
	put(t, s, revoked)
	if c := chatOf(t, s, phoneChat); c.LastMessageID != "3EB0L0002" {
		t.Fatalf("after the revoke: %q", c.LastMessageID)
	}
	if c := chatOf(t, s, phoneChat); c.HistoryFromMs != base+1000 {
		t.Errorf("historyFromMs = %d, want the oldest line", c.HistoryFromMs)
	}
}

func TestUnreadCountsIncomingAfterHisReadMarker(t *testing.T) {
	s := openTest(t)
	for i := range 4 {
		put(t, s, text(phoneChat, idN("3EB0U", i), base+int64(i+1)*1000, "incoming"))
	}
	put(t, s, mine(phoneChat, "3EB0U0100", base+5000, "mine"))
	hidden := text(phoneChat, "3EB0U0101", base+6000, "notice")
	hidden.Hidden = true
	put(t, s, hidden)

	if c := chatOf(t, s, phoneChat); c.Unread != 4 {
		t.Fatalf("unread = %d, want 4", c.Unread)
	}
	through := base + 2000
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, ReadThroughMs: &through}); err != nil {
		t.Fatal(err)
	}
	if c := chatOf(t, s, phoneChat); c.Unread != 2 {
		t.Fatalf("unread after reading through the second = %d, want 2", c.Unread)
	}
	put(t, s, text(phoneChat, "3EB0U0102", base+7000, "another"))
	if c := chatOf(t, s, phoneChat); c.Unread != 3 {
		t.Fatalf("unread after a new line = %d, want 3", c.Unread)
	}
}

func TestAnAliasLandsInTheSameChat(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0A0001", base, "by number"))
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatalf("Alias: %v", err)
	}
	if got, ok := s.ChatFor(net, lidChat); !ok || got != phoneChat {
		t.Fatalf("ChatFor(lid) = %q, %v", got, ok)
	}
	if got, ok := s.ChatFor(net, phoneChat); !ok || got != phoneChat {
		t.Fatalf("ChatFor(phone) = %q, %v", got, ok)
	}
	if _, ok := s.ChatFor(net, otherChat); ok {
		t.Fatalf("an unknown JID has a chat")
	}

	put(t, s, text(lidChat, "3EB0A0002", base+1000, "by lid"))
	if got := ids(all(t, s, phoneChat)); !slices.Equal(got, []string{"3EB0A0001", "3EB0A0002"}) {
		t.Fatalf("the phone chat holds %v", got)
	}
	if c := chatOf(t, s, lidChat); c.ID != phoneChat {
		t.Fatalf("the LID reads as chat %q", c.ID)
	}
	p, err := s.Chats(net, 0, "", 200)
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("Chats = %d items, %v; want one", len(p.Items), err)
	}
}

func TestAliasingAChatThatHoldsLinesMergesIt(t *testing.T) {
	s := openTest(t)
	// WhatsApp's sync made a chat under the LID...
	name := "Alex Example"
	if err := s.PutChat(net, ChatInfo{ID: lidChat, Title: &name,
		Participants: []Participant{{ID: lidChat, Name: name}}}); err != nil {
		t.Fatal(err)
	}
	put(t, s, text(lidChat, "3EB0Z0001", base+1000, "only under the lid"))
	clash := text(lidChat, "3EB0Z0002", base+2000, "the same line, edited")
	clash.Edited = true
	put(t, s, clash)
	put(t, s, text(lidChat, "3EB0Z0003", base+3000, "Dinner?"))
	if err := s.SetReaction(net, lidChat, "3EB0Z0001", Reaction{Emoji: "👍", SenderID: lidChat}); err != nil {
		t.Fatal(err)
	}

	// ...while the phone chat holds the clash, a Beeper line and its own line.
	put(t, s, text(phoneChat, "3EB0Z0002", base+2000, "the same line"))
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-dinner", base+3500, "<p>Dinner?</p>")}})
	put(t, s, text(phoneChat, "3EB0Z0004", base+4000, "only under the number"))

	// The number arrives for the LID: an alias, never a new chat.
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatalf("Alias: %v", err)
	}

	p, err := s.Chats(net, 0, "", 200)
	if err != nil || len(p.Items) != 1 || p.Items[0].ID != phoneChat {
		t.Fatalf("Chats = %+v, %v; want the phone chat alone", p.Items, err)
	}
	got := all(t, s, phoneChat)
	if g := ids(got); !slices.Equal(g, []string{"3EB0Z0001", "3EB0Z0002", "3EB0Z0003", "3EB0Z0004"}) {
		t.Fatalf("the merged chat holds %v", g)
	}
	if len(got[0].Reactions) != 1 {
		t.Errorf("the moved line's reactions = %+v", got[0].Reactions)
	}
	if !got[1].Edited {
		t.Errorf("the clashing line lost its edit")
	}
	if lines := lineIDs(t, s, []string{"bm-dinner"}); lines["bm-dinner"] != "3EB0Z0003" {
		t.Errorf("the Beeper line was not taken over: %v", lines)
	}
	c := chatOf(t, s, lidChat)
	if c.ID != phoneChat || c.Title != name || len(c.Participants) != 1 || c.LastMessageID != "3EB0Z0004" {
		t.Errorf("the merged chat is %+v", c)
	}
	if n, err := s.Counts(net); err != nil || n.Chats != 1 || n.Messages != 4 {
		t.Errorf("Counts = %+v, %v", n, err)
	}
	// A line for the LID now lands in the phone chat.
	put(t, s, text(lidChat, "3EB0Z0005", base+5000, "after"))
	if got := len(all(t, s, phoneChat)); got != 5 {
		t.Errorf("after a new LID line the chat holds %d lines", got)
	}
}

func TestAnUpdateThatOmitsKindKeepsIt(t *testing.T) {
	s := openTest(t)
	photo := text(phoneChat, "3EB0K0001", base, "")
	photo.Kind, photo.Media = "photo", []Media{{Kind: "photo", HasFile: true}}
	put(t, s, photo)
	notice := text(phoneChat, "3EB0K0002", base+1000, "settings changed")
	notice.Kind, notice.Hidden = "notice", true
	put(t, s, notice)

	// A read receipt names only the line and when it was seen.
	for _, id := range []string{"3EB0K0001", "3EB0K0002"} {
		if _, err := s.PutMessage(net, Message{ID: id, ChatID: phoneChat, TimeMs: base, SeenMs: base + 9000}); err != nil {
			t.Fatal(err)
		}
	}
	got := all(t, s, phoneChat)
	if got[0].Kind != "photo" || len(got[0].Media) != 1 || got[0].SeenMs != base+9000 {
		t.Errorf("the photo became %+v", got[0])
	}
	if got[1].Kind != "notice" || !got[1].Hidden {
		t.Errorf("the notice became %+v", got[1])
	}
}

func TestAnOlderSchemaMigratesAndANewerIsOutdated(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, text(phoneChat, "3EB0V0001", base, "kept"))
	setSchema := func(v string) {
		t.Helper()
		if _, err := s.db.Exec(`UPDATE meta SET value = ? WHERE key = 'schema'`, v); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}

	setSchema("0")
	if s, err = Open(dir, testKey); err != nil {
		t.Fatalf("an older store did not open: %v", err)
	}
	var v string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&v); err != nil || v != strconv.Itoa(schemaVersion) {
		t.Fatalf("after migrating the schema is %q (%v)", v, err)
	}

	setSchema("99")
	if _, err := Open(dir, testKey); wire.KindOf(err) != wire.Outdated {
		t.Fatalf("a newer store gave %v, want outdated", err)
	}
}
