package store

import (
	"bytes"
	"testing"
)

// What a network keeps in the store besides lines: history notifications
// before they are acknowledged, its read marker, and what it needs later.

func TestPendingHistoryIsKeptInOrderOnceAndDeleted(t *testing.T) {
	s := openTest(t)
	if _, _, ok, err := s.NextPendingHistory(net); err != nil || ok {
		t.Fatalf("an empty store has a pending row (ok %v, err %v)", ok, err)
	}
	for _, b := range [][]byte{[]byte("first"), []byte("second"), []byte("first")} {
		if err := s.PutPendingHistory(net, b); err != nil {
			t.Fatalf("PutPendingHistory: %v", err)
		}
	}
	if err := s.PutPendingHistory("other", []byte("elsewhere")); err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	for {
		id, blob, ok, err := s.NextPendingHistory(net)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, blob)
		if err := s.DeletePendingHistory(net, id); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || !bytes.Equal(got[0], []byte("first")) || !bytes.Equal(got[1], []byte("second")) {
		t.Fatalf("pending rows came back as %q, want first then second, once each", got)
	}
	if _, _, ok, _ := s.NextPendingHistory("other"); !ok {
		t.Fatalf("another network's row went too")
	}
}

func TestAForwardOnlyReadMarkerNeverMovesBack(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0R0001", base+1000, "one"))
	put(t, s, text(phoneChat, "3EB0R0002", base+2000, "two"))
	put(t, s, text(phoneChat, "3EB0R0003", base+3000, "three"))

	setRead := func(ms int64, forward bool) {
		t.Helper()
		if err := s.PutChat(net, ChatInfo{ID: phoneChat, ReadThroughMs: &ms, ReadForwardOnly: forward}); err != nil {
			t.Fatal(err)
		}
	}
	unread := func() int {
		t.Helper()
		c, _, err := s.Chat(net, phoneChat)
		if err != nil {
			t.Fatal(err)
		}
		return c.Unread
	}
	setRead(base+2000, true)
	if u := unread(); u != 1 {
		t.Fatalf("unread = %d after reading through two, want 1", u)
	}
	setRead(base+1000, true)
	if u := unread(); u != 1 {
		t.Fatalf("a forward-only marker moved back: unread = %d, want 1", u)
	}
	setRead(base+1000, false)
	if u := unread(); u != 2 {
		t.Fatalf("a set marker did not move back: unread = %d, want 2", u)
	}
}

func TestLineTimeAndRawAreReadBack(t *testing.T) {
	s := openTest(t)
	m := text(phoneChat, "3EB0L0001", base+1000, "")
	m.Kind = "photo"
	m.Media = []Media{{Kind: "photo", Width: 4, Height: 3, HasFile: true}}
	m.Raw = []byte("invented-raw")
	put(t, s, m)
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}

	if ms, ok := s.LineTime(net, lidChat, "3EB0L0001"); !ok || ms != base+1000 {
		t.Fatalf("LineTime through an alias = %d, %v", ms, ok)
	}
	if _, ok := s.LineTime(net, phoneChat, "3EB0L0404"); ok {
		t.Fatalf("LineTime found a line nothing holds")
	}
	raw, err := s.Raw(net, phoneChat, "3EB0L0001")
	if err != nil || !bytes.Equal(raw, []byte("invented-raw")) {
		t.Fatalf("Raw = %q, %v", raw, err)
	}
	if _, err := s.Raw(net, phoneChat, "3EB0L0404"); err == nil {
		t.Fatalf("Raw of a line nothing holds has no error")
	}
}

func TestChatOrderIsTheOrderChatsWereMade(t *testing.T) {
	s := openTest(t)
	put(t, s, text(lidChat, "3EB0O0001", base+5000, "made first, later time"))
	put(t, s, text(phoneChat, "3EB0O0002", base+1000, "made second, earlier time"))
	first, ok1 := s.ChatOrder(net, lidChat)
	second, ok2 := s.ChatOrder(net, phoneChat)
	if !ok1 || !ok2 || first >= second {
		t.Fatalf("ChatOrder = %d (%v), %d (%v); want the first-made chat lower", first, ok1, second, ok2)
	}
	if err := s.Alias(net, otherChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	if o, ok := s.ChatOrder(net, otherChat); !ok || o != second {
		t.Fatalf("ChatOrder through an alias = %d, %v", o, ok)
	}
	if _, ok := s.ChatOrder(net, groupChat); ok {
		t.Fatalf("ChatOrder found a chat nothing holds")
	}
}

func TestOldestLineIsTheNetworksOwnOldest(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "mid.$invented-2", base+2000, ""))
	put(t, s, text(phoneChat, "other-invented-0", base+500, ""))
	put(t, s, text(phoneChat, "mid.$invented-1", base+1000, ""))
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	if ms, id, ok := s.OldestLine(net, lidChat, "mid."); !ok || id != "mid.$invented-1" || ms != base+1000 {
		t.Fatalf("OldestLine = %d, %q, %v", ms, id, ok)
	}
	if _, _, ok := s.OldestLine(net, otherChat, "mid."); ok {
		t.Fatalf("OldestLine found a line in a chat that holds none")
	}
}

func TestAliasTargetIsOnlyAnAliasesChat(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0A0001", base+1000, ""))
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	if got, ok := s.AliasTarget(net, lidChat); !ok || got != phoneChat {
		t.Fatalf("AliasTarget = %q, %v", got, ok)
	}
	if _, ok := s.AliasTarget(net, phoneChat); ok {
		t.Fatalf("a chat of its own reads as an alias")
	}
}

// LineState is what shows of a row, so a network can tell whether a write
// changed it: an early row, a hidden notice, the line itself, edited,
// deleted.
func TestLineStateReadsWhatShows(t *testing.T) {
	s := openTest(t)
	if _, ok := s.LineState(net, phoneChat, "3EB0S0001"); ok {
		t.Fatalf("LineState found a row nothing holds")
	}
	if _, err := s.PutMessage(net, Message{ID: "3EB0S0001", ChatID: phoneChat, Text: "invented edit", Edited: true}); err != nil {
		t.Fatal(err)
	}
	early, ok := s.LineState(net, phoneChat, "3EB0S0001")
	if !ok || !early.Early {
		t.Fatalf("an edit before its line reads %+v, %v", early, ok)
	}
	hidden := true
	put(t, s, Message{ID: "3EB0S0002", ChatID: phoneChat, TimeMs: base, Kind: "notice", Hidden: &hidden, Source: SourceLive})
	notice, _ := s.LineState(net, phoneChat, "3EB0S0002")
	readable := text(phoneChat, "3EB0S0002", base, "invented now readable")
	shownFlag := false
	readable.Hidden = &shownFlag
	put(t, s, readable)
	shown, _ := s.LineState(net, phoneChat, "3EB0S0002")
	if notice == shown || !notice.Hidden || shown.Hidden || shown.Text != "invented now readable" || shown.Kind != "text" {
		t.Fatalf("a notice taken over reads %+v, then %+v", notice, shown)
	}
	put(t, s, readable)
	if again, _ := s.LineState(net, phoneChat, "3EB0S0002"); again != shown {
		t.Fatalf("the same line again reads %+v, was %+v", again, shown)
	}
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutMessage(net, Message{ID: "3EB0S0002", ChatID: phoneChat, Deleted: true}); err != nil {
		t.Fatal(err)
	}
	if gone, ok := s.LineState(net, lidChat, "3EB0S0002"); !ok || !gone.Deleted {
		t.Fatalf("a revoked line read through an alias is %+v, %v", gone, ok)
	}
}

// HoldsUnreadable says whether a chat still shows a line from someone that
// couldn't be read: one taken over, settled or his own doesn't count.
func TestHoldsUnreadableCountsShownPlaceholdersFromOthers(t *testing.T) {
	s := openTest(t)
	shown, hidden := false, true
	placeholder := func(id, kind string, fromMe bool, h *bool) Message {
		return Message{ID: id, ChatID: phoneChat, TimeMs: base, Kind: kind, FromMe: fromMe, Hidden: h, Source: SourceLive}
	}
	if s.HoldsUnreadable(net, phoneChat) {
		t.Fatalf("an empty chat holds an unreadable line")
	}
	put(t, s, text(phoneChat, "3EB0U0001", base, "invented readable"))
	put(t, s, placeholder("3EB0U0002", "undecryptable", true, &shown))
	put(t, s, placeholder("3EB0U0003", "unavailable", false, &hidden))
	if s.HoldsUnreadable(net, phoneChat) {
		t.Fatalf("a chat with only his own or settled placeholders holds an unreadable line")
	}
	put(t, s, placeholder("3EB0U0004", "unavailable", false, &shown))
	if err := s.Alias(net, lidChat, phoneChat); err != nil {
		t.Fatal(err)
	}
	if !s.HoldsUnreadable(net, lidChat) {
		t.Fatalf("a shown unavailable line read through an alias doesn't count")
	}
	readable := text(phoneChat, "3EB0U0004", base, "invented now readable")
	readable.Hidden = &shown
	put(t, s, readable)
	if s.HoldsUnreadable(net, phoneChat) {
		t.Fatalf("a line taken over still counts")
	}
	put(t, s, placeholder("3EB0U0005", "undecryptable", false, &shown))
	if _, err := s.SettlePlaceholder(net, phoneChat, "3EB0U0005"); err != nil {
		t.Fatal(err)
	}
	if s.HoldsUnreadable(net, phoneChat) || s.HoldsUnreadable(net, otherChat) {
		t.Fatalf("a settled line, or another chat, counts")
	}
}
