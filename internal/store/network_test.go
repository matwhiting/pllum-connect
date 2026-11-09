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
