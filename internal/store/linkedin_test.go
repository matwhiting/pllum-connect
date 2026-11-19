package store

import (
	"testing"
)

// The store's additions for LinkedIn (step 4): read-only chats, reactions
// known only as a count per emoji, seen times, and the chat of a line.

func TestAChatCanBeMadeReadOnlyAndKeepsItsLines(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0L0001", base, "kept"))
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, ReadOnly: ptr(true)}); err != nil {
		t.Fatal(err)
	}
	c := chatOf(t, s, phoneChat)
	if !c.ReadOnly {
		t.Fatalf("the chat is not read-only")
	}
	if got := all(t, s, phoneChat); len(got) != 1 {
		t.Fatalf("%d lines after read-only, want 1", len(got))
	}
	// A write that says nothing about it keeps it.
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Title: ptr("Invented Title")}); err != nil {
		t.Fatal(err)
	}
	if !chatOf(t, s, phoneChat).ReadOnly {
		t.Fatalf("a later write cleared read-only")
	}
}

func TestASummaryReactionHasNoSenderAndOnePerEmoji(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0L0002", base, "react to me"))
	err := s.SetReactionSummary(net, phoneChat, "3EB0L0002", []Reaction{
		{Emoji: "👍", FromMe: true},
		{Emoji: "🎉"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := all(t, s, phoneChat)[0].Reactions
	if len(got) != 2 {
		t.Fatalf("reactions = %+v, want two", got)
	}
	byEmoji := map[string]Reaction{}
	for _, r := range got {
		if r.SenderID != "" {
			t.Errorf("a summary reaction has sender %q", r.SenderID)
		}
		byEmoji[r.Emoji] = r
	}
	if !byEmoji["👍"].FromMe || byEmoji["🎉"].FromMe {
		t.Errorf("fromMe = %+v, want only 👍 his", byEmoji)
	}

	// A new summary replaces the old one whole.
	if err := s.SetReactionSummary(net, phoneChat, "3EB0L0002", []Reaction{{Emoji: "🎉"}}); err != nil {
		t.Fatal(err)
	}
	if got := all(t, s, phoneChat)[0].Reactions; len(got) != 1 || got[0].Emoji != "🎉" {
		t.Fatalf("reactions = %+v, want only 🎉", got)
	}

	// One emoji at a time.
	if err := s.SetSummaryEmoji(net, phoneChat, "3EB0L0002", "❤️", true, true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSummaryEmoji(net, phoneChat, "3EB0L0002", "🎉", false, false); err != nil {
		t.Fatal(err)
	}
	if got := all(t, s, phoneChat)[0].Reactions; len(got) != 1 || got[0].Emoji != "❤️" || !got[0].FromMe {
		t.Fatalf("reactions = %+v, want only his ❤️", got)
	}
}

func TestSeenThroughAtKeepsTheSeenTime(t *testing.T) {
	s := openTest(t)
	put(t, s, mine(phoneChat, "3EB0L0003", base, "one"))
	put(t, s, mine(phoneChat, "3EB0L0004", base+1000, "two"))
	put(t, s, mine(phoneChat, "3EB0L0005", base+2000, "three"))
	if err := s.SeenThroughAt(net, phoneChat, base+1000, base+5000); err != nil {
		t.Fatal(err)
	}
	got := all(t, s, phoneChat)
	if got[0].SeenMs != base+5000 || got[1].SeenMs != base+5000 || got[2].SeenMs != 0 {
		t.Fatalf("seen = %d %d %d, want the first two seen at +5 s", got[0].SeenMs, got[1].SeenMs, got[2].SeenMs)
	}
}

func TestChatOfLineFindsTheChatHoldingIt(t *testing.T) {
	s := openTest(t)
	put(t, s, text(otherChat, "3EB0L0006", base, "here"))
	if chat, ok := s.ChatOfLine(net, "3EB0L0006"); !ok || chat != otherChat {
		t.Fatalf("ChatOfLine = %q, %v; want %q", chat, ok, otherChat)
	}
	if _, ok := s.ChatOfLine(net, "3EB0L0099"); ok {
		t.Fatalf("ChatOfLine found a line nothing holds")
	}
}
