package store

import "testing"

// nearMisses reads the count for the test network.
func nearMisses(t *testing.T, s *Store) int {
	t.Helper()
	c, err := s.Counts(net)
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	return c.NearMisses
}

// beeperThenDirect makes a Beeper line, then a direct line gapMs later.
func beeperThenDirect(t *testing.T, s *Store, chat string, gapMs int64, beeperFromMe, directFromMe bool, beeperText, directText string) {
	t.Helper()
	bm := beeperMsg("bm-"+idN("nm", int(gapMs)), base, beeperText)
	bm.FromMe = beeperFromMe
	importOne(t, s, ImportBatch{BeeperChatID: "bc-nm", ChatID: chat, Chat: ImportChat{Kind: "single"}, Messages: []ImportMessage{bm}})
	d := text(chat, "3EB0NM"+idN("", int(gapMs)), base+gapMs, directText)
	d.FromMe = directFromMe
	put(t, s, d)
}

func TestANearMissIsABeeperLineWithADifferentDirectLineBeside(t *testing.T) {
	// Different text 500 ms apart: two lines shown where one message was.
	s := openTest(t)
	beeperThenDirect(t, s, phoneChat, 500, false, false, "Hi Sam, are you coming", "Hi @10000000009, are you coming")
	if got := nearMisses(t, s); got != 1 {
		t.Fatalf("nearMisses = %d, want 1", got)
	}
	if c, _ := s.Counts(net); c.Messages != 2 {
		t.Fatalf("the store holds %d lines, want the two the miss left", c.Messages)
	}

	// The same text matches, so nothing is left to miss.
	s = openTest(t)
	beeperThenDirect(t, s, phoneChat, 500, false, false, "Are you coming", "are you  coming")
	if got := nearMisses(t, s); got != 0 {
		t.Fatalf("a same-text pair counted %d, want 0", got)
	}

	// Five seconds apart is outside the matching window.
	s = openTest(t)
	beeperThenDirect(t, s, phoneChat, 5000, false, false, "one thing", "another thing")
	if got := nearMisses(t, s); got != 0 {
		t.Fatalf("a pair 5 s apart counted %d, want 0", got)
	}

	// The window is the matcher's: 2000 ms is inside it, 2001 is not.
	s = openTest(t)
	beeperThenDirect(t, s, phoneChat, 2000, false, false, "one thing", "another thing")
	if got := nearMisses(t, s); got != 1 {
		t.Fatalf("a pair 2000 ms apart counted %d, want 1", got)
	}
	s = openTest(t)
	beeperThenDirect(t, s, phoneChat, 2001, false, false, "one thing", "another thing")
	if got := nearMisses(t, s); got != 0 {
		t.Fatalf("a pair 2001 ms apart counted %d, want 0", got)
	}

	// The other side of the conversation is not a twin.
	s = openTest(t)
	beeperThenDirect(t, s, phoneChat, 500, false, true, "one thing", "another thing")
	if got := nearMisses(t, s); got != 0 {
		t.Fatalf("a pair from different sides counted %d, want 0", got)
	}
}

func TestNearMissesCountOnlyPlacedChatsAndOnlyBeeperLines(t *testing.T) {
	s := openTest(t)
	// Two direct lines close together: no Beeper line, no miss.
	put(t, s, text(phoneChat, "3EB0NM0001", base, "one thing"))
	put(t, s, text(phoneChat, "3EB0NM0002", base+500, "another thing"))
	if got := nearMisses(t, s); got != 0 {
		t.Fatalf("two direct lines counted %d, want 0", got)
	}
	// A Beeper chat the copy could not place has no direct line beside it.
	importOne(t, s, ImportBatch{BeeperChatID: "bc-lost", Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-lost-1", base, "x one"), beeperMsg("bm-lost-2", base+500, "x two")}})
	if got := nearMisses(t, s); got != 0 {
		t.Fatalf("an unplaced chat counted %d, want 0", got)
	}
	// A Beeper line in the placed chat, one near each of the two direct lines
	// and one far from either: the two near ones miss; the far one does not.
	importOne(t, s, ImportBatch{BeeperChatID: "bc-nm2", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{
			beeperMsg("bm-n1", base+200, "Sam said hi"),
			beeperMsg("bm-n2", base+400, "Sam said bye"),
			beeperMsg("bm-n3", base+60_000, "much later"),
		}})
	// Counted per Beeper line, not per pair: two Beeper lines, each beside two direct lines.
	if got := nearMisses(t, s); got != 2 {
		t.Fatalf("nearMisses = %d, want 2", got)
	}
	// A different network's lines are not counted.
	c, err := s.Counts("other")
	if err != nil || c.NearMisses != 0 {
		t.Fatalf("another network counted %+v, %v", c, err)
	}
}
