package store

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
)

// merged is how many copied lines network has merged since its day line.
func merged(t *testing.T, s *Store, network string) int {
	t.Helper()
	return mergeColumn(t, s, network, "merged")
}

// news is how many times network's news about a line (a receipt, an edit,
// a revoke) was matched to the copy naming it since its day line.
func news(t *testing.T, s *Store, network string) int {
	t.Helper()
	return mergeColumn(t, s, network, "news")
}

func mergeColumn(t *testing.T, s *Store, network, column string) int {
	t.Helper()
	var n int
	err := s.db.QueryRow(`SELECT `+column+` FROM beeper_merges WHERE network = ?`, network).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Every way a direct line takes over or absorbs a copied line counts one
// merge for its network; every ID and text is invented.
func TestEveryMergeOfACopyIsCounted(t *testing.T) {
	t.Run("takeover by network ID", func(t *testing.T) {
		s := openTest(t)
		seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-1", ms: base, text: "on my way", networkID: "3EB0M0001"})
		put(t, s, mine(phoneChat, "3EB0M0001", base+100, "on my way"))
		if n := merged(t, s, net); n != 1 {
			t.Fatalf("merged = %d, want 1", n)
		}
	})
	t.Run("takeover by content", func(t *testing.T) {
		s := openTest(t)
		seedCopies(t, s, phoneChat, "bc-1",
			copyLine{beeperID: "bm-1", ms: base, text: "Dinner at 7?"},
			copyLine{beeperID: "bm-2", ms: base + 60_000, text: "Sounds good"})
		put(t, s, text(phoneChat, "3EB0M0002", base+800, "Dinner at 7?"))
		put(t, s, text(phoneChat, "3EB0M0003", base+60_500, "Sounds good"))
		if n := merged(t, s, net); n != 2 {
			t.Fatalf("merged = %d, want 2", n)
		}
	})
	t.Run("twin fold on an edit's early row", func(t *testing.T) {
		s := openTest(t)
		put(t, s, Message{ID: "3EB0M0010", ChatID: groupChat, Text: "@6421000099 hi!", Edited: true})
		groupCopies(t, s, copyLine{beeperID: "bm-tw", ms: base, text: "Alex hi"})
		put(t, s, groupLine("3EB0M0010", base, "@6421000099 hi"))
		if n := merged(t, s, net); n != 1 {
			t.Fatalf("merged = %d, want 1", n)
		}
	})
	t.Run("an alias folds a copy into its line", func(t *testing.T) {
		s := openTest(t)
		seedCopies(t, s, phoneChat, "bc-p", copyLine{beeperID: "bm-1", ms: base + 400, text: "see you there"},
			copyLine{beeperID: "bm-2", ms: base + 60_000, text: "only copied"})
		put(t, s, text(lidChat, "3EB0M0020", base, "see you there"))
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		if n := merged(t, s, net); n != 1 {
			t.Fatalf("merged = %d, want 1 (the copy only copied stays)", n)
		}
	})
	t.Run("per network", func(t *testing.T) {
		s := openTest(t)
		if _, err := s.SeedCopied("instagram", "ig-chat", "bc-ig", []CopiedLine{{BeeperID: "bm-ig", NetworkID: "ig-m1", TimeMs: base, Text: "hey"}}); err != nil {
			t.Fatal(err)
		}
		m := text("ig-chat", "ig-m1", base+100, "hey")
		m.Kind, m.Source, m.SenderID = "text", SourceLive, "ig-sender"
		if _, err := s.PutMessage("instagram", m); err != nil {
			t.Fatal(err)
		}
		if a, b := merged(t, s, "instagram"), merged(t, s, net); a != 1 || b != 0 {
			t.Fatalf("instagram merged %d, whatsapp %d; want 1 and 0", a, b)
		}
	})
}

// News about a line that lands on the copy naming it, rather than merging
// it, is counted apart: a receipt, an edit and a revoke the network sends
// under the ID a copy named, and a merge of chats bringing an early row
// (news that came before its line) to the copy naming it. Every ID and
// text is invented.
func TestEveryMatchedNewsIsCounted(t *testing.T) {
	t.Run("a receipt, an edit and a revoke", func(t *testing.T) {
		s := openTest(t)
		seedCopies(t, s, phoneChat, "bc-1",
			copyLine{beeperID: "bm-1", ms: base, text: "typo", networkID: "3EB0W0001"},
			copyLine{beeperID: "bm-2", ms: base + 60_000, text: "gone soon", networkID: "3EB0W0002"})
		for _, m := range []Message{
			{ID: "3EB0W0001", ChatID: phoneChat, SeenMs: base + 5},
			{ID: "3EB0W0001", ChatID: phoneChat, Text: "fixed", Edited: true},
			{ID: "3EB0W0002", ChatID: phoneChat, Deleted: true},
		} {
			if added, err := s.PutMessage(net, m); err != nil || added {
				t.Fatalf("PutMessage(%+v) = %v, %v", m, added, err)
			}
		}
		if n, k := news(t, s, net), merged(t, s, net); n != 3 || k != 0 {
			t.Fatalf("news = %d, merged = %d; want 3 and 0", n, k)
		}
	})
	t.Run("a chat merge brings an early row to its copy", func(t *testing.T) {
		s := openTest(t)
		seedCopies(t, s, phoneChat, "bc-p", copyLine{beeperID: "bm-1", ms: base, text: "typo", networkID: "3EB0W0010"})
		put(t, s, Message{ID: "3EB0W0010", ChatID: lidChat, Text: "fixed", Edited: true})
		if n := news(t, s, net); n != 0 {
			t.Fatalf("news = %d before the merge, want 0 (no copy in that chat)", n)
		}
		if err := s.Alias(net, lidChat, phoneChat); err != nil {
			t.Fatal(err)
		}
		if got := ids(all(t, s, phoneChat)); len(got) != 1 || got[0] != "beeper:bm-1" {
			t.Fatalf("the merged chat holds %v", got)
		}
		if n, k := news(t, s, net), merged(t, s, net); n != 1 || k != 0 {
			t.Fatalf("news = %d, merged = %d; want 1 and 0", n, k)
		}
	})
}

// A write that merges nothing counts nothing: a live line in a chat with
// copies far from it, a line beside a copy of other content, and a copy
// merged into nothing.
func TestAWriteThatMergesNothingCountsNothing(t *testing.T) {
	s := openTest(t)
	seedCopies(t, s, phoneChat, "bc-1",
		copyLine{beeperID: "bm-1", ms: base, text: "hello", networkID: "3EB0N0001"},
		copyLine{beeperID: "bm-2", ms: base + 60_000, text: "copied only"})
	put(t, s, text(phoneChat, "3EB0N0002", base+10*60_000, "a live line"))
	put(t, s, text(phoneChat, "3EB0N0003", base+60_400, "something else"))
	put(t, s, Message{ID: "3EB0N0004", ChatID: phoneChat, SeenMs: base + 5}) // a receipt for no line
	if n, k := merged(t, s, net), news(t, s, net); n != 0 || k != 0 {
		t.Fatalf("merged = %d, news = %d; want 0 and 0", n, k)
	}
}

// CloseMergeDays gives each closed day once, the zero days between
// included, and opens today; every day here is invented.
func TestCloseMergeDays(t *testing.T) {
	s := openTest(t)
	close := func(today string) []MergeDay {
		t.Helper()
		got, err := s.CloseMergeDays(net, today)
		if err != nil {
			t.Fatalf("CloseMergeDays(%s): %v", today, err)
		}
		return got
	}

	// Merges before the first call count for the day it opens.
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-1", ms: base, text: "hi", networkID: "3EB0C0001"})
	put(t, s, text(phoneChat, "3EB0C0001", base+100, "hi"))
	seedCopies(t, s, phoneChat, "bc-1", copyLine{beeperID: "bm-9", ms: base + 120_000, text: "later", networkID: "3EB0C0009"})
	put(t, s, Message{ID: "3EB0C0009", ChatID: phoneChat, SeenMs: base + 5})
	if got := close("2026-03-01"); got != nil {
		t.Fatalf("the first call gave %v, want nothing", got)
	}
	if got := close("2026-03-01"); got != nil {
		t.Fatalf("a second call the same day gave %v", got)
	}
	if got := close("2026-03-02"); !reflect.DeepEqual(got, []MergeDay{{Day: "2026-03-01", Merged: 1, News: 1}}) {
		t.Fatalf("closing 1 March gave %v, want one merge and one news on it", got)
	}
	if got := close("2026-03-02"); got != nil {
		t.Fatalf("closing 1 March again gave %v", got)
	}

	// A zero day is given as a line too, and so is each day the helper
	// was not running.
	if got := close("2026-03-03"); !reflect.DeepEqual(got, []MergeDay{{Day: "2026-03-02"}}) {
		t.Fatalf("closing a day with no merge gave %v", got)
	}
	seedCopies(t, s, otherChat, "bc-2",
		copyLine{beeperID: "bm-2", ms: base, text: "a", networkID: "3EB0C0002"},
		copyLine{beeperID: "bm-3", ms: base + 60_000, text: "b", networkID: "3EB0C0003"})
	put(t, s, text(otherChat, "3EB0C0002", base, "a"))
	put(t, s, text(otherChat, "3EB0C0003", base+60_000, "b"))
	want := []MergeDay{{Day: "2026-03-03", Merged: 2}, {Day: "2026-03-04"}, {Day: "2026-03-05"}}
	if got := close("2026-03-06"); !reflect.DeepEqual(got, want) {
		t.Fatalf("closing three days at once gave %v, want %v", got, want)
	}

	// A clock moved back gives nothing and keeps the open day.
	if got := close("2026-03-01"); got != nil {
		t.Fatalf("a day before the open one gave %v", got)
	}
	if got := close("2026-03-07"); !reflect.DeepEqual(got, []MergeDay{{Day: "2026-03-06"}}) {
		t.Fatalf("after the clock moved back, closing 6 March gave %v", got)
	}

	// A long gap gives at most maxZeroDays zero days.
	if got := close("2029-03-07"); len(got) != 1+maxZeroDays || got[0] != (MergeDay{Day: "2026-03-07"}) ||
		got[len(got)-1] != (MergeDay{Day: "2029-03-06"}) {
		t.Fatalf("a three-year gap gave %d days, from %v to %v", len(got), got[0], got[len(got)-1])
	}

	if _, err := s.CloseMergeDays(net, "1 March"); err == nil {
		t.Fatalf("a day not YYYY-MM-DD was taken")
	}
	if _, err := s.CloseMergeDays("", "2026-03-01"); err == nil {
		t.Fatalf("no network was taken")
	}
}
