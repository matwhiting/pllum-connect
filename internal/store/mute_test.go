package store

import (
	"testing"
	"time"
)

// testClock is a clock a test moves.
type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func clockedStore(t *testing.T) (*Store, *testClock) {
	t.Helper()
	s := openTest(t)
	c := &testClock{now: time.UnixMilli(base)}
	s.SetClock(c.Now)
	return s, c
}

func TestATimedMuteReadsMutedUntilItsEnd(t *testing.T) {
	s, c := clockedStore(t)
	put(t, s, text(phoneChat, "3EB0Z0001", base, "one"))
	end := c.now.Add(8 * time.Hour).UnixMilli()
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true), MuteEndMs: end}); err != nil {
		t.Fatal(err)
	}
	if !chatOf(t, s, phoneChat).Muted {
		t.Fatalf("a mute ending in 8 h did not read muted now")
	}
	list := func() bool {
		t.Helper()
		p, err := s.Chats(net, 0, "", 50)
		if err != nil || len(p.Items) != 1 {
			t.Fatalf("Chats: %v, %d items", err, len(p.Items))
		}
		return p.Items[0].Muted
	}
	if !list() {
		t.Fatalf("the chat list did not read the timed mute as muted")
	}
	c.advance(8*time.Hour - time.Millisecond)
	if !chatOf(t, s, phoneChat).Muted {
		t.Fatalf("the mute lapsed a millisecond early")
	}
	c.advance(time.Millisecond)
	if chatOf(t, s, phoneChat).Muted || list() {
		t.Fatalf("the mute still read muted after its end")
	}
	// The row still says muted: only the read lapses it.
	var muted int
	if err := s.db.QueryRow(`SELECT muted FROM chats WHERE chat_id = ?`, phoneChat).Scan(&muted); err != nil || muted != 1 {
		t.Fatalf("stored muted = %d (%v)", muted, err)
	}
}

func TestAForeverMuteStaysMutedAndAnUnmuteClearsBoth(t *testing.T) {
	s, c := clockedStore(t)
	put(t, s, text(phoneChat, "3EB0Z0002", base, "one"))
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true)}); err != nil {
		t.Fatal(err)
	}
	c.advance(24 * 365 * time.Hour)
	if !chatOf(t, s, phoneChat).Muted {
		t.Fatalf("a mute for ever lapsed")
	}
	// A timed mute replaces it, then an unmute clears the muted flag and the end.
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true), MuteEndMs: c.now.Add(time.Hour).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	// A write that says nothing about the mute keeps both.
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Title: ptr("Invented")}); err != nil {
		t.Fatal(err)
	}
	var muted int
	var end int64
	row := func() {
		t.Helper()
		if err := s.db.QueryRow(`SELECT muted, mute_end_ms FROM chats WHERE chat_id = ?`, phoneChat).Scan(&muted, &end); err != nil {
			t.Fatal(err)
		}
	}
	row()
	if muted != 1 || end != c.now.Add(time.Hour).UnixMilli() {
		t.Fatalf("an unrelated write changed the mute: muted %d, end %d", muted, end)
	}
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(false), MuteEndMs: c.now.Add(time.Hour).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	row()
	if muted != 0 || end != 0 || chatOf(t, s, phoneChat).Muted {
		t.Fatalf("after an unmute: muted %d, end %d", muted, end)
	}
	// A forever mute after a timed one clears the end.
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true), MuteEndMs: c.now.Add(time.Hour).UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true)}); err != nil {
		t.Fatal(err)
	}
	row()
	if muted != 1 || end != 0 {
		t.Fatalf("a mute for ever left an end: muted %d, end %d", muted, end)
	}
}

func TestNextMuteEndIsTheSoonestStillToCome(t *testing.T) {
	s, c := clockedStore(t)
	put(t, s, text(phoneChat, "3EB0Z0003", base, "one"))
	put(t, s, text(otherChat, "3EB0Z0004", base, "two"))
	if _, ok, err := s.NextMuteEnd(net); err != nil || ok {
		t.Fatalf("with no timed mute: ok %v, %v", ok, err)
	}
	soon, later := c.now.Add(2*time.Hour).UnixMilli(), c.now.Add(9*time.Hour).UnixMilli()
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true), MuteEndMs: later}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(net, ChatInfo{ID: otherChat, Muted: ptr(true), MuteEndMs: soon}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := s.NextMuteEnd(net); err != nil || !ok || got != soon {
		t.Fatalf("NextMuteEnd = %d, %v, %v; want %d", got, ok, err, soon)
	}
	c.advance(2 * time.Hour)
	if got, ok, err := s.NextMuteEnd(net); err != nil || !ok || got != later {
		t.Fatalf("after the first lapsed NextMuteEnd = %d, %v, %v; want %d", got, ok, err, later)
	}
	c.advance(7 * time.Hour)
	if _, ok, err := s.NextMuteEnd(net); err != nil || ok {
		t.Fatalf("with every mute lapsed: ok %v, %v", ok, err)
	}
}

func TestAStoreFromBeforeMuteEndsKeepsItsMutes(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, text(phoneChat, "3EB0Z0005", base, "one"))
	put(t, s, text(otherChat, "3EB0Z0006", base, "two"))
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Muted: ptr(true)}); err != nil {
		t.Fatal(err)
	}
	// A schema-7 store: no mute_end_ms column.
	for _, q := range []string{
		`ALTER TABLE chats DROP COLUMN mute_end_ms`,
		`UPDATE meta SET value = '7' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening a schema-7 store: %v", err)
	}
	defer s.Close()
	if !chatOf(t, s, phoneChat).Muted || chatOf(t, s, otherChat).Muted {
		t.Fatalf("the upgrade changed which chats are muted")
	}
	var end int64
	if err := s.db.QueryRow(`SELECT mute_end_ms FROM chats WHERE chat_id = ?`, phoneChat).Scan(&end); err != nil || end != 0 {
		t.Fatalf("an upgraded mute has end %d (%v), want 0 for ever", end, err)
	}
}
