package network

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/testkit"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// newChatsOn is a NewChats over a real store's values for network, on clock.
func newChatsOn(t *testing.T, st *store.Store, clock Clock, perDay int) *NewChats {
	t.Helper()
	return &NewChats{Values: NewValues(st, "whatsapp"), Clock: clock, PerDay: perDay}
}

func openNewChatsStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "connect"), [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestNewChatsCapsADayAndKeepsIt(t *testing.T) {
	st := openNewChatsStore(t)
	clock := testkit.NewClock(time.Date(2026, 6, 10, 9, 0, 0, 0, time.Local))
	n := newChatsOn(t, st, clock, 2)
	if n.Left() != 2 {
		t.Fatalf("Left = %d at the day's start, want 2", n.Left())
	}
	for i := range 2 {
		if err := n.Take(); err != nil {
			t.Fatalf("Take %d = %v", i+1, err)
		}
	}
	if err := n.Take(); wire.KindOf(err) != wire.DailyCap {
		t.Fatalf("a third Take = %v, want dailyCap", err)
	}
	if n.Left() != 0 {
		t.Errorf("Left = %d past the cap", n.Left())
	}
	// A new helper on the same values still refuses.
	again := newChatsOn(t, st, clock, 2)
	if err := again.Take(); wire.KindOf(err) != wire.DailyCap {
		t.Fatalf("after a restart Take = %v, want dailyCap", err)
	}
	// The next local day passes again.
	clock.Advance(time.Date(2026, 6, 11, 0, 0, 1, 0, time.Local).Sub(clock.Now()))
	if err := again.Take(); err != nil {
		t.Fatalf("the next day Take = %v", err)
	}
	if again.Left() != 1 {
		t.Errorf("Left = %d after one the next day, want 1", again.Left())
	}
}

func TestNewChatsHoldADayAfterAPushback(t *testing.T) {
	st := openNewChatsStore(t)
	clock := testkit.NewClock(time.Date(2026, 6, 10, 9, 0, 0, 0, time.Local))
	n := newChatsOn(t, st, clock, 10)
	n.Hold(wire.RateLimited)
	for _, d := range []time.Duration{0, time.Hour, 22 * time.Hour} {
		clock.Advance(d)
		if err := n.Take(); wire.KindOf(err) != wire.NewChatHold {
			t.Fatalf("%v into the hold Take = %v, want newChatHold", d, err)
		}
	}
	// Across a new helper too, and a held Take counts nothing.
	again := newChatsOn(t, st, clock, 10)
	if err := again.Take(); wire.KindOf(err) != wire.NewChatHold {
		t.Fatalf("after a restart Take = %v, want newChatHold", err)
	}
	if again.Left() != 0 {
		t.Errorf("Left = %d during a hold, want 0", again.Left())
	}
	if v := again.Values.Get(newChatDayKey); v != "" {
		t.Errorf("held Takes counted: %s is %q", newChatDayKey, v)
	}
	clock.Advance(time.Hour)
	if again.Left() != 10 {
		t.Errorf("held Takes counted: Left = %d after the hold, want 10", again.Left())
	}
	if err := again.Take(); err != nil {
		t.Fatalf("after the day Take = %v", err)
	}
}
