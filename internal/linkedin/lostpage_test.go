package linkedin

import (
	"path/filepath"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// A history page the store loses (a write that fails in SQLite) keeps none
// of its lines and does not move the chat's cursor past it: the same page is
// asked again once the chat's turn aside is over, and lands then (quality
// review of Batch B, minor 1).
func TestAHistoryPageTheStoreLosesIsAskedAgain(t *testing.T) {
	r := newRig(t)
	historyFeed(r, 400)

	// An invented failure: the store cannot insert one line of the page
	// starting at hist-invented-4, so that page's batch is lost.
	raw, err := store.OpenDevice(filepath.Join(r.dir, "messages.db"), testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(`CREATE TRIGGER invented_failure BEFORE INSERT ON messages
			WHEN NEW.msg_id = '` + msgURN(5004) + `' BEGIN SELECT RAISE(ABORT, 'invented failure'); END`); err != nil {
		t.Fatal(err)
	}

	r.live()
	if got := r.value("cursor." + convURN(1)); got != "hist-invented-4" {
		t.Fatalf("the cursor moved to %q past a page the store lost", got)
	}
	for _, id := range []int{5004, 5005} {
		if _, ok := r.st.LineState(Name, convURN(1), msgURN(id)); ok {
			t.Fatalf("a lost page kept line %d", id)
		}
	}
	if r.chat(convURN(1)).HistoryComplete || r.value("histDone."+convURN(1)) != "" {
		t.Fatal("a lost page ended the chat's history")
	}

	if _, err := raw.Exec(`DROP TRIGGER invented_failure`); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		r.clock.Advance(asideCap)
	}
	eventually(t, "the lost page asked again, and the history paged to its start", func() bool {
		c, ok, _ := r.st.Chat(Name, convURN(1))
		return ok && c.HistoryComplete
	})
	if _, ok := r.st.LineState(Name, convURN(1), msgURN(5004)); !ok {
		t.Fatal("the page asked again did not land")
	}
}
