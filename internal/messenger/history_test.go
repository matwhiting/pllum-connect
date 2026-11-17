package messenger

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// Review Focus 4: a line restored from his encrypted backup and also
// received live is one row. The backup is read paced (2 to 4 s apart),
// page by page from the newest back, a few pages a chat, each chat's
// progress kept; a page that does not reach back is held and never asked
// again the same way, and a later sign-in asks nothing it has had.
func TestRestoredHistoryAndLiveMessagesShowOnce(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	t0 := time.Date(2026, 2, 10, 8, 0, 0, 0, time.UTC)
	t1, t2 := t0.Add(time.Hour), t0.Add(2*time.Hour)
	alice, bob := fmt.Sprint(aliceFBID)+"@msgr", fmt.Sprint(bobFBID)+"@msgr"

	edit := backup("WAINVENTED42-edit", selfFBID, t2.Add(time.Minute), editContent("WAINVENTED42", canary+" edited"))
	react := backup("WAINVENTED42-react", aliceFBID, t2.Add(2*time.Minute), reactionContent("WAINVENTED42", "❤"))
	h.f.history.script(fmt.Sprint(aliceFBID),
		fakePage{before: t1.UnixMilli(), more: true, msgs: []*securehistory.DecryptedMessage{
			archived("WAINVENTED41", aliceFBID, t1, textContent(canary+" restored")),
			archived("WAINVENTED42", selfFBID, t2, textContent(canary+" mine"), edit, react),
		}},
		fakePage{more: false, msgs: []*securehistory.DecryptedMessage{
			archived("WAINVENTED40", aliceFBID, t0, imageContent("")),
		}},
	)
	// Bob's page does not reach back past where it was asked from.
	h.f.history.script(fmt.Sprint(bobFBID), fakePage{more: true, unmoved: true,
		msgs: []*securehistory.DecryptedMessage{archived("WAINVENTED50", bobFBID, t0, textContent("x"))}})

	h.signIn(t)
	// His own line arrives live first, as he sent it.
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(selfFBID), "WAINVENTED42", t2, textContent(canary+" mine"), ""))
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.waitValue(t, "ebackfilled")
	h.settle()

	lines := h.lines(t, alice)
	var ids []string
	for id := range lines {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if want := []string{"WAINVENTED40", "WAINVENTED41", "WAINVENTED42"}; !slices.Equal(ids, want) {
		t.Fatalf("Alice's chat holds %v, want %v", ids, want)
	}
	mine := lines["WAINVENTED42"]
	if !mine.FromMe || mine.Text != canary+" edited" || !mine.Edited {
		t.Errorf("his line, live and restored, reads %+v", mine)
	}
	if len(mine.Reactions) != 1 || mine.Reactions[0].Emoji != "❤" || mine.Reactions[0].SenderID != fmt.Sprint(aliceFBID) {
		t.Errorf("the restored reaction reads %+v", mine.Reactions)
	}
	if l := lines["WAINVENTED41"]; l.Source != store.SourceSync || l.Text != canary+" restored" || l.FromMe {
		t.Errorf("a restored line reads %+v", l)
	}
	if l := lines["WAINVENTED40"]; l.Kind != "photo" {
		t.Errorf("the restored photo reads %+v", l)
	}
	if raw, err := h.st.Raw(Name, alice, "WAINVENTED40"); err != nil || len(raw) == 0 {
		t.Errorf("the restored photo kept no raw (err %v)", err)
	}
	if _, ok := h.line(t, bob, "WAINVENTED50"); !ok {
		t.Errorf("Bob's restored line is missing")
	}

	asks := h.f.history.asked(fmt.Sprint(aliceFBID))
	if len(asks) != 2 || asks[1] != t1.UnixMilli() || asks[0] <= t1.UnixMilli() {
		t.Errorf("Alice's backup was asked from %v, want from now, then from %d", asks, t1.UnixMilli())
	}
	if n := len(h.f.history.asked(fmt.Sprint(bobFBID))); n != 1 {
		t.Errorf("Bob's backup was asked %d times, want 1", n)
	}
	gaps(t, h.f.requests())
	if c := h.chat(t, alice); c.HistoryComplete {
		t.Errorf("Alice's chat is complete though her older thread may hold more")
	}

	// Reading to the oldest line asks nothing more: Alice's backup is done
	// and her older thread has no line to anchor on; Bob's is held.
	before := h.f.history.total()
	for _, chat := range []string{alice, bob} {
		if _, err := h.m.FetchOlder(context.Background(), chat); err != nil {
			t.Errorf("FetchOlder(%s): %v", chat, err)
		}
	}
	if after := h.f.history.total(); after != before {
		t.Errorf("FetchOlder asked the backup %d more times, want none", after-before)
	}

	// A later sign-in asks nothing it has had.
	h.login(t, network.Session{"xs": "xs-invented-2", "c_user": "2000001", "datr": "datr-invented-1"})
	h.waitValue(t, "backfilled")
	h.settle()
	if after := h.f.history.total(); after != before {
		t.Errorf("a later sign-in asked the backup %d more times, want none", after-before)
	}
}
