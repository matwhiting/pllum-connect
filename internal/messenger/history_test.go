package messenger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
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

// encryptedChats are the encrypted chats the store holds.
func (h *harness) encryptedChats(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, c := range h.chatIDs(t) {
		if h.m.hasEncryptedSide(c) {
			out = append(out, c)
		}
	}
	return out
}

// Review I2 (the reviewer's probe): an answer that fails across the whole
// backup is asked at most twice at the first restore, and every request
// counts towards its cap; then each failure holds longer, and two in a row
// pause the whole backup, so a day of Pllum reading every encrypted chat
// at its oldest line asks at most once more per chat.
func TestABackupWideFailureIsAskedAtMostTwice(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	h.f.history.script("0")
	h.f.history.failWith = errors.New("archive server rejected the range request")
	h.signIn(t)
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.waitValue(t, "ebackfilled")
	h.settle()
	if n := h.f.history.total(); n > 2 {
		t.Fatalf("the first restore asked a failing backup %d times, want at most 2", n)
	}
	if got := h.m.value("ebfPages"); got != strconv.Itoa(h.f.history.total()) {
		t.Errorf("ebfPages = %q, want every request (%d) counted", got, h.f.history.total())
	}
	chats := h.encryptedChats(t)
	first := map[string]int{}
	for _, c := range chats {
		thread, _ := encThread(c)
		first[c] = len(h.f.history.asked(thread))
	}
	for range 4 {
		h.f.clock.Advance(6*time.Hour + time.Minute)
		for _, c := range chats {
			_, _ = h.m.FetchOlder(context.Background(), c)
		}
	}
	for _, c := range chats {
		thread, _ := encThread(c)
		if more := len(h.f.history.asked(thread)) - first[c]; more > 1 {
			t.Errorf("over a day, %s's failing backup was asked %d more times, want at most 1", c, more)
		}
	}
	gaps(t, h.f.requests())
}

// Review I2: Meta's definitive answers are kept. A chat with no backup is
// done (its older thread is next); a backup that is gone drops the history
// device, and nothing of the backup is asked again until his PIN restores
// it again; any other failure holds the chat longer each time, 6 h, then
// 24 h, then 7 days, across restarts.
func TestADefinitiveBackupAnswerIsKept(t *testing.T) {
	t.Run("no backup for the chat", func(t *testing.T) {
		h := newHarness(t)
		h.m.gate = false
		h.f.history.script("0")
		h.f.history.fail = map[string]error{fmt.Sprint(bobFBID): errors.New("archive thread was not found")}
		h.signIn(t)
		if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
			t.Fatal(err)
		}
		h.waitValue(t, "ebackfilled")
		h.settle()
		bob := fmt.Sprint(bobFBID) + "@msgr"
		if h.m.value("encDone."+bob) == "" {
			t.Fatalf("a chat with no backup is not done")
		}
		h.f.clock.Advance(8 * 24 * time.Hour)
		_, _ = h.m.FetchOlder(context.Background(), bob)
		if n := len(h.f.history.asked(fmt.Sprint(bobFBID))); n != 1 {
			t.Errorf("a chat with no backup was asked %d times, want once", n)
		}
	})
	t.Run("the backup is gone", func(t *testing.T) {
		h := newHarness(t)
		h.m.gate = false
		h.f.history.script("0")
		h.f.history.failWith = errors.New("archive is no longer available")
		h.signIn(t)
		if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the history device dropped", func() bool { return h.m.HistoryState() == "needsPIN" })
		h.settle()
		if n := h.f.history.total(); n != 1 {
			t.Errorf("a backup that is gone was asked %d times, want once", n)
		}
		if v, _, _ := h.st.NetworkValue(Name, "history"); v != "" {
			t.Errorf("the history device is still kept")
		}
		h.f.clock.Advance(8 * 24 * time.Hour)
		for _, c := range h.encryptedChats(t) {
			_, _ = h.m.FetchOlder(context.Background(), c)
		}
		h.login(t, network.Session{"xs": "xs-invented-3", "c_user": "2000001", "datr": "datr-invented-1"})
		if n := h.f.history.total(); n != 1 {
			t.Errorf("after it was gone, the backup was asked %d times in all, want once", n)
		}
	})
	t.Run("a failure holds longer each time", func(t *testing.T) {
		h := newHarness(t)
		h.m.gate = false
		h.f.history.script("0")
		h.signIn(t)
		if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
			t.Fatal(err)
		}
		h.waitValue(t, "ebackfilled")
		h.settle()
		alice := fmt.Sprint(aliceFBID) + "@msgr"
		_ = h.m.keep("encDone."+alice, "")
		h.f.history.mu.Lock()
		h.f.history.fail = map[string]error{fmt.Sprint(aliceFBID): errors.New("archive server rejected the range request")}
		h.f.history.mu.Unlock()
		for i, want := range []time.Duration{6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 7 * 24 * time.Hour} {
			_ = h.m.keep("epause", "") // one chat alone, not the whole backup
			_ = h.m.keep("efails", "")
			_, _ = h.m.FetchOlder(context.Background(), alice)
			until, _ := strconv.ParseInt(h.m.value("estalled."+alice), 10, 64)
			if got := time.UnixMilli(until).Sub(h.f.clock.Now()); got < want-time.Minute || got > want {
				t.Errorf("failure %d holds the chat for %v, want %v", i+1, got, want)
			}
			// Held: nothing is asked until it ends.
			n := len(h.f.history.asked(fmt.Sprint(aliceFBID)))
			_, _ = h.m.FetchOlder(context.Background(), alice)
			if len(h.f.history.asked(fmt.Sprint(aliceFBID))) != n {
				t.Errorf("a held chat was asked")
			}
			h.f.clock.Advance(want + time.Minute)
		}
	})
}

// Review M10: reading to the oldest line pages the backup a page at a time,
// at most onDemandPages a day across every chat.
func TestBackupPagesOnDemandAreCapped(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	h.f.history.script("0")
	h.signIn(t)
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatal(err)
	}
	h.waitValue(t, "ebackfilled")
	h.settle()
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	_ = h.m.keep("encDone."+alice, "")
	h.f.history.mu.Lock()
	h.f.history.endless = true
	h.f.history.mu.Unlock()
	before := h.f.history.total()
	for range onDemandPages + 20 {
		_, _ = h.m.FetchOlder(context.Background(), alice)
	}
	if n := h.f.history.total() - before; n != onDemandPages {
		t.Errorf("reading to the oldest line asked %d pages in a day, want %d", n, onDemandPages)
	}
	h.f.clock.Advance(25 * time.Hour)
	_, _ = h.m.FetchOlder(context.Background(), alice)
	if n := h.f.history.total() - before; n != onDemandPages+1 {
		t.Errorf("the next day asked %d in all, want %d", n, onDemandPages+1)
	}
}
