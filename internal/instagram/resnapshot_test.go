package instagram

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// resnapshotHarness is signed in and live, with one line from Alice that he
// has read; pages answers every PaginateMessages from then on.
func resnapshotHarness(t *testing.T) (h *harness, old *slidetypes.Message, pages func(func(igChatID string) (*slidetypes.PaginateMessagesResponse, error))) {
	t.Helper()
	h = newHarness(t)
	old = textMsg("mid.$invented-old", aliceFBI, start.Add(-time.Hour), "invented old line")
	var mu sync.Mutex
	answer := func(string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(false, "", old), nil
	}
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		mu.Lock()
		f := answer
		mu.Unlock()
		return f(igChatID)
	}
	pages = func(f func(string) (*slidetypes.PaginateMessagesResponse, error)) {
		mu.Lock()
		answer = f
		mu.Unlock()
	}
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	_ = h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 101, Timestamp: start})
	if h.lastState() != "live" {
		t.Fatalf("not live: %s", h.lastState())
	}
	read := old.TimestampMS.UnixMilli()
	if err := h.st.PutChat(Name, store.ChatInfo{ID: aliceChat, ReadThroughMs: &read}); err != nil {
		t.Fatal(err)
	}
	if c := h.chat(aliceChat); c.Unread != 0 || c.LastMessageID != old.ID {
		t.Fatalf("before: unread %d, last %s", c.Unread, c.LastMessageID)
	}
	return h, old, pages
}

func TestAResnapshotFetchesWhatCameWhileTheSocketWasBehind(t *testing.T) {
	h, old, pages := resnapshotHarness(t)

	// While the socket was behind, Alice wrote: the inbox shows her thread
	// active since, and its newest page holds the new line.
	h.clock.Advance(10 * time.Minute)
	newer := textMsg("mid.$invented-new", aliceFBI, h.clock.Now(), "invented new line")
	h.m.mailbox = mailboxOf(false, threadWith(aliceFBI, aliceThread, alice(), h.clock.Now()))
	pages(func(string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(false, "", newer, old), nil
	})
	connects := h.m.count("Connect")
	before := len(h.m.requests())
	_ = h.m.dispatch(&slidetypes.ResnapshotRequired{})
	h.settle()
	eventually(t, "the socket to connect again", func() bool { return h.m.count("Connect") > connects })

	if _, ok := h.lines(aliceChat)[newer.ID]; !ok {
		t.Fatalf("the line that came while the socket was behind was never fetched")
	}
	c := h.chat(aliceChat)
	if c.Unread != 1 || c.LastMessageID != newer.ID {
		t.Errorf("after the resnapshot: unread %d, last line %s; want 1, %s", c.Unread, c.LastMessageID, newer.ID)
	}
	// One inbox reload and one page, paced: no bigger than a catch-up.
	got := h.m.requests()[before:]
	if len(got) != 2 || got[0].name != "LoadIndex" || got[1].name != "PaginateMessages" {
		t.Errorf("the resnapshot made %v, want LoadIndex then one PaginateMessages", got)
	}
	gaps(t, h.m.requests())
}

func TestACatchUpCutShortIsCaughtUpLater(t *testing.T) {
	h, old, pages := resnapshotHarness(t)

	h.clock.Advance(10 * time.Minute)
	newer := textMsg("mid.$invented-new", aliceFBI, h.clock.Now(), "invented new line")
	h.m.mailbox = mailboxOf(false, threadWith(aliceFBI, aliceThread, alice(), h.clock.Now()))
	// The resnapshot's inbox reload works, but the thread's page fails.
	pages(func(string) (*slidetypes.PaginateMessagesResponse, error) {
		return nil, fmt.Errorf("%w: invented", httpclient.ErrRequestFailed)
	})
	connects := h.m.count("Connect")
	_ = h.m.dispatch(&slidetypes.ResnapshotRequired{})
	h.settle()
	eventually(t, "the socket to connect again", func() bool { return h.m.count("Connect") > connects })
	if _, ok := h.lines(aliceChat)[newer.ID]; ok {
		t.Fatalf("the line was stored though its page failed")
	}

	// The listing has moved the chat's activity on; the gap must not hide
	// behind it. The next inbox load (a new sign-in) catches it up.
	pages(func(string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(false, "", newer, old), nil
	})
	h.login(validSession())
	if _, ok := h.lines(aliceChat)[newer.ID]; !ok {
		t.Fatalf("a catch-up cut short was forgotten: the line was never fetched")
	}
	if c := h.chat(aliceChat); c.Unread != 1 || c.LastMessageID != newer.ID {
		t.Errorf("after catching up: unread %d, last line %s", c.Unread, c.LastMessageID)
	}

	// Once caught up, it is not asked again.
	pagesBefore := h.m.count("PaginateMessages")
	h.login(validSession())
	if n := h.m.count("PaginateMessages") - pagesBefore; n != 0 {
		t.Errorf("a thread already caught up was fetched %d more times", n)
	}
}
