package linkedin

import (
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// The next chat to page is the newest conversation with history left, past
// read-only chats, finished histories, chats set aside and other networks'
// IDs, even beyond the first 200 chats; the newest stored activity is a
// conversation's (review C2).
func TestTheNextChatToPageIsFoundPastTheFirstPage(t *testing.T) {
	r := newRig(t)
	yes := true
	put := func(id string, at time.Time, c store.ChatInfo) {
		t.Helper()
		c.ID, c.LastActivityMs = id, at.UnixMilli()
		if err := r.st.PutChat(Name, c); err != nil {
			t.Fatal(err)
		}
	}
	// Newest first: a chat that is no conversation, then 250 conversations
	// whose history is done one way or another, then the one with history
	// left, then an older one with history left.
	put("invented-not-a-conversation", start, store.ChatInfo{})
	for i := range 250 {
		c := store.ChatInfo{}
		switch i % 3 {
		case 0:
			c.ReadOnly = &yes
		case 1:
			c.HistoryComplete = &yes
		}
		id := convURN(1000 + i)
		put(id, start.Add(-time.Duration(i+1)*time.Minute), c)
		if i%3 == 2 {
			_ = r.l.keep("histDone."+id, "1")
		}
	}
	want := convURN(2000)
	put(want, start.Add(-300*time.Minute), store.ChatInfo{})
	put(convURN(2001), start.Add(-301*time.Minute), store.ChatInfo{})

	if got, ok := r.l.nextHistoryChat(); !ok || got != want {
		t.Fatalf("nextHistoryChat = %q, %v; want %q", got, ok, want)
	}
	if at, ok := r.l.newestStored(); !ok || !at.Equal(start.Add(-time.Minute).Truncate(time.Millisecond)) {
		t.Fatalf("newestStored = %v, %v", at, ok)
	}
	// A fresh mark on a read-only chat is passed over; one on a writable
	// chat is found.
	_ = r.l.keep("fresh."+convURN(1000), "1")
	_ = r.l.keep("fresh."+convURN(2001), "1")
	chat, ok := r.l.nextChat(store.ChatFilter{Writable: true}, func(c string) bool { return r.l.value("fresh."+c) != "" })
	if !ok || chat != convURN(2001) {
		t.Fatalf("fresh chat = %q, %v", chat, ok)
	}
}
