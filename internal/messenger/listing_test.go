package messenger

import (
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// The folders a listing pages: the inbox (parent -1) in sync group 1 and in
// sync group 95, where message requests live.
var (
	inboxOne = folderKey{group: 1, parent: -1}
	inbox95  = folderKey{group: 95, parent: -1}
)

// The gate's counts cover his whole inbox: the messages page's first
// listing, then every older page of each folder's threads, fetched through
// the pace until the folder ends; a folder that never ends stops at
// folderPages and says so.
func TestThreadsArePagedUntilTheListingEnds(t *testing.T) {
	h := newHarness(t)
	at := h.f.clock.Now().Add(-48 * time.Hour)
	h.f.folders[inboxOne] = []*table.LSTable{
		threadsPage(inboxOne, true, thread(2000055, table.ENCRYPTED_OVER_WA_ONE_TO_ONE, at, "inbox")),
		threadsPage(inboxOne, true, thread(8800000000000003, table.GROUP_THREAD, at.Add(-time.Hour), "inbox")),
		// the first one-to-one again, as a later page may repeat it: counted once
		threadsPage(inboxOne, false, thread(2000055, table.ENCRYPTED_OVER_WA_ONE_TO_ONE, at.Add(-2*time.Hour), "inbox")),
	}
	h.signIn(t)
	eventually(t, "the listing to end", func() bool { return h.m.GateCounts().(GateCounts).ListComplete })
	c := h.m.GateCounts().(GateCounts)
	// three pages of the inbox, and one (empty) of sync group 95's
	if c.ListPages != 4 || c.ListCapped {
		t.Fatalf("pages %d, capped %v; want 4 and not capped", c.ListPages, c.ListCapped)
	}
	if c.EncryptedOneToOnes != 4 || c.Groups != 3 {
		t.Fatalf("encrypted one-to-ones %d, groups %d; want 4 and 3 (the first listing's and the pages')", c.EncryptedOneToOnes, c.Groups)
	}
	h.f.mu.Lock()
	var inboxAsks int
	for _, task := range h.f.threadTasks {
		if task.SyncGroup == 1 && task.ParentThreadKey == -1 {
			inboxAsks++
		}
	}
	h.f.mu.Unlock()
	if inboxAsks != 3 {
		t.Fatalf("asked the inbox for %d pages, want 3 (none after HasMoreBefore went false)", inboxAsks)
	}

	endless := newHarness(t)
	endless.f.endless = true
	endless.signIn(t)
	eventually(t, "the listing to stop", func() bool {
		c := endless.m.GateCounts().(GateCounts)
		return c.ListComplete || c.ListCapped
	})
	c = endless.m.GateCounts().(GateCounts)
	if !c.ListCapped || c.ListComplete || c.ListPages != 2*folderPages {
		t.Fatalf("an endless listing: pages %d, capped %v, complete %v; want %d (each folder's cap), capped", c.ListPages, c.ListCapped, c.ListComplete, 2*folderPages)
	}
	if c.FolderStops["capped"] != 2 {
		t.Fatalf("folder stops %v, want both folders capped", c.FolderStops)
	}
}

// Every folder a Messenger client shows is listed, not only the inbox in
// sync group 1: sync group 95 (message requests), every parent the
// messages page names, and every folder thread a listing shows. The
// breakdown says, in counts only, where each thread was found and how many
// were last active over a year ago.
func TestEveryFolderIsListed(t *testing.T) {
	h := newHarness(t)
	now := h.f.clock.Now()
	const marketplace int64 = -10 // an invented second parent the page names
	const community int64 = 6600000000000001
	h.f.table.LSThreadsRangesQuery = []*table.LSThreadsRangesQuery{{ParentThreadKey: -1}, {ParentThreadKey: marketplace}}
	h.f.table.LSUpsertFolder = []*table.LSUpsertFolder{{ThreadKey: community, ThreadName: canary + " community"}}
	request := thread(2000066, table.ONE_TO_ONE, now.Add(-24*time.Hour), "pending")
	spam := thread(2000077, table.ONE_TO_ONE, now.Add(-25*time.Hour), "spam")
	archived := thread(8800000000000009, table.GROUP_THREAD, now.Add(-400*24*time.Hour), "archived")
	h.f.folders[inbox95] = []*table.LSTable{threadsPage(inbox95, true, request), threadsPage(inbox95, false, spam)}
	h.f.folders[inboxOne] = []*table.LSTable{threadsPage(inboxOne, false, archived)}
	sale := thread(5500000000000001, table.MARKETPLACE, now.Add(-26*time.Hour), "inbox")
	h.f.folders[folderKey{1, marketplace}] = []*table.LSTable{threadsPage(folderKey{1, marketplace}, false, sale)}
	cg := thread(8800000000000010, table.COMMUNITY_GROUP, now.Add(-27*time.Hour), "community")
	h.f.folders[folderKey{1, community}] = []*table.LSTable{threadsPage(folderKey{1, community}, false, cg)}

	h.signIn(t)
	eventually(t, "the listing to end", func() bool { return h.m.GateCounts().(GateCounts).ListComplete })

	asked := map[folderKey]bool{}
	h.f.mu.Lock()
	for _, task := range h.f.threadTasks {
		asked[folderKey{int64(task.SyncGroup), task.ParentThreadKey}] = true
		if task.SyncGroup == 1 && task.Cursor != "cursor-invented-1" {
			t.Errorf("a sync group 1 page went without the group's cursor")
		}
	}
	h.f.mu.Unlock()
	for _, f := range []folderKey{inboxOne, inbox95, {1, marketplace}, {95, marketplace}, {1, community}} {
		if !asked[f] {
			t.Errorf("folder (group %d, parent %d) was never listed", f.group, f.parent)
		}
	}
	// Sync group 95 only under the parents the messages page names, as the
	// library's own first fetch: never under a folder thread.
	if asked[folderKey{95, community}] {
		t.Errorf("sync group 95 was listed under a folder thread")
	}
	h.f.mu.Lock()
	history, posted := len(h.f.messageTasks), h.f.posted
	h.f.mu.Unlock()
	// The gate only counts: no history is fetched for it (review I4).
	if history != 0 {
		t.Errorf("the gate fetched %d pages of history", history)
	}
	// Each page's answer goes back to the library for its sync cursors.
	if posted == 0 {
		t.Errorf("no thread page was handed back to the library")
	}

	c := h.m.GateCounts().(GateCounts)
	if c.ByFolder["pending"] != 1 || c.ByFolder["spam"] != 1 || c.ByFolder["archived"] != 1 || c.ByFolder["community"] != 1 {
		t.Errorf("by folder %v, want one each of pending, spam, archived and community", c.ByFolder)
	}
	if c.ByType["marketplace"] != 1 || c.ByType["communityGroup"] != 1 || c.ByType["oneToOne"] < 2 {
		t.Errorf("by type %v", c.ByType)
	}
	if c.BySyncGroup["95"] != 2 {
		t.Errorf("by sync group %v, want two threads in 95", c.BySyncGroup)
	}
	if c.OlderThanYear != 1 || c.OlderThanYearByType["group"] != 1 {
		t.Errorf("older than a year %d (%v), want the one archived group", c.OlderThanYear, c.OlderThanYearByType)
	}
	if c.OtherParents != 2 || c.InOtherParents != 2 {
		t.Errorf("other parents %d holding %d threads, want 2 and 2", c.OtherParents, c.InOtherParents)
	}
	b, _ := json.Marshal(c)
	for _, id := range []string{"6600000000000001", "-10", "2000066", canary} {
		if containsText(b, id) {
			t.Errorf("the counts hold %q", id)
		}
	}

	// A spam thread is listed and counted, but never becomes a chat, as the
	// library's own connector leaves it.
	if _, ok, _ := h.st.Chat(Name, "2000077@msgr"); ok {
		t.Errorf("a spam thread became a chat")
	}
	if _, ok, _ := h.st.Chat(Name, "2000066@msgr"); !ok {
		t.Errorf("a message request did not become a chat")
	}
}

// His FBID is not written to the store while nothing reads it (review M4).
func TestHisFBIDIsNotKept(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	if _, ok, _ := h.st.NetworkValue(Name, "viewer"); ok {
		t.Fatalf("his FBID was kept in the store")
	}
}
