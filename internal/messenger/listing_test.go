package messenger

import (
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// The gate's counts cover his whole inbox: the messages page's first
// listing, then every older page of threads, fetched through the pace until
// the listing ends; a listing that never ends stops at maxThreadPages and
// says so.
func TestThreadsArePagedUntilTheListingEnds(t *testing.T) {
	h := newHarness(t)
	one := func(key int64, tt table.ThreadType) *table.LSTable {
		return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: key, ThreadType: tt}}}
	}
	h.f.pages = []threadPage{
		{&socket.KeyStoreData{HasMoreBefore: true, MinThreadKey: 500}, one(2000055, table.ENCRYPTED_OVER_WA_ONE_TO_ONE)},
		{&socket.KeyStoreData{HasMoreBefore: true, MinThreadKey: 400}, one(8800000000000003, table.GROUP_THREAD)},
		// the first one-to-one again, as a later page may repeat it: counted once
		{&socket.KeyStoreData{HasMoreBefore: false, MinThreadKey: 300}, one(2000055, table.ENCRYPTED_OVER_WA_ONE_TO_ONE)},
	}
	h.signIn(t)
	eventually(t, "the listing to end", func() bool { return h.m.GateCounts().(GateCounts).ListComplete })
	c := h.m.GateCounts().(GateCounts)
	if c.ListPages != 3 || c.ListCapped {
		t.Fatalf("pages %d, capped %v; want 3 and not capped", c.ListPages, c.ListCapped)
	}
	if c.EncryptedOneToOnes != 4 || c.Groups != 3 {
		t.Fatalf("encrypted one-to-ones %d, groups %d; want 4 and 3 (the first listing's and the pages')", c.EncryptedOneToOnes, c.Groups)
	}
	h.f.mu.Lock()
	fetches := h.f.fetches
	h.f.mu.Unlock()
	if fetches != 3 {
		t.Fatalf("asked for %d pages, want 3 (none after HasMoreBefore went false)", fetches)
	}

	endless := newHarness(t)
	endless.f.endless = true
	endless.signIn(t)
	eventually(t, "the listing to stop", func() bool {
		c := endless.m.GateCounts().(GateCounts)
		return c.ListComplete || c.ListCapped
	})
	c = endless.m.GateCounts().(GateCounts)
	if !c.ListCapped || c.ListComplete || c.ListPages != maxThreadPages {
		t.Fatalf("an endless listing: pages %d, capped %v, complete %v; want %d, capped", c.ListPages, c.ListCapped, c.ListComplete, maxThreadPages)
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
