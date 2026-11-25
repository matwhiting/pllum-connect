package messenger

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every ID and text here is invented.

func TestEveryRequestIsCountedInTraffic(t *testing.T) {
	h := newHarness(t)
	at := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	tbl := history(groupOne, true, line("mid.$invented-t2", aliceFBID, at, "newest"))
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(groupOne, table.GROUP_THREAD, at, "inbox")}
	h.f.table = tbl
	h.f.older[groupOne] = []*table.LSTable{history(groupOne, true, line("mid.$invented-t1", selfFBID, at.Add(-48*time.Hour), "older"))}
	if err := h.st.PutNetworkValue(Name, "backfilled", "1"); err != nil {
		t.Fatal(err)
	}
	h.signIn(t)
	h.settle()
	ended := h.m.traffic.Ended()

	h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " hello"})
	if _, err := h.m.FetchOlder(context.Background(), fmt.Sprint(groupOne)); err != nil {
		t.Fatalf("FetchOlder: %v", err)
	}
	if n := h.m.traffic.Ended() - ended; n != 2 {
		t.Errorf("traffic saw %d ends, want 2 (a send and a page)", n)
	}
	if !h.m.traffic.Quiet(h.f.clock.Now(), 0) {
		t.Errorf("a request is still counted in flight")
	}

	// A request the pace refuses never reaches Meta, and is not counted.
	h.m.pace.Fail(wire.RateLimited)
	ended = h.m.traffic.Ended()
	if _, err := h.m.FetchOlder(context.Background(), fmt.Sprint(groupOne)); kindErr(err) != wire.RateLimited {
		t.Fatalf("FetchOlder under a rate limit = %v, want rateLimited", err)
	}
	if n := h.m.traffic.Ended() - ended; n != 0 {
		t.Errorf("traffic saw %d ends of a refused request, want none", n)
	}
}
