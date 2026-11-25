package instagram

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every ID and text here is invented.

func TestEveryRequestIsCountedInTraffic(t *testing.T) {
	h := newHarness(t)
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(true, "older-t", textMsg("mid.$invented-t1", aliceFBI, start.Add(-time.Hour), "x")), nil
	}
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	ended, calls := h.g.traffic.Ended(), h.m.mark()

	h.send(network.Outgoing{Text: "invented hello"})
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); err != nil {
		t.Fatalf("FetchOlder: %v", err)
	}
	// The send asks for the thread's ID first: three requests in all.
	got := h.m.namesSince(calls)
	if !slices.Contains(got, "SendMessage") || !slices.Contains(got, "PaginateMessages") {
		t.Fatalf("Meta was asked %v, want a send and a page", got)
	}
	if n := h.g.traffic.Ended() - ended; n != int64(len(got)) {
		t.Errorf("traffic saw %d ends of %d requests %v", n, len(got), got)
	}
	if !h.g.traffic.Quiet(h.clock.Now(), 0) {
		t.Errorf("a request is still counted in flight")
	}

	// A request the pace refuses never reaches Meta, and is not counted.
	h.g.pace.Fail(wire.RateLimited)
	ended = h.g.traffic.Ended()
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); kindErr(err) != wire.RateLimited {
		t.Fatalf("FetchOlder under a rate limit = %v, want rateLimited", err)
	}
	if n := h.g.traffic.Ended() - ended; n != 0 {
		t.Errorf("traffic saw %d ends of a refused request, want none", n)
	}
}
