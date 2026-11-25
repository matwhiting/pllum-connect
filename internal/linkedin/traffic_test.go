package linkedin

import (
	"context"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every ID and text here is invented.

func TestEveryRequestIsCountedInTraffic(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	r.arrive(c, text(1, 5, person(aliceID, "Alice", "Invented"), start.Add(time.Minute), "invented hello"))
	ended, calls := r.l.traffic.Ended(), len(r.f.callsNow())

	id := r.send(network.Outgoing{ChatID: chat, Text: "invented reply"})
	r.waitSend(id, network.SendSent, "")
	if err := r.l.MarkRead(context.Background(), chat, ""); err != nil {
		t.Fatalf("MarkRead: %v", err)
	}
	if got := r.f.namesSince(calls); len(got) != 2 {
		t.Fatalf("LinkedIn was asked %v, want a send and a mark", got)
	}
	if n := r.l.traffic.Ended() - ended; n != 2 {
		t.Errorf("traffic saw %d ends, want 2", n)
	}
	if !r.l.traffic.Quiet(r.clock.Now(), 0) {
		t.Errorf("a request is still counted in flight")
	}

	// A request the pause refuses never reaches LinkedIn, and is not
	// counted.
	r.l.pause.start()
	ended = r.l.traffic.Ended()
	if err := r.l.MarkUnread(context.Background(), chat); wire.KindOf(err) != wire.RateLimited {
		t.Fatalf("MarkUnread during a pause = %v, want rateLimited", err)
	}
	if n := r.l.traffic.Ended() - ended; n != 0 {
		t.Errorf("traffic saw %d ends of a refused request, want none", n)
	}
}
