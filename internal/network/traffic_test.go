package network

import (
	"context"
	"testing"
	"time"
)

var trafficStart = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

func TestTrafficIsQuietOnlyWhenNothingIsInFlight(t *testing.T) {
	var tr Traffic
	if !tr.Quiet(trafficStart, 10*time.Second) {
		t.Fatalf("a network that has asked nothing is not quiet")
	}
	end := tr.Begin(context.Background(), trafficStart)
	if tr.Quiet(trafficStart, 0) {
		t.Fatalf("quiet with a request in flight")
	}
	if tr.Quiet(trafficStart.Add(time.Hour), 0) {
		t.Fatalf("quiet with a request in flight an hour on")
	}
	at := trafficStart.Add(2 * time.Second)
	end(at)
	if tr.Quiet(at.Add(9*time.Second), 10*time.Second) {
		t.Errorf("quiet 9 s after a request ended, want not before 10 s")
	}
	if !tr.Quiet(at.Add(10*time.Second), 10*time.Second) {
		t.Errorf("not quiet 10 s after a request ended")
	}
	if tr.Ended() != 1 {
		t.Errorf("%d requests ended, want 1", tr.Ended())
	}
	// An end called twice counts once.
	end(at.Add(time.Minute))
	if tr.Ended() != 1 || !tr.Quiet(at.Add(10*time.Second), 10*time.Second) {
		t.Errorf("a second end moved the traffic: %d ended", tr.Ended())
	}
}

func TestTrafficQuietIgnoresTheQueuesOwnRequests(t *testing.T) {
	var tr Traffic
	end := tr.Begin(WithBacklog(context.Background()), trafficStart)
	if tr.Quiet(trafficStart.Add(time.Second), 10*time.Second) {
		t.Fatalf("quiet while the queue's own request is in flight")
	}
	at := trafficStart.Add(3 * time.Second)
	end(at)
	if !tr.Quiet(at.Add(time.Second), 10*time.Second) {
		t.Errorf("the queue's own request kept the network from being quiet")
	}
}

func TestTrafficCountsTheQueuesRequests(t *testing.T) {
	var tr Traffic
	q := WithBacklog(context.Background())
	if !IsBacklog(q) || IsBacklog(context.Background()) {
		t.Fatalf("IsBacklog does not tell the queue's context")
	}
	if !IsBacklog(context.WithoutCancel(q)) {
		t.Fatalf("a context made from the queue's is not the queue's")
	}
	tr.Begin(q, trafficStart)(trafficStart)
	tr.Begin(context.Background(), trafficStart)(trafficStart)
	tr.Begin(q, trafficStart)(trafficStart)
	if n := tr.BacklogAsked(); n != 2 {
		t.Errorf("BacklogAsked = %d, want 2", n)
	}
	if n := tr.Ended(); n != 3 {
		t.Errorf("Ended = %d, want 3", n)
	}
}
