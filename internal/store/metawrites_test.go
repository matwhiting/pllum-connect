package store

import "testing"

// What a Meta network's writes need of the store (Instagram, Task A3).
// Every ID and text here is invented.

func TestAnUnknownSendIsNeitherSendingNorFailed(t *testing.T) {
	s := openTest(t)
	o := Outgoing{ID: "otid:7000000000000001", ChatID: phoneChat, Kind: "message", Client: "key-1", MadeMs: base}
	if err := s.PutOutgoing(net, o); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOutgoingState(net, o.ID, OutUnknown, "refused"); err != nil {
		t.Fatalf("SetOutgoingState(unknown) = %v", err)
	}
	got, ok, err := s.Outgoing(net, o.ID)
	if err != nil || !ok || got.State != OutUnknown || got.Fail != "" {
		t.Errorf("the row is %+v (ok %v, err %v), want unknown with no fail kind", got, ok, err)
	}
	if sending, _ := s.OutgoingSending(net); len(sending) != 0 {
		t.Errorf("an unknown send is still listed as sending: %+v", sending)
	}
	// Its key still names it: a send asked again with the key is this one.
	if byKey, ok, _ := s.OutgoingClient(net, "key-1"); !ok || byKey.ID != o.ID {
		t.Errorf("the key no longer names the unknown send: %+v, %v", byKey, ok)
	}
	// A late answer still settles it.
	if err := s.SetOutgoingState(net, o.ID, OutSent, ""); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := s.Outgoing(net, o.ID); got.State != OutSent {
		t.Errorf("after settling the row is %q", got.State)
	}
}

func TestLineAfterIsTheNextLineInTheChat(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "MID0001", base+1000, "first"))
	put(t, s, text(phoneChat, "MID0002", base+2000, "second"))
	put(t, s, text(otherChat, "MID0009", base+1500, "elsewhere"))
	put(t, s, text(phoneChat, "MID0003", base+3000, "third"))

	next, ok, err := s.LineAfter(net, phoneChat, "MID0001")
	if err != nil || !ok || next.ID != "MID0002" {
		t.Errorf("LineAfter(first) = %+v, %v, %v; want the second", next, ok, err)
	}
	if next, ok, _ := s.LineAfter(net, phoneChat, "MID0002"); !ok || next.ID != "MID0003" {
		t.Errorf("LineAfter(second) = %+v, %v; want the third", next, ok)
	}
	if next, ok, err := s.LineAfter(net, phoneChat, "MID0003"); ok || err != nil {
		t.Errorf("LineAfter(newest) = %+v, %v, %v; want none", next, ok, err)
	}
	if _, ok, err := s.LineAfter(net, phoneChat, "MID-NONE"); ok || err != nil {
		t.Errorf("LineAfter(a line not held) = %v, %v; want none", ok, err)
	}
}

func TestOwnReactionIsHisOnly(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "MID0001", base+1000, "first"))
	if err := s.SetReaction(net, phoneChat, "MID0001", Reaction{Emoji: "👍", SenderID: phoneChat}); err != nil {
		t.Fatal(err)
	}
	if e, ok, err := s.OwnReaction(net, phoneChat, "MID0001"); ok || err != nil {
		t.Errorf("OwnReaction with only theirs = %q, %v, %v; want none", e, ok, err)
	}
	if err := s.SetReaction(net, phoneChat, "MID0001", Reaction{Emoji: "❤️", SenderID: "self", FromMe: true}); err != nil {
		t.Fatal(err)
	}
	if e, ok, err := s.OwnReaction(net, phoneChat, "MID0001"); e != "❤️" || !ok || err != nil {
		t.Errorf("OwnReaction = %q, %v, %v; want his heart", e, ok, err)
	}
}
