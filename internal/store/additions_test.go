package store

import "testing"

// Every ID, name and text here is invented.

func TestChatByParticipantFindsTheOneToOneNotTheGroup(t *testing.T) {
	s := openTest(t)
	person := Participant{ID: "ig-1001", Name: "Alex Example"}
	me := Participant{ID: "ig-self", IsSelf: true}
	put(t, s, text(phoneChat, "3EB0P0001", base, "hi"))
	put(t, s, text(groupChat, "3EB0P0002", base+1, "group hi"))
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Participants: []Participant{me, person}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(net, ChatInfo{ID: groupChat, Kind: "group", Participants: []Participant{me, person, {ID: "ig-1002"}}}); err != nil {
		t.Fatal(err)
	}

	c, ok, err := s.ChatByParticipant(net, "ig-1001")
	if err != nil || !ok || c.ID != phoneChat || len(c.Participants) != 2 {
		t.Fatalf("ChatByParticipant = %+v, %v, %v; want the one-to-one with both participants", c, ok, err)
	}
	if _, ok, _ := s.ChatByParticipant(net, "ig-1002"); ok {
		t.Errorf("a person who is only in a group has a one-to-one")
	}
	if _, ok, _ := s.ChatByParticipant(net, "ig-self"); ok {
		t.Errorf("himself is nobody's one-to-one")
	}
	if _, ok, _ := s.ChatByParticipant(net, "ig-nobody"); ok {
		t.Errorf("an unknown person has a one-to-one")
	}
	if _, ok, _ := s.ChatByParticipant("instagram", "ig-1001"); ok {
		t.Errorf("another network's person answered")
	}
	if _, _, err := s.ChatByParticipant(net, ""); err == nil {
		t.Errorf("an empty participant ID was accepted")
	}
}

func TestChatByParticipantTakesTheMostRecentlyActive(t *testing.T) {
	s := openTest(t)
	person := Participant{ID: "ig-1001"}
	put(t, s, text(phoneChat, "3EB0Q0001", base, "older"))
	put(t, s, text(otherChat, "3EB0Q0002", base+5000, "newer"))
	for _, id := range []string{phoneChat, otherChat} {
		if err := s.PutChat(net, ChatInfo{ID: id, Participants: []Participant{person}}); err != nil {
			t.Fatal(err)
		}
	}
	if c, ok, err := s.ChatByParticipant(net, "ig-1001"); err != nil || !ok || c.ID != otherChat {
		t.Fatalf("got %+v, %v, %v; want the newer chat", c, ok, err)
	}
}

func TestTheLineIDOfASendIsKeptAndSchemaNineGainsTheColumn(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, text(phoneChat, "3EB0L0001", base, "hi"))
	if err := s.PutOutgoing(net, Outgoing{ID: "made-1", ChatID: phoneChat, Kind: "message", MadeMs: base}); err != nil {
		t.Fatal(err)
	}
	if o, _, _ := s.Outgoing(net, "made-1"); o.LineID != "" {
		t.Fatalf("a new send has line ID %q", o.LineID)
	}
	if err := s.SetOutgoingLine(net, "made-1", "line-77"); err != nil {
		t.Fatal(err)
	}
	if o, ok, err := s.Outgoing(net, "made-1"); err != nil || !ok || o.LineID != "line-77" {
		t.Fatalf("Outgoing = %+v, %v, %v; want line-77", o, ok, err)
	}

	// A schema-9 store: no line_id column. It keeps its sends and gains the column.
	for _, q := range []string{
		`ALTER TABLE outgoing DROP COLUMN line_id`,
		`UPDATE meta SET value = '9' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	s.Close()
	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening a schema-9 store: %v", err)
	}
	defer s.Close()
	if o, ok, err := s.Outgoing(net, "made-1"); err != nil || !ok || o.LineID != "" {
		t.Fatalf("after the upgrade Outgoing = %+v, %v, %v", o, ok, err)
	}
	if err := s.SetOutgoingLine(net, "made-1", "line-78"); err != nil {
		t.Fatalf("the upgraded store has no line_id: %v", err)
	}
}
