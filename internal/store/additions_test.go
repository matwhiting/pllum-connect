package store

import (
	"database/sql"
	"testing"
)

// Every ID, name and text here is invented.

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

func TestAParticipantKeepsItsHandle(t *testing.T) {
	const ig = "instagram"
	const chat, person = "340282366841710300000000000000001", "17840000000000001"
	s := openTest(t)
	if err := s.PutChat(ig, ChatInfo{ID: chat, Participants: []Participant{
		{ID: person, Name: "Pat Example", Handle: "pllum.test"}, {ID: "self", IsSelf: true}}}); err != nil {
		t.Fatal(err)
	}
	c := chatIn(t, s, ig, chat)
	if len(c.Participants) != 2 || c.Participants[0].Handle != "pllum.test" || c.Participants[1].Handle != "" {
		t.Fatalf("participants = %+v", c.Participants)
	}
	// A later write that names the person without a handle keeps the one held.
	if err := s.PutChat(ig, ChatInfo{ID: chat, Participants: []Participant{
		{ID: person, Name: "Pat Example"}, {ID: "self", IsSelf: true}}}); err != nil {
		t.Fatal(err)
	}
	if c := chatIn(t, s, ig, chat); c.Participants[0].Handle != "pllum.test" {
		t.Fatalf("after a write with no handle, participants = %+v", c.Participants)
	}
	// A new handle replaces it.
	if err := s.PutChat(ig, ChatInfo{ID: chat, Participants: []Participant{{ID: person, Handle: "pllum.renamed"}}}); err != nil {
		t.Fatal(err)
	}
	if c := chatIn(t, s, ig, chat); len(c.Participants) != 1 || c.Participants[0].Handle != "pllum.renamed" {
		t.Fatalf("after a new handle, participants = %+v", c.Participants)
	}
}

func TestAStoreFromSchemaTenGainsHandles(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(net, ChatInfo{ID: groupChat, Kind: "group", Participants: []Participant{
		{ID: phoneChat, Name: "Ana"}, {ID: otherChat, Name: "Ben"}}}); err != nil {
		t.Fatal(err)
	}
	// A schema-10 store: no handle column. It keeps its participants and gains the column, every row null.
	for _, q := range []string{
		`ALTER TABLE participants DROP COLUMN handle`,
		`UPDATE meta SET value = '10' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	s.Close()
	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening a schema-10 store: %v", err)
	}
	defer s.Close()
	var version string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&version); err != nil || version != "11" {
		t.Fatalf("schema = %q, %v; want 11", version, err)
	}
	rows, err := s.db.Query(`SELECT handle FROM participants`)
	if err != nil {
		t.Fatalf("the upgraded store has no participants.handle: %v", err)
	}
	n := 0
	for rows.Next() {
		var h sql.NullString
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		if h.Valid {
			t.Errorf("an upgraded row has handle %q", h.String)
		}
		n++
	}
	rows.Close()
	if n != 2 {
		t.Fatalf("the upgraded store holds %d participants, want 2", n)
	}
	if c := chatOf(t, s, groupChat); len(c.Participants) != 2 || c.Participants[0].Handle != "" {
		t.Fatalf("participants = %+v", c.Participants)
	}
}

func TestAChatMergeKeepsHandles(t *testing.T) {
	const ig = "instagram"
	const first, later = "340282366841710300000000000000001", "340282366841710300000000000000002"
	s := openTest(t)
	if err := s.PutChat(ig, ChatInfo{ID: first, Participants: []Participant{{ID: "17840000000000001", Name: "Pat Example"},
		{ID: "17840000000000002", Name: "Sam Example"}, {ID: "17840000000000004", Handle: "invented.before"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChat(ig, ChatInfo{ID: later, Participants: []Participant{
		{ID: "17840000000000001", Handle: "pllum.test"}, {ID: "17840000000000003", Handle: "invented.user"},
		{ID: "17840000000000004", Handle: "invented.after"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Alias(ig, later, first); err != nil {
		t.Fatalf("Alias: %v", err)
	}
	c := chatIn(t, s, ig, first)
	got := map[string]string{}
	for _, p := range c.Participants {
		got[p.ID] = p.Handle
	}
	// Where both chats hold a different handle, the chat merged in (the later
	// one) wins: it is the newer name.
	want := map[string]string{"17840000000000001": "pllum.test", "17840000000000002": "", "17840000000000003": "invented.user",
		"17840000000000004": "invented.after"}
	if len(got) != len(want) {
		t.Fatalf("the merged chat's participants = %+v", c.Participants)
	}
	for id, h := range want {
		if got[id] != h {
			t.Errorf("participant %s has handle %q, want %q", id, got[id], h)
		}
	}
}

// chatIn reads one chat of a network.
func chatIn(t *testing.T, s *Store, network, id string) Chat {
	t.Helper()
	c, ok, err := s.Chat(network, id)
	if err != nil || !ok {
		t.Fatalf("Chat(%s, %s): %v, %v", network, id, ok, err)
	}
	return c
}
