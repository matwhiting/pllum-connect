package store

import "testing"

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
