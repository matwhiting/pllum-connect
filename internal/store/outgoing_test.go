package store

import (
	"bytes"
	"path/filepath"
	"testing"
)

// The outbox keeps each write a network makes, under the ID it made before
// sending, so a send cut off by a crash is sent again with that ID.

func TestTheOutboxKeepsAWriteAndItsStateAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "c"), testKey)
	if err != nil {
		t.Fatal(err)
	}
	o := Outgoing{ID: "3EB0O0001", ChatID: phoneChat, Kind: "message", MadeMs: base, Request: []byte(`{"text":"invented"}`)}
	if err := s.PutOutgoing(net, o); err != nil {
		t.Fatalf("PutOutgoing: %v", err)
	}
	if err := s.PutOutgoing(net, o); err == nil {
		t.Errorf("the same ID was kept twice")
	}
	r := Outgoing{ID: "3EB0O0002", ChatID: groupChat, Kind: "reaction", Txn: "txn-1", MadeMs: base + 1, Message: []byte("built")}
	if err := s.PutOutgoing(net, r); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOutgoingMessage(net, o.ID, []byte("now built")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	if s, err = Open(filepath.Join(dir, "c"), testKey); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sending, err := s.OutgoingSending(net)
	if err != nil || len(sending) != 2 || sending[0].ID != o.ID || sending[1].ID != r.ID {
		t.Fatalf("OutgoingSending = %+v, %v; want both, oldest first", sending, err)
	}
	got := sending[0]
	if got.State != OutSending || got.ChatID != phoneChat || got.Kind != "message" || got.MadeMs != base ||
		!bytes.Equal(got.Request, o.Request) || !bytes.Equal(got.Message, []byte("now built")) {
		t.Errorf("the row came back as %+v", got)
	}
	if byTxn, ok, err := s.OutgoingTxn(net, "txn-1"); err != nil || !ok || byTxn.ID != r.ID {
		t.Errorf("OutgoingTxn = %+v, %v, %v", byTxn, ok, err)
	}
	if _, ok, _ := s.OutgoingTxn(net, "txn-2"); ok {
		t.Errorf("an unknown transaction was found")
	}

	if err := s.SetOutgoingState(net, o.ID, OutSent, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetOutgoingState(net, r.ID, OutFailed, "refused"); err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := s.Outgoing(net, r.ID); !ok || got.State != OutFailed || got.Fail != "refused" {
		t.Errorf("the failed row is %+v", got)
	}
	if got, ok, _ := s.Outgoing(net, o.ID); !ok || got.State != OutSent || got.Fail != "" {
		t.Errorf("the sent row is %+v", got)
	}
	if sending, _ := s.OutgoingSending(net); len(sending) != 0 {
		t.Errorf("rows still sending: %+v", sending)
	}
	if _, ok, _ := s.Outgoing("other", o.ID); ok {
		t.Errorf("another network sees the row")
	}
	if err := s.SetOutgoingState(net, o.ID, "lost", ""); err == nil {
		t.Errorf("an unknown state was kept")
	}
}

func TestLineAndUnreadAreWhatAWriteNeeds(t *testing.T) {
	s := openTest(t)
	photo := text(groupChat, "3EB0L0001", base+1000, "caption")
	photo.Kind, photo.SenderID, photo.Raw = "photo", "10000000001@s.whatsapp.net", []byte("raw bytes")
	photo.Media = []Media{{Kind: "photo", HasFile: true}}
	put(t, s, photo)
	b := text(groupChat, "3EB0L0002", base+2000, "two")
	b.SenderID = "10000000002@s.whatsapp.net"
	put(t, s, b)
	put(t, s, mine(groupChat, "3EB0L0003", base+3000, "mine"))
	hidden := text(groupChat, "3EB0L0004", base+4000, "")
	hidden.Kind, hidden.Hidden = "notice", ptr(true)
	put(t, s, hidden)
	put(t, s, text(groupChat, "3EB0L0005", base+5000, "five"))

	l, ok, err := s.Line(net, groupChat, "3EB0L0001")
	if err != nil || !ok || l.SenderID != photo.SenderID || l.FromMe || l.TimeMs != base+1000 ||
		l.Kind != "photo" || l.Text != "caption" || !bytes.Equal(l.Raw, photo.Raw) {
		t.Fatalf("Line = %+v, %v, %v", l, ok, err)
	}
	if _, ok, _ := s.Line(net, groupChat, "3EB0L0099"); ok {
		t.Errorf("a line nobody holds was found")
	}

	newest, ok, err := s.NewestIncoming(net, groupChat)
	if err != nil || !ok || newest.ID != "3EB0L0005" {
		t.Errorf("NewestIncoming = %+v, %v, %v", newest, ok, err)
	}
	un, err := s.Unread(net, groupChat, base+2000)
	if err != nil || len(un) != 2 || un[0].ID != "3EB0L0001" || un[1].ID != "3EB0L0002" {
		t.Fatalf("Unread through two = %+v, %v", un, err)
	}
	ms := base + 1000
	if err := s.PutChat(net, ChatInfo{ID: groupChat, ReadThroughMs: &ms}); err != nil {
		t.Fatal(err)
	}
	un, _ = s.Unread(net, groupChat, base+9000)
	if len(un) != 2 || un[0].ID != "3EB0L0002" || un[1].ID != "3EB0L0005" {
		t.Errorf("Unread after reading one = %+v: want the incoming shown lines after the marker", un)
	}
}

func TestASchema4StoreGainsTheOutboxsWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`DROP INDEX outgoing_txn_once`,
		`ALTER TABLE outgoing DROP COLUMN fail`,
		`ALTER TABLE outgoing DROP COLUMN request`,
		`ALTER TABLE outgoing DROP COLUMN message`,
		`UPDATE meta SET value = '4' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	s.Close()
	if s, err = Open(dir, testKey); err != nil {
		t.Fatalf("a schema 4 store did not open: %v", err)
	}
	defer s.Close()
	if err := s.PutOutgoing(net, Outgoing{ID: "3EB0O0009", ChatID: phoneChat, Kind: "message", MadeMs: base, Request: []byte("r")}); err != nil {
		t.Fatalf("PutOutgoing after the upgrade: %v", err)
	}
	if err := s.SetOutgoingState(net, "3EB0O0009", OutFailed, "refused"); err != nil {
		t.Fatal(err)
	}
	if o, ok, err := s.Outgoing(net, "3EB0O0009"); err != nil || !ok || o.Fail != "refused" || string(o.Request) != "r" {
		t.Fatalf("Outgoing = %+v, %v, %v", o, ok, err)
	}
}

func TestATransactionIsKeptOnce(t *testing.T) {
	s := openTest(t)
	first := Outgoing{ID: "3EB0O0011", ChatID: phoneChat, Kind: "reaction", Txn: "txn-same", MadeMs: base}
	if err := s.PutOutgoing(net, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "3EB0O0012"
	if err := s.PutOutgoing(net, second); err == nil {
		t.Fatalf("a second row was kept for one transaction")
	}
	// Rows with no transaction, and the same transaction on another network, are fine.
	for i, o := range []Outgoing{
		{ID: "3EB0O0013", ChatID: phoneChat, Kind: "message", MadeMs: base},
		{ID: "3EB0O0014", ChatID: phoneChat, Kind: "message", MadeMs: base},
	} {
		if err := s.PutOutgoing(net, o); err != nil {
			t.Fatalf("row %d with no transaction: %v", i, err)
		}
	}
	if err := s.PutOutgoing("other", second); err != nil {
		t.Fatalf("another network's transaction: %v", err)
	}
}

func TestAClientIDIsKeptOnceAndMovesOffAFailedSend(t *testing.T) {
	s := openTest(t)
	first := Outgoing{ID: "3EB0O0021", ChatID: phoneChat, Kind: "message", Client: "client-same", MadeMs: base}
	if err := s.PutOutgoing(net, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "3EB0O0022"
	if err := s.PutOutgoing(net, second); err == nil {
		t.Fatalf("a second row was kept for one client ID while the first is sending")
	}
	if o, ok, err := s.OutgoingClient(net, "client-same"); err != nil || !ok || o.ID != "3EB0O0021" || o.Client != "client-same" {
		t.Fatalf("OutgoingClient = %+v, %v, %v", o, ok, err)
	}
	// Once the first failed, the key moves to a new row.
	if err := s.SetOutgoingState(net, "3EB0O0021", OutFailed, "refused"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutOutgoing(net, second); err != nil {
		t.Fatalf("a new row for a failed send's key: %v", err)
	}
	if o, ok, _ := s.OutgoingClient(net, "client-same"); !ok || o.ID != "3EB0O0022" {
		t.Fatalf("the key names %+v", o)
	}
	if o, _, _ := s.Outgoing(net, "3EB0O0021"); o.Client != "" || o.State != OutFailed {
		t.Fatalf("the failed row is %+v", o)
	}
	if _, ok, err := s.OutgoingClient(net, ""); ok || err != nil {
		t.Fatalf("no key found a row")
	}
}

func TestASchema6StoreGainsTheClientID(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`DROP INDEX outgoing_client_once`,
		`ALTER TABLE outgoing DROP COLUMN client_id`,
		`UPDATE meta SET value = '6' WHERE key = 'schema'`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	s.Close()
	if s, err = Open(dir, testKey); err != nil {
		t.Fatalf("a schema 6 store did not open: %v", err)
	}
	defer s.Close()
	o := Outgoing{ID: "3EB0O0031", ChatID: phoneChat, Kind: "message", Client: "client-up", MadeMs: base}
	if err := s.PutOutgoing(net, o); err != nil {
		t.Fatalf("PutOutgoing after the upgrade: %v", err)
	}
	o.ID = "3EB0O0032"
	if err := s.PutOutgoing(net, o); err == nil {
		t.Fatalf("the upgraded store kept two rows for one client ID")
	}
}
