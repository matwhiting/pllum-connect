package store

import (
	"path/filepath"
	"testing"
)

// Every ID and value here is invented.

func TestANetworkKeepsItsOwnValues(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "connect")
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.NetworkValue("instagram", "seq"); ok || err != nil {
		t.Fatalf("a value nothing kept: ok %v, err %v", ok, err)
	}
	if err := s.PutNetworkValue("instagram", "seq", "4242"); err != nil {
		t.Fatal(err)
	}
	if err := s.PutNetworkValue("messenger", "seq", "7"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := s.NetworkValue("instagram", "seq"); v != "4242" || !ok || err != nil {
		t.Errorf("NetworkValue = %q, %v, %v; want 4242", v, ok, err)
	}
	// It is the network's own: another's of the same name is apart, and the
	// store's schema version is out of reach.
	if v, _, _ := s.NetworkValue("messenger", "seq"); v != "7" {
		t.Errorf("messenger's seq = %q, want 7", v)
	}
	if err := s.PutNetworkValue("", "schema", "1"); err == nil {
		t.Errorf("a value with no network was kept")
	}
	if _, _, err := s.NetworkValue("instagram", ""); err == nil {
		t.Errorf("a value with no name was read")
	}
	// It survives a reopen, and an empty value removes it.
	s.Close()
	if s, err = Open(dir, testKey); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, ok, _ := s.NetworkValue("instagram", "seq"); v != "4242" || !ok {
		t.Errorf("after a reopen NetworkValue = %q, %v", v, ok)
	}
	if err := s.PutNetworkValue("instagram", "seq", ""); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.NetworkValue("instagram", "seq"); ok {
		t.Errorf("an empty value did not remove it")
	}
}

func TestSeenThroughMarksHisLinesUpToTheTime(t *testing.T) {
	s := openTest(t)
	mine := func(id string, ms int64) Message {
		m := text(phoneChat, id, ms, "mine")
		m.FromMe, m.SenderID = true, "self"
		return m
	}
	put(t, s, mine("3EB0S0001", base+1000))
	put(t, s, mine("3EB0S0002", base+2000))
	put(t, s, text(phoneChat, "3EB0S0003", base+1500, "theirs"))
	put(t, s, mine("3EB0S0004", base+3000))
	seenAt := func(id string) int64 {
		t.Helper()
		page, err := s.Messages(net, phoneChat, "", 50)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range page.Items {
			if m.ID == id {
				return m.SeenMs
			}
		}
		t.Fatalf("no line %s", id)
		return 0
	}
	if err := s.SeenThrough(net, phoneChat, base+2000); err != nil {
		t.Fatal(err)
	}
	if seenAt("3EB0S0001") != base+2000 || seenAt("3EB0S0002") != base+2000 {
		t.Errorf("his lines up to the time were not seen")
	}
	if seenAt("3EB0S0003") != 0 || seenAt("3EB0S0004") != 0 {
		t.Errorf("their line, or his later one, was marked seen")
	}
	// A later receipt keeps the first time.
	if err := s.SeenThrough(net, phoneChat, base+5000); err != nil {
		t.Fatal(err)
	}
	if seenAt("3EB0S0001") != base+2000 || seenAt("3EB0S0004") != base+5000 {
		t.Errorf("a later receipt moved a seen time, or missed the newer line")
	}
	if err := s.SeenThrough(net, phoneChat, 0); err == nil {
		t.Errorf("a zero time was accepted")
	}
}
