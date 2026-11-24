package store

import (
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"
)

// plan is SQLite's query plan for q, one detail a line.
func plan(t *testing.T, s *Store, q string, args ...any) string {
	t.Helper()
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN `+q, args...)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, detail)
	}
	return strings.Join(out, "\n")
}

// ChatOfLine looks the line up by its ID through an index, not by walking
// every line of the network and sorting (review C11).
func TestChatOfLineIsAnIndexedLookup(t *testing.T) {
	s := openTest(t)
	p := plan(t, s, chatOfLineQuery, net, "3EB0L0001")
	if !strings.Contains(p, "messages_id") || strings.Contains(p, "TEMP B-TREE") {
		t.Fatalf("ChatOfLine plan:\n%s", p)
	}
}

// The first line by rid answers when two chats hold the same ID.
func TestChatOfLinePicksTheFirstRow(t *testing.T) {
	s := openTest(t)
	put(t, s, text(otherChat, "3EB0L0001", base, "first"))
	put(t, s, text(phoneChat, "3EB0L0001", base+1, "second"))
	if chat, ok := s.ChatOfLine(net, "3EB0L0001"); !ok || chat != otherChat {
		t.Fatalf("ChatOfLine = %q, %v; want %q", chat, ok, otherChat)
	}
}

// A store from before messages_id gains it on its next open, with its rows
// and schema version as they were.
func TestOldStoreGainsTheLineIDIndex(t *testing.T) {
	dir := t.TempDir() + "/connect"
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, s, text(phoneChat, "3EB0L0001", base, "kept"))
	if _, err := s.db.Exec(`DROP INDEX messages_id`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	s.Close()

	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 'messages_id'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("messages_id: %d, %v", n, err)
	}
	var v string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&v); err != nil || v != "10" {
		t.Fatalf("schema = %q, %v; want 10", v, err)
	}
	if chat, ok := s.ChatOfLine(net, "3EB0L0001"); !ok || chat != phoneChat {
		t.Fatalf("ChatOfLine = %q, %v", chat, ok)
	}
}

// The store keeps a small pool of connections (each a SQLite instance of its
// own) and reuses them: a burst of reads beside writes neither opens one per
// request nor waits for ever (review C10).
func TestPoolIsBoundedAndKeptWarm(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0P0001", base, "hello"))
	if got := s.db.Stats().MaxOpenConnections; got != maxConns {
		t.Fatalf("MaxOpenConnections = %d; want %d", got, maxConns)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for i := range 32 {
			wg.Go(func() {
				if _, ok, err := s.Chat(net, phoneChat); err != nil || !ok {
					t.Errorf("Chat: %v %v", ok, err)
				}
			})
			if i%8 == 0 {
				wg.Go(func() {
					// A write that also reads through the pool while it holds
					// its own connection.
					err := s.write(func(tx *sql.Tx) error {
						var n int
						return s.db.QueryRow(`SELECT count(*) FROM chats`).Scan(&n)
					})
					if err != nil {
						t.Errorf("write: %v", err)
					}
				})
			}
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a burst of reads beside writes never finished")
	}
	st := s.db.Stats()
	if st.OpenConnections > maxConns {
		t.Fatalf("OpenConnections = %d; want at most %d", st.OpenConnections, maxConns)
	}
	if st.MaxIdleClosed > 0 {
		t.Fatalf("closed %d idle connections; want them kept", st.MaxIdleClosed)
	}
}
