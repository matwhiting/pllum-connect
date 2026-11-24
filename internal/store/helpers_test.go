package store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// Every name, number, JID, ID and text in these tests is invented.

const net = "whatsapp"

// testKey is an invented store key: 32 bytes counting up from one.
var testKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(i + 1)
	}
	return k
}()

// otherKey is a second invented key.
var otherKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(0xF0 - i)
	}
	return k
}()

const (
	phoneChat = "10000000001@s.whatsapp.net"
	otherChat = "10000000002@s.whatsapp.net"
	groupChat = "120000000000000001@g.us"
	lidChat   = "100000000000001@lid"
)

// base is an invented moment: 2026-01-01T00:00:00Z.
const base int64 = 1767225600000

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "connect"), testKey)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func put(t *testing.T, s *Store, m Message) bool {
	t.Helper()
	if m.Kind == "" {
		m.Kind = "text"
	}
	if m.Source == "" {
		m.Source = SourceLive
	}
	if m.SenderID == "" {
		if m.FromMe {
			m.SenderID = "self"
		} else {
			m.SenderID = m.ChatID
		}
	}
	added, err := s.PutMessage(net, m)
	if err != nil {
		t.Fatalf("PutMessage: %v", err)
	}
	return added
}

func text(chat, id string, ms int64, body string) Message {
	return Message{ID: id, ChatID: chat, TimeMs: ms, Text: body, Kind: "text"}
}

func mine(chat, id string, ms int64, body string) Message {
	m := text(chat, id, ms, body)
	m.FromMe = true
	return m
}

func chatOf(t *testing.T, s *Store, id string) Chat {
	t.Helper()
	c, ok, err := s.Chat(net, id)
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if !ok {
		t.Fatalf("no chat %q", id)
	}
	return c
}

// all reads every message of a chat, oldest first.
func all(t *testing.T, s *Store, chat string) []Message {
	t.Helper()
	var out []Message
	before := ""
	for range 1000 {
		p, err := s.Messages(net, chat, before, 200)
		if err != nil {
			t.Fatalf("Messages: %v", err)
		}
		out = append(p.Items, out...)
		if !p.HasMore {
			return out
		}
		before = p.OldestCursor
	}
	t.Fatalf("Messages never ended")
	return nil
}

func ids(ms []Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func importOne(t *testing.T, s *Store, in ImportBatch) ImportResult {
	t.Helper()
	r, err := s.Import(net, in)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	return r
}

func beeperMsg(id string, ms int64, body string) ImportMessage {
	return ImportMessage{BeeperID: id, TimeMs: ms, Text: body, Kind: "text", SenderID: "beeper-sender"}
}

func idN(prefix string, i int) string { return fmt.Sprintf("%s%04d", prefix, i) }

func lineIDs(t *testing.T, s *Store, beeperIDs []string) map[string]string {
	t.Helper()
	m, err := s.LineIDs(net, beeperIDs)
	if err != nil {
		t.Fatalf("LineIDs: %v", err)
	}
	return m
}

func beeperChats(t *testing.T, s *Store) map[string]string {
	t.Helper()
	m, err := s.BeeperChats(net)
	if err != nil {
		t.Fatalf("BeeperChats: %v", err)
	}
	return m
}

func ptr[T any](v T) *T { return &v }

// copyLine is one line of Beeper's copy as a store made before the switch
// holds it.
type copyLine struct {
	beeperID  string
	networkID string // the network's own ID Beeper named, "" for none
	fromMe    bool
	ms        int64
	text      string
	kind      string // "" is text
	deleted   bool
	replyTo   string // a Beeper ID
	reactions []Reaction
}

// seedCopies writes lines of Beeper's copy straight in SQL, as the retired
// import left them: chat made if new (placed; or, for chat "", the
// read-only unplaced:<beeperChat>), and each line a beeper:<id> row of
// source beeper carrying its Beeper message. Nothing writes such rows any
// more; a store from before the switch still holds them. It returns the
// chat written into.
func seedCopies(tb testing.TB, s *Store, chat, beeperChat string, lines ...copyLine) string {
	tb.Helper()
	placed := chat != ""
	if !placed {
		chat = unplacedPrefix + beeperChat
	}
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO chats (network, chat_id, kind, placed, read_only, beeper_chat_id)
				VALUES (?, ?, 'single', ?, ?, ?)`, net, chat, placed, !placed, beeperChat); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO beeper_chats (network, beeper_chat_id, chat_id) VALUES (?, ?, ?)`,
			net, beeperChat, chat); err != nil {
			return err
		}
		for _, l := range lines {
			var text, reply any
			if l.text != "" && !l.deleted {
				text = l.text
			}
			if l.replyTo != "" {
				reply = beeperPrefix + l.replyTo
			}
			var rid int64
			if err := tx.QueryRow(`INSERT INTO messages
					(network, chat_id, msg_id, sender_id, from_me, ts_ms, text, kind, deleted, reply_to,
					 source, beeper_id, network_id)
					VALUES (?, ?, ?, 'beeper-sender', ?, ?, ?, ?, ?, ?, 'beeper', ?, NULLIF(?, ''))
					RETURNING rid`,
				net, chat, beeperPrefix+l.beeperID, l.fromMe, l.ms, text, kindOr(l.kind), l.deleted, reply,
				l.beeperID, l.networkID).Scan(&rid); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO beeper_ids (rid, network, beeper_id, beeper_chat_id) VALUES (?, ?, ?, ?)`,
				rid, net, l.beeperID, beeperChat); err != nil {
				return err
			}
			for _, r := range l.reactions {
				if _, err := tx.Exec(`INSERT OR IGNORE INTO reactions (network, chat_id, msg_id, sender_id, emoji, from_me)
						VALUES (?, ?, ?, ?, ?, ?)`, net, chat, beeperPrefix+l.beeperID, r.SenderID, r.Emoji, r.FromMe); err != nil {
					return err
				}
			}
		}
		if err := resolveReplies(tx, net, chat); err != nil {
			return err
		}
		return refresh(tx, net, chat)
	})
	if err != nil {
		tb.Fatalf("seedCopies: %v", err)
	}
	return chat
}
