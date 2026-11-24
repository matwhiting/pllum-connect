package store

import (
	"database/sql"
	"errors"
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

func idN(prefix string, i int) string { return fmt.Sprintf("%s%04d", prefix, i) }

func ptr[T any](v T) *T { return &v }

// copyLine is one line of Beeper's copy as a store made before the switch
// holds it (CopiedLine, shorter to write).
type copyLine struct {
	beeperID  string
	networkID string // the network's own ID Beeper named, "" for none
	fromMe    bool
	ms        int64
	text      string
	kind      string // "" is text
	deleted   bool
	media     []Media
	replyTo   string // a Beeper ID
	reactions []Reaction
}

// seedCopies writes lines of Beeper's copy into chat (SeedCopied; chat ""
// is the unplaced chat of beeperChat) and returns the chat written into.
func seedCopies(tb testing.TB, s *Store, chat, beeperChat string, lines ...copyLine) string {
	tb.Helper()
	ls := make([]CopiedLine, len(lines))
	for i, l := range lines {
		ls[i] = CopiedLine{BeeperID: l.beeperID, NetworkID: l.networkID, FromMe: l.fromMe, TimeMs: l.ms,
			Text: l.text, Kind: l.kind, Deleted: l.deleted, ReplyToBeeperID: l.replyTo, Media: l.media, Reactions: l.reactions}
	}
	got, err := s.SeedCopied(net, chat, beeperChat, ls)
	if err != nil {
		tb.Fatalf("SeedCopied: %v", err)
	}
	return got
}

// fillCopy fills the early row id of chat with a line of Beeper's copy that
// named it, as the retired import did (fillEarly): the row shows at the
// copy's time and side, as copied history, and carries the Beeper message.
func fillCopy(tb testing.TB, s *Store, chat, beeperChat, id string, l copyLine) {
	tb.Helper()
	err := s.write(func(tx *sql.Tx) error {
		var rid int64
		if err := tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
			net, chat, id).Scan(&rid); err != nil {
			return err
		}
		media, err := mediaJSON(l.media)
		if err != nil {
			return err
		}
		if err := fillEarly(tx, rid, Message{FromMe: l.fromMe, SenderID: "beeper-sender", TimeMs: l.ms,
			Text: l.text, Kind: kindOr(l.kind), Hidden: ptr(false), Deleted: l.deleted}, media, id, ""); err != nil {
			return err
		}
		if err := carryIn(tx, rid, l.beeperID, beeperChat); err != nil {
			return err
		}
		return refresh(tx, net, chat)
	})
	if err != nil {
		tb.Fatalf("fillCopy: %v", err)
	}
}

// carry marks the direct line id of chat as carrying a Beeper message, as
// the retired import left a line it matched.
func carry(tb testing.TB, s *Store, chat, id, beeperID, beeperChat string) {
	tb.Helper()
	err := s.write(func(tx *sql.Tx) error {
		var rid int64
		if err := tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
			net, chat, id).Scan(&rid); err != nil {
			return err
		}
		return carryIn(tx, rid, beeperID, beeperChat)
	})
	if err != nil {
		tb.Fatalf("carry: %v", err)
	}
}

func carryIn(tx *sql.Tx, rid int64, beeperID, beeperChat string) error {
	if _, err := tx.Exec(`UPDATE messages SET beeper_id = COALESCE(beeper_id, ?) WHERE rid = ?`, beeperID, rid); err != nil {
		return err
	}
	_, err := tx.Exec(`INSERT OR IGNORE INTO beeper_ids (rid, network, beeper_id, beeper_chat_id) VALUES (?, ?, ?, ?)`,
		rid, net, beeperID, beeperChat)
	return err
}

// carrier is the ID of the line that carries a Beeper message now (a line
// in a placed chat first), "" for none: where the copied message lives on.
func carrier(tb testing.TB, s *Store, beeperID string) string {
	tb.Helper()
	var id string
	err := s.db.QueryRow(`SELECT m.msg_id FROM beeper_ids AS b
			JOIN messages AS m ON m.rid = b.rid
			JOIN chats AS c ON c.network = m.network AND c.chat_id = m.chat_id
			WHERE b.network = ? AND b.beeper_id = ?
			ORDER BY c.placed DESC, m.rid ASC LIMIT 1`, net, beeperID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		tb.Fatalf("carrier: %v", err)
	}
	return id
}
