package store

import (
	"database/sql"
	"testing"
)

// CopiedLine is one line of Beeper's copy, as SeedCopied writes it.
type CopiedLine struct {
	BeeperID        string
	NetworkID       string // the network's own ID Beeper named; "" for none
	FromMe          bool
	SenderID        string // "" is "beeper-sender"
	TimeMs          int64
	Text            string
	Kind            string // "" is text
	Deleted         bool
	ReplyToBeeperID string
	Media           []Media
	Reactions       []Reaction
}

// SeedCopied writes lines of Beeper's copy the way the retired import left
// them, so tests can hold what a store from before the switch holds: the
// chat made if new (placed, or for chatID "" the read-only
// unplaced:<beeperChatID>), each line a beeper:<Beeper ID> row of source
// beeper carrying its Beeper message, and the chat refreshed. It returns
// the chat written into.
//
// Tests only: outside a test binary it refuses (internal) and writes
// nothing, so the helper itself never makes a copied line again.
func (s *Store) SeedCopied(network, chatID, beeperChatID string, lines []CopiedLine) (string, error) {
	if !testing.Testing() {
		return "", errInternal
	}
	if network == "" || beeperChatID == "" {
		return "", errBadRequest
	}
	placed := chatID != ""
	chat := chatID
	if !placed {
		chat = unplacedPrefix + beeperChatID
	}
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO chats (network, chat_id, kind, placed, read_only, beeper_chat_id)
				VALUES (?, ?, 'single', ?, ?, ?)`, network, chat, placed, !placed, beeperChatID); err != nil {
			return err
		}
		for _, l := range lines {
			var text, reply any
			if l.Text != "" && !l.Deleted {
				text = l.Text
			}
			if l.ReplyToBeeperID != "" {
				reply = beeperPrefix + l.ReplyToBeeperID
			}
			sender := l.SenderID
			if sender == "" {
				sender = "beeper-sender"
			}
			media, err := mediaJSON(l.Media)
			if err != nil {
				return err
			}
			var rid int64
			if err := tx.QueryRow(`INSERT INTO messages
					(network, chat_id, msg_id, sender_id, from_me, ts_ms, text, kind, deleted, reply_to, media_json,
					 source, beeper_id, network_id)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'beeper', ?, NULLIF(?, ''))
					RETURNING rid`,
				network, chat, beeperPrefix+l.BeeperID, sender, l.FromMe, l.TimeMs, text, kindOr(l.Kind), l.Deleted,
				reply, media, l.BeeperID, l.NetworkID).Scan(&rid); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO beeper_ids (rid, network, beeper_id, beeper_chat_id) VALUES (?, ?, ?, ?)`,
				rid, network, l.BeeperID, beeperChatID); err != nil {
				return err
			}
			for _, r := range l.Reactions {
				if _, err := tx.Exec(`INSERT OR IGNORE INTO reactions (network, chat_id, msg_id, sender_id, emoji, from_me)
						VALUES (?, ?, ?, ?, ?, ?)`, network, chat, beeperPrefix+l.BeeperID, r.SenderID, r.Emoji, r.FromMe); err != nil {
					return err
				}
			}
		}
		if err := resolveReplies(tx, network, chat); err != nil {
			return err
		}
		return refresh(tx, network, chat)
	})
	if err != nil {
		return "", err
	}
	return chat, nil
}
