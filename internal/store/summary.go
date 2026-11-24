package store

import (
	"database/sql"
	"errors"
)

// summarySender starts the sender key of a reaction known only as a count per
// emoji (LinkedIn's reaction summaries name no one): the key is this prefix
// and the emoji, so a line holds one such reaction per emoji, and reads send
// its senderID empty. No network's own sender ID starts with a unit
// separator.
const summarySender = "\x1fsummary:"

// SetReactionSummary replaces the reactions known only as counts on a line
// (or the line an alias's chat holds) with summary: one per emoji, with no
// sender, FromMe when he is among those who reacted. Reactions with a sender
// are left alone. The line need not be held yet.
func (s *Store) SetReactionSummary(network, chatID, msgID string, summary []Reaction) error {
	if network == "" || chatID == "" || msgID == "" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		return setReactionSummary(tx, network, chatID, msgID, summary)
	})
}

// setReactionSummary is SetReactionSummary in tx.
func setReactionSummary(tx *sql.Tx, network, chatID, msgID string, summary []Reaction) error {
	chat, err := resolve(tx, network, chatID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM reactions WHERE network = ? AND chat_id = ? AND msg_id = ?
			AND substr(sender_id, 1, ?) = ?`,
		network, chat, msgID, len(summarySender), summarySender); err != nil {
		return err
	}
	for _, r := range summary {
		if r.Emoji == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO reactions (network, chat_id, msg_id, sender_id, emoji, from_me)
				VALUES (?, ?, ?, ?, ?, ?)`, network, chat, msgID, summarySender+r.Emoji, r.Emoji, r.FromMe); err != nil {
			return err
		}
	}
	return nil
}

// SetSummaryEmoji sets one emoji of a line's reaction counts: present adds or
// keeps it (with fromMe), and false removes it.
func (s *Store) SetSummaryEmoji(network, chatID, msgID, emoji string, present, fromMe bool) error {
	if network == "" || chatID == "" || msgID == "" || emoji == "" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, chatID)
		if err != nil {
			return err
		}
		if !present {
			_, err = tx.Exec(`DELETE FROM reactions WHERE network = ? AND chat_id = ? AND msg_id = ? AND sender_id = ?`,
				network, chat, msgID, summarySender+emoji)
			return err
		}
		_, err = tx.Exec(`INSERT OR REPLACE INTO reactions (network, chat_id, msg_id, sender_id, emoji, from_me)
				VALUES (?, ?, ?, ?, ?, ?)`, network, chat, msgID, summarySender+emoji, emoji, fromMe)
		return err
	})
}

// SeenThroughAt marks his lines in a chat (or the chat an alias names) sent
// at or before throughMs as seen at seenMs (throughMs when 0), for a network
// whose receipt names the newest line seen and when. A line already seen
// keeps its time.
func (s *Store) SeenThroughAt(network, chatID string, throughMs, seenMs int64) error {
	if network == "" || chatID == "" || throughMs <= 0 || seenMs < 0 {
		return errBadRequest
	}
	if seenMs == 0 {
		seenMs = throughMs
	}
	return s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, chatID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE messages SET seen_ms = ?
				WHERE network = ? AND chat_id = ? AND from_me = 1 AND early = 0
					AND ts_ms <= ? AND seen_ms IS NULL`,
			seenMs, network, chat, throughMs)
		return err
	})
}

// ChatOfLine is the chat holding the line msgID, for a network whose events
// name a line but not its chat; ok is false when no chat holds it.
func (s *Store) ChatOfLine(network, msgID string) (chatID string, ok bool) {
	if s.ready() != nil || network == "" || msgID == "" {
		return "", false
	}
	return chatOfLine(s.db, network, msgID)
}

// chatOfLineQuery finds a line's chat by its ID alone, through messages_id.
const chatOfLineQuery = `SELECT chat_id FROM messages WHERE network = ? AND msg_id = ? ORDER BY rid LIMIT 1`

// chatOfLine is ChatOfLine read through q (the store, or a batch's
// transaction, which sees its own writes).
func chatOfLine(q querier, network, msgID string) (chatID string, ok bool) {
	err := q.QueryRow(chatOfLineQuery, network, msgID).Scan(&chatID)
	if errors.Is(err, sql.ErrNoRows) || err != nil {
		return "", false
	}
	return chatID, true
}
