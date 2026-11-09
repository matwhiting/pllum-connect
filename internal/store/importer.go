package store

import (
	"database/sql"
	"errors"
	"strings"
)

// ImportChat is what Beeper says about the chat a batch comes from.
type ImportChat struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	Phone string `json:"phone"`
}

// ImportMedia is one attachment's details from Beeper; the file stays there.
type ImportMedia struct {
	Kind   string `json:"kind"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

// ImportMessage is one line of Beeper's copy (the protocol's ImportMessage).
type ImportMessage struct {
	BeeperID        string        `json:"beeperID"`
	NetworkID       string        `json:"networkID"`
	FromMe          bool          `json:"fromMe"`
	SenderID        string        `json:"senderID"`
	SenderName      string        `json:"senderName"`
	TimeMs          int64         `json:"timeMs"`
	Text            string        `json:"text"`
	Kind            string        `json:"kind"`
	Hidden          bool          `json:"hidden"`
	Deleted         bool          `json:"deleted"`
	ReplyToBeeperID string        `json:"replyToBeeperID"`
	Media           []ImportMedia `json:"media"`
	Reactions       []Reaction    `json:"reactions"`
}

// ImportBatch is one `import`: lines of one Beeper chat. An empty ChatID
// makes or extends the read-only chat unplaced:<BeeperChatID>; Replaces names
// an unplaced chat whose lines are dropped once this placed chat holds them.
type ImportBatch struct {
	BeeperChatID string          `json:"beeperChatID"`
	ChatID       string          `json:"chatID"`
	Chat         ImportChat      `json:"chat"`
	Messages     []ImportMessage `json:"messages"`
	Replaces     string          `json:"replaces"`
}

// ImportResult counts what an import did: lines added as Beeper lines, lines
// matched to ones already held, and lines skipped (carried before, or with
// no Beeper ID).
type ImportResult struct {
	Added   int `json:"added"`
	Matched int `json:"matched"`
	Skipped int `json:"skipped"`
}

// Counts is what the store holds for a network: numbers and one time only.
type Counts struct {
	Chats         int    `json:"chats"`
	Messages      int    `json:"messages"`
	Imported      int    `json:"imported"` // lines carrying a Beeper ID, matched or not
	Matched       int    `json:"matched"`  // of those, lines held under the network's own ID
	UnplacedChats int    `json:"unplacedChats"`
	OldestMs      *int64 `json:"oldestMs"`
}

// lineKinds are the protocol's kinds of line.
var lineKinds = map[string]bool{
	"text": true, "photo": true, "video": true, "voiceNote": true, "sticker": true, "file": true, "notice": true,
}

func importMediaKind(m ImportMessage) string {
	if len(m.Media) > 0 && m.Media[0].Kind != "" {
		return m.Media[0].Kind
	}
	return kindOr(m.Kind)
}

// Import carries one batch of Beeper's copy into the store, in one
// transaction. A line already held (by its network ID, or by side, time and
// content) gets the Beeper ID and adds nothing; any other becomes the line
// beeper:<Beeper ID>. A batch imported twice adds nothing the second time.
func (s *Store) Import(network string, in ImportBatch) (ImportResult, error) {
	var r ImportResult
	if network == "" || in.BeeperChatID == "" || strings.HasPrefix(in.ChatID, unplacedPrefix) {
		return r, errBadRequest
	}
	kind := in.Chat.Kind
	if kind == "" {
		kind = "single"
	}
	if kind != "single" && kind != "group" {
		return r, errBadRequest
	}
	for _, m := range in.Messages {
		if !lineKinds[kindOr(m.Kind)] {
			return r, errBadRequest
		}
	}
	err := s.write(func(tx *sql.Tx) error {
		r = ImportResult{}
		placed := in.ChatID != ""
		target := unplacedPrefix + in.BeeperChatID
		if placed {
			var err error
			if target, err = resolve(tx, network, in.ChatID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO chats
				(network, chat_id, kind, title, phone, placed, read_only, beeper_chat_id)
				VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?)`,
			network, target, kind, in.Chat.Title, in.Chat.Phone, placed, !placed, in.BeeperChatID); err != nil {
			return err
		}
		// The network's own names win; Beeper's fill gaps.
		if _, err := tx.Exec(`UPDATE chats SET
				beeper_chat_id = COALESCE(beeper_chat_id, ?),
				title = COALESCE(title, NULLIF(?, '')),
				phone = COALESCE(phone, NULLIF(?, ''))
			WHERE network = ? AND chat_id = ?`,
			in.BeeperChatID, in.Chat.Title, in.Chat.Phone, network, target); err != nil {
			return err
		}
		if err := mapBeeperChat(tx, network, in.BeeperChatID, target, placed); err != nil {
			return err
		}

		for _, m := range in.Messages {
			outcome, err := importLine(tx, network, target, in.BeeperChatID, m)
			if err != nil {
				return err
			}
			switch outcome {
			case added:
				r.Added++
			case matched:
				r.Matched++
			default:
				r.Skipped++
			}
		}
		if err := resolveReplies(tx, network, target); err != nil {
			return err
		}
		if placed && in.Replaces != "" && in.Replaces != target {
			if err := dropCarried(tx, network, in.Replaces, target); err != nil {
				return err
			}
		}
		return refresh(tx, network, target)
	})
	if err != nil {
		return ImportResult{}, err
	}
	return r, nil
}

type outcome int

const (
	skipped outcome = iota
	matched
	added
)

func importLine(tx *sql.Tx, network, chat, beeperChatID string, m ImportMessage) (outcome, error) {
	if m.BeeperID == "" {
		return skipped, nil
	}
	var one int
	err := tx.QueryRow(`SELECT 1 FROM beeper_ids AS b JOIN messages AS m ON m.rid = b.rid
			WHERE m.network = ? AND m.chat_id = ? AND b.beeper_id = ? LIMIT 1`,
		network, chat, m.BeeperID).Scan(&one)
	if err == nil {
		return skipped, nil // carried before
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return skipped, err
	}

	rid, err := matchBeeperCopy(tx, network, chat, beeperCopy{
		beeperChatID: beeperChatID, networkID: m.NetworkID, fromMe: m.FromMe,
		timeMs: m.TimeMs, text: m.Text, mediaKind: importMediaKind(m),
	})
	if err != nil {
		return skipped, err
	}
	if rid != 0 {
		var lineID string
		if err := tx.QueryRow(`UPDATE messages SET
				beeper_id = COALESCE(beeper_id, ?),
				network_id = CASE WHEN source = 'beeper' THEN COALESCE(network_id, NULLIF(?, '')) ELSE network_id END
			WHERE rid = ? RETURNING msg_id`, m.BeeperID, m.NetworkID, rid).Scan(&lineID); err != nil {
			return skipped, err
		}
		if err := carryBeeperID(tx, network, rid, m.BeeperID, beeperChatID); err != nil {
			return skipped, err
		}
		// Beeper's reactions fill in; the network's own stay as they are.
		return matched, addReactions(tx, network, chat, lineID, m.Reactions)
	}

	media := make([]Media, 0, len(m.Media))
	for _, x := range m.Media {
		media = append(media, Media{Kind: x.Kind, Width: x.Width, Height: x.Height})
	}
	mj, err := mediaJSON(media)
	if err != nil {
		return skipped, err
	}
	var text any
	if m.Text != "" && !m.Deleted {
		text = m.Text
	}
	var replyTo any
	if m.ReplyToBeeperID != "" {
		replyTo = beeperPrefix + m.ReplyToBeeperID // resolveReplies points it at its line
	}
	lineID := beeperPrefix + m.BeeperID
	if err := tx.QueryRow(`INSERT INTO messages
			(network, chat_id, msg_id, sender_id, sender_name, from_me, ts_ms, text, kind,
			 hidden, deleted, edited, reply_to, media_json, source, beeper_id, network_id)
			VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, 0, ?, ?, 'beeper', ?, NULLIF(?, ''))
			RETURNING rid`,
		network, chat, lineID, m.SenderID, m.SenderName, m.FromMe, m.TimeMs, text, kindOr(m.Kind),
		m.Hidden, m.Deleted, replyTo, mj, m.BeeperID, m.NetworkID).Scan(&rid); err != nil {
		return skipped, err
	}
	if err := carryBeeperID(tx, network, rid, m.BeeperID, beeperChatID); err != nil {
		return skipped, err
	}
	return added, addReactions(tx, network, chat, lineID, m.Reactions)
}

// carryBeeperID records that line rid carries a Beeper message.
func carryBeeperID(tx *sql.Tx, network string, rid int64, beeperID, beeperChatID string) error {
	_, err := tx.Exec(`INSERT OR IGNORE INTO beeper_ids (rid, network, beeper_id, beeper_chat_id) VALUES (?, ?, ?, ?)`,
		rid, network, beeperID, beeperChatID)
	return err
}

// addReactions adds Beeper's reactions to a line, keeping any already held.
func addReactions(tx *sql.Tx, network, chat, lineID string, rs []Reaction) error {
	for _, x := range rs {
		if x.Emoji == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO reactions (network, chat_id, msg_id, sender_id, emoji, from_me)
				VALUES (?, ?, ?, ?, ?, ?)`, network, chat, lineID, x.SenderID, x.Emoji, x.FromMe); err != nil {
			return err
		}
	}
	return nil
}

// resolveReplies points every reply to beeper:<id> in a chat at the line now
// carrying that Beeper message, wherever it arrived from.
func resolveReplies(tx *sql.Tx, network, chat string) error {
	const carrier = `SELECT t.msg_id FROM beeper_ids AS b JOIN messages AS t ON t.rid = b.rid
			WHERE t.network = m.network AND t.chat_id = m.chat_id AND b.beeper_id = substr(m.reply_to, 8)`
	_, err := tx.Exec(`UPDATE messages AS m SET reply_to = (`+carrier+` ORDER BY t.rid LIMIT 1)
		WHERE m.network = ? AND m.chat_id = ? AND m.reply_to LIKE 'beeper:%'
		AND EXISTS (`+carrier+` AND t.msg_id <> m.reply_to)`, network, chat)
	return err
}

// mapBeeperChat records which chat a Beeper chat's lines went to. A placed
// chat wins over an unplaced one.
func mapBeeperChat(tx *sql.Tx, network, beeperChatID, chat string, placed bool) error {
	if placed {
		_, err := tx.Exec(`INSERT OR REPLACE INTO beeper_chats (network, beeper_chat_id, chat_id) VALUES (?, ?, ?)`,
			network, beeperChatID, chat)
		return err
	}
	_, err := tx.Exec(`INSERT OR IGNORE INTO beeper_chats (network, beeper_chat_id, chat_id) VALUES (?, ?, ?)`,
		network, beeperChatID, chat)
	return err
}

// dropCarried drops from the unplaced chat every line the placed chat now
// carries, and the unplaced chat itself once it is empty. A placed chat is
// never dropped, and a line not yet carried is never dropped.
func dropCarried(tx *sql.Tx, network, unplaced, placed string) error {
	var isPlaced bool
	err := tx.QueryRow(`SELECT placed FROM chats WHERE network = ? AND chat_id = ?`, network, unplaced).Scan(&isPlaced)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && isPlaced) {
		return nil
	}
	if err != nil {
		return err
	}
	// The unplaced lines carrying a Beeper message the placed chat carries.
	const carried = `SELECT b.rid FROM beeper_ids AS b JOIN messages AS m ON m.rid = b.rid
		WHERE m.network = ?1 AND m.chat_id = ?2 AND b.beeper_id IN (
			SELECT b2.beeper_id FROM beeper_ids AS b2 JOIN messages AS m2 ON m2.rid = b2.rid
			WHERE m2.network = ?1 AND m2.chat_id = ?3)`
	if _, err := tx.Exec(`DELETE FROM reactions WHERE network = ?1 AND chat_id = ?2 AND msg_id IN (
			SELECT msg_id FROM messages WHERE rid IN (`+carried+`))`, network, unplaced, placed); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM messages WHERE rid IN (`+carried+`)`, network, unplaced, placed); err != nil {
		return err
	}
	var left int
	if err := tx.QueryRow(`SELECT count(*) FROM messages WHERE network = ? AND chat_id = ?`,
		network, unplaced).Scan(&left); err != nil {
		return err
	}
	if left > 0 {
		return refresh(tx, network, unplaced)
	}
	for _, q := range []string{
		`DELETE FROM reactions WHERE network = ? AND chat_id = ?`,
		`DELETE FROM participants WHERE network = ? AND chat_id = ?`,
		`DELETE FROM chats WHERE network = ? AND chat_id = ?`,
	} {
		if _, err := tx.Exec(q, network, unplaced); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`UPDATE beeper_chats SET chat_id = ? WHERE network = ? AND chat_id = ?`, placed, network, unplaced)
	return err
}

// BeeperChats maps each Beeper chat imported to the chat its lines went to:
// a chat ID, or unplaced:<Beeper chat ID>.
func (s *Store) BeeperChats(network string) (map[string]string, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT beeper_chat_id, chat_id FROM beeper_chats WHERE network = ?`, network)
	if err != nil {
		return nil, dbErr(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var b, c string
		if err := rows.Scan(&b, &c); err != nil {
			return nil, dbErr(err)
		}
		out[b] = c
	}
	return out, dbErr(rows.Err())
}

// maxLineIDs is the most Beeper IDs one lineIDs call takes.
const maxLineIDs = 1000

// LineIDs maps each Beeper message ID the store carries to its line's ID:
// the network's own ID when matched, else beeper:<id>. An ID never imported
// is left out. A line in a placed chat wins over one in an unplaced chat.
func (s *Store) LineIDs(network string, beeperIDs []string) (map[string]string, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if len(beeperIDs) > maxLineIDs {
		return nil, errBadRequest
	}
	out := map[string]string{}
	for start := 0; start < len(beeperIDs); start += 250 {
		chunk := beeperIDs[start:min(start+250, len(beeperIDs))]
		args := []any{network}
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := s.db.Query(`SELECT b.beeper_id, m.msg_id FROM beeper_ids AS b
				JOIN messages AS m ON m.rid = b.rid
				JOIN chats AS c ON c.network = m.network AND c.chat_id = m.chat_id
				WHERE b.network = ? AND b.beeper_id IN (?`+strings.Repeat(", ?", len(chunk)-1)+`)
				ORDER BY c.placed ASC, m.rid DESC`, args...)
		if err != nil {
			return nil, dbErr(err)
		}
		for rows.Next() {
			var b, id string
			if err := rows.Scan(&b, &id); err != nil {
				rows.Close()
				return nil, dbErr(err)
			}
			out[b] = id // later rows win: placed chats come last
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, dbErr(err)
		}
	}
	return out, nil
}

// Counts counts what the store holds for a network.
func (s *Store) Counts(network string) (Counts, error) {
	var c Counts
	if err := s.ready(); err != nil {
		return c, err
	}
	var oldest sql.NullInt64
	err := s.db.QueryRow(`SELECT
			(SELECT count(*) FROM chats WHERE network = ?1),
			(SELECT count(*) FROM chats WHERE network = ?1 AND placed = 0),
			(SELECT count(*) FROM messages WHERE network = ?1),
			(SELECT count(*) FROM messages WHERE network = ?1 AND beeper_id IS NOT NULL),
			(SELECT count(*) FROM messages WHERE network = ?1 AND beeper_id IS NOT NULL AND source <> 'beeper'),
			(SELECT MIN(ts_ms) FROM messages WHERE network = ?1)`, network).
		Scan(&c.Chats, &c.UnplacedChats, &c.Messages, &c.Imported, &c.Matched, &oldest)
	if err != nil {
		return Counts{}, dbErr(err)
	}
	if oldest.Valid {
		c.OldestMs = &oldest.Int64
	}
	return c, nil
}
