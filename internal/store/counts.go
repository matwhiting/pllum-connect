package store

import "database/sql"

// Counts is what the store holds for a network: numbers and one time only.
type Counts struct {
	Chats         int    `json:"chats"`
	Messages      int    `json:"messages"`
	Imported      int    `json:"imported"` // lines carrying a Beeper ID, matched or not
	Matched       int    `json:"matched"`  // of those, lines held under the network's own ID
	UnplacedChats int    `json:"unplacedChats"`
	OldestMs      *int64 `json:"oldestMs"`
	// NearMisses counts Beeper-only lines in placed chats with a direct line
	// of the same side within the matching window that is not the same
	// content (see nearMisses). A measure only: it changes nothing.
	NearMisses int `json:"nearMisses"`
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
			(SELECT count(*) FROM messages WHERE network = ?1 AND early = 0),
			(SELECT count(*) FROM messages WHERE network = ?1 AND beeper_id IS NOT NULL),
			(SELECT count(*) FROM messages WHERE network = ?1 AND beeper_id IS NOT NULL AND source <> 'beeper'),
			(SELECT MIN(ts_ms) FROM messages WHERE network = ?1 AND early = 0 AND ts_ms > 0)`, network).
		Scan(&c.Chats, &c.UnplacedChats, &c.Messages, &c.Imported, &c.Matched, &oldest)
	if err != nil {
		return Counts{}, dbErr(err)
	}
	if oldest.Valid {
		c.OldestMs = &oldest.Int64
	}
	if c.NearMisses, err = s.nearMisses(network); err != nil {
		return Counts{}, err
	}
	return c, nil
}

// nearMisses counts the Beeper-only lines in placed chats that have a direct
// (non-Beeper) line of the same side within the matching window, and no
// direct line of the same content there: the lines the matcher would have
// shown twice had the pair been one message, as when a mention is a name in
// Beeper's text and a number in WhatsApp's. It reads text only to compare it
// and returns a count; matching itself is not touched.
func (s *Store) nearMisses(network string) (int, error) {
	rows, err := s.db.Query(`WITH b AS (
				SELECT rid, chat_id, from_me, ts_ms, text, kind, media_json FROM messages
				WHERE network = ?1 AND source = 'beeper' AND `+fuzzable+`
				AND chat_id IN (SELECT chat_id FROM chats WHERE network = ?1 AND placed = 1)),
			d AS (
				SELECT chat_id, from_me, ts_ms, text, kind, media_json FROM messages
				WHERE network = ?1 AND source <> 'beeper' AND `+fuzzable+`)
			SELECT b.rid, b.text, b.kind, b.media_json, d.text, d.kind, d.media_json
			FROM b JOIN d ON d.chat_id = b.chat_id AND d.from_me = b.from_me
				AND d.ts_ms BETWEEN b.ts_ms - ?2 AND b.ts_ms + ?2
			ORDER BY b.rid`, network, matchWindowMs)
	if err != nil {
		return 0, dbErr(err)
	}
	defer rows.Close()
	var misses int
	var cur int64
	var seen, matched bool
	end := func() {
		if seen && !matched {
			misses++
		}
	}
	for rows.Next() {
		var rid int64
		var bt, bk, dt, dk sql.NullString
		var bm, dm sql.NullString
		if err := rows.Scan(&rid, &bt, &bk, &bm, &dt, &dk, &dm); err != nil {
			return 0, dbErr(err)
		}
		if !seen || rid != cur {
			end()
			cur, seen, matched = rid, true, false
		}
		if !matched && sameContent(bt.String, storedMediaKind(bk.String, bm), false, dt.String, storedMediaKind(dk.String, dm), true) {
			matched = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, dbErr(err)
	}
	end()
	return misses, nil
}
