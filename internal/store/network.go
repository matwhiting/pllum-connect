package store

import (
	"database/sql"
	"errors"
)

// PutPendingHistory keeps one history notification (as the network's own
// bytes) until it has been downloaded and stored, so it survives a restart
// between WhatsApp's acknowledgement and the download. The same bytes already
// waiting are not kept twice.
func (s *Store) PutPendingHistory(network string, blob []byte) error {
	if network == "" || len(blob) == 0 {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO pending_history (network, id, blob)
				SELECT ?1, COALESCE((SELECT MAX(id) FROM pending_history WHERE network = ?1), 0) + 1, ?2
				WHERE NOT EXISTS (SELECT 1 FROM pending_history WHERE network = ?1 AND blob = ?2)`,
			network, blob)
		return err
	})
}

// NextPendingHistory is the oldest history notification still waiting; ok
// is false when none is.
func (s *Store) NextPendingHistory(network string) (id int64, blob []byte, ok bool, err error) {
	if err := s.ready(); err != nil {
		return 0, nil, false, err
	}
	err = s.db.QueryRow(`SELECT id, blob FROM pending_history WHERE network = ? ORDER BY id LIMIT 1`,
		network).Scan(&id, &blob)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, dbErr(err)
	}
	return id, blob, true, nil
}

// DeletePendingHistory drops a history notification once its lines are
// stored.
func (s *Store) DeletePendingHistory(network string, id int64) error {
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM pending_history WHERE network = ? AND id = ?`, network, id)
		return err
	})
}

// LineTime is the time of a line the chat (or the chat an alias names)
// holds and shows; ok is false for a line it does not hold, or holds only as
// news about the line (an early row).
func (s *Store) LineTime(network, chatID, msgID string) (ms int64, ok bool) {
	if s.ready() != nil || chatID == "" || msgID == "" {
		return 0, false
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return 0, false
	}
	err = s.db.QueryRow(`SELECT ts_ms FROM messages
			WHERE network = ? AND chat_id = ? AND msg_id = ? AND early = 0 AND ts_ms > 0`,
		network, chat, msgID).Scan(&ms)
	if err != nil {
		return 0, false
	}
	return ms, true
}

// OldestLine is the time and ID of the oldest line a chat (or the chat an
// alias names) holds whose ID starts with idPrefix (a network's own lines,
// not another's in the same chat); ok is false when it holds none.
func (s *Store) OldestLine(network, chatID, idPrefix string) (ms int64, id string, ok bool) {
	if s.ready() != nil || chatID == "" {
		return 0, "", false
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return 0, "", false
	}
	err = s.db.QueryRow(`SELECT ts_ms, msg_id FROM messages
			WHERE network = ? AND chat_id = ? AND early = 0 AND ts_ms > 0 AND substr(msg_id, 1, ?) = ?
			ORDER BY ts_ms, rid LIMIT 1`,
		network, chat, len(idPrefix), idPrefix).Scan(&ms, &id)
	if err != nil {
		return 0, "", false
	}
	return ms, id, true
}

// Raw is what the network kept with a line to fetch its media later. A line
// the chat does not hold is notFound; one held with nothing kept is empty.
func (s *Store) Raw(network, chatID, msgID string) ([]byte, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if chatID == "" || msgID == "" {
		return nil, errBadRequest
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return nil, dbErr(err)
	}
	var raw []byte
	err = s.db.QueryRow(`SELECT raw FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		network, chat, msgID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, dbErr(err)
	}
	return raw, nil
}

// ChatOrder is the place of a chat (or the chat an alias names) in the order
// chats were made: a chat made earlier has a lower order. ok is false for a
// chat the store does not hold.
func (s *Store) ChatOrder(network, chatID string) (order int64, ok bool) {
	if s.ready() != nil || chatID == "" {
		return 0, false
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return 0, false
	}
	if err := s.db.QueryRow(`SELECT rid FROM chats WHERE network = ? AND chat_id = ?`, network, chat).Scan(&order); err != nil {
		return 0, false
	}
	return order, true
}

// NetworkValue is a value a network keeps for itself (its client's state,
// where it got to), in the meta table under "<network>.<name>"; ok is false
// when none is kept.
func (s *Store) NetworkValue(network, name string) (value string, ok bool, err error) {
	if err := s.ready(); err != nil {
		return "", false, err
	}
	if network == "" || name == "" {
		return "", false, errBadRequest
	}
	err = s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, network+"."+name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, dbErr(err)
	}
	return value, true, nil
}

// PutNetworkValue keeps a network's value under "<network>.<name>"; an
// empty value removes it.
func (s *Store) PutNetworkValue(network, name, value string) error {
	if network == "" || name == "" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		if value == "" {
			_, err := tx.Exec(`DELETE FROM meta WHERE key = ?`, network+"."+name)
			return err
		}
		_, err := tx.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)`, network+"."+name, value)
		return err
	})
}

// SeenThrough marks his lines in a chat (or the chat an alias names) sent at
// or before throughMs as seen then, for a network whose read receipts say
// "read up to this time" rather than naming lines. A line already seen keeps
// its time.
func (s *Store) SeenThrough(network, chatID string, throughMs int64) error {
	if network == "" || chatID == "" || throughMs <= 0 {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, chatID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE messages SET seen_ms = ?
				WHERE network = ? AND chat_id = ? AND from_me = 1 AND early = 0
					AND ts_ms <= ? AND seen_ms IS NULL`,
			throughMs, network, chat, throughMs)
		return err
	})
}
