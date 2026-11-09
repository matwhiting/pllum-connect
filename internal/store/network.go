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
