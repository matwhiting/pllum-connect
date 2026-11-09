package store

import (
	"database/sql"
	"errors"
)

// The outbox's states.
const (
	OutSending = "sending" // made and kept; not yet acknowledged
	OutSent    = "sent"    // the network acknowledged it
	OutFailed  = "failed"  // it did not go; Fail says why
)

// Outgoing is one write a network makes, kept under the ID it made before
// sending. Request is the network's own record of what to send; Message is
// what it built from it (an upload done, a quote filled), kept before the
// send is tried, so a send cut off by a crash goes again unchanged.
type Outgoing struct {
	ID      string
	ChatID  string
	Kind    string // what is sent: "message" or "reaction"
	State   string // OutSending, OutSent or OutFailed
	Fail    string // the error kind, when failed
	Txn     string // the caller's transaction ID, when it gave one
	Client  string // the caller's own key for a send, when it gave one
	MadeMs  int64
	Request []byte
	Message []byte
}

// PutOutgoing keeps a new write as sending. An ID already kept is an error:
// each write has its own. So is a send key another row still holds, unless
// that row failed: the key moves to the new row, as the failed send is done.
func (s *Store) PutOutgoing(network string, o Outgoing) error {
	if network == "" || o.ID == "" || o.ChatID == "" || o.Kind == "" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, o.ChatID)
		if err != nil {
			return err
		}
		if o.Client != "" {
			if _, err := tx.Exec(`UPDATE outgoing SET client_id = NULL WHERE network = ? AND client_id = ? AND state = ?`,
				network, o.Client, OutFailed); err != nil {
				return err
			}
		}
		_, err = tx.Exec(`INSERT INTO outgoing (network, msg_id, chat_id, kind, state, txn, made_ms, request, message, client_id)
				VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, NULLIF(?, ''))`,
			network, o.ID, chat, o.Kind, OutSending, o.Txn, o.MadeMs, nilIfEmpty(o.Request), nilIfEmpty(o.Message), o.Client)
		return err
	})
}

// SetOutgoingMessage keeps what the network built for a write.
func (s *Store) SetOutgoingMessage(network, id string, message []byte) error {
	if network == "" || id == "" || len(message) == 0 {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE outgoing SET message = ? WHERE network = ? AND msg_id = ?`, message, network, id)
		return err
	})
}

// SetOutgoingState moves a write to sent or failed (with the error kind), or
// back to sending.
func (s *Store) SetOutgoingState(network, id, state, fail string) error {
	if network == "" || id == "" || (state != OutSending && state != OutSent && state != OutFailed) {
		return errBadRequest
	}
	if state != OutFailed {
		fail = ""
	}
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE outgoing SET state = ?, fail = NULLIF(?, '') WHERE network = ? AND msg_id = ?`,
			state, fail, network, id)
		return err
	})
}

const outgoingColumns = `msg_id, chat_id, kind, state, COALESCE(fail, ''), COALESCE(txn, ''), made_ms, request, message,
	COALESCE(client_id, '')`

func scanOutgoing(row interface{ Scan(...any) error }) (Outgoing, error) {
	var o Outgoing
	err := row.Scan(&o.ID, &o.ChatID, &o.Kind, &o.State, &o.Fail, &o.Txn, &o.MadeMs, &o.Request, &o.Message, &o.Client)
	return o, err
}

func (s *Store) oneOutgoing(q string, args ...any) (Outgoing, bool, error) {
	if err := s.ready(); err != nil {
		return Outgoing{}, false, err
	}
	o, err := scanOutgoing(s.db.QueryRow(q, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return Outgoing{}, false, nil
	}
	if err != nil {
		return Outgoing{}, false, dbErr(err)
	}
	return o, true, nil
}

// Outgoing is the write kept under id; ok is false when there is none.
func (s *Store) Outgoing(network, id string) (Outgoing, bool, error) {
	return s.oneOutgoing(`SELECT `+outgoingColumns+` FROM outgoing WHERE network = ? AND msg_id = ?`, network, id)
}

// OutgoingTxn is the write kept for a caller's transaction ID.
func (s *Store) OutgoingTxn(network, txn string) (Outgoing, bool, error) {
	if txn == "" {
		return Outgoing{}, false, nil
	}
	return s.oneOutgoing(`SELECT `+outgoingColumns+` FROM outgoing WHERE network = ? AND txn = ?
			ORDER BY made_ms DESC, rowid DESC LIMIT 1`, network, txn)
}

// OutgoingClient is the send kept for a caller's send key.
func (s *Store) OutgoingClient(network, client string) (Outgoing, bool, error) {
	if client == "" {
		return Outgoing{}, false, nil
	}
	return s.oneOutgoing(`SELECT `+outgoingColumns+` FROM outgoing WHERE network = ? AND client_id = ?`, network, client)
}

// OutgoingSending is every write still sending, oldest first.
func (s *Store) OutgoingSending(network string) ([]Outgoing, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT `+outgoingColumns+` FROM outgoing WHERE network = ? AND state = ?
			ORDER BY made_ms, rowid`, network, OutSending)
	if err != nil {
		return nil, dbErr(err)
	}
	defer rows.Close()
	var out []Outgoing
	for rows.Next() {
		o, err := scanOutgoing(rows)
		if err != nil {
			return nil, dbErr(err)
		}
		out = append(out, o)
	}
	return out, dbErr(rows.Err())
}

// Line is one held line as a network needs it to write about it: to quote
// it, react to it, mark it read or fetch its media.
type Line struct {
	ID       string
	ChatID   string
	SenderID string
	FromMe   bool
	TimeMs   int64
	Kind     string
	Text     string
	Raw      []byte
}

const lineColumns = `msg_id, chat_id, sender_id, from_me, ts_ms, kind, COALESCE(text, ''), raw`

func scanLine(row interface{ Scan(...any) error }) (Line, error) {
	var l Line
	err := row.Scan(&l.ID, &l.ChatID, &l.SenderID, &l.FromMe, &l.TimeMs, &l.Kind, &l.Text, &l.Raw)
	return l, err
}

// Line is a line the chat (or the chat an alias names) holds; ok is false
// for one it does not hold, or holds only as news about it (an early row).
func (s *Store) Line(network, chatID, msgID string) (Line, bool, error) {
	if err := s.ready(); err != nil {
		return Line{}, false, err
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return Line{}, false, dbErr(err)
	}
	l, err := scanLine(s.db.QueryRow(`SELECT `+lineColumns+` FROM messages
			WHERE network = ? AND chat_id = ? AND msg_id = ? AND early = 0`, network, chat, msgID))
	if errors.Is(err, sql.ErrNoRows) {
		return Line{}, false, nil
	}
	if err != nil {
		return Line{}, false, dbErr(err)
	}
	return l, true, nil
}

// unreadLine is the SQL test for a line that counts as unread once it is
// after the read marker: an incoming line that shows and arrived directly.
const unreadLine = visible + ` AND from_me = 0 AND source <> 'beeper' AND copied = 0`

// NewestIncoming is the chat's newest incoming line that shows.
func (s *Store) NewestIncoming(network, chatID string) (Line, bool, error) {
	if err := s.ready(); err != nil {
		return Line{}, false, err
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return Line{}, false, dbErr(err)
	}
	l, err := scanLine(s.db.QueryRow(`SELECT `+lineColumns+` FROM messages
			WHERE network = ? AND chat_id = ? AND `+visible+` AND from_me = 0
			ORDER BY ts_ms DESC, rid DESC LIMIT 1`, network, chat))
	if errors.Is(err, sql.ErrNoRows) {
		return Line{}, false, nil
	}
	if err != nil {
		return Line{}, false, dbErr(err)
	}
	return l, true, nil
}

// Unread is the chat's unread lines up to and including throughMs, oldest
// first: the lines its unread count counts, no newer.
func (s *Store) Unread(network, chatID string, throughMs int64) ([]Line, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return nil, dbErr(err)
	}
	rows, err := s.db.Query(`SELECT `+lineColumns+` FROM messages
			WHERE network = ? AND chat_id = ? AND `+unreadLine+` AND ts_ms <= ?
			AND ts_ms > (SELECT read_through_ms FROM chats WHERE network = ? AND chat_id = ?)
			ORDER BY ts_ms, rid`, network, chat, throughMs, network, chat)
	if err != nil {
		return nil, dbErr(err)
	}
	defer rows.Close()
	var out []Line
	for rows.Next() {
		l, err := scanLine(rows)
		if err != nil {
			return nil, dbErr(err)
		}
		out = append(out, l)
	}
	return out, dbErr(rows.Err())
}
