// Package store is the helper's encrypted message store: one SQLite database
// every network writes into, from which the server answers every read and
// Beeper call. Each database the helper keeps is encrypted with Adiantum
// under the 32-byte key hello carries, in a folder only he can open.
package store

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/ext/fts5"
	_ "github.com/ncruces/go-sqlite3/vfs/adiantum" // registers vfs=adiantum

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The sources a line comes from.
const (
	SourceLive   = "live"   // as it arrived
	SourceSync   = "sync"   // in the network's own history sync
	SourceBeeper = "beeper" // copied from Beeper and not yet seen directly
)

// beeperPrefix starts the ID of a line that exists only in Beeper's copy.
const beeperPrefix = "beeper:"

// unplacedPrefix starts the ID of a Beeper chat the copy could not place.
const unplacedPrefix = "unplaced:"

// Store is the helper's message store. Its methods are safe to call from any
// goroutine; writes take turns.
type Store struct {
	db  *sql.DB
	wmu sync.Mutex // one write transaction at a time
}

// Media is one attachment's details. Width and Height are 0 when unknown.
type Media struct {
	Kind    string `json:"kind"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	HasFile bool   `json:"hasFile,omitempty"`
}

// Reaction is one person's reaction to a line.
type Reaction struct {
	Emoji    string `json:"emoji"`
	SenderID string `json:"senderID"`
	FromMe   bool   `json:"fromMe"`
}

// Message is one line. An empty Text, ReplyTo or SenderName is sent as null,
// and a SeenMs of 0 as null.
type Message struct {
	ID         string
	ChatID     string
	SenderID   string
	SenderName string
	FromMe     bool
	TimeMs     int64
	Text       string
	Kind       string // text, photo, video, voiceNote, sticker, file or notice
	Hidden     bool
	Deleted    bool
	Edited     bool
	ReplyTo    string
	Media      []Media
	Reactions  []Reaction // read only: PutMessage ignores it (see SetReaction)
	SeenMs     int64
	Source     string // SourceLive, SourceSync or SourceBeeper
	BeeperID   string // read only: the Beeper message this line carries
	Raw        []byte // what the network needs to fetch its media; never read back here
}

// Participant is one member of a chat.
type Participant struct {
	ID     string
	Name   string
	Phone  string
	IsSelf bool
}

// Chat is one conversation as the protocol sends it. HistoryFromMs is 0 when
// the chat holds no lines.
type Chat struct {
	ID              string
	Kind            string
	Title           string
	Phone           string
	LastActivityMs  int64
	LastMessageID   string
	LastFromMe      bool
	Unread          int
	Muted           bool
	ReadOnly        bool
	Participants    []Participant
	HistoryFromMs   int64
	HistoryComplete bool
}

// ChatInfo is what a network knows about a chat. A nil pointer or a nil
// Participants keeps what the store has; LastActivityMs only moves the
// chat's activity forward.
type ChatInfo struct {
	ID              string
	Kind            string // "single" or "group"; empty keeps (a new chat is single)
	Title           *string
	Phone           *string
	Muted           *bool
	ReadThroughMs   *int64 // his read marker: incoming lines after it are unread
	HistoryComplete *bool
	LastActivityMs  int64
	Participants    []Participant
}

// maxParticipants is the most participants a Chat carries.
const maxParticipants = 64

var (
	errBadRequest = &wire.Error{Kind: wire.BadRequest}
	errNotFound   = &wire.Error{Kind: wire.NotFound}
	errInternal   = &wire.Error{Kind: wire.Internal}
	errStoreKey   = &wire.Error{Kind: wire.StoreKey}
)

// Open opens <dir>/messages.db, encrypted under key, making dir 0700 and its
// files 0600. A key that does not open an existing store is storeKey, and the
// store is left exactly as it was.
func Open(dir string, key [32]byte) (*Store, error) {
	if err := privateFolder(dir); err != nil {
		return nil, errInternal
	}
	db, err := openEncrypted(filepath.Join(dir, "messages.db"), key, true)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// OpenDevice opens a network's session database at path the same way, for
// whatsmeow's sqlstore.NewWithDB(db, "sqlite3", waLog.Noop). Its folder is
// made 0700.
func OpenDevice(path string, key [32]byte) (*sql.DB, error) {
	if err := privateFolder(filepath.Dir(path)); err != nil {
		return nil, errInternal
	}
	return openEncrypted(path, key, false)
}

// Close closes the store.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

var umaskOnce sync.Once

// privateFolder makes dir 0700 and, once per process, sets the umask so that
// every file the helper makes (SQLite's write-ahead log and shared-memory
// files among them, which the driver makes 0666 less the umask) is 0600.
func privateFolder(dir string) error {
	umaskOnce.Do(func() { syscall.Umask(0o077) })
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// openEncrypted opens one Adiantum-encrypted database and reads its header,
// so a wrong key shows here rather than at a later query.
func openEncrypted(path string, key [32]byte, withFTS bool) (*sql.DB, error) {
	// Made 0600 before SQLite sees it; one made earlier is closed up.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, errInternal
	}
	f.Close()
	if err := privateFiles(path); err != nil {
		return nil, errInternal
	}

	q := url.Values{}
	q.Set("vfs", "adiantum")
	q.Set("hexkey", hex.EncodeToString(key[:]))
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(wal)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}).String()

	db, err := driver.Open(dsn, func(c *sqlite3.Conn) error {
		if withFTS {
			if err := fts5.Register(c); err != nil {
				return err
			}
		}
		return c.Exec(`PRAGMA temp_store=memory`)
	})
	if err != nil {
		return nil, dbErr(err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&n); err != nil {
		db.Close()
		return nil, dbErr(err)
	}
	if err := privateFiles(path); err != nil {
		db.Close()
		return nil, errInternal
	}
	return db, nil
}

// privateFiles makes the database and its companions 0600.
func privateFiles(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Chmod(p, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// dbErr gives err a fixed kind: a file that is not a database under this key
// is storeKey, a *wire.Error keeps its kind, and anything else is internal.
// SQLite's own text never leaves the store.
func dbErr(err error) error {
	if err == nil {
		return nil
	}
	var we *wire.Error
	if errors.As(err, &we) {
		return we
	}
	if errors.Is(err, sqlite3.NOTADB) {
		return errStoreKey
	}
	return errInternal
}

func (s *Store) migrate() error {
	return s.write(func(tx *sql.Tx) error {
		var hasMeta int
		if err := tx.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'`).Scan(&hasMeta); err != nil {
			return err
		}
		if hasMeta > 0 {
			var v string
			err := tx.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&v)
			switch {
			case err == nil && v != schemaVersion:
				// A newer helper's store: this helper is out of date.
				return &wire.Error{Kind: wire.Outdated}
			case err != nil && !errors.Is(err, sql.ErrNoRows):
				return err
			}
		}
		for _, stmt := range schema {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT OR IGNORE INTO meta (key, value) VALUES ('schema', ?)`, schemaVersion)
		return err
	})
}

// write runs fn in one write transaction, one at a time.
func (s *Store) write(fn func(tx *sql.Tx) error) error {
	if s == nil || s.db == nil {
		return errInternal
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return dbErr(err)
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return dbErr(err)
	}
	return dbErr(tx.Commit())
}

// ready reports whether the store can be read.
func (s *Store) ready() error {
	if s == nil || s.db == nil {
		return errInternal
	}
	return nil
}

// querier is a *sql.DB or a *sql.Tx.
type querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// resolve is the chat a JID belongs to: its alias's chat, else itself.
func resolve(q querier, network, jid string) (string, error) {
	var chat string
	err := q.QueryRow(`SELECT chat_id FROM aliases WHERE network = ? AND alias_jid = ?`, network, jid).Scan(&chat)
	if errors.Is(err, sql.ErrNoRows) {
		return jid, nil
	}
	return chat, err
}

// ensureChat makes a placed chat if there is none.
func ensureChat(tx *sql.Tx, network, chatID, kind string) error {
	if kind == "" {
		kind = "single"
	}
	_, err := tx.Exec(`INSERT OR IGNORE INTO chats (network, chat_id, kind) VALUES (?, ?, ?)`, network, chatID, kind)
	return err
}

// ChatFor is the chat a JID belongs to, through an alias or as a chat of its
// own; ok is false when the store has neither.
func (s *Store) ChatFor(network, jid string) (chatID string, ok bool) {
	if s.ready() != nil || jid == "" {
		return "", false
	}
	chat, err := resolve(s.db, network, jid)
	if err != nil {
		return "", false
	}
	var one int
	if err := s.db.QueryRow(`SELECT 1 FROM chats WHERE network = ? AND chat_id = ?`, network, chat).Scan(&one); err != nil {
		return "", false
	}
	return chat, true
}

// Alias makes alias another name for chatID (itself resolved through its own
// alias), so lines for either land in the one chat. A chat's ID never changes.
func (s *Store) Alias(network, alias, chatID string) error {
	if network == "" || alias == "" || chatID == "" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		target, err := resolve(tx, network, chatID)
		if err != nil {
			return err
		}
		if target == alias {
			return nil
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO aliases (network, alias_jid, chat_id) VALUES (?, ?, ?)`,
			network, alias, target); err != nil {
			return err
		}
		// Aliases that led to alias now lead to its chat.
		_, err = tx.Exec(`UPDATE aliases SET chat_id = ? WHERE network = ? AND chat_id = ?`, target, network, alias)
		return err
	})
}

// PutChat writes what a network knows about a chat, making it if new.
func (s *Store) PutChat(network string, c ChatInfo) error {
	if network == "" || c.ID == "" || strings.HasPrefix(c.ID, unplacedPrefix) {
		return errBadRequest
	}
	if c.Kind != "" && c.Kind != "single" && c.Kind != "group" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, c.ID)
		if err != nil {
			return err
		}
		if err := ensureChat(tx, network, chat, c.Kind); err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE chats SET
				kind = COALESCE(NULLIF(?, ''), kind),
				title = CASE WHEN ? THEN NULLIF(?, '') ELSE title END,
				phone = CASE WHEN ? THEN NULLIF(?, '') ELSE phone END,
				muted = COALESCE(?, muted),
				read_through_ms = COALESCE(?, read_through_ms),
				history_complete = COALESCE(?, history_complete),
				last_activity_ms = MAX(last_activity_ms, ?)
			WHERE network = ? AND chat_id = ?`,
			c.Kind,
			c.Title != nil, deref(c.Title),
			c.Phone != nil, deref(c.Phone),
			c.Muted, c.ReadThroughMs, c.HistoryComplete,
			c.LastActivityMs,
			network, chat)
		if err != nil {
			return err
		}
		if c.Participants != nil {
			if _, err := tx.Exec(`DELETE FROM participants WHERE network = ? AND chat_id = ?`, network, chat); err != nil {
				return err
			}
			for i, p := range c.Participants {
				if p.ID == "" {
					continue
				}
				if _, err := tx.Exec(`INSERT OR REPLACE INTO participants
						(network, chat_id, participant_id, name, phone, is_self, position)
						VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?)`,
					network, chat, p.ID, p.Name, p.Phone, p.IsSelf, i); err != nil {
					return err
				}
			}
		}
		return refresh(tx, network, chat)
	})
}

// PutMessage stores one line a network received, sent or synced. It reports
// whether a new line was added: a line it already holds is updated (an edit,
// a revoke, a later copy), and a line matching one held only in Beeper's copy
// takes that line over, keeping its Beeper ID, so it shows once.
func (s *Store) PutMessage(network string, m Message) (added bool, err error) {
	if network == "" || m.ChatID == "" || m.ID == "" || m.TimeMs < 0 ||
		strings.HasPrefix(m.ChatID, unplacedPrefix) || strings.HasPrefix(m.ID, beeperPrefix) {
		return false, errBadRequest
	}
	if m.Kind == "" {
		m.Kind = "text"
	}
	if m.Source == "" {
		m.Source = SourceLive
	}
	if m.Source != SourceLive && m.Source != SourceSync {
		return false, errBadRequest
	}
	err = s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, m.ChatID)
		if err != nil {
			return err
		}
		m.ChatID = chat
		if err := ensureChat(tx, network, chat, ""); err != nil {
			return err
		}
		media, err := mediaJSON(m.Media)
		if err != nil {
			return err
		}

		var rid int64
		err = tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
			network, chat, m.ID).Scan(&rid)
		switch {
		case err == nil:
			err = updateLine(tx, rid, m, media)
		case errors.Is(err, sql.ErrNoRows):
			var oldID string
			rid, oldID, err = beeperLineFor(tx, network, m)
			switch {
			case err != nil:
			case rid != 0:
				err = takeOver(tx, network, rid, oldID, m, media)
			default:
				err = insertLine(tx, network, m, media)
				added = err == nil
			}
		}
		if err != nil {
			return err
		}
		return refresh(tx, network, chat)
	})
	if err != nil {
		return false, err
	}
	return added, nil
}

func insertLine(tx *sql.Tx, network string, m Message, media any) error {
	_, err := tx.Exec(`INSERT INTO messages
			(network, chat_id, msg_id, sender_id, sender_name, from_me, ts_ms, text, kind,
			 hidden, deleted, edited, reply_to, media_json, raw, source, seen_ms)
			VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, NULLIF(?, 0))`,
		network, m.ChatID, m.ID, m.SenderID, m.SenderName, m.FromMe, m.TimeMs, lineText(m), m.Kind,
		m.Hidden, m.Deleted, m.Edited, m.ReplyTo, media, nilIfEmpty(m.Raw), m.Source, m.SeenMs)
	return err
}

// updateLine folds a later copy of a line into the one held. Nothing it
// lacks erases what is there: text, media, raw, a reply and a name are kept
// when the copy has none; deleted and edited stay set once set.
func updateLine(tx *sql.Tx, rid int64, m Message, media any) error {
	_, err := tx.Exec(`UPDATE messages SET
			text = CASE WHEN ? THEN NULL WHEN ? <> '' THEN ? ELSE text END,
			kind = ?,
			sender_name = COALESCE(NULLIF(?, ''), sender_name),
			hidden = ?,
			deleted = deleted OR ?,
			edited = edited OR ?,
			reply_to = COALESCE(NULLIF(?, ''), reply_to),
			media_json = COALESCE(?, media_json),
			raw = COALESCE(?, raw),
			seen_ms = NULLIF(MAX(COALESCE(seen_ms, 0), ?), 0)
		WHERE rid = ?`,
		m.Deleted, m.Text, m.Text,
		m.Kind,
		m.SenderName,
		m.Hidden,
		m.Deleted,
		m.Edited,
		m.ReplyTo,
		media,
		nilIfEmpty(m.Raw),
		m.SeenMs,
		rid)
	return err
}

// takeOver gives the Beeper line rid the direct line's ID and content. Its
// Beeper ID stays, and its reactions and the replies to it follow the new ID.
func takeOver(tx *sql.Tx, network string, rid int64, oldID string, m Message, media any) error {
	_, err := tx.Exec(`UPDATE messages SET
			msg_id = ?,
			source = ?,
			sender_id = COALESCE(NULLIF(?, ''), sender_id),
			sender_name = COALESCE(NULLIF(?, ''), sender_name),
			ts_ms = ?,
			text = CASE WHEN ? THEN NULL WHEN ? <> '' THEN ? ELSE text END,
			kind = ?,
			hidden = ?,
			deleted = deleted OR ?,
			edited = edited OR ?,
			reply_to = COALESCE(NULLIF(?, ''), reply_to),
			media_json = COALESCE(?, media_json),
			raw = COALESCE(?, raw),
			seen_ms = NULLIF(MAX(COALESCE(seen_ms, 0), ?), 0)
		WHERE rid = ?`,
		m.ID, m.Source, m.SenderID, m.SenderName, m.TimeMs,
		m.Deleted, m.Text, m.Text,
		m.Kind, m.Hidden, m.Deleted, m.Edited, m.ReplyTo, media, nilIfEmpty(m.Raw), m.SeenMs,
		rid)
	if err != nil {
		return err
	}
	return renameLine(tx, network, m.ChatID, oldID, m.ID)
}

// renameLine points a line's reactions and replies at its new ID.
func renameLine(tx *sql.Tx, network, chat, oldID, newID string) error {
	if _, err := tx.Exec(`UPDATE OR IGNORE reactions SET msg_id = ? WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		newID, network, chat, oldID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM reactions WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		network, chat, oldID); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE messages SET reply_to = ? WHERE network = ? AND chat_id = ? AND reply_to = ?`,
		newID, network, chat, oldID)
	return err
}

// SetReaction sets one person's reaction to a line; an empty emoji removes it.
// The line need not be held yet.
func (s *Store) SetReaction(network, chatID, msgID string, r Reaction) error {
	if network == "" || chatID == "" || msgID == "" {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		chat, err := resolve(tx, network, chatID)
		if err != nil {
			return err
		}
		if r.Emoji == "" {
			_, err = tx.Exec(`DELETE FROM reactions WHERE network = ? AND chat_id = ? AND msg_id = ? AND sender_id = ?`,
				network, chat, msgID, r.SenderID)
			return err
		}
		_, err = tx.Exec(`INSERT OR REPLACE INTO reactions (network, chat_id, msg_id, sender_id, emoji, from_me)
				VALUES (?, ?, ?, ?, ?, ?)`, network, chat, msgID, r.SenderID, r.Emoji, r.FromMe)
		return err
	})
}

// refresh recomputes a chat's derived values: its last visible line, its
// unread count and the oldest line it holds. Activity only moves forward.
// Lines only in Beeper's copy are history, and never count as unread.
func refresh(tx *sql.Tx, network, chat string) error {
	var lastID sql.NullString
	var lastFromMe bool
	var lastMs sql.NullInt64
	err := tx.QueryRow(`SELECT msg_id, from_me, ts_ms FROM messages
			WHERE network = ? AND chat_id = ? AND `+visible+`
			ORDER BY ts_ms DESC, rid DESC LIMIT 1`, network, chat).Scan(&lastID, &lastFromMe, &lastMs)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var unread int
	if err := tx.QueryRow(`SELECT count(*) FROM messages
			WHERE network = ? AND chat_id = ? AND `+visible+` AND from_me = 0 AND source <> 'beeper'
			AND ts_ms > (SELECT read_through_ms FROM chats WHERE network = ? AND chat_id = ?)`,
		network, chat, network, chat).Scan(&unread); err != nil {
		return err
	}
	var oldest sql.NullInt64
	if err := tx.QueryRow(`SELECT MIN(ts_ms) FROM messages WHERE network = ? AND chat_id = ?`,
		network, chat).Scan(&oldest); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE chats SET
			last_message_id = ?,
			last_from_me = ?,
			last_activity_ms = MAX(last_activity_ms, ?),
			unread = ?,
			history_from_ms = ?
		WHERE network = ? AND chat_id = ?`,
		lastID, lastFromMe && lastID.Valid, lastMs.Int64, unread, oldest, network, chat)
	return err
}

// lineText is the text to store: none for a deleted line.
func lineText(m Message) any {
	if m.Deleted || m.Text == "" {
		return nil
	}
	return m.Text
}

// mediaJSON is media as stored: NULL when there is none.
func mediaJSON(media []Media) (any, error) {
	if len(media) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(media)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func nilIfEmpty(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
