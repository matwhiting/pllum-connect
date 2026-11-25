// Package store is the helper's encrypted message store: one SQLite database
// every network writes into, from which the server answers every read. Each
// database the helper keeps is encrypted with Adiantum under the 32-byte key
// hello carries, in a folder only he can open.
package store

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// Beeper's copy. Before Pllum left Beeper, its history was copied into the
// store (the retired `import`): a copied line the network had not delivered
// became a line of its own under beeperPrefix and source beeper, and a Beeper
// chat the copy could not place became a read-only chat under
// unplacedPrefix. Nothing makes such rows any more (checkMessage refuses both
// prefixes and the source), but a store from before the switch still holds
// them, and they are read, listed, searched and shown like any other line.
// The networks can still deliver the lines they copied (history syncs,
// backfills, older pages), so a direct line still takes over its copy
// (putNewID, absorbBeeperCopy) and a merge still joins a copy to its line
// (mergeLine); a write whose chat holds no copy near it skips all of that
// in one query (copyNear). Every merge, and every news matched to a copy,
// is counted (merges.go), so the day it stops firing shows.

// beeperPrefix starts the ID of a line that exists only in Beeper's copy.
const beeperPrefix = "beeper:"

// unplacedPrefix starts the ID of a Beeper chat the copy could not place.
const unplacedPrefix = "unplaced:"

// Store is the helper's message store. Its methods are safe to call from any
// goroutine; writes take turns.
type Store struct {
	db    *sql.DB
	wmu   sync.Mutex                       // one write transaction at a time
	clock atomic.Pointer[func() time.Time] // what a timed mute is read against; nil is the real time
}

// SetClock sets the time a timed mute is read against (the adapter's own
// clock, which tests move). Without it the store reads the real time.
func (s *Store) SetClock(now func() time.Time) {
	if now == nil {
		s.clock.Store(nil)
		return
	}
	s.clock.Store(&now)
}

// nowMs is the store's time, in milliseconds.
func (s *Store) nowMs() int64 {
	if f := s.clock.Load(); f != nil {
		return (*f)().UnixMilli()
	}
	return time.Now().UnixMilli()
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
	Hidden     *bool  // nil: not given (an update keeps what is held); reads always set it
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

// Participant is one member of a chat. Handle is the name the network
// shows them by, from what it already sends (an Instagram username, a
// LinkedIn public profile ID), or empty; a write with none keeps the one
// held.
type Participant struct {
	ID     string
	Name   string
	Phone  string
	IsSelf bool
	Handle string
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
	MuteEndMs       int64  // with Muted true: when the mute ends; 0 is for ever. Muted false clears it
	ReadThroughMs   *int64 // his read marker: incoming lines after it are unread
	ReadForwardOnly bool   // ReadThroughMs only moves the marker forward, never back
	HistoryComplete *bool
	ReadOnly        *bool // the chat can no longer be written to (LinkedIn deleted it); its lines stay
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
	// Every connection is a SQLite instance of its own (its own memory, the
	// Adiantum VFS, FTS5), so a few are kept and reused rather than one
	// made and thrown away for each request in a burst. Writes take turns
	// on wmu, so a write holds one connection and the rest serve reads. No
	// read holds a connection while it waits on a write, so a full pool
	// only ever waits for a read to finish, even for a write that reads
	// through s.db while it holds its own.
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
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
	db, err := openEncrypted(path, key, false)
	if err != nil {
		return nil, err
	}
	// The network's own library runs its transactions on this database as
	// it sees fit, so its pool is not capped (a cap could leave a
	// transaction waiting on a connection its own callers hold); its idle
	// connections are kept, so a burst does not make and throw away
	// SQLite instances.
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

// maxConns is the most connections the store keeps open, and idle.
const maxConns = 4

// Close closes the store.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// privateFolder makes dir 0700. The helper's main sets the umask to 077
// before anything runs, so a file the driver makes later (0666 less the
// umask: a write-ahead log made again, say) is 0600 too.
func privateFolder(dir string) error {
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
		version := 0 // a new store
		if hasMeta > 0 {
			var v string
			err := tx.QueryRow(`SELECT value FROM meta WHERE key = 'schema'`).Scan(&v)
			switch {
			case err == nil:
				if version, err = strconv.Atoi(v); err != nil {
					return errInternal
				}
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}
		}
		if version > schemaVersion {
			// A newer helper's store: this helper is out of date.
			return &wire.Error{Kind: wire.Outdated}
		}
		for _, stmt := range schema {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		if hasMeta > 0 && version < schemaVersion {
			for _, c := range addedColumns {
				if err := addColumn(tx, c.table, c.column, c.definition); err != nil {
					return err
				}
			}
			for _, stmt := range upgrades {
				if _, err := tx.Exec(stmt); err != nil {
					return err
				}
			}
			// 8 → 9: Beeper lines left beside their exact-time twin are folded.
			if version < 9 {
				if err := foldStoredTwins(tx); err != nil {
					return err
				}
			}
			// An upgrade can change which rows show, so every chat's derived
			// values are made again.
			if err := refreshAll(tx); err != nil {
				return err
			}
		}
		for _, stmt := range lateSchema {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?)`, strconv.Itoa(schemaVersion))
		return err
	})
}

// addColumn adds a column to a table that lacks it.
func addColumn(tx *sql.Tx, table, column, definition string) error {
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := tx.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + definition)
	return err
}

// refreshAll recomputes every chat's derived values.
func refreshAll(tx *sql.Tx) error {
	rows, err := tx.Query(`SELECT network, chat_id FROM chats`)
	if err != nil {
		return err
	}
	var chats [][2]string
	for rows.Next() {
		var c [2]string
		if err := rows.Scan(&c[0], &c[1]); err != nil {
			rows.Close()
			return err
		}
		chats = append(chats, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, c := range chats {
		if err := refresh(tx, c[0], c[1]); err != nil {
			return err
		}
	}
	return nil
}

// write runs fn in one write transaction, one at a time.
func (s *Store) write(fn func(tx *sql.Tx) error) error {
	if s == nil || s.db == nil {
		return errInternal
	}
	if !s.wmu.TryLock() {
		// Held: by another goroutine's write, which this one waits out, or
		// by a write this very call runs inside (a network calling a Store
		// write from a Batch's function), which would never end.
		if insideWrite() {
			return writeInsideWrite()
		}
		s.wmu.Lock()
	}
	defer s.wmu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return dbErr(err)
	}
	// A panic in fn (a batch runs a network's own code) rolls back, so the
	// connection never goes back to the pool holding the write lock.
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()
	if err := writing(tx, fn); err != nil {
		return dbErr(err)
	}
	committed = true
	return dbErr(tx.Commit())
}

// writing runs a write's function. It is a named function so a write called
// from inside one can be told by its own stack (insideWrite).
//
//go:noinline
func writing(tx *sql.Tx, fn func(tx *sql.Tx) error) error {
	return fn(tx)
}

// writingFunc is writing's name as the runtime reports it.
var writingFunc = func() string {
	pc := reflect.ValueOf(writing).Pointer()
	return runtime.FuncForPC(pc).Name()
}()

// insideWrite reports whether this goroutine is running a write's function:
// whether writing is on its stack. It is asked only when the write lock is
// held, so an uncontended write never pays for it.
func insideWrite() bool {
	pcs := make([]uintptr, 512)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if f.Function == writingFunc {
			return true
		}
		if !more {
			return false
		}
	}
}

// writeInsideWrite answers a Store write called from inside another write's
// function (a Batch's, most likely), which would otherwise wait for ever on
// the lock its own caller holds. The write is refused (internal); under test
// it panics, so the mistake fails the test that made it rather than hanging
// it.
func writeInsideWrite() error {
	if testing.Testing() {
		panic("store: a write called inside a write (a Batch's function?) would wait for ever; " +
			"use the Batch's own methods, or write after the batch")
	}
	return errInternal
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

// AliasTarget is the chat an alias leads to; ok is false when jid is no
// alias (a chat of its own, or unknown).
func (s *Store) AliasTarget(network, jid string) (chatID string, ok bool) {
	if s.ready() != nil || jid == "" {
		return "", false
	}
	if err := s.db.QueryRow(`SELECT chat_id FROM aliases WHERE network = ? AND alias_jid = ?`, network, jid).Scan(&chatID); err != nil {
		return "", false
	}
	return chatID, true
}

// Alias makes alias another name for chatID (itself resolved through its own
// alias), so lines for either land in the one chat. A chat's ID never changes.
func (s *Store) Alias(network, alias, chatID string) error {
	if network == "" || alias == "" || chatID == "" ||
		strings.HasPrefix(alias, unplacedPrefix) || strings.HasPrefix(chatID, unplacedPrefix) {
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
		if _, err := tx.Exec(`UPDATE aliases SET chat_id = ? WHERE network = ? AND chat_id = ?`,
			target, network, alias); err != nil {
			return err
		}
		var held int
		if err := tx.QueryRow(`SELECT count(*) FROM chats WHERE network = ? AND chat_id = ?`,
			network, alias).Scan(&held); err != nil {
			return err
		}
		if held == 0 {
			return nil
		}
		// The alias was a chat of its own: its lines join the target, or reads
		// through the alias would never reach them.
		return mergeChat(tx, network, alias, target)
	})
}

// Twins are the chats "<alias><suffix>" whose alias already leads to
// another chat, by chat: the chat each should join.
func (s *Store) Twins(network, suffix string) (map[string]string, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT c.chat_id, a.chat_id FROM aliases a
			JOIN chats c ON c.network = a.network AND c.chat_id = a.alias_jid || ?
			WHERE a.network = ? AND a.chat_id <> c.chat_id`, suffix, network)
	if err != nil {
		return nil, dbErr(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var from, into string
		if err := rows.Scan(&from, &into); err != nil {
			return nil, dbErr(err)
		}
		out[from] = into
	}
	return out, dbErr(rows.Err())
}

// mergeChat moves every line, reaction, member and mapping of chat from into
// chat into, folding in lines into already holds, and deletes from.
func mergeChat(tx *sql.Tx, network, from, into string) error {
	if _, err := tx.Exec(`INSERT OR IGNORE INTO chats
			(network, chat_id, kind, title, phone, muted, mute_end_ms, placed, read_only, beeper_chat_id,
			 history_complete, read_through_ms, last_activity_ms)
			SELECT network, ?, kind, title, phone, muted, mute_end_ms, placed, read_only, beeper_chat_id,
			 history_complete, read_through_ms, last_activity_ms
			FROM chats WHERE network = ? AND chat_id = ?`, into, network, from); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE chats AS t SET
			title = COALESCE(t.title, f.title),
			phone = COALESCE(t.phone, f.phone),
			beeper_chat_id = COALESCE(t.beeper_chat_id, f.beeper_chat_id),
			history_complete = MIN(t.history_complete, f.history_complete),
			read_through_ms = MAX(t.read_through_ms, f.read_through_ms),
			last_activity_ms = MAX(t.last_activity_ms, f.last_activity_ms)
		FROM (SELECT * FROM chats WHERE network = ?1 AND chat_id = ?2) AS f
		WHERE t.network = ?1 AND t.chat_id = ?3`, network, from, into); err != nil {
		return err
	}
	if err := execAll(tx, []any{into, network, from},
		`INSERT OR IGNORE INTO participants (network, chat_id, participant_id, name, phone, is_self, position, handle)
			SELECT network, ?, participant_id, name, phone, is_self, position, handle FROM participants
			WHERE network = ? AND chat_id = ?`,
		// One the target already held keeps its own fields but takes the
		// later chat's handle, when it has one: the chat merged in is the
		// later one, so its handle is the newer name.
		`UPDATE participants AS t SET handle = f.handle FROM participants AS f
			WHERE t.network = f.network AND t.participant_id = f.participant_id
			AND t.chat_id = ?1 AND t.network = ?2 AND f.chat_id = ?3
			AND f.handle IS NOT NULL`,
		`UPDATE outgoing SET chat_id = ? WHERE network = ? AND chat_id = ?`,
	); err != nil {
		return err
	}
	if err := execAll(tx, []any{network, from}, `DELETE FROM participants WHERE network = ? AND chat_id = ?`); err != nil {
		return err
	}

	rows, err := tx.Query(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? ORDER BY rid`, network, from)
	if err != nil {
		return err
	}
	var rids []int64
	for rows.Next() {
		var rid int64
		if err := rows.Scan(&rid); err != nil {
			rows.Close()
			return err
		}
		rids = append(rids, rid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, rid := range rids {
		if err := mergeLine(tx, network, from, into, rid); err != nil {
			return err
		}
	}

	// Reactions to lines neither chat holds yet go too.
	if err := execAll(tx, []any{into, network, from},
		`UPDATE OR IGNORE reactions SET chat_id = ? WHERE network = ? AND chat_id = ?`); err != nil {
		return err
	}
	if err := execAll(tx, []any{network, from},
		`DELETE FROM reactions WHERE network = ? AND chat_id = ?`,
		`DELETE FROM chats WHERE network = ? AND chat_id = ?`,
	); err != nil {
		return err
	}
	if err := resolveReplies(tx, network, into); err != nil {
		return err
	}
	return refresh(tx, network, into)
}

// resolveReplies points every reply to beeper:<id> in a chat at the line now
// carrying that Beeper message, wherever it arrived from: after a merge or a
// fold, the copied line a reply named may live on under the network's ID.
func resolveReplies(tx *sql.Tx, network, chat string) error {
	const carrier = `SELECT t.msg_id FROM beeper_ids AS b JOIN messages AS t ON t.rid = b.rid
			WHERE t.network = m.network AND t.chat_id = m.chat_id AND b.beeper_id = substr(m.reply_to, 8)`
	_, err := tx.Exec(`UPDATE messages AS m SET reply_to = (`+carrier+` ORDER BY t.rid LIMIT 1)
		WHERE m.network = ? AND m.chat_id = ? AND m.reply_to LIKE 'beeper:%'
		AND EXISTS (`+carrier+` AND t.msg_id <> m.reply_to)`, network, chat)
	return err
}

// heldHandles are the handles a chat's participants hold, by participant.
func heldHandles(tx *sql.Tx, network, chat string) (map[string]string, error) {
	rows, err := tx.Query(`SELECT participant_id, handle FROM participants
		WHERE network = ? AND chat_id = ? AND handle IS NOT NULL`, network, chat)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, handle string
		if err := rows.Scan(&id, &handle); err != nil {
			return nil, err
		}
		out[id] = handle
	}
	return out, rows.Err()
}

// execAll runs each statement with the same arguments.
func execAll(tx *sql.Tx, args []any, stmts ...string) error {
	for _, q := range stmts {
		if _, err := tx.Exec(q, args...); err != nil {
			return err
		}
	}
	return nil
}

// mergeLine moves line rid of chat from into chat into. A line into already
// holds under the same ID is folded in; a direct line takes over its Beeper
// copy there; a Beeper line joins the line into holds for its message. Any
// other line moves as it is.
func mergeLine(tx *sql.Tx, network, from, into string, rid int64) error {
	m, media, networkID, err := loadLine(tx, rid)
	if err != nil {
		return err
	}
	m.ChatID = into
	fromEarly, fromCopied, err := rowFlags(tx, rid)
	if err != nil {
		return err
	}

	var keep int64
	err = tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		network, into, m.ID).Scan(&keep)
	switch {
	case err == nil:
		if m.Source == SourceBeeper {
			return foldLine(tx, network, from, into, rid, keep, m.ID)
		}
		keepEarly, keepCopied, err := rowFlags(tx, keep)
		if err != nil {
			return err
		}
		// A line that shows stays shown: a filled row makes an early row in
		// into the line Beeper's copy is.
		if fromCopied && keepEarly {
			if err := fillEarly(tx, keep, m, media, networkID, m.ReplyTo); err != nil {
				return err
			}
		}
		// A direct line the network sent whole knows its time and side; an
		// early or filled row never moves them. Its deleted, edited and
		// text always come across.
		whole := !fromEarly && !fromCopied && (keepEarly || keepCopied || isWhole(m))
		if err := updateLine(tx, keep, m, media, whole); err != nil {
			return err
		}
		if err := foldLine(tx, network, from, into, rid, keep, m.ID); err != nil {
			return err
		}
		if whole || m.Edited {
			return absorbBeeperCopy(tx, network, keep)
		}
		return nil
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}

	if m.Source != SourceBeeper && fromEarly {
		// News about a line: it updates the Beeper line naming it, as
		// PutMessage would, and otherwise moves across still early.
		brid, _, err := beeperLineNaming(tx, network, m)
		if err != nil {
			return err
		}
		if brid != 0 {
			if err := countNews(tx, network); err != nil {
				return err
			}
			if err := updateLine(tx, brid, m, media, false); err != nil {
				return err
			}
			return foldLine(tx, network, from, into, rid, brid, m.ID)
		}
	} else if m.Source != SourceBeeper {
		brid, oldID, err := beeperLineFor(tx, network, m, rid)
		if err != nil {
			return err
		}
		if brid != 0 {
			if err := takeOver(tx, network, brid, oldID, m, media, fromCopied); err != nil {
				return err
			}
			return foldLine(tx, network, from, into, rid, brid, m.ID)
		}
	} else {
		beeperChatIDs, err := beeperChatsOf(tx, rid)
		if err != nil {
			return err
		}
		// The line into holds for one of its Beeper messages, else the line
		// it is by network ID or content.
		err = tx.QueryRow(`SELECT m.rid FROM beeper_ids AS b JOIN messages AS m ON m.rid = b.rid
				WHERE m.network = ? AND m.chat_id = ? AND b.beeper_id IN (SELECT beeper_id FROM beeper_ids WHERE rid = ?)
				ORDER BY m.rid LIMIT 1`, network, into, rid).Scan(&keep)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if keep == 0 {
			if keep, err = matchBeeperCopy(tx, network, into, beeperCopy{
				beeperChatIDs: beeperChatIDs, networkID: networkID, fromMe: m.FromMe,
				timeMs: m.TimeMs, text: m.Text, mediaKind: mediaKindOf(m.Kind, m.Media),
				deleted: m.Deleted,
			}); err != nil {
				return err
			}
		}
		if keep != 0 {
			// An early row the Beeper line names becomes the line.
			if err := fillEarly(tx, keep, m, media, networkID, m.ReplyTo); err != nil {
				return err
			}
			return foldLine(tx, network, from, into, rid, keep, m.ID)
		}
	}

	if _, err := tx.Exec(`UPDATE messages SET chat_id = ? WHERE rid = ?`, into, rid); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE OR IGNORE reactions SET chat_id = ? WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		into, network, from, m.ID)
	return err
}

// rowFlags reports whether row rid is early (news about a line held before
// the line itself) and whether it is copied (an early row Beeper's copy
// filled, not yet sent whole by the network).
func rowFlags(tx *sql.Tx, rid int64) (early, copied bool, err error) {
	err = tx.QueryRow(`SELECT early, copied FROM messages WHERE rid = ?`, rid).Scan(&early, &copied)
	return early, copied, err
}

// fillEarly makes the early row rid the line Beeper's copy m is, when Beeper
// names it: Beeper's time, side, sender, kind, media and text come in, and
// the row shows. What the early row already knows stays: a revoke keeps it
// deleted with no text, and an edit keeps its text. A placeholder's hidden
// gives way to Beeper's, so readable text shows. The row keeps its direct ID
// and source and is marked copied: history, never unread, and never taken
// for another line's Beeper copy, until the line itself lands in it. A row
// that is not early, or a copy with no time, leaves the row as it is, so no
// shown line is ever at time 0.
func fillEarly(tx *sql.Tx, rid int64, m Message, media any, networkID, replyTo string) error {
	_, err := tx.Exec(`UPDATE messages SET
			early = 0,
			copied = 1,
			ts_ms = ?1,
			from_me = ?2,
			sender_id = COALESCE(NULLIF(?3, ''), sender_id),
			sender_name = COALESCE(sender_name, NULLIF(?4, '')),
			kind = COALESCE(NULLIF(?5, ''), kind),
			text = CASE WHEN deleted OR ?6 THEN NULL WHEN edited AND COALESCE(text, '') <> '' THEN text
				ELSE NULLIF(?7, '') END,
			hidden = ?8,
			deleted = deleted OR ?6,
			reply_to = COALESCE(reply_to, NULLIF(?9, '')),
			media_json = COALESCE(media_json, ?10),
			network_id = COALESCE(network_id, NULLIF(?11, ''))
		WHERE rid = ?12 AND early = 1 AND ?1 > 0`,
		m.TimeMs, m.FromMe, m.SenderID, m.SenderName, m.Kind, m.Deleted, m.Text,
		m.Hidden != nil && *m.Hidden, replyTo, media, networkID, rid)
	return err
}

// beeperChatsOf lists every Beeper chat line rid carries a message of.
func beeperChatsOf(tx *sql.Tx, rid int64) ([]string, error) {
	rows, err := tx.Query(`SELECT DISTINCT beeper_chat_id FROM beeper_ids WHERE rid = ?`, rid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// foldLine folds line fromRid (ID fromID, in chat from) into line toRid of
// chat into: its Beeper IDs, reactions and the replies to it go to toRid, and
// it is deleted.
func foldLine(tx *sql.Tx, network, from, into string, fromRid, toRid int64, fromID string) error {
	var toID string
	if err := tx.QueryRow(`SELECT msg_id FROM messages WHERE rid = ?`, toRid).Scan(&toID); err != nil {
		return err
	}
	if err := countMergeOf(tx, network, fromID, toID); err != nil {
		return err
	}
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`UPDATE OR IGNORE beeper_ids SET rid = ? WHERE rid = ?`, []any{toRid, fromRid}},
		{`UPDATE messages SET beeper_id = COALESCE(beeper_id, (SELECT beeper_id FROM messages WHERE rid = ?)) WHERE rid = ?`,
			[]any{fromRid, toRid}},
		{`UPDATE OR IGNORE reactions SET chat_id = ?, msg_id = ? WHERE network = ? AND chat_id = ? AND msg_id = ?`,
			[]any{into, toID, network, from, fromID}},
		{`DELETE FROM reactions WHERE network = ? AND chat_id = ? AND msg_id = ?`, []any{network, from, fromID}},
		{`UPDATE messages SET reply_to = ? WHERE network = ? AND chat_id IN (?, ?) AND reply_to = ?`,
			[]any{toID, network, from, into, fromID}},
		{`DELETE FROM messages WHERE rid = ?`, []any{fromRid}},
	} {
		if _, err := tx.Exec(q.sql, q.args...); err != nil {
			return err
		}
	}
	return nil
}

// loadLine reads one stored line whole, with its media as stored and the
// network ID a Beeper copy named.
func loadLine(tx *sql.Tx, rid int64) (m Message, media any, networkID string, err error) {
	var name, text, reply, mj, beeper, nid sql.NullString
	var seen sql.NullInt64
	var hidden bool
	err = tx.QueryRow(`SELECT chat_id, msg_id, sender_id, sender_name, from_me, ts_ms, text, kind,
			hidden, deleted, edited, reply_to, media_json, raw, source, beeper_id, network_id, seen_ms
			FROM messages WHERE rid = ?`, rid).Scan(&m.ChatID, &m.ID, &m.SenderID, &name, &m.FromMe, &m.TimeMs,
		&text, &m.Kind, &hidden, &m.Deleted, &m.Edited, &reply, &mj, &m.Raw, &m.Source, &beeper, &nid, &seen)
	if err != nil {
		return m, nil, "", err
	}
	m.SenderName, m.Text, m.ReplyTo, m.BeeperID, m.SeenMs = name.String, text.String, reply.String, beeper.String, seen.Int64
	m.Hidden = &hidden
	if mj.Valid {
		media = mj.String
		if err := json.Unmarshal([]byte(mj.String), &m.Media); err != nil {
			return m, nil, "", err
		}
	}
	return m, media, nid.String, nil
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
				mute_end_ms = CASE WHEN ? IS NULL THEN mute_end_ms WHEN ? THEN MAX(?, 0) ELSE 0 END,
				read_through_ms = CASE WHEN ? THEN MAX(read_through_ms, COALESCE(?, read_through_ms))
					ELSE COALESCE(?, read_through_ms) END,
				history_complete = COALESCE(?, history_complete),
				read_only = COALESCE(?, read_only),
				last_activity_ms = MAX(last_activity_ms, ?)
			WHERE network = ? AND chat_id = ?`,
			c.Kind,
			c.Title != nil, deref(c.Title),
			c.Phone != nil, deref(c.Phone),
			c.Muted, c.Muted, c.Muted, c.MuteEndMs,
			c.ReadForwardOnly, c.ReadThroughMs, c.ReadThroughMs, c.HistoryComplete,
			c.ReadOnly,
			c.LastActivityMs,
			network, chat)
		if err != nil {
			return err
		}
		if c.Participants != nil {
			// The handles held, so a write that names a person without one
			// keeps it.
			held, err := heldHandles(tx, network, chat)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM participants WHERE network = ? AND chat_id = ?`, network, chat); err != nil {
				return err
			}
			for i, p := range c.Participants {
				if p.ID == "" {
					continue
				}
				handle := p.Handle
				if handle == "" {
					handle = held[p.ID]
				}
				if _, err := tx.Exec(`INSERT OR REPLACE INTO participants
						(network, chat_id, participant_id, name, phone, is_self, position, handle)
						VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, NULLIF(?, ''))`,
					network, chat, p.ID, p.Name, p.Phone, p.IsSelf, i, handle); err != nil {
					return err
				}
			}
		}
		return refresh(tx, network, chat)
	})
}

// PutMessage stores one line a network received, sent or synced. It reports
// whether a new row was made: a line it already holds is updated (an edit,
// a revoke, a later copy), and a line matching one held only in Beeper's copy
// takes that line over, keeping its Beeper ID, so it shows once.
//
// The shapes a network writes:
//   - a whole line: a kind, text or media, a time above 0, its side and
//     sender, and Hidden when known. Only a whole line shows, takes over a
//     Beeper line, or makes an early row a line;
//   - a read receipt: ID, ChatID and SeenMs only. For an ID nothing holds it
//     is dropped;
//   - an edit: ID, ChatID, Text and Edited; a revoke: ID, ChatID and Deleted,
//     with a time if known; a placeholder: ID, ChatID, TimeMs and Hidden true,
//     with no Kind. Before their line these are kept as early rows, which show
//     nowhere until the line, or Beeper's copy naming it, lands in them;
//   - a line first seen already edited or revoked (a history sync's final
//     state) is written as the whole line, with its kind, time and side,
//     followed by the edit or revoke.
//
// Every Media entry's Kind must be photo, video, voiceNote, sticker or file.
func (s *Store) PutMessage(network string, m Message) (added bool, err error) {
	if network == "" {
		return false, errBadRequest
	}
	if err := checkMessage(&m); err != nil {
		return false, err
	}
	err = s.Batch(network, func(b *Batch) error {
		added, err = b.PutMessage(m)
		return err
	})
	if err != nil {
		return false, err
	}
	return added, nil
}

// putMessage writes one line (PutMessage, checked) in tx and names the chat
// it went to; the chat is not refreshed here.
func putMessage(tx *sql.Tx, network string, m Message) (chat string, added bool, err error) {
	chat, err = resolve(tx, network, m.ChatID)
	if err != nil {
		return "", false, err
	}
	m.ChatID = chat
	if err := ensureChat(tx, network, chat, ""); err != nil {
		return "", false, err
	}
	media, err := mediaJSON(m.Media)
	if err != nil {
		return "", false, err
	}

	var rid int64
	err = tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		network, chat, m.ID).Scan(&rid)
	switch {
	case err == nil:
		if err = updateLine(tx, rid, m, media, isWhole(m)); err == nil && (isWhole(m) || m.Edited) {
			// The line's text may now be Beeper's copy's.
			err = absorbBeeperCopy(tx, network, rid)
		}
	case errors.Is(err, sql.ErrNoRows):
		added, err = putNewID(tx, network, m, media)
	}
	if err != nil {
		return "", false, err
	}
	return chat, added, nil
}

// hasContent reports whether m carries text, media or a kind.
func hasContent(m Message) bool {
	return m.Text != "" || len(m.Media) > 0 || m.Kind != ""
}

// isWhole reports whether m is a line as first sent, not news about one: it
// has content and a time, and is neither an edit nor a revoke. Only a whole
// line knows its own time and side, only a whole line takes over a Beeper
// line, and only a whole line makes an early row a line that shows.
func isWhole(m Message) bool {
	return hasContent(m) && m.TimeMs > 0 && !m.Edited && !m.Deleted
}

// isReceipt reports whether m only says the line was seen.
func isReceipt(m Message) bool {
	return !hasContent(m) && !m.Edited && !m.Deleted && m.Hidden == nil
}

// putNewID stores a line whose ID the chat does not hold yet.
//   - A Beeper line naming the ID: a whole line takes it over; anything else
//     (a receipt, an edit, a revoke) updates it, keeping its ID, time and side.
//   - Else a whole line takes over the Beeper line it matches by content, or
//     is added.
//   - Else a receipt is dropped: there is nothing to mark.
//   - Else (an edit, a revoke, a hidden placeholder) the line is kept as it
//     is known so far, under its ID, as an early row: it never shows, counts
//     or moves the chat, and the line itself, arriving later under the same
//     ID (or Beeper's copy naming it), lands in it and stays edited, deleted
//     or hidden.
func putNewID(tx *sql.Tx, network string, m Message, media any) (added bool, err error) {
	near, err := copyNear(tx, network, m.ChatID, m.ID, m.TimeMs, isWhole(m))
	if err != nil {
		return false, err
	}
	var rid int64
	var oldID string
	if near {
		if rid, oldID, err = beeperLineNaming(tx, network, m); err != nil {
			return false, err
		}
	}
	switch {
	case rid != 0 && isWhole(m):
		return false, takeOver(tx, network, rid, oldID, m, media, false)
	case rid != 0:
		if err := countNews(tx, network); err != nil {
			return false, err
		}
		return false, updateLine(tx, rid, m, media, false)
	case isWhole(m) && near:
		if rid, oldID, err = beeperLineLike(tx, network, m, 0); err != nil {
			return false, err
		}
		if rid != 0 {
			return false, takeOver(tx, network, rid, oldID, m, media, false)
		}
	case isReceipt(m):
		return false, nil
	}
	if err := insertLine(tx, network, m, media); err != nil {
		return false, err
	}
	return true, nil
}

// copyNear reports whether chat holds a line of Beeper's copy that a line
// arriving with ID id could be: one naming id as its network ID, or, when
// byTime (a whole line, the only kind that matches by content), one within
// matchWindowMs of ms. When it holds none, beeperLineNaming, beeperLineLike
// and beeperTwin can find nothing (each looks only at copied lines, named by
// id or in that window), so a write skips them in this one query: every
// line written live, whose time is after the last copy was made, and every
// line in a chat that holds no copy.
func copyNear(tx *sql.Tx, network, chat, id string, ms int64, byTime bool) (bool, error) {
	var near bool
	err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM messages
				WHERE network = ?1 AND chat_id = ?2 AND network_id = ?3 AND source = 'beeper')
			OR (?4 AND EXISTS (SELECT 1 FROM messages
				WHERE network = ?1 AND chat_id = ?2 AND ts_ms BETWEEN ?5 AND ?6 AND source = 'beeper'))`,
		network, chat, id, byTime, ms-matchWindowMs, ms+matchWindowMs).Scan(&near)
	return near, err
}

// insertLine adds m as a new row: a line when it is whole, else an early row.
func insertLine(tx *sql.Tx, network string, m Message, media any) error {
	_, err := tx.Exec(`INSERT INTO messages
			(network, chat_id, msg_id, sender_id, sender_name, from_me, ts_ms, text, kind,
			 hidden, deleted, edited, reply_to, media_json, raw, source, seen_ms, early)
			VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, NULLIF(?, 0), ?)`,
		network, m.ChatID, m.ID, m.SenderID, m.SenderName, m.FromMe, m.TimeMs, lineText(m), kindOr(m.Kind),
		m.Hidden != nil && *m.Hidden, m.Deleted, m.Edited, m.ReplyTo, media, nilIfEmpty(m.Raw), m.Source, m.SeenMs,
		!isWhole(m))
	return err
}

// updateLine folds a later copy of a line into the one held. Nothing it
// lacks erases what is there: a kind, text, media, raw, a reply and a name
// are kept when the copy has none (a read receipt names only the line), and
// hidden when it is not given; deleted and edited stay set once set. A
// deleted line holds no text, and an edited line's text gives way only to a
// later edit. A copy that is not whole never hides a line that has a body
// (a placeholder arriving after its readable line, or for a Beeper line).
//
// whole says the copy knows the line's own time and side (isWhole, for a
// network's update, or a shown direct row in a merge): only then do time,
// side, sender and source move, an early row becomes a line that shows, and
// a filled row stops being copied history.
func updateLine(tx *sql.Tx, rid int64, m Message, media any, whole bool) error {
	_, err := tx.Exec(`UPDATE messages SET
			ts_ms = CASE WHEN ?1 AND ?2 > 0 THEN ?2 ELSE ts_ms END,
			from_me = CASE WHEN ?1 THEN ?3 ELSE from_me END,
			sender_id = CASE WHEN ?1 AND ?4 <> '' THEN ?4 ELSE sender_id END,
			source = CASE WHEN ?1 THEN ?5 ELSE source END,
			early = CASE WHEN ?1 THEN 0 ELSE early END,
			copied = CASE WHEN ?1 THEN 0 ELSE copied END,
			text = CASE WHEN ?6 OR deleted THEN NULL WHEN ?7 <> '' AND (?8 OR edited = 0) THEN ?7 ELSE text END,
			kind = COALESCE(NULLIF(?9, ''), kind),
			sender_name = COALESCE(NULLIF(?10, ''), sender_name),
			hidden = CASE WHEN ?11 IS NULL THEN hidden WHEN ?11 = 0 THEN 0
				WHEN ?1 THEN 1 WHEN `+hasBody+` THEN hidden ELSE 1 END,
			deleted = deleted OR ?6,
			edited = edited OR ?8,
			reply_to = COALESCE(NULLIF(?12, ''), reply_to),
			media_json = COALESCE(?13, media_json),
			raw = COALESCE(?14, raw),
			seen_ms = NULLIF(MAX(COALESCE(seen_ms, 0), ?15), 0)
		WHERE rid = ?16`,
		whole, m.TimeMs, m.FromMe, m.SenderID, m.Source,
		m.Deleted, m.Text, m.Edited,
		m.Kind,
		m.SenderName,
		m.Hidden,
		m.ReplyTo,
		media,
		nilIfEmpty(m.Raw),
		m.SeenMs,
		rid)
	return err
}

// absorbBeeperCopy folds into the direct line rid a line held only in
// Beeper's copy that is the same message by side, time and content, else its
// exact-time twin (beeperTwin), when the direct line has just become
// matchable (its original landed in an early row, or an edit made its text
// Beeper's). A Beeper line from a Beeper chat the line already carries a
// message of is a different message.
func absorbBeeperCopy(tx *sql.Tx, network string, rid int64) error {
	// The copies this could fold are all within the window of the line's
	// time, in its chat: with none there, nothing is read further.
	var near bool
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM messages AS d JOIN messages AS b
				ON b.network = d.network AND b.chat_id = d.chat_id
				AND b.ts_ms BETWEEN d.ts_ms - ?2 AND d.ts_ms + ?2 AND b.source = 'beeper'
			WHERE d.rid = ?1)`, rid, matchWindowMs).Scan(&near); err != nil || !near {
		return err
	}
	m, _, _, err := loadLine(tx, rid)
	if err != nil {
		return err
	}
	early, _, err := rowFlags(tx, rid)
	if err != nil {
		return err
	}
	kind := mediaKindOf(m.Kind, m.Media)
	if early || m.Source == SourceBeeper || !matchable(m.Text, kind, m.Deleted) {
		return nil
	}
	rows, err := tx.Query(`SELECT `+candidateColumns+` FROM messages AS m
			WHERE network = ? AND chat_id = ? AND source = 'beeper' AND msg_id LIKE 'beeper:%' AND from_me = ? AND rid <> ?
			AND ts_ms BETWEEN ? AND ? AND `+fuzzable+`
			AND NOT EXISTS (SELECT 1 FROM beeper_ids AS b WHERE b.rid = m.rid AND b.beeper_chat_id IN (
				SELECT beeper_chat_id FROM beeper_ids WHERE rid = ?))`,
		network, m.ChatID, m.FromMe, rid, m.TimeMs-matchWindowMs, m.TimeMs+matchWindowMs, rid)
	if err != nil {
		return err
	}
	brid, oldID, err := nearestMatch(rows, m.TimeMs, m.Text, kind, true)
	if err == nil && brid == 0 {
		brid, oldID, err = beeperTwin(tx, network, m.ChatID, m.FromMe, m.TimeMs, m.Text, kind, rid)
	}
	if err != nil || brid == 0 {
		return err
	}
	return foldBeeperLine(tx, network, m.ChatID, brid, oldID, rid)
}

// foldBeeperLine folds the Beeper-only line brid (ID oldID) into the direct
// line rid of the same chat: what it was seen at, its Beeper IDs, reactions
// and the replies to it go to rid, and it is deleted.
func foldBeeperLine(tx *sql.Tx, network, chat string, brid int64, oldID string, rid int64) error {
	if _, err := tx.Exec(`UPDATE messages SET seen_ms = NULLIF(MAX(COALESCE(seen_ms, 0),
			COALESCE((SELECT seen_ms FROM messages WHERE rid = ?), 0)), 0) WHERE rid = ?`, brid, rid); err != nil {
		return err
	}
	return foldLine(tx, network, chat, chat, brid, rid, oldID)
}

// foldStoredTwins folds every exact-time twin the store already holds (the
// twin rule, see directTwin), as absorbBeeperCopy would have had the rule
// been there when the second line landed. It runs once, in the upgrade to
// schema 9, and is safe to run again: a folded pair is a pair no more.
func foldStoredTwins(tx *sql.Tx) error {
	rows, err := tx.Query(`SELECT rid FROM messages WHERE ts_ms > 0 AND ` + beeperOnly + ` ORDER BY rid`)
	if err != nil {
		return err
	}
	var rids []int64
	for rows.Next() {
		var rid int64
		if err := rows.Scan(&rid); err != nil {
			rows.Close()
			return err
		}
		rids = append(rids, rid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	touched := map[[2]string]bool{}
	for _, brid := range rids {
		var network string
		var b Message
		var text, mj sql.NullString
		err := tx.QueryRow(`SELECT network, chat_id, msg_id, from_me, ts_ms, text, kind, media_json
				FROM messages WHERE rid = ? AND `+beeperOnly, brid).
			Scan(&network, &b.ChatID, &b.ID, &b.FromMe, &b.TimeMs, &text, &b.Kind, &mj)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		drid, err := directTwin(tx, network, b.ChatID, b.FromMe, b.TimeMs, text.String, storedMediaKind(b.Kind, mj), brid)
		if err != nil {
			return err
		}
		if drid == 0 {
			continue
		}
		if err := foldBeeperLine(tx, network, b.ChatID, brid, b.ID, drid); err != nil {
			return err
		}
		touched[[2]string{network, b.ChatID}] = true
	}
	for c := range touched {
		if err := resolveReplies(tx, c[0], c[1]); err != nil {
			return err
		}
	}
	return nil
}

// takeOver gives the Beeper line rid the direct line's ID, time, side and
// content. Its Beeper IDs stay, and its reactions and the replies to it
// follow the new ID. The direct line is the truth: Beeper may have had the
// side wrong. Only a line that knows its time and side takes over: a whole
// line (isWhole, so its time is never 0), or in a merge a shown row, which
// copied says was itself filled from Beeper's copy. A hidden not given keeps
// what the Beeper line has, and an edit already made to the Beeper line
// keeps its text.
func takeOver(tx *sql.Tx, network string, rid int64, oldID string, m Message, media any, copied bool) error {
	_, err := tx.Exec(`UPDATE messages SET
			msg_id = ?,
			source = ?,
			copied = ?,
			from_me = ?,
			sender_id = COALESCE(NULLIF(?, ''), sender_id),
			sender_name = COALESCE(NULLIF(?, ''), sender_name),
			ts_ms = CASE WHEN ? > 0 THEN ? ELSE ts_ms END,
			text = CASE WHEN ? OR deleted THEN NULL WHEN ? <> '' AND edited = 0 THEN ? ELSE text END,
			kind = COALESCE(NULLIF(?, ''), kind),
			hidden = COALESCE(?, hidden),
			deleted = deleted OR ?,
			edited = edited OR ?,
			reply_to = COALESCE(NULLIF(?, ''), reply_to),
			media_json = COALESCE(?, media_json),
			raw = COALESCE(?, raw),
			seen_ms = NULLIF(MAX(COALESCE(seen_ms, 0), ?), 0)
		WHERE rid = ?`,
		m.ID, m.Source, copied, m.FromMe, m.SenderID, m.SenderName, m.TimeMs, m.TimeMs,
		m.Deleted, m.Text, m.Text,
		m.Kind, m.Hidden, m.Deleted, m.Edited, m.ReplyTo, media, nilIfEmpty(m.Raw), m.SeenMs,
		rid)
	if err != nil {
		return err
	}
	if err := countMergeOf(tx, network, oldID, m.ID); err != nil {
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
		return setReaction(tx, network, chatID, msgID, r)
	})
}

// setReaction is SetReaction in tx.
func setReaction(tx *sql.Tx, network, chatID, msgID string, r Reaction) error {
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
}

// refresh recomputes a chat's derived values: its last visible line, its
// unread count and the oldest line it holds. Activity only moves forward.
// Lines only in Beeper's copy, or shown only through it (copied), are
// history, and never count as unread. Early
// rows are not lines yet, and count for none of these.
func refresh(tx *sql.Tx, network, chat string) error {
	return refreshAt(tx, network, chat, 0)
}

// refreshAt is refresh, with the chat's activity moved forward to at least
// shownMs too: the newest time a line showed at in a batch that refreshes
// the chat once, at its end.
func refreshAt(tx *sql.Tx, network, chat string, shownMs int64) error {
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
			WHERE network = ? AND chat_id = ? AND `+visible+` AND from_me = 0 AND source <> 'beeper' AND copied = 0
			AND ts_ms > (SELECT read_through_ms FROM chats WHERE network = ? AND chat_id = ?)`,
		network, chat, network, chat).Scan(&unread); err != nil {
		return err
	}
	var oldest sql.NullInt64
	if err := tx.QueryRow(`SELECT MIN(ts_ms) FROM messages WHERE network = ? AND chat_id = ? AND early = 0 AND ts_ms > 0`,
		network, chat).Scan(&oldest); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE chats SET
			last_message_id = ?,
			last_from_me = ?,
			last_activity_ms = MAX(last_activity_ms, ?, ?),
			unread = ?,
			history_from_ms = ?
		WHERE network = ? AND chat_id = ?`,
		lastID, lastFromMe && lastID.Valid, lastMs.Int64, shownMs, unread, oldest, network, chat)
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
