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
	"strconv"
	"strings"
	"sync"

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
			for _, stmt := range upgrades {
				if _, err := tx.Exec(stmt); err != nil {
					return err
				}
			}
		}
		_, err := tx.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?)`, strconv.Itoa(schemaVersion))
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

// mergeChat moves every line, reaction, member and mapping of chat from into
// chat into, folding in lines into already holds, and deletes from.
func mergeChat(tx *sql.Tx, network, from, into string) error {
	if _, err := tx.Exec(`INSERT OR IGNORE INTO chats
			(network, chat_id, kind, title, phone, muted, placed, read_only, beeper_chat_id,
			 history_complete, read_through_ms, last_activity_ms)
			SELECT network, ?, kind, title, phone, muted, placed, read_only, beeper_chat_id,
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
		`INSERT OR IGNORE INTO participants (network, chat_id, participant_id, name, phone, is_self, position)
			SELECT network, ?, participant_id, name, phone, is_self, position FROM participants
			WHERE network = ? AND chat_id = ?`,
		`UPDATE beeper_chats SET chat_id = ? WHERE network = ? AND chat_id = ?`,
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

	var keep int64
	err = tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
		network, into, m.ID).Scan(&keep)
	switch {
	case err == nil:
		if m.Source != SourceBeeper {
			if err := updateLine(tx, keep, m, media); err != nil {
				return err
			}
		}
		return foldLine(tx, network, from, into, rid, keep, m.ID)
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}

	if m.Source != SourceBeeper {
		brid, oldID, err := beeperLineFor(tx, network, m)
		if err != nil {
			return err
		}
		if brid != 0 {
			if err := takeOver(tx, network, brid, oldID, m, media); err != nil {
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
				hasMedia: len(m.Media) > 0, deleted: m.Deleted,
			}); err != nil {
				return err
			}
		}
		if keep != 0 {
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
	if m.Kind != "" && !lineKinds[m.Kind] {
		return false, errBadRequest
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
		case errors.Is(err, sql.ErrNoRows) && !hasContent(m):
			// A receipt, revoke or the like names the line and nothing more.
			// It updates the Beeper line naming it, keeping all it omits, and
			// never makes a line of its own.
			rid, _, err = beeperLineNaming(tx, network, m)
			if err == nil && rid != 0 {
				err = updateLine(tx, rid, m, media)
			}
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

// hasContent reports whether m is a line itself, not just news of one: it
// carries text, media or a kind.
func hasContent(m Message) bool {
	return m.Text != "" || len(m.Media) > 0 || m.Kind != ""
}

func insertLine(tx *sql.Tx, network string, m Message, media any) error {
	_, err := tx.Exec(`INSERT INTO messages
			(network, chat_id, msg_id, sender_id, sender_name, from_me, ts_ms, text, kind,
			 hidden, deleted, edited, reply_to, media_json, raw, source, seen_ms)
			VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, NULLIF(?, 0))`,
		network, m.ChatID, m.ID, m.SenderID, m.SenderName, m.FromMe, m.TimeMs, lineText(m), kindOr(m.Kind),
		m.Hidden != nil && *m.Hidden, m.Deleted, m.Edited, m.ReplyTo, media, nilIfEmpty(m.Raw), m.Source, m.SeenMs)
	return err
}

// updateLine folds a later copy of a line into the one held. Nothing it
// lacks erases what is there: a kind, text, media, raw, a reply and a name
// are kept when the copy has none (a read receipt names only the line), and
// hidden when it is not given; deleted and edited stay set once set.
func updateLine(tx *sql.Tx, rid int64, m Message, media any) error {
	_, err := tx.Exec(`UPDATE messages SET
			text = CASE WHEN ? THEN NULL WHEN ? <> '' THEN ? ELSE text END,
			kind = COALESCE(NULLIF(?, ''), kind),
			sender_name = COALESCE(NULLIF(?, ''), sender_name),
			hidden = COALESCE(?, hidden),
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

// takeOver gives the Beeper line rid the direct line's ID, side and content.
// Its Beeper IDs stay, and its reactions and the replies to it follow the new
// ID. The direct line is the truth: Beeper may have had the side wrong. Only
// a line with content takes over (see hasContent), so its side is given; a
// time of 0 or a hidden not given keeps what the Beeper line has.
func takeOver(tx *sql.Tx, network string, rid int64, oldID string, m Message, media any) error {
	_, err := tx.Exec(`UPDATE messages SET
			msg_id = ?,
			source = ?,
			from_me = ?,
			sender_id = COALESCE(NULLIF(?, ''), sender_id),
			sender_name = COALESCE(NULLIF(?, ''), sender_name),
			ts_ms = CASE WHEN ? > 0 THEN ? ELSE ts_ms END,
			text = CASE WHEN ? THEN NULL WHEN ? <> '' THEN ? ELSE text END,
			kind = COALESCE(NULLIF(?, ''), kind),
			hidden = COALESCE(?, hidden),
			deleted = deleted OR ?,
			edited = edited OR ?,
			reply_to = COALESCE(NULLIF(?, ''), reply_to),
			media_json = COALESCE(?, media_json),
			raw = COALESCE(?, raw),
			seen_ms = NULLIF(MAX(COALESCE(seen_ms, 0), ?), 0)
		WHERE rid = ?`,
		m.ID, m.Source, m.FromMe, m.SenderID, m.SenderName, m.TimeMs, m.TimeMs,
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
