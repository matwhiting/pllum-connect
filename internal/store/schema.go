package store

// schemaVersion is the version written to meta. An older store is brought up
// to it; a store from a newer helper is refused rather than guessed at.
const schemaVersion = 2 // 2 added beeper_ids

// schema makes every table. Every key starts with network, so each network
// keeps to its own rows. Chats and messages have an integer rid, stable for
// the row's life, which cursors and the full-text index use: a message
// taking over its Beeper copy keeps its rid while its msg_id changes.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS meta (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`,

	`CREATE TABLE IF NOT EXISTS chats (
		rid              INTEGER PRIMARY KEY,
		network          TEXT    NOT NULL,
		chat_id          TEXT    NOT NULL,
		kind             TEXT    NOT NULL DEFAULT 'single',
		title            TEXT,
		phone            TEXT,
		last_activity_ms INTEGER NOT NULL DEFAULT 0,
		last_message_id  TEXT,
		last_from_me     INTEGER NOT NULL DEFAULT 0,
		unread           INTEGER NOT NULL DEFAULT 0,
		muted            INTEGER NOT NULL DEFAULT 0,
		read_only        INTEGER NOT NULL DEFAULT 0,
		placed           INTEGER NOT NULL DEFAULT 1,
		beeper_chat_id   TEXT,
		history_from_ms  INTEGER,
		history_complete INTEGER NOT NULL DEFAULT 0,
		read_through_ms  INTEGER NOT NULL DEFAULT 0,
		UNIQUE (network, chat_id)
	)`,
	`CREATE INDEX IF NOT EXISTS chats_activity ON chats (network, last_activity_ms DESC, rid DESC)`,
	`CREATE INDEX IF NOT EXISTS chats_beeper ON chats (network, beeper_chat_id)`,

	// Which chat each Beeper chat's lines went to. One direct chat may hold
	// several Beeper chats (Beeper can split one person across two).
	`CREATE TABLE IF NOT EXISTS beeper_chats (
		network        TEXT NOT NULL,
		beeper_chat_id TEXT NOT NULL,
		chat_id        TEXT NOT NULL,
		PRIMARY KEY (network, beeper_chat_id)
	)`,

	// An alias is another JID for a chat that already has its ID.
	`CREATE TABLE IF NOT EXISTS aliases (
		network   TEXT NOT NULL,
		alias_jid TEXT NOT NULL,
		chat_id   TEXT NOT NULL,
		PRIMARY KEY (network, alias_jid)
	)`,

	`CREATE TABLE IF NOT EXISTS participants (
		network        TEXT    NOT NULL,
		chat_id        TEXT    NOT NULL,
		participant_id TEXT    NOT NULL,
		name           TEXT,
		phone          TEXT,
		is_self        INTEGER NOT NULL DEFAULT 0,
		position       INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (network, chat_id, participant_id)
	)`,

	// source is 'live', 'sync' or 'beeper'. beeper_id is the Beeper message
	// this line was matched to or copied from. network_id is the network's
	// own ID a Beeper copy named, kept so the direct line can take it over.
	`CREATE TABLE IF NOT EXISTS messages (
		rid         INTEGER PRIMARY KEY,
		network     TEXT    NOT NULL,
		chat_id     TEXT    NOT NULL,
		msg_id      TEXT    NOT NULL,
		sender_id   TEXT    NOT NULL DEFAULT '',
		sender_name TEXT,
		from_me     INTEGER NOT NULL DEFAULT 0,
		ts_ms       INTEGER NOT NULL,
		text        TEXT,
		kind        TEXT    NOT NULL DEFAULT 'text',
		hidden      INTEGER NOT NULL DEFAULT 0,
		deleted     INTEGER NOT NULL DEFAULT 0,
		edited      INTEGER NOT NULL DEFAULT 0,
		reply_to    TEXT,
		media_json  TEXT,
		raw         BLOB,
		source      TEXT    NOT NULL,
		beeper_id   TEXT,
		network_id  TEXT,
		seen_ms     INTEGER,
		UNIQUE (network, chat_id, msg_id)
	)`,
	`CREATE INDEX IF NOT EXISTS messages_time ON messages (network, chat_id, ts_ms)`,
	`CREATE INDEX IF NOT EXISTS messages_beeper ON messages (network, beeper_id)`,
	`CREATE INDEX IF NOT EXISTS messages_network_id ON messages (network, chat_id, network_id)`,
	`CREATE INDEX IF NOT EXISTS messages_all_time ON messages (network, ts_ms)`,

	// Every Beeper message a line carries, and the Beeper chat it came from.
	// messages.beeper_id is the first of them; one line can carry several when
	// Beeper held the same message twice. Keyed by rid, so a line keeps them
	// through a takeover or a move to another chat.
	`CREATE TABLE IF NOT EXISTS beeper_ids (
		rid            INTEGER NOT NULL REFERENCES messages (rid) ON DELETE CASCADE,
		network        TEXT    NOT NULL,
		beeper_id      TEXT    NOT NULL,
		beeper_chat_id TEXT    NOT NULL DEFAULT '',
		PRIMARY KEY (rid, beeper_id)
	)`,
	`CREATE INDEX IF NOT EXISTS beeper_ids_id ON beeper_ids (network, beeper_id)`,

	`CREATE TABLE IF NOT EXISTS reactions (
		network   TEXT    NOT NULL,
		chat_id   TEXT    NOT NULL,
		msg_id    TEXT    NOT NULL,
		sender_id TEXT    NOT NULL,
		emoji     TEXT    NOT NULL,
		from_me   INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (network, chat_id, msg_id, sender_id)
	)`,

	// The outbox: a send's ID is made and kept here before it is sent.
	`CREATE TABLE IF NOT EXISTS outgoing (
		network TEXT    NOT NULL,
		msg_id  TEXT    NOT NULL,
		chat_id TEXT    NOT NULL,
		kind    TEXT    NOT NULL,
		state   TEXT    NOT NULL,
		txn     TEXT,
		made_ms INTEGER NOT NULL,
		PRIMARY KEY (network, msg_id)
	)`,

	// History notifications kept before they are acknowledged.
	`CREATE TABLE IF NOT EXISTS pending_history (
		network TEXT NOT NULL,
		id      INTEGER NOT NULL,
		blob    BLOB NOT NULL,
		PRIMARY KEY (network, id)
	)`,

	`CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5 (
		text,
		content = 'messages',
		content_rowid = 'rid',
		tokenize = 'unicode61 remove_diacritics 2'
	)`,
	`CREATE TRIGGER IF NOT EXISTS messages_fts_insert AFTER INSERT ON messages BEGIN
		INSERT INTO messages_fts (rowid, text) VALUES (new.rid, new.text);
	END`,
	`CREATE TRIGGER IF NOT EXISTS messages_fts_delete AFTER DELETE ON messages BEGIN
		INSERT INTO messages_fts (messages_fts, rowid, text) VALUES ('delete', old.rid, old.text);
	END`,
	`CREATE TRIGGER IF NOT EXISTS messages_fts_update AFTER UPDATE OF text ON messages BEGIN
		INSERT INTO messages_fts (messages_fts, rowid, text) VALUES ('delete', old.rid, old.text);
		INSERT INTO messages_fts (rowid, text) VALUES (new.rid, new.text);
	END`,
}

// upgrades bring a store of an older version up to schemaVersion, after
// schema has run. Each must be safe to run again.
var upgrades = []string{
	// 1 → 2: each line's Beeper ID, with its chat's (first) Beeper chat.
	`INSERT OR IGNORE INTO beeper_ids (rid, network, beeper_id, beeper_chat_id)
		SELECT m.rid, m.network, m.beeper_id, COALESCE(c.beeper_chat_id, '')
		FROM messages AS m LEFT JOIN chats AS c ON c.network = m.network AND c.chat_id = m.chat_id
		WHERE m.beeper_id IS NOT NULL`,
}

// hasBody is the SQL test for a line holding text or media: media entries,
// or a media kind alone (Beeper may name a photo without its details).
const hasBody = `(COALESCE(text, '') <> '' OR media_json IS NOT NULL
	OR kind IN ('photo', 'video', 'voiceNote', 'sticker', 'file'))`

// mediaKinds are the kinds of line that are media.
var mediaKinds = map[string]bool{"photo": true, "video": true, "voiceNote": true, "sticker": true, "file": true}

// visible is the SQL test for a line that shows: not hidden, not deleted, and
// with a body.
const visible = `hidden = 0 AND deleted = 0 AND ` + hasBody
