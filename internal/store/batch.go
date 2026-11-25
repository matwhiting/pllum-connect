package store

import (
	"database/sql"
	"errors"
	"strings"
)

// Batch is a run of one network's writes in one write transaction
// (Store.Batch): lines, reactions, reaction counts and settles, made in
// order exactly as the store's own methods make them, each through the
// same code. What changes is what they share: one transaction instead of
// one each, and one refresh of each chat they touched, at the end, instead
// of one after every line (review C1). A chat's activity still only moves
// forward, and still to the newest time any of its lines showed at along
// the way, so the end state is the one the writes made one at a time.
//
// A write the store refuses (badRequest) changes nothing and the batch
// goes on, as a refused write one at a time did. A write that fails in
// SQLite fails the whole batch: every later write fails too, Batch returns
// the error, and nothing of the batch is kept.
//
// A Batch is good only inside the function given to Store.Batch, and only
// on its goroutine. That function must not call the Store's own writes
// (PutMessage, PutChat and the rest): they wait for the batch to end, so
// they would wait for ever. Reads through the Store do not wait, but do not
// see the batch's writes either; Batch's own reads do.
type Batch struct {
	tx      *sql.Tx
	network string
	err     error            // the first write that failed in SQLite: the batch is lost
	newest  map[string]int64 // each touched chat → the newest time a line written in it showed at
	order   []string         // the touched chats, in the order first touched
	oldest  map[string]int64 // each chat a line was written to → its oldest line of the network's own before the batch (0: none)
}

// Batch runs fn's writes for network in one write transaction, then
// refreshes each chat they touched, once. fn's error, or a write's that
// failed in SQLite, rolls the whole batch back and is returned.
func (s *Store) Batch(network string, fn func(b *Batch) error) error {
	if network == "" || fn == nil {
		return errBadRequest
	}
	return s.write(func(tx *sql.Tx) error {
		b := &Batch{tx: tx, network: network, newest: map[string]int64{}, oldest: map[string]int64{}}
		defer func() { b.tx = nil }() // a Batch kept past fn writes nothing
		if err := fn(b); err != nil {
			return err
		}
		if b.err != nil {
			return b.err
		}
		for _, chat := range b.order {
			if err := refreshAt(tx, network, chat, b.newest[chat]); err != nil {
				return err
			}
		}
		return nil
	})
}

// Put is what one line's write did: Added when it made a new row, Changed
// when anything Pllum shows of the row moved (a new row, a row not held
// before, or a LineState that differs after the write). The same line
// written again changes nothing.
type Put struct {
	Added, Changed bool
}

// ready reports whether the batch can still write: it is inside its
// function, and no write has failed in SQLite.
func (b *Batch) ready() error {
	if b == nil || b.tx == nil {
		return errInternal
	}
	return b.err
}

// fail records a write that failed in SQLite: the batch is lost.
func (b *Batch) fail(err error) error {
	if b.err == nil {
		b.err = dbErr(err)
	}
	return b.err
}

// touch marks a chat for its refresh at the end of the batch, with the
// newest time a line written in it showed at (0 when none showed).
func (b *Batch) touch(chat string, shownMs int64) {
	prev, ok := b.newest[chat]
	if !ok {
		b.order = append(b.order, chat)
	}
	b.newest[chat] = max(prev, shownMs)
}

// PutMessage stores one line, as Store.PutMessage does, and reports
// whether it made a new row.
func (b *Batch) PutMessage(m Message) (added bool, err error) {
	if err := checkMessage(&m); err != nil {
		return false, err
	}
	if err := b.ready(); err != nil {
		return false, err
	}
	floor, err := b.oldestHeld(m.ChatID)
	if err != nil {
		return false, b.fail(err)
	}
	chat, added, err := putMessage(b.tx, b.network, m)
	if err != nil {
		return false, b.fail(err)
	}
	// A line older than every line of the network's own the chat held
	// before this batch came from an older-history page (the history
	// queue, a scroll-back, a backfill's older pages): history, never
	// unread. Only lines newer than what the chat held can be unread; a
	// chat that held none takes its first page as it always has.
	if floor > 0 && m.TimeMs > 0 && m.TimeMs < floor {
		if _, err := b.tx.Exec(`UPDATE messages SET older = 1
				WHERE network = ? AND chat_id = ? AND msg_id = ? AND early = 0 AND ts_ms > 0 AND ts_ms < ?`,
			b.network, chat, m.ID, floor); err != nil {
			return false, b.fail(err)
		}
	}
	// Writing one line can make only that line's row show (a new line, an
	// early row become a line, a Beeper line it took over), so the newest
	// time the chat's lines showed at along the way is this row's time when
	// it shows now. A refresh after every line moved the chat's activity to
	// at least that; the one refresh at the end moves it there too.
	var shownMs int64
	err = b.tx.QueryRow(`SELECT ts_ms FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ? AND `+visible,
		b.network, chat, m.ID).Scan(&shownMs)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, b.fail(err)
	}
	b.touch(chat, shownMs)
	return added, nil
}

// oldestHeld is the time of the oldest line of the network's own (not
// Beeper's copy, not copied, not early) that chat held when the batch first
// wrote a line to it; 0 when it held none.
func (b *Batch) oldestHeld(chatID string) (int64, error) {
	if ms, ok := b.oldest[chatID]; ok {
		return ms, nil // by the ID as written (most writes of a batch name one chat)
	}
	chat, err := resolve(b.tx, b.network, chatID)
	if err != nil {
		return 0, err
	}
	if ms, ok := b.oldest[chat]; ok {
		b.oldest[chatID] = ms
		return ms, nil
	}
	var ms sql.NullInt64
	if err := b.tx.QueryRow(`SELECT MIN(ts_ms) FROM messages
			WHERE network = ? AND chat_id = ? AND early = 0 AND copied = 0 AND source <> 'beeper' AND ts_ms > 0`,
		b.network, chat).Scan(&ms); err != nil {
		return 0, err
	}
	b.oldest[chat], b.oldest[chatID] = ms.Int64, ms.Int64
	return ms.Int64, nil
}

// Put stores one line, as PutMessage does, and reports what it did, change
// included, read in the batch: what a network uses to tell a line sent
// again from one that moved.
func (b *Batch) Put(m Message) (Put, error) {
	if err := checkMessage(&m); err != nil {
		return Put{}, err
	}
	before, held := b.LineState(m.ChatID, m.ID)
	added, err := b.PutMessage(m)
	if err != nil {
		return Put{}, err
	}
	if added {
		return Put{Added: true, Changed: true}, nil
	}
	after, _ := b.LineState(m.ChatID, m.ID)
	return Put{Changed: !held || after != before}, nil
}

// SetReaction sets one person's reaction to a line, as Store.SetReaction
// does.
func (b *Batch) SetReaction(chatID, msgID string, r Reaction) error {
	if chatID == "" || msgID == "" {
		return errBadRequest
	}
	if err := b.ready(); err != nil {
		return err
	}
	if err := setReaction(b.tx, b.network, chatID, msgID, r); err != nil {
		return b.fail(err)
	}
	return nil
}

// SetReactionSummary replaces a line's reaction counts, as
// Store.SetReactionSummary does.
func (b *Batch) SetReactionSummary(chatID, msgID string, summary []Reaction) error {
	if chatID == "" || msgID == "" {
		return errBadRequest
	}
	if err := b.ready(); err != nil {
		return err
	}
	if err := setReactionSummary(b.tx, b.network, chatID, msgID, summary); err != nil {
		return b.fail(err)
	}
	return nil
}

// SettlePlaceholder hides a line standing in for a message that couldn't be
// read, as Store.SettlePlaceholder does; its chat is refreshed at the end.
func (b *Batch) SettlePlaceholder(chatID, msgID string) (settled bool, err error) {
	if chatID == "" || msgID == "" {
		return false, errBadRequest
	}
	if err := b.ready(); err != nil {
		return false, err
	}
	chat, settled, err := settlePlaceholder(b.tx, b.network, chatID, msgID)
	if err != nil {
		return false, b.fail(err)
	}
	if settled {
		b.touch(chat, 0)
	}
	return settled, nil
}

// LineState is Store.LineState read in the batch, its own writes included.
func (b *Batch) LineState(chatID, msgID string) (LineState, bool) {
	if b.ready() != nil || chatID == "" || msgID == "" {
		return LineState{}, false
	}
	return lineState(b.tx, b.network, chatID, msgID)
}

// ChatOfLine is Store.ChatOfLine read in the batch, its own writes
// included.
func (b *Batch) ChatOfLine(msgID string) (chatID string, ok bool) {
	if b.ready() != nil || msgID == "" {
		return "", false
	}
	return chatOfLine(b.tx, b.network, msgID)
}

// lineKinds are the protocol's kinds of line.
var lineKinds = map[string]bool{
	"text": true, "photo": true, "video": true, "voiceNote": true, "sticker": true, "file": true, "notice": true, "undecryptable": true, "unavailable": true,
}

// checkMessage refuses a line the store cannot take (badRequest) and gives
// one with no source the source live.
func checkMessage(m *Message) error {
	if m.ChatID == "" || m.ID == "" || m.TimeMs < 0 ||
		strings.HasPrefix(m.ChatID, unplacedPrefix) || strings.HasPrefix(m.ID, beeperPrefix) {
		return errBadRequest
	}
	if m.Kind != "" && !lineKinds[m.Kind] {
		return errBadRequest
	}
	for _, x := range m.Media {
		if !mediaKinds[x.Kind] {
			return errBadRequest
		}
	}
	if m.Source == "" {
		m.Source = SourceLive
	}
	if m.Source != SourceLive && m.Source != SourceSync {
		return errBadRequest
	}
	return nil
}
