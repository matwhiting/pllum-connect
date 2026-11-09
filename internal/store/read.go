package store

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// The most items one page of each read holds.
const (
	maxChats     = 200
	maxMessages  = 200
	maxSearch    = 50
	maxFindChats = 20
)

// ChatPage is one page of chats, newest activity first. Cursor is empty on
// the last page.
type ChatPage struct {
	Items  []Chat
	Cursor string
}

// MessagePage is one page of a chat's lines, oldest first. OldestCursor,
// passed as before, gives the page before this one.
type MessagePage struct {
	Items        []Message
	HasMore      bool
	OldestCursor string
}

// pageLimit checks a page size: below one is badRequest, above max is max.
func pageLimit(limit, max int) (int, error) {
	if limit < 1 {
		return 0, errBadRequest
	}
	return min(limit, max), nil
}

// A cursor is a (time, rid) position, opaque to Pllum and holding no ID.
func encodeCursor(ms, rid int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(ms, 10) + "." + strconv.FormatInt(rid, 10)))
}

func decodeCursor(c string) (ms, rid int64, err error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, 0, errBadRequest
	}
	a, r, ok := strings.Cut(string(b), ".")
	if !ok {
		return 0, 0, errBadRequest
	}
	if ms, err = strconv.ParseInt(a, 10, 64); err != nil {
		return 0, 0, errBadRequest
	}
	if rid, err = strconv.ParseInt(r, 10, 64); err != nil || rid < 1 {
		return 0, 0, errBadRequest
	}
	return ms, rid, nil
}

const chatColumns = `rid, chat_id, kind, title, phone, last_activity_ms, last_message_id, last_from_me,
	unread, muted, read_only, history_from_ms, history_complete`

func scanChat(rows interface{ Scan(...any) error }) (Chat, int64, error) {
	var c Chat
	var rid int64
	var title, phone, last sql.NullString
	var from sql.NullInt64
	err := rows.Scan(&rid, &c.ID, &c.Kind, &title, &phone, &c.LastActivityMs, &last, &c.LastFromMe,
		&c.Unread, &c.Muted, &c.ReadOnly, &from, &c.HistoryComplete)
	c.Title, c.Phone, c.LastMessageID, c.HistoryFromMs = title.String, phone.String, last.String, from.Int64
	return c, rid, err
}

func (s *Store) participants(network, chat string) ([]Participant, error) {
	rows, err := s.db.Query(`SELECT participant_id, name, phone, is_self FROM participants
			WHERE network = ? AND chat_id = ? ORDER BY position, participant_id LIMIT ?`,
		network, chat, maxParticipants)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Participant{}
	for rows.Next() {
		var p Participant
		var name, phone sql.NullString
		if err := rows.Scan(&p.ID, &name, &phone, &p.IsSelf); err != nil {
			return nil, err
		}
		p.Name, p.Phone = name.String, phone.String
		out = append(out, p)
	}
	return out, rows.Err()
}

// Chats is a page of chats active since sinceMs, newest activity first.
// Unplaced chats are among them, read-only.
func (s *Store) Chats(network string, sinceMs int64, cursor string, limit int) (ChatPage, error) {
	if err := s.ready(); err != nil {
		return ChatPage{}, err
	}
	limit, err := pageLimit(limit, maxChats)
	if err != nil {
		return ChatPage{}, err
	}
	q := `SELECT ` + chatColumns + ` FROM chats WHERE network = ? AND last_activity_ms >= ?`
	args := []any{network, sinceMs}
	if cursor != "" {
		ms, rid, err := decodeCursor(cursor)
		if err != nil {
			return ChatPage{}, err
		}
		q += ` AND (last_activity_ms < ? OR (last_activity_ms = ? AND rid < ?))`
		args = append(args, ms, ms, rid)
	}
	q += ` ORDER BY last_activity_ms DESC, rid DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return ChatPage{}, dbErr(err)
	}
	var page ChatPage
	var rids []int64
	for rows.Next() {
		c, rid, err := scanChat(rows)
		if err != nil {
			rows.Close()
			return ChatPage{}, dbErr(err)
		}
		page.Items = append(page.Items, c)
		rids = append(rids, rid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ChatPage{}, dbErr(err)
	}
	if len(page.Items) > limit {
		page.Items, rids = page.Items[:limit], rids[:limit]
		last := page.Items[limit-1]
		page.Cursor = encodeCursor(last.LastActivityMs, rids[limit-1])
	}
	for i := range page.Items {
		if page.Items[i].Participants, err = s.participants(network, page.Items[i].ID); err != nil {
			return ChatPage{}, dbErr(err)
		}
	}
	if page.Items == nil {
		page.Items = []Chat{}
	}
	return page, nil
}

// Chat is one chat, found by its ID or an alias; ok is false when there is
// none.
func (s *Store) Chat(network, chatID string) (c Chat, ok bool, err error) {
	if err := s.ready(); err != nil {
		return Chat{}, false, err
	}
	if chatID == "" {
		return Chat{}, false, errBadRequest
	}
	chat, err := resolve(s.db, network, chatID)
	if err != nil {
		return Chat{}, false, dbErr(err)
	}
	c, _, err = scanChat(s.db.QueryRow(`SELECT `+chatColumns+` FROM chats WHERE network = ? AND chat_id = ?`, network, chat))
	if errors.Is(err, sql.ErrNoRows) {
		return Chat{}, false, nil
	}
	if err != nil {
		return Chat{}, false, dbErr(err)
	}
	if c.Participants, err = s.participants(network, chat); err != nil {
		return Chat{}, false, dbErr(err)
	}
	return c, true, nil
}

const messageColumns = `rid, chat_id, msg_id, sender_id, sender_name, from_me, ts_ms, text, kind,
	hidden, deleted, edited, reply_to, media_json, seen_ms, source, beeper_id`

func scanMessage(rows *sql.Rows) (Message, int64, error) {
	var m Message
	var rid int64
	var name, text, reply, media, beeper sql.NullString
	var seen sql.NullInt64
	var hidden bool
	err := rows.Scan(&rid, &m.ChatID, &m.ID, &m.SenderID, &name, &m.FromMe, &m.TimeMs, &text, &m.Kind,
		&hidden, &m.Deleted, &m.Edited, &reply, &media, &seen, &m.Source, &beeper)
	if err != nil {
		return m, 0, err
	}
	m.SenderName, m.Text, m.ReplyTo, m.SeenMs, m.BeeperID = name.String, text.String, reply.String, seen.Int64, beeper.String
	m.Hidden = &hidden
	if media.Valid {
		if err := json.Unmarshal([]byte(media.String), &m.Media); err != nil {
			return m, 0, err
		}
	}
	return m, rid, nil
}

// readMessages runs a query of messageColumns and adds each line's reactions.
func (s *Store) readMessages(network, q string, args ...any) ([]Message, []int64, error) {
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, nil, err
	}
	var out []Message
	var rids []int64
	for rows.Next() {
		m, rid, err := scanMessage(rows)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		out = append(out, m)
		rids = append(rids, rid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	for i := range out {
		if out[i].Reactions, err = s.reactions(network, out[i].ChatID, out[i].ID); err != nil {
			return nil, nil, err
		}
	}
	return out, rids, nil
}

func (s *Store) reactions(network, chat, msgID string) ([]Reaction, error) {
	rows, err := s.db.Query(`SELECT emoji, sender_id, from_me FROM reactions
			WHERE network = ? AND chat_id = ? AND msg_id = ? ORDER BY sender_id`, network, chat, msgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Reaction{}
	for rows.Next() {
		var r Reaction
		if err := rows.Scan(&r.Emoji, &r.SenderID, &r.FromMe); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Messages is a page of a chat's lines, oldest first within the page, from
// the newest back: before is the previous page's OldestCursor, or empty for
// the newest page. HasMore is false only on the page holding the oldest line.
// An unknown chat is notFound.
func (s *Store) Messages(network, chatID, before string, limit int) (MessagePage, error) {
	if err := s.ready(); err != nil {
		return MessagePage{}, err
	}
	if chatID == "" {
		return MessagePage{}, errBadRequest
	}
	limit, err := pageLimit(limit, maxMessages)
	if err != nil {
		return MessagePage{}, err
	}
	c, ok, err := s.Chat(network, chatID)
	if err != nil {
		return MessagePage{}, err
	}
	if !ok {
		return MessagePage{}, errNotFound
	}
	q := `SELECT ` + messageColumns + ` FROM messages WHERE network = ? AND chat_id = ?`
	args := []any{network, c.ID}
	if before != "" {
		ms, rid, err := decodeCursor(before)
		if err != nil {
			return MessagePage{}, err
		}
		q += ` AND (ts_ms < ? OR (ts_ms = ? AND rid < ?))`
		args = append(args, ms, ms, rid)
	}
	q += ` ORDER BY ts_ms DESC, rid DESC LIMIT ?`
	args = append(args, limit+1)
	items, rids, err := s.readMessages(network, q, args...)
	if err != nil {
		return MessagePage{}, dbErr(err)
	}
	page := MessagePage{HasMore: len(items) > limit}
	if page.HasMore {
		items, rids = items[:limit], rids[:limit]
	}
	if n := len(items); n > 0 {
		page.OldestCursor = encodeCursor(items[n-1].TimeMs, rids[n-1])
	}
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	page.Items = items
	if page.Items == nil {
		page.Items = []Message{}
	}
	return page, nil
}

// nullable sends an empty string as null.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullableMs sends 0 as null.
func nullableMs(ms int64) any {
	if ms == 0 {
		return nil
	}
	return ms
}

// MarshalJSON writes the protocol's Chat.
func (c Chat) MarshalJSON() ([]byte, error) {
	ps := make([]map[string]any, 0, len(c.Participants))
	for _, p := range c.Participants {
		ps = append(ps, map[string]any{
			"id": p.ID, "name": nullable(p.Name), "phone": nullable(p.Phone), "isSelf": p.IsSelf,
		})
	}
	return json.Marshal(map[string]any{
		"id":              c.ID,
		"kind":            c.Kind,
		"title":           nullable(c.Title),
		"phone":           nullable(c.Phone),
		"lastActivityMs":  c.LastActivityMs,
		"lastMessageID":   nullable(c.LastMessageID),
		"lastFromMe":      c.LastFromMe,
		"unread":          c.Unread,
		"muted":           c.Muted,
		"readOnly":        c.ReadOnly,
		"participants":    ps,
		"historyFromMs":   nullableMs(c.HistoryFromMs),
		"historyComplete": c.HistoryComplete,
	})
}

// MarshalJSON writes the protocol's Message.
func (m Message) MarshalJSON() ([]byte, error) {
	media := make([]map[string]any, 0, len(m.Media))
	for _, x := range m.Media {
		var w, h any
		if x.Width > 0 {
			w = x.Width
		}
		if x.Height > 0 {
			h = x.Height
		}
		media = append(media, map[string]any{"kind": x.Kind, "width": w, "height": h, "hasFile": x.HasFile})
	}
	reactions := m.Reactions
	if reactions == nil {
		reactions = []Reaction{}
	}
	return json.Marshal(map[string]any{
		"id":         m.ID,
		"chatID":     m.ChatID,
		"senderID":   m.SenderID,
		"senderName": nullable(m.SenderName),
		"fromMe":     m.FromMe,
		"timeMs":     m.TimeMs,
		"text":       nullable(m.Text),
		"kind":       m.Kind,
		"hidden":     m.Hidden != nil && *m.Hidden,
		"deleted":    m.Deleted,
		"edited":     m.Edited,
		"replyTo":    nullable(m.ReplyTo),
		"media":      media,
		"reactions":  reactions,
		"seenMs":     nullableMs(m.SeenMs),
	})
}
