package store

import (
	"database/sql"
	"strings"
	"unicode"
)

// Query is one `search`. An empty Text finds lines by the other filters
// alone; empty ChatIDs means every chat. AfterMs and BeforeMs, when not 0,
// bound the time strictly.
type Query struct {
	Text     string
	ChatIDs  []string
	FromMe   *bool
	SenderID string
	AfterMs  int64
	BeforeMs int64
	Limit    int
	Cursor   string
}

// SearchPage is one page of found lines, newest first. Cursor is empty on
// the last page.
type SearchPage struct {
	Items  []Message
	Cursor string
}

// ftsQuery turns his words into an FTS5 query: every word, each as a prefix,
// quoted so nothing he types is FTS syntax. ok is false when the text holds
// no words.
func ftsQuery(text string) (q string, ok bool) {
	words := strings.FieldsFunc(text, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) })
	if len(words) == 0 {
		return "", false
	}
	for i, w := range words {
		words[i] = `"` + w + `"*`
	}
	return strings.Join(words, " "), true
}

// Search finds visible lines, newest first. Unplaced chats are searched too.
func (s *Store) Search(network string, q Query) (SearchPage, error) {
	if err := s.ready(); err != nil {
		return SearchPage{}, err
	}
	limit, err := pageLimit(q.Limit, maxSearch)
	if err != nil {
		return SearchPage{}, err
	}
	sqlq := `SELECT ` + messageColumns + ` FROM messages WHERE network = ? AND ` + visible
	args := []any{network}
	if strings.TrimSpace(q.Text) != "" {
		fts, ok := ftsQuery(q.Text)
		if !ok {
			return SearchPage{Items: []Message{}}, nil
		}
		sqlq += ` AND rid IN (SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?)`
		args = append(args, fts)
	}
	if len(q.ChatIDs) > 0 {
		chats := make([]string, 0, len(q.ChatIDs))
		for _, id := range q.ChatIDs {
			chat, err := resolve(s.db, network, id)
			if err != nil {
				return SearchPage{}, dbErr(err)
			}
			chats = append(chats, chat)
		}
		sqlq += ` AND chat_id IN (?` + strings.Repeat(", ?", len(chats)-1) + `)`
		for _, c := range chats {
			args = append(args, c)
		}
	}
	if q.FromMe != nil {
		sqlq += ` AND from_me = ?`
		args = append(args, *q.FromMe)
	}
	if q.SenderID != "" {
		sqlq += ` AND sender_id = ?`
		args = append(args, q.SenderID)
	}
	if q.AfterMs != 0 {
		sqlq += ` AND ts_ms > ?`
		args = append(args, q.AfterMs)
	}
	if q.BeforeMs != 0 {
		sqlq += ` AND ts_ms < ?`
		args = append(args, q.BeforeMs)
	}
	if q.Cursor != "" {
		ms, rid, err := decodeCursor(q.Cursor)
		if err != nil {
			return SearchPage{}, err
		}
		sqlq += ` AND (ts_ms < ? OR (ts_ms = ? AND rid < ?))`
		args = append(args, ms, ms, rid)
	}
	sqlq += ` ORDER BY ts_ms DESC, rid DESC LIMIT ?`
	args = append(args, limit+1)

	items, rids, err := s.readMessages(network, sqlq, args...)
	if err != nil {
		return SearchPage{}, dbErr(err)
	}
	var page SearchPage
	if len(items) > limit {
		items, rids = items[:limit], rids[:limit]
		page.Cursor = encodeCursor(items[limit-1].TimeMs, rids[limit-1])
	}
	page.Items = items
	if page.Items == nil {
		page.Items = []Message{}
	}
	return page, nil
}

// FindChats finds chats by name, number or a member's name, most recently
// active first. Unplaced chats are found too.
func (s *Store) FindChats(network, query string, limit int) ([]Chat, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	limit, err := pageLimit(limit, maxFindChats)
	if err != nil {
		return nil, err
	}
	key := MatchKey(query)
	digits := onlyDigits(query)
	numeric := len(digits) >= 3 && strings.TrimFunc(query, func(r rune) bool {
		return unicode.IsDigit(r) || unicode.IsSpace(r) || strings.ContainsRune("+-().", r)
	}) == ""
	if key == "" {
		return nil, errBadRequest
	}

	members := map[string][]string{}
	rows, err := s.db.Query(`SELECT chat_id, name FROM participants WHERE network = ? AND name IS NOT NULL`, network)
	if err != nil {
		return nil, dbErr(err)
	}
	for rows.Next() {
		var chat, name string
		if err := rows.Scan(&chat, &name); err != nil {
			rows.Close()
			return nil, dbErr(err)
		}
		members[chat] = append(members[chat], name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbErr(err)
	}

	rows, err = s.db.Query(`SELECT chat_id, title, phone FROM chats WHERE network = ?
			ORDER BY last_activity_ms DESC, rid DESC`, network)
	if err != nil {
		return nil, dbErr(err)
	}
	var found []string
	for rows.Next() && len(found) < limit {
		var chat string
		var title, phone sql.NullString
		if err := rows.Scan(&chat, &title, &phone); err != nil {
			rows.Close()
			return nil, dbErr(err)
		}
		hit := strings.Contains(MatchKey(title.String), key)
		if !hit && numeric {
			hit = strings.Contains(onlyDigits(phone.String), digits) ||
				strings.Contains(onlyDigits(strings.SplitN(chat, "@", 2)[0]), digits)
		}
		for _, name := range members[chat] {
			hit = hit || strings.Contains(MatchKey(name), key)
		}
		if hit {
			found = append(found, chat)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, dbErr(err)
	}

	out := make([]Chat, 0, len(found))
	for _, id := range found {
		c, ok, err := s.Chat(network, id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, c)
		}
	}
	return out, nil
}

func onlyDigits(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
}
