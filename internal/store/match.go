package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"html"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// matchWindowMs is how far apart in time two copies of one line may be.
const matchWindowMs = 2000

// tagRE finds an HTML comment or tag: "<" then a letter or "/letter". A
// bare "<" in prose ("a < b", "<3") is not a tag.
var tagRE = regexp.MustCompile(`(?s)<!--.*?-->|</?([A-Za-z][A-Za-z0-9]*)\b[^<>]*>`)

// blockTags break a line, so they become a space rather than nothing.
var blockTags = map[string]bool{
	"br": true, "p": true, "div": true, "li": true, "ul": true, "ol": true,
	"blockquote": true, "pre": true, "tr": true, "td": true, "th": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
}

// MatchKey is the form two copies of a line's text are compared in: HTML
// tags and entities stripped, NFC, lowercase, whitespace collapsed and
// zero-width characters dropped.
func MatchKey(text string) string {
	s := tagRE.ReplaceAllStringFunc(text, func(tag string) string {
		if m := tagRE.FindStringSubmatch(tag); len(m) > 1 && blockTags[strings.ToLower(m[1])] {
			return " "
		}
		return ""
	})
	s = html.UnescapeString(s)
	s = strings.Map(func(r rune) rune {
		switch r {
		case '\u200b', '\u200c', '\u200d', '\u200e', '\u200f', '\u2060', '\ufeff':
			return -1
		}
		return r
	}, s)
	s = norm.NFC.String(s)
	s = strings.ToLower(s)
	return strings.Join(strings.Fields(s), " ")
}

// sameContent reports whether two copies carry the same content: equal match
// keys, or both keys empty with the same media kind.
func sameContent(textA, mediaKindA, textB, mediaKindB string) bool {
	a, b := MatchKey(textA), MatchKey(textB)
	if a != "" || b != "" {
		return a == b
	}
	return mediaKindA == mediaKindB
}

// mediaKindOf is a line's first attachment's kind, else its own kind.
func mediaKindOf(kind string, media []Media) string {
	if len(media) > 0 && media[0].Kind != "" {
		return media[0].Kind
	}
	return kind
}

// storedMediaKind is mediaKindOf for a stored row.
func storedMediaKind(kind string, mediaJSON sql.NullString) string {
	var media []Media
	if mediaJSON.Valid {
		_ = json.Unmarshal([]byte(mediaJSON.String), &media)
	}
	return mediaKindOf(kind, media)
}

// candidate is a stored line that may be another copy of an incoming one.
type candidate struct {
	rid   int64
	id    string
	ts    int64
	text  sql.NullString
	kind  string
	media sql.NullString
}

// nearestMatch picks, from rows of (rid, msg_id, ts_ms, text, kind,
// media_json), the line with the same content nearest in time to ts; ties go
// to the earlier-stored line. rid is 0 when none matches.
func nearestMatch(rows *sql.Rows, ts int64, text, mediaKind string) (rid int64, id string, err error) {
	defer rows.Close()
	best := int64(-1)
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.rid, &c.id, &c.ts, &c.text, &c.kind, &c.media); err != nil {
			return 0, "", err
		}
		if !sameContent(c.text.String, storedMediaKind(c.kind, c.media), text, mediaKind) {
			continue
		}
		d := c.ts - ts
		if d < 0 {
			d = -d
		}
		if best < 0 || d < best || (d == best && c.rid < rid) {
			best, rid, id = d, c.rid, c.id
		}
	}
	return rid, id, rows.Err()
}

// beeperLineFor finds the line held only in Beeper's copy that a direct line
// m is: one whose network ID Beeper named as m's ID, else one on the same
// side within the time window with the same content. rid is 0 when none.
func beeperLineFor(tx *sql.Tx, network string, m Message) (rid int64, oldID string, err error) {
	err = tx.QueryRow(`SELECT rid, msg_id FROM messages
			WHERE network = ? AND chat_id = ? AND source = 'beeper' AND network_id = ?
			ORDER BY rid LIMIT 1`, network, m.ChatID, m.ID).Scan(&rid, &oldID)
	if err == nil {
		return rid, oldID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, "", err
	}
	rows, err := tx.Query(`SELECT rid, msg_id, ts_ms, text, kind, media_json FROM messages
			WHERE network = ? AND chat_id = ? AND source = 'beeper' AND from_me = ?
			AND ts_ms BETWEEN ? AND ?`,
		network, m.ChatID, m.FromMe, m.TimeMs-matchWindowMs, m.TimeMs+matchWindowMs)
	if err != nil {
		return 0, "", err
	}
	return nearestMatch(rows, m.TimeMs, m.Text, mediaKindOf(m.Kind, m.Media))
}

// directLineFor finds the line an imported Beeper message is: the line named
// by its network ID, else a line not yet matched to any Beeper message, on
// the same side, within the time window, with the same content. rid is 0 when
// none.
func directLineFor(tx *sql.Tx, network, chat string, m ImportMessage) (rid int64, err error) {
	if m.NetworkID != "" {
		err = tx.QueryRow(`SELECT rid FROM messages WHERE network = ? AND chat_id = ? AND msg_id = ?`,
			network, chat, m.NetworkID).Scan(&rid)
		if err == nil {
			return rid, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	rows, err := tx.Query(`SELECT rid, msg_id, ts_ms, text, kind, media_json FROM messages
			WHERE network = ? AND chat_id = ? AND beeper_id IS NULL AND from_me = ?
			AND ts_ms BETWEEN ? AND ?`,
		network, chat, m.FromMe, m.TimeMs-matchWindowMs, m.TimeMs+matchWindowMs)
	if err != nil {
		return 0, err
	}
	rid, _, err = nearestMatch(rows, m.TimeMs, m.Text, importMediaKind(m))
	return rid, err
}
