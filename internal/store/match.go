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

// sameContent reports whether two copies carry the same content: the same
// non-empty match key, or no text on either side and the same media kind.
//
// WhatsApp's own text is plain, so a direct line's text is read as escaped
// text: "Alex <alex@example.com>" keeps its address, as Beeper's escaped HTML
// does. Only when the Beeper text itself holds a tag (plain text that looks
// like one, or real markup) is the direct text also read as it is, tags
// stripped alike on both sides.
func sameContent(textA, mediaKindA string, directA bool, textB, mediaKindB string, directB bool) bool {
	if directB && !directA {
		textA, mediaKindA, directA, textB, mediaKindB, directB = textB, mediaKindB, directB, textA, mediaKindA, directA
	}
	var a []string // the first copy's keys, direct when either is
	b := MatchKey(textB)
	switch {
	case directA && !directB:
		a = []string{MatchKey(html.EscapeString(textA))}
		if tagRE.MatchString(textB) {
			a = append(a, MatchKey(textA))
		}
	default:
		a = []string{MatchKey(textA)}
	}
	anyText := b != ""
	for _, x := range a {
		if x != "" && x == b {
			return true
		}
		anyText = anyText || x != ""
	}
	return !anyText && mediaKindA == mediaKindB
}

// mediaKindOf is a line's first attachment's kind, else its own kind.
func mediaKindOf(kind string, media []Media) string {
	if len(media) > 0 && media[0].Kind != "" {
		return media[0].Kind
	}
	return kindOr(kind)
}

// kindOr is a line's kind, text when it names none.
func kindOr(kind string) string {
	if kind == "" {
		return "text"
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

// candidateColumns are what nearestMatch reads of each stored line.
const candidateColumns = `rid, msg_id, ts_ms, text, kind, media_json, source`

// nearestMatch picks, from rows of candidateColumns, the line with the same
// content as the incoming copy nearest in time to ts; ties go to the
// earlier-stored line. rid is 0 when none matches.
func nearestMatch(rows *sql.Rows, ts int64, text, mediaKind string, direct bool) (rid int64, id string, err error) {
	defer rows.Close()
	best := int64(-1)
	for rows.Next() {
		var c struct {
			rid, ts          int64
			id, kind, source string
			text, media      sql.NullString
		}
		if err := rows.Scan(&c.rid, &c.id, &c.ts, &c.text, &c.kind, &c.media, &c.source); err != nil {
			return 0, "", err
		}
		if !sameContent(c.text.String, storedMediaKind(c.kind, c.media), c.source != SourceBeeper, text, mediaKind, direct) {
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
	rows, err := tx.Query(`SELECT `+candidateColumns+` FROM messages
			WHERE network = ? AND chat_id = ? AND source = 'beeper' AND from_me = ?
			AND ts_ms BETWEEN ? AND ?`,
		network, m.ChatID, m.FromMe, m.TimeMs-matchWindowMs, m.TimeMs+matchWindowMs)
	if err != nil {
		return 0, "", err
	}
	return nearestMatch(rows, m.TimeMs, m.Text, mediaKindOf(m.Kind, m.Media), true)
}

// beeperCopy is one Beeper message, as matchBeeperCopy compares it.
type beeperCopy struct {
	beeperChatID string // the Beeper chat it was copied from
	networkID    string
	fromMe       bool
	timeMs       int64
	text         string
	mediaKind    string
}

// matchBeeperCopy finds the line a Beeper message is: the line its network ID
// names (or a Beeper line naming the same network ID), else a line on the
// same side, within the time window, with the same content, that no other
// message of the same Beeper chat already holds. Within one Beeper chat two
// alike messages are two lines; across Beeper chats (Beeper can split one
// person into two) the same message is one line. rid is 0 when none.
func matchBeeperCopy(tx *sql.Tx, network, chat string, c beeperCopy) (rid int64, err error) {
	if c.networkID != "" {
		err = tx.QueryRow(`SELECT rid FROM messages
				WHERE network = ?1 AND chat_id = ?2 AND (msg_id = ?3 OR (source = 'beeper' AND network_id = ?3))
				ORDER BY msg_id = ?3 DESC, rid LIMIT 1`,
			network, chat, c.networkID).Scan(&rid)
		if err == nil {
			return rid, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
	}
	rows, err := tx.Query(`SELECT `+candidateColumns+` FROM messages AS m
			WHERE network = ? AND chat_id = ? AND from_me = ? AND ts_ms BETWEEN ? AND ?
			AND NOT EXISTS (SELECT 1 FROM beeper_ids AS b WHERE b.rid = m.rid AND b.beeper_chat_id = ?)`,
		network, chat, c.fromMe, c.timeMs-matchWindowMs, c.timeMs+matchWindowMs, c.beeperChatID)
	if err != nil {
		return 0, err
	}
	rid, _, err = nearestMatch(rows, c.timeMs, c.text, c.mediaKind, false)
	return rid, err
}
