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

// fuzzable is the SQL test for a stored line content may match: not early,
// not deleted, and with a body (text, media, or a media kind). An empty or
// deleted line says nothing about which message it was, and an early row
// has no time or side of its own yet, so only an ID can match them.
const fuzzable = `early = 0 AND deleted = 0 AND ` + hasBody

// matchable reports the same of an incoming copy, given its media kind
// (mediaKindOf, which falls back to the line's own kind).
func matchable(text, mediaKind string, deleted bool) bool {
	return !deleted && (text != "" || mediaKinds[mediaKind])
}

// beeperLineNaming finds the line held only in Beeper's copy whose network
// ID Beeper named as m's ID. rid is 0 when none.
func beeperLineNaming(tx *sql.Tx, network string, m Message) (rid int64, oldID string, err error) {
	err = tx.QueryRow(`SELECT rid, msg_id FROM messages
			WHERE network = ? AND chat_id = ? AND source = 'beeper' AND network_id = ?
			ORDER BY rid LIMIT 1`, network, m.ChatID, m.ID).Scan(&rid, &oldID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", nil
	}
	return rid, oldID, err
}

// beeperLineFor finds the line held only in Beeper's copy that a direct line
// m (stored as self, in another chat) is: the one naming m's ID
// (beeperLineNaming), else beeperLineLike.
func beeperLineFor(tx *sql.Tx, network string, m Message, self int64) (rid int64, oldID string, err error) {
	if rid, oldID, err = beeperLineNaming(tx, network, m); err != nil || rid != 0 {
		return rid, oldID, err
	}
	return beeperLineLike(tx, network, m, self)
}

// beeperLineLike finds, when m is matchable, the Beeper line on the same side
// within the time window with the same content, else its exact-time twin
// (beeperTwin; self is m's row when stored, 0 when arriving). rid is 0 when
// none.
func beeperLineLike(tx *sql.Tx, network string, m Message, self int64) (rid int64, oldID string, err error) {
	if !matchable(m.Text, mediaKindOf(m.Kind, m.Media), m.Deleted) {
		return 0, "", nil
	}
	rows, err := tx.Query(`SELECT `+candidateColumns+` FROM messages
			WHERE network = ? AND chat_id = ? AND source = 'beeper' AND msg_id LIKE 'beeper:%' AND from_me = ?
			AND ts_ms BETWEEN ? AND ? AND `+fuzzable,
		network, m.ChatID, m.FromMe, m.TimeMs-matchWindowMs, m.TimeMs+matchWindowMs)
	if err != nil {
		return 0, "", err
	}
	if rid, oldID, err = nearestMatch(rows, m.TimeMs, m.Text, mediaKindOf(m.Kind, m.Media), true); err != nil || rid != 0 {
		return rid, oldID, err
	}
	return beeperTwin(tx, network, m.ChatID, m.FromMe, m.TimeMs, m.Text, mediaKindOf(m.Kind, m.Media), self)
}

// beeperCopy is one Beeper message, as matchBeeperCopy compares it.
type beeperCopy struct {
	beeperChatIDs []string // the Beeper chats it was copied from
	networkID     string
	fromMe        bool
	timeMs        int64
	text          string
	mediaKind     string
	deleted       bool
	peers         int // other copies arriving with it at its exact time and side; the twin rule needs none
}

// matchBeeperCopy finds the line a Beeper message is: the line its network ID
// names (or a Beeper line naming the same network ID), else a line on the
// same side, within the time window, with the same content, that no other
// message of the same Beeper chats already holds, else its exact-time twin
// (directTwin). Content never matches an empty or deleted line, on either
// side. Within one Beeper chat two alike messages are two lines; across
// Beeper chats (Beeper can split one person into two) the same message is
// one line. rid is 0 when none.
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
	if !matchable(c.text, c.mediaKind, c.deleted) {
		return 0, nil
	}
	args := []any{network, chat, c.fromMe, c.timeMs - matchWindowMs, c.timeMs + matchWindowMs}
	exclude := "NULL" // no Beeper chat: nothing is excluded
	if len(c.beeperChatIDs) > 0 {
		exclude = "?" + strings.Repeat(", ?", len(c.beeperChatIDs)-1)
		for _, id := range c.beeperChatIDs {
			args = append(args, id)
		}
	}
	rows, err := tx.Query(`SELECT `+candidateColumns+` FROM messages AS m
			WHERE network = ? AND chat_id = ? AND from_me = ? AND ts_ms BETWEEN ? AND ? AND `+fuzzable+`
			AND NOT EXISTS (SELECT 1 FROM beeper_ids AS b WHERE b.rid = m.rid AND b.beeper_chat_id IN (`+exclude+`))`,
		args...)
	if err != nil {
		return 0, err
	}
	if rid, _, err = nearestMatch(rows, c.timeMs, c.text, c.mediaKind, false); err != nil || rid != 0 || c.peers > 0 {
		return rid, err
	}
	return directTwin(tx, network, chat, c.fromMe, c.timeMs, c.text, c.mediaKind, 0)
}

// The twin rule: a line held only in Beeper's copy and a direct line that
// carries no Beeper message are one message, whatever their texts say, when
// they are the only such pair at one exact, non-zero time, in one chat, on
// one side, and both have text or share a media kind. Beeper and WhatsApp
// stamp one message with the same millisecond, but their texts can differ
// (WhatsApp writes a mention as @<number>, Beeper as the name), so where the
// content match finds nothing, time and uniqueness settle it. It is a
// fallback only: an ID or content match always comes first.

// beeperOnly is the SQL test for a line held only in Beeper's copy that the
// twin rule may pair.
const beeperOnly = `source = 'beeper' AND msg_id LIKE 'beeper:%' AND ` + fuzzable

// directFree is the SQL test for a direct line carrying no Beeper message
// that the twin rule may pair. It reads the row as messages, unaliased.
const directFree = `source <> 'beeper' AND ` + fuzzable +
	` AND NOT EXISTS (SELECT 1 FROM beeper_ids AS x WHERE x.rid = messages.rid)`

// twinLine is one candidate for the twin rule.
type twinLine struct {
	rid       int64
	id        string
	text      string
	mediaKind string
}

// twinsAt reads the lines meeting which (beeperOnly or directFree) at one
// exact time, chat and side, leaving out the line except: the first, and how
// many there are (at most 2, which is enough to say it is not the only one).
func twinsAt(tx *sql.Tx, network, chat string, fromMe bool, ts int64, which string, except int64) (first twinLine, n int, err error) {
	rows, err := tx.Query(`SELECT rid, msg_id, text, kind, media_json FROM messages
			WHERE network = ? AND chat_id = ? AND from_me = ? AND ts_ms = ? AND rid <> ? AND `+which+`
			ORDER BY rid LIMIT 2`, network, chat, fromMe, ts, except)
	if err != nil {
		return first, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var l twinLine
		var text, mj sql.NullString
		var kind string
		if err := rows.Scan(&l.rid, &l.id, &text, &kind, &mj); err != nil {
			return first, 0, err
		}
		l.text, l.mediaKind = text.String, storedMediaKind(kind, mj)
		if n == 0 {
			first = l
		}
		n++
	}
	return first, n, rows.Err()
}

// twinBodies reports whether two lines may be twins by what they hold: both
// have text (a media line's caption is its text), or they share a media kind.
func twinBodies(textA, mediaKindA, textB, mediaKindB string) bool {
	return (MatchKey(textA) != "" && MatchKey(textB) != "") || mediaKindA == mediaKindB
}

// directTwin finds, by the twin rule, the direct line a Beeper copy is: a
// copy arriving (self 0), or the stored Beeper-only line self. The copy must
// be the only Beeper-only line at its time, chat and side, and the direct
// line the only one there carrying no Beeper message. rid is 0 when none.
func directTwin(tx *sql.Tx, network, chat string, fromMe bool, ts int64, text, mediaKind string, self int64) (rid int64, err error) {
	if ts <= 0 {
		return 0, nil
	}
	if _, n, err := twinsAt(tx, network, chat, fromMe, ts, beeperOnly, self); err != nil || n != 0 {
		return 0, err
	}
	d, n, err := twinsAt(tx, network, chat, fromMe, ts, directFree, 0)
	if err != nil || n != 1 || !twinBodies(text, mediaKind, d.text, d.mediaKind) {
		return 0, err
	}
	return d.rid, nil
}

// beeperTwin finds, by the twin rule, the Beeper-only line a direct line is:
// a line arriving (self 0), or the stored line self, which must carry no
// Beeper message. The direct line must be the only one at its time, chat and
// side carrying no Beeper message, and the Beeper line the only Beeper-only
// line there. rid is 0 when none.
func beeperTwin(tx *sql.Tx, network, chat string, fromMe bool, ts int64, text, mediaKind string, self int64) (rid int64, id string, err error) {
	if ts <= 0 {
		return 0, "", nil
	}
	if self != 0 {
		var carries int
		if err := tx.QueryRow(`SELECT count(*) FROM beeper_ids WHERE rid = ?`, self).Scan(&carries); err != nil || carries > 0 {
			return 0, "", err
		}
	}
	if _, n, err := twinsAt(tx, network, chat, fromMe, ts, directFree, self); err != nil || n != 0 {
		return 0, "", err
	}
	b, n, err := twinsAt(tx, network, chat, fromMe, ts, beeperOnly, 0)
	if err != nil || n != 1 || !twinBodies(text, mediaKind, b.text, b.mediaKind) {
		return 0, "", err
	}
	return b.rid, b.id, nil
}
