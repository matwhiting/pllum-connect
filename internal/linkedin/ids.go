package linkedin

import (
	"net/url"
	"strings"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// LinkedIn's IDs, as the protocol and the store use them:
//
//   - a chat is its conversation URN, unchanged
//     ("urn:li:msg_conversation:(urn:li:fsd_profile:…,2-…)"); Pllum keys it
//     "linkedin:<URN>".
//   - a line is its message's EntityURN, unchanged.
//   - a person is their profile ID (the last part of a participant's URN,
//     URN.ID()), his own included.
//
// Both URNs carry his own profile ID, so none of them is ever logged; the
// helper logs nothing.
//
// What the network keeps for itself lives in the store's meta table, under
// "linkedin.<name>": "self" (his profile ID, set at the first sign-in and
// compared at every later one), "syncToken" (LinkedIn's conversations sync
// token), "listed", "listedFolders" and "listCursor" (the listing of the
// folders, INBOX then ARCHIVE: done, done over both folders (listedFolders,
// "2"; an account listed before it lists them once more), or where it got
// to, as "<folder>:<cursor>"), "cursor.<chat>" (where that chat's history goes
// on), "histDone.<chat>" (its history reached 730 days), "fresh.<chat>"
// (what came in it is still to fetch), "pause" (the back-off) and "block"
// (the session LinkedIn ended, by its hash). His Disconnect forgets all but
// the pause (forgetAccount).

// Name is the network's name in the protocol and in the store.
const Name = "linkedin"

// conversationPrefix starts every conversation URN: a chat ID the network
// can ask LinkedIn about (unlike a chat Beeper's copy made).
const conversationPrefix = "urn:li:msg_conversation:"

// profileID is a participant's (or a profile's) ID: the last part of its URN.
func profileID(u linkedingo.URN) string { return u.ID() }

// publicID is the public profile ID in a member's profile URL
// ("https://www.linkedin.com/in/<public ID>/", on linkedin.com or one of its
// country hosts), decoded; "" for any other URL, and for one that names the
// member by a member ID ("ACoAA…" or "ACwAA…"), which is never a public ID.
// It reads the URL only.
func publicID(profileURL string) string {
	u, err := url.Parse(profileURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if host != "linkedin.com" && !strings.HasSuffix(host, ".linkedin.com") {
		return ""
	}
	rest, ok := strings.CutPrefix(u.Path, "/in/")
	if !ok {
		return ""
	}
	id, _, _ := strings.Cut(rest, "/")
	if isMemberID(id) {
		return ""
	}
	return id
}

// isMemberID reports whether id has the shape of a LinkedIn member ID (a
// profile URN's last part) rather than a public profile ID.
func isMemberID(id string) bool {
	return strings.HasPrefix(id, "ACoAA") || strings.HasPrefix(id, "ACwAA")
}

// isConversation reports whether a chat ID is a LinkedIn conversation's.
func isConversation(chatID string) bool { return strings.HasPrefix(chatID, conversationPrefix) }

// value is the network's stored value called name, or "".
func (l *LinkedIn) value(name string) string { return l.values.Get(name) }

// keep stores the network's value called name ("" removes it), writing only
// when it changes.
func (l *LinkedIn) keep(name, v string) error { return l.values.Keep(name, v) }

// self is his profile ID, or "" before his first sign-in and after his
// Disconnect.
func (l *LinkedIn) self() string { return l.value("self") }

// conversationOwner is the profile ID a conversation URN carries, the
// account whose inbox it is in ("urn:li:msg_conversation:(urn:li:fsd_profile:
// <owner>,2-…)"), or "" when it carries none.
func conversationOwner(chatID string) string {
	rest, ok := strings.CutPrefix(chatID, conversationPrefix+"(")
	if !ok {
		return ""
	}
	owner, _, ok := strings.Cut(rest, ",")
	if !ok {
		return ""
	}
	return owner[strings.LastIndex(owner, ":")+1:]
}

// accountValues are the stored values that belong to the account signed in,
// besides the per-chat ones (chatValues).
var accountValues = []string{"self", "syncToken", "listed", "listedFolders", "listCursor", "block"}

// chatValues prefix the per-chat stored values: a chat's fresh mark, where
// its history goes on, and its history being done.
var chatValues = []string{"fresh.", "cursor.", "histDone."}

// forgetAccount forgets the account signed in (his Disconnect): his profile
// ID, the sync token, the listing of the inbox, every chat's fresh mark and
// history progress, and the session LinkedIn ended. The chats and their
// lines stay. l.mu is held.
func (l *LinkedIn) forgetAccount() {
	for _, name := range accountValues {
		l.forget(name)
	}
	for _, c := range l.conversations() {
		for _, prefix := range chatValues {
			l.forget(prefix + c.ID)
		}
	}
}

// forget removes the stored value called name, whatever the memory of it
// says.
func (l *LinkedIn) forget(name string) { _ = l.values.Put(name, "") }

// retireOtherAccounts makes read-only every stored conversation of another
// account than self's: they stay readable, and are never written to or
// worked on beside his. l.mu is held.
func (l *LinkedIn) retireOtherAccounts(self string) {
	for _, c := range l.conversations() {
		if owner := conversationOwner(c.ID); owner != "" && owner != self && !c.ReadOnly {
			l.readOnly(c.ID)
		}
	}
}

// conversations is every LinkedIn conversation the store holds, newest
// first.
func (l *LinkedIn) conversations() []store.Chat {
	var out []store.Chat
	cursor := ""
	for range 50 {
		page, err := l.st.Chats(Name, 0, cursor, 200)
		if err != nil {
			return out
		}
		for _, c := range page.Items {
			if isConversation(c.ID) {
				out = append(out, c)
			}
		}
		if page.Cursor == "" {
			break
		}
		cursor = page.Cursor
	}
	return out
}
