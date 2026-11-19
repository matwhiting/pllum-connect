package linkedin

import (
	"strings"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"
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
// token), "listed" and "listCursor" (the first sign-in's listing of the inbox,
// done or where it got to), "cursor.<chat>" (where that chat's history goes
// on), "histDone.<chat>" (its history reached 730 days), "pause" (the
// back-off) and "block" (the session LinkedIn ended, by its hash).

// Name is the network's name in the protocol and in the store.
const Name = "linkedin"

// conversationPrefix starts every conversation URN: a chat ID the network
// can ask LinkedIn about (unlike a chat Beeper's copy made).
const conversationPrefix = "urn:li:msg_conversation:"

// profileID is a participant's (or a profile's) ID: the last part of its URN.
func profileID(u linkedingo.URN) string { return u.ID() }

// isConversation reports whether a chat ID is a LinkedIn conversation's.
func isConversation(chatID string) bool { return strings.HasPrefix(chatID, conversationPrefix) }

// value is the network's stored value called name, or "".
func (l *LinkedIn) value(name string) string {
	l.vmu.Lock()
	v, ok := l.values[name]
	l.vmu.Unlock()
	if ok {
		return v
	}
	v, _, err := l.st.NetworkValue(Name, name)
	if err != nil {
		return ""
	}
	l.vmu.Lock()
	l.values[name] = v
	l.vmu.Unlock()
	return v
}

// keep stores the network's value called name ("" removes it), writing only
// when it changes.
func (l *LinkedIn) keep(name, v string) error {
	if l.value(name) == v {
		return nil
	}
	if err := l.st.PutNetworkValue(Name, name, v); err != nil {
		return err
	}
	l.vmu.Lock()
	l.values[name] = v
	l.vmu.Unlock()
	return nil
}

// self is his profile ID, or "" before his first sign-in.
func (l *LinkedIn) self() string { return l.value("self") }
