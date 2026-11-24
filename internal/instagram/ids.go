package instagram

import (
	"cmp"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// Instagram's IDs, as the protocol and the store use them:
//
//   - a chat is its thread key, in decimal (ThreadKey: for a one-to-one, the
//     other person's FBID; for a group, the group's ID). It never changes.
//   - a line is Meta's message ID (the message's `id`, "mid.$…", which every
//     reaction, edit and unsend names as its `message_id`), unchanged.
//   - a person is their Instagram ID (a user's `id`, the IGID). Messages,
//     reactions and receipts name people by FBID, so the FBID → IGID of
//     everyone a thread lists is kept.
//   - Meta's own requests and the socket's deltas name a thread by its
//     Instagram chat ID (the thread's `id`), which for a one-to-one is not
//     the thread key: both ways are kept.
//
// What the network keeps for itself lives in the store's meta table, under
// "instagram.<name>": "thread.<IG chat ID>" → chat, "igid.<chat>" → IG chat
// ID, "user.<FBID>" → IGID, "cursor.<chat>" → where older history goes on,
// "viewer" and "self" (his FBID and IGID), and the client's state ("state",
// "account", "seq", "seqTS").

// chatIDOf is the chat ID of a thread key.
func chatIDOf(threadKey int64) string { return strconv.FormatInt(threadKey, 10) }

// value is the network's stored value called name, or "".
func (g *Instagram) value(name string) string { return g.values.Get(name) }

// keep stores the network's value called name ("" removes it), writing
// only when it changes.
func (g *Instagram) keep(name, v string) error { return g.values.Keep(name, v) }

// viewer is his FBID, or 0 before he has signed in.
func (g *Instagram) viewer() int64 {
	id, _ := strconv.ParseInt(g.value("viewer"), 10, 64)
	return id
}

// self is his IGID, or "".
func (g *Instagram) self() string { return g.value("self") }

// setViewer keeps his FBID and IGID, when known.
func (g *Instagram) setViewer(fbid int64, igid string) {
	if fbid != 0 {
		_ = g.keep("viewer", strconv.FormatInt(fbid, 10))
	}
	if igid != "" {
		_ = g.keep("self", igid)
	}
}

// chatOfThread is the chat of an Instagram chat ID, when a thread with it
// has been listed.
func (g *Instagram) chatOfThread(igChatID string) (string, bool) {
	if igChatID == "" {
		return "", false
	}
	chat := g.value("thread." + igChatID)
	return chat, chat != ""
}

// threadOfChat is the Instagram chat ID of a chat, the one Meta's requests
// take.
func (g *Instagram) threadOfChat(chat string) (string, bool) {
	if chat == "" {
		return "", false
	}
	id := g.value("igid." + chat)
	return id, id != ""
}

// place keeps a thread's IDs (both ways, and the FBID → IGID of everyone it
// lists) and is its chat ID; ok is false for a thread with no key.
func (g *Instagram) place(t *slidetypes.ThreadInfo) (chat string, ok bool) {
	if t == nil || t.ThreadKey == 0 {
		return "", false
	}
	chat = chatIDOf(t.ThreadKey)
	if t.ID != "" {
		_ = g.keep("thread."+t.ID, chat)
		_ = g.keep("igid."+chat, t.ID)
	}
	for _, u := range t.Users {
		g.learnUser(u)
	}
	if v := t.Viewer; v != nil {
		g.setViewer(v.InteropMessagingUserFBID, v.ID)
	}
	return chat, true
}

// learnUser keeps one person's FBID → IGID.
func (g *Instagram) learnUser(u *slidetypes.User) {
	if u == nil || u.InteropMessagingUserFBID == 0 || u.ID == "" {
		return
	}
	_ = g.keep("user."+strconv.FormatInt(u.InteropMessagingUserFBID, 10), u.ID)
}

// personOf is the participant ID (IGID) of the person with this FBID, and
// whether it is him. A person no thread has listed keeps their FBID.
func (g *Instagram) personOf(fbid int64) (id string, isSelf bool) {
	if fbid == 0 {
		return "", false
	}
	if fbid == g.viewer() {
		return cmp.Or(g.self(), strconv.FormatInt(fbid, 10)), true
	}
	if igid := g.value("user." + strconv.FormatInt(fbid, 10)); igid != "" {
		return igid, false
	}
	return strconv.FormatInt(fbid, 10), false
}

// participants are the people a thread lists, then him.
func (g *Instagram) participants(t *slidetypes.ThreadInfo) []store.Participant {
	out := make([]store.Participant, 0, len(t.Users)+1)
	for _, u := range t.Users {
		if u == nil || u.ID == "" {
			continue
		}
		out = append(out, store.Participant{ID: u.ID, Name: nameOf(u)})
	}
	self := g.self()
	if t.Viewer != nil && t.Viewer.ID != "" {
		self = t.Viewer.ID
	}
	if self == "" {
		self = t.ViewerID
	}
	if self != "" {
		p := store.Participant{ID: self, IsSelf: true}
		if t.Viewer != nil {
			p.Name = nameOf(t.Viewer)
		}
		out = append(out, p)
	}
	return out
}

// nameOf is what Instagram shows for a person: their full name, else their
// username.
func nameOf(u *slidetypes.User) string {
	if u == nil {
		return ""
	}
	return cmp.Or(u.FullName, u.Username)
}

// ParticipantAlias is the participant ID (IGID) of the person Beeper names by
// FBID, through the people the network has seen; false when the ID is not an
// FBID it knows. Only digits are asked about, so nothing else reaches the
// stored values.
func (g *Instagram) ParticipantAlias(id string) (string, bool) {
	if id == "" || strings.Trim(id, "0123456789") != "" {
		return "", false
	}
	igid := g.value("user." + id)
	return igid, igid != ""
}
