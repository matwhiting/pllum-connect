package messenger

import (
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// Messenger's IDs, as the protocol and the store use them (step 3's Global
// Constraints):
//
//   - a group without encryption is its thread key, in decimal;
//   - an encrypted one-to-one is "<their FBID>@msgr", and an encrypted group
//     "<id>@g.us", as whatsmeow names the chat;
//   - a one-to-one without encryption (its thread key is their FBID) is an
//     alias of "<FBID>@msgr", never a chat of its own: its lines are his
//     older history with that person. A pointer from an older thread to an
//     encrypted one (a hybrid thread mapping) makes the older thread's key
//     an alias of the encrypted chat too;
//   - any other thread Meta lists as a conversation (Marketplace, a page,
//     a community group) is its thread key, as a group is;
//   - a line is Meta's message ID ("mid.$…") unchanged; a person is their
//     FBID, in decimal.
//
// Folders (the threads that hold other threads), spam and threads of no
// known type are never chats.
//
// What the network keeps for itself lives in the store's meta table, under
// "messenger.<name>": "thread.<chat>" → the unencrypted thread key whose
// history FetchOlder pages, "encrypted.<chat>" when the chat has an
// encrypted side, "cursor.<chat>" → where its older history goes on
// ("<ms>\n<message ID>"), "olderDone.<chat>" when an encrypted chat's
// older thread has no more, "backfilled" once the first sign-in's
// backfill has run, "block" and "rate" (messenger.go), and "ereregistered"
// (the day the one re-registration began) and "fbdevice" (a Facebook
// device ID made once, for a session that names none) (e2ee.go). His restored
// backup's (history.go): "ecursor.<chat>" → where a chat's backup goes on
// (ms), "encDone.<chat>" when it has no more, "estalled.<chat>" while it is
// held, "ebf.<chat>" and "ebfPages" (the first restore's pages, a chat's
// and in all) and "ebackfilled" once that has run.

// Server suffixes of the encrypted chats.
const (
	oneToOneSuffix = "@msgr"
	groupSuffix    = "@g.us"
)

// keyID is a thread key or an FBID in decimal.
func keyID(key int64) string { return strconv.FormatInt(key, 10) }

// placement is where a thread's chat is.
type placement struct {
	chat  string // the chat ID
	kind  string // "single" or "group"
	alias string // the thread key, when it is an alias of chat
	enc   bool   // the chat is an encrypted one
}

// placeThread is where a thread of this key and type belongs; ok is false
// for a thread that is not a chat.
func placeThread(key int64, tt table.ThreadType) (p placement, ok bool) {
	if key == 0 {
		return p, false
	}
	switch {
	case tt == table.ENCRYPTED_OVER_WA_ONE_TO_ONE:
		return placement{chat: keyID(key) + oneToOneSuffix, kind: "single", enc: true}, true
	case tt == table.ENCRYPTED_OVER_WA_GROUP:
		return placement{chat: keyID(key) + groupSuffix, kind: "group", enc: true}, true
	case tt.IsOneToOne():
		return placement{chat: keyID(key) + oneToOneSuffix, kind: "single", alias: keyID(key)}, true
	case notAChat(tt):
		return p, false
	case isGroup(tt) || tt == table.ROOM:
		return placement{chat: keyID(key), kind: "group"}, true
	}
	return placement{chat: keyID(key), kind: "single"}, true
}

// notAChat reports whether a thread type is no conversation: a folder, a
// community's folder or category, or a type Meta did not say.
func notAChat(tt table.ThreadType) bool {
	switch tt {
	case table.UNKNOWN_THREAD_TYPE, table.FOLDER, table.COMMUNITY_FOLDER, table.COMMUNITY_CHANNEL_CATEGORY:
		return true
	}
	return false
}

// encryptedChat is the chat of an encrypted thread named by its JID's user
// part: a group's when group is set, else a one-to-one's.
func encryptedChat(jid int64, group bool) string {
	if group {
		return keyID(jid) + groupSuffix
	}
	return keyID(jid) + oneToOneSuffix
}

// isEncryptedChat reports whether a chat ID is one of the encrypted forms.
func isEncryptedChat(chat string) bool {
	return strings.HasSuffix(chat, oneToOneSuffix) || strings.HasSuffix(chat, groupSuffix)
}

// value is the network's stored value called name, or "".
func (m *Messenger) value(name string) string {
	m.vmu.Lock()
	v, ok := m.values[name]
	m.vmu.Unlock()
	if ok {
		return v
	}
	v, _, err := m.st.NetworkValue(Name, name)
	if err != nil {
		return ""
	}
	m.vmu.Lock()
	m.values[name] = v
	m.vmu.Unlock()
	return v
}

// keep stores the network's value called name ("" removes it), writing
// only when it changes.
func (m *Messenger) keep(name, v string) error {
	if m.value(name) == v {
		return nil
	}
	if err := m.st.PutNetworkValue(Name, name, v); err != nil {
		return err
	}
	m.vmu.Lock()
	m.values[name] = v
	m.vmu.Unlock()
	return nil
}

// alias makes the unencrypted thread key an alias of chat, and keeps the
// key as the thread whose history FetchOlder pages for it; it reports
// whether it made a new alias. A key that already leads to an encrypted
// chat is never re-pointed by a row's own placement, only by a pointer
// (fromPointer): an older one-to-one listed again without its pointer stays
// where the pointer put it (Review Focus 4).
func (m *Messenger) alias(key, chat string, fromPointer bool) bool {
	if key == "" || chat == "" || key == chat {
		return false
	}
	if got, ok := m.st.AliasTarget(Name, key); ok {
		if got == chat || !fromPointer && isEncryptedChat(got) {
			_ = m.keep("thread."+got, key)
			return false
		}
	}
	if m.st.Alias(Name, key, chat) != nil {
		return false
	}
	_ = m.keep("thread."+chat, key)
	return true
}

// encryptedAlias is the encrypted chat an unencrypted thread key already
// leads to, if any.
func (m *Messenger) encryptedAlias(key int64) (string, bool) {
	got, ok := m.st.AliasTarget(Name, keyID(key))
	return got, ok && isEncryptedChat(got)
}

// chatOfKey is the chat an unencrypted thread key's lines belong to, from
// what the store holds (an alias, or a chat of its own); ok is false for a
// thread never placed.
func (m *Messenger) chatOfKey(key int64) (string, bool) {
	if key == 0 {
		return "", false
	}
	return m.st.ChatFor(Name, keyID(key))
}

// threadOfChat is the unencrypted thread key whose history FetchOlder pages
// for a chat; ok is false for a chat with none.
func (m *Messenger) threadOfChat(chat string) (int64, bool) {
	if chat == "" {
		return 0, false
	}
	if !isEncryptedChat(chat) {
		key, err := strconv.ParseInt(chat, 10, 64)
		return key, err == nil && key != 0
	}
	key, err := strconv.ParseInt(m.value("thread."+chat), 10, 64)
	return key, err == nil && key != 0
}

// personOf is the participant ID of the person with this FBID, and whether
// it is him.
func (m *Messenger) personOf(fbid int64) (id string, isSelf bool) {
	if fbid == 0 {
		return "", false
	}
	m.mu.Lock()
	self := m.fbid
	m.mu.Unlock()
	return keyID(fbid), fbid == self && self != 0
}

// nameOf is the name of a person seen in a contact row this run, or "".
func (m *Messenger) nameOf(fbid int64) string {
	m.nmu.Lock()
	defer m.nmu.Unlock()
	return m.names[fbid]
}

// learnName keeps a person's name for this run (only in memory; the store
// keeps it as a participant's).
func (m *Messenger) learnName(fbid int64, name string) {
	if fbid == 0 || name == "" {
		return
	}
	m.nmu.Lock()
	defer m.nmu.Unlock()
	m.names[fbid] = name
}

// foldTwins makes every "<key>@msgr" chat whose key already leads to
// another chat an alias of that chat, its lines and state joining it: the
// nameless twin an encrypted thread listed under a key of its own made
// before its pointer came (or before writeThread knew to follow one).
func (m *Messenger) foldTwins() {
	twins, err := m.st.Twins(Name, oneToOneSuffix)
	if err != nil {
		return
	}
	folded := false
	for from, into := range twins {
		if m.st.Alias(Name, from, into) == nil {
			folded = true
		}
	}
	if folded {
		m.changed()
	}
}
