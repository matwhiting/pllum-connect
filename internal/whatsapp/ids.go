package whatsapp

import (
	"context"

	"go.mau.fi/whatsmeow/types"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// A chat's ID is a JID and never changes once given:
//   - a person whose number is known when the chat is first seen is keyed by
//     the number (<digits>@s.whatsapp.net), with the LID as an alias;
//   - a person known only by a LID is keyed by the LID, and the number,
//     learnt later, only becomes an alias;
//   - a group is keyed by its own JID.
//
// When a LID chat and a number chat turn out to be one person, the LID chat
// joins the number's (the store merges it, and its ID becomes an alias).

// allowedChat reports whether lines in chat are kept: people (by number or
// LID) and groups. Status, broadcast lists, Channels and bots are not.
func allowedChat(chat types.JID) bool {
	j := chat.ToNonAD()
	switch j.Server {
	case types.DefaultUserServer:
		return j.User != "" && !j.IsBot()
	case types.HiddenUserServer, types.GroupServer:
		return j.User != ""
	}
	return false
}

// norm is a JID as chats and senders are keyed: no device, and the old
// c.us server read as s.whatsapp.net.
func norm(jid types.JID) types.JID {
	j := jid.ToNonAD()
	if j.Server == types.LegacyUserServer {
		j.Server = types.DefaultUserServer
	}
	return j
}

// counterpart is the number for a LID, or the LID for a number, when the
// device knows it; else the empty JID.
func (w *WhatsApp) counterpart(jid types.JID) types.JID {
	dev := w.device()
	if dev == nil || dev.LIDs == nil {
		return types.EmptyJID
	}
	ctx := w.bg()
	var other types.JID
	var err error
	switch jid.Server {
	case types.HiddenUserServer:
		other, err = dev.LIDs.GetPNForLID(ctx, jid)
	case types.DefaultUserServer:
		other, err = dev.LIDs.GetLIDForPN(ctx, jid)
	default:
		return types.EmptyJID
	}
	if err != nil || other.IsEmpty() {
		return types.EmptyJID
	}
	return norm(other)
}

// chatID is the store's chat for a chat JID, adding aliases as the device
// learns that a LID and a number are one person.
func (w *WhatsApp) chatID(jid types.JID) (string, error) {
	j := norm(jid)
	id := j.String()
	other := w.counterpart(j)
	held, ok := w.st.ChatFor(Name, id)
	if other.IsEmpty() {
		if ok {
			return held, nil
		}
		return id, nil
	}
	pn, lid := other, j
	if j.Server == types.DefaultUserServer {
		pn, lid = j, other
	}
	oheld, ook := w.st.ChatFor(Name, other.String())
	switch {
	case ok && ook && held == oheld:
		return held, nil
	case ok && ook:
		// Two chats are one person: the number's chat takes the LID's.
		pnChat, _ := w.st.ChatFor(Name, pn.String())
		if err := w.st.Alias(Name, lid.String(), pnChat); err != nil {
			return "", err
		}
		return pnChat, nil
	case ok:
		return held, w.st.Alias(Name, other.String(), held)
	case ook:
		return oheld, w.st.Alias(Name, id, oheld)
	default:
		// A new chat: keyed by the number, the LID an alias.
		return pn.String(), w.st.Alias(Name, lid.String(), pn.String())
	}
}

// heldChat is the store's chat for a JID if the store has one; nothing is
// made or aliased.
func (w *WhatsApp) heldChat(jid types.JID) (string, bool) {
	j := norm(jid)
	if id, ok := w.st.ChatFor(Name, j.String()); ok {
		return id, true
	}
	if other := w.counterpart(j); !other.IsEmpty() {
		return w.st.ChatFor(Name, other.String())
	}
	return "", false
}

// link records that lid and pn are one person, for the chats the store
// already holds under either.
func (w *WhatsApp) link(lid, pn types.JID) error {
	lid, pn = norm(lid), norm(pn)
	if lid.Server != types.HiddenUserServer || pn.Server != types.DefaultUserServer {
		return nil
	}
	lidChat, lok := w.st.ChatFor(Name, lid.String())
	pnChat, pok := w.st.ChatFor(Name, pn.String())
	switch {
	case lok && pok && lidChat == pnChat:
		return nil
	case lok && pok:
		return w.st.Alias(Name, lid.String(), pnChat)
	case lok:
		return w.st.Alias(Name, pn.String(), lidChat)
	case pok:
		return w.st.Alias(Name, lid.String(), pnChat)
	}
	return nil
}

// personID is how a person is named as a sender or member: the number when
// it is known, else the LID.
func (w *WhatsApp) personID(jid types.JID) string {
	j := norm(jid)
	if j.Server == types.HiddenUserServer {
		if pn := w.counterpart(j); !pn.IsEmpty() {
			return pn.String()
		}
	}
	return j.String()
}

// senderID names a line's sender: him, or the person by number when known.
func (w *WhatsApp) senderID(src types.MessageSource) string {
	if src.IsFromMe {
		if self := w.self(); self != "" {
			return self
		}
		return norm(src.Sender).String()
	}
	s := norm(src.Sender)
	if s.Server == types.HiddenUserServer && norm(src.SenderAlt).Server == types.DefaultUserServer {
		return norm(src.SenderAlt).String()
	}
	if s.IsEmpty() {
		s = norm(src.Chat)
	}
	return w.personID(s)
}

// isSelf reports whether jid is his own number or LID.
func (w *WhatsApp) isSelf(jid types.JID) bool {
	j := norm(jid)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.selfID != "" && j.String() == w.selfID {
		return true
	}
	return w.dev != nil && !w.dev.LID.IsEmpty() && j == norm(w.dev.LID)
}

// phoneOf is a person's number as "+<digits>", or empty when unknown.
func (w *WhatsApp) phoneOf(jid types.JID) string {
	j := norm(jid)
	if j.Server == types.HiddenUserServer {
		j = w.counterpart(j)
	}
	if j.Server != types.DefaultUserServer || j.User == "" {
		return ""
	}
	return "+" + j.User
}

// titleOf is WhatsApp's name for a person: his phone's contact name, then
// their push name (or business name); empty when there is none.
func (w *WhatsApp) titleOf(jid types.JID) string {
	dev := w.device()
	if dev == nil || dev.Contacts == nil {
		return ""
	}
	ctx := context.WithoutCancel(w.bg())
	j := norm(jid)
	cands := []types.JID{j}
	if other := w.counterpart(j); !other.IsEmpty() {
		cands = append(cands, other)
	}
	var infos []types.ContactInfo
	for _, c := range cands {
		if ci, err := dev.Contacts.GetContact(ctx, c); err == nil && ci.Found {
			infos = append(infos, ci)
		}
	}
	for _, ci := range infos {
		if ci.FullName != "" {
			return ci.FullName
		}
		if ci.FirstName != "" {
			return ci.FirstName
		}
	}
	for _, ci := range infos {
		if ci.PushName != "" {
			return ci.PushName
		}
		if ci.BusinessName != "" {
			return ci.BusinessName
		}
	}
	return ""
}

// ensureChat makes the chat with what is known of it, if the store lacks
// it: its kind, and for a person their name and number.
func (w *WhatsApp) ensureChat(chatID string, jid types.JID) error {
	if _, ok := w.st.ChatFor(Name, chatID); ok {
		return nil
	}
	info := store.ChatInfo{ID: chatID, Kind: "single"}
	if norm(jid).Server == types.GroupServer {
		info.Kind = "group"
	} else {
		if t := w.titleOf(jid); t != "" {
			info.Title = &t
		}
		if p := w.phoneOf(jid); p != "" {
			info.Phone = &p
		}
	}
	return w.st.PutChat(Name, info)
}
