package whatsapp

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// New chats by number. A send to a person by number goes into the
// one-to-one the store holds with them (their number's chat, or the LID it
// maps to) as any send does, and counts nothing. Otherwise it is a new chat:
// counted first (network.NewChats: at most WhatsAppNewChatsPerDay a local
// day, none for a day after WhatsApp refuses one), then, unless the device
// already knows the number (its contact store, or a LID mapped to it), one
// IsOnWhatsApp of that one number. That is the only lookup, and it happens
// only inside a counted new chat he sent. A number not on WhatsApp is a send
// failed notOnNetwork, with nothing sent and no chat made. A new chat that
// WhatsApp's server refuses (other than "not on WhatsApp") holds new chats a
// day, and is never sent again.

var (
	_ network.ChatFinder    = (*WhatsApp)(nil)
	_ network.NewChatSender = (*WhatsApp)(nil)
)

// ChatWith is the one-to-one the store holds with a person, by number (its
// chat, or the chat of the LID it maps to) or by JID. It reads the store and
// the device's LID map only; nothing goes to WhatsApp.
func (w *WhatsApp) ChatWith(ctx context.Context, to network.Recipient) (string, bool, error) {
	jid, err := recipientJID(to)
	if err != nil {
		return "", false, err
	}
	chat, ok := w.heldChat(jid)
	if !ok {
		return "", false, nil
	}
	if c, err := types.ParseJID(chat); err != nil || norm(c).Server == types.GroupServer {
		return "", false, nil // one-to-ones only
	}
	return chat, true, nil
}

// recipientJID is the person a recipient names: a number's JID, or a
// person's JID (by number or LID) as given.
func recipientJID(to network.Recipient) (types.JID, error) {
	if !to.Valid() {
		return types.EmptyJID, errBadRequest
	}
	if to.Phone != "" {
		return types.NewJID(strings.TrimPrefix(to.Phone, "+"), types.DefaultUserServer), nil
	}
	jid, err := types.ParseJID(to.UserID)
	if err != nil || !isPerson(jid) {
		return types.EmptyJID, errBadRequest
	}
	return norm(jid), nil
}

// SendTo sends m (text only) to the person m.To names (see the top of this
// file). A repeat under the caller's key (m.ClientID) answers the send it
// made, whatever chat it went to, and asks nothing: a send or a new chat
// still going or gone, or a new chat refused because the number is not on
// WhatsApp or WhatsApp turned it down.
func (w *WhatsApp) SendTo(ctx context.Context, m network.Outgoing) (string, string, error) {
	if m.To == nil || m.ChatID != "" {
		return "", "", errBadRequest
	}
	jid, err := recipientJID(*m.To)
	if err != nil {
		return "", "", err
	}
	if id, chat, done, err := w.repeatTo(m.ClientID); err != nil || done {
		return id, chat, err
	}
	if err := w.writeGate(); err != nil && err != errOffline {
		return "", "", err
	}
	if m.Text == "" || m.File != "" {
		return "", "", errBadRequest // a new chat's first message is text
	}
	if chat, ok, err := w.ChatWith(ctx, *m.To); err != nil {
		return "", "", err
	} else if ok {
		m.ChatID, m.To = chat, nil
		id, err := w.Send(ctx, m)
		return id, chat, err
	}
	if jid.Server != types.DefaultUserServer {
		return "", "", errBadRequest // a new chat starts by number
	}
	// A new chat: it goes only while connected, as the check needs to.
	if err := w.writeGate(); err != nil {
		return "", "", err
	}
	cli := w.client()
	if cli == nil {
		return "", "", errNotConnected
	}
	if err := w.newChats.Take(); err != nil {
		return "", "", err
	}
	now := w.clock.Now()
	id := cli.GenerateMessageID()
	body, err := json.Marshal(sendRequest{Text: m.Text, Kind: "text", NewChat: true})
	if err != nil {
		return "", "", errInternal
	}
	target := jid
	if !w.knownOnWhatsApp(ctx, jid) {
		resp, err := cli.IsOnWhatsApp(ctx, []string{"+" + jid.User})
		if err != nil {
			return "", "", writeErr(err)
		}
		if len(resp) != 1 || !resp[0].IsIn {
			// Kept failed under the caller's key, so a repeat asks nothing.
			if err := w.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: jid.String(), Kind: outMessage, Client: m.ClientID,
				State: store.OutFailed, Fail: string(wire.NotOnNetwork), MadeMs: now.UnixMilli(), Request: body}); err != nil {
				return "", "", err
			}
			return id, "", nil
		}
		if j := norm(resp[0].JID); j.Server == types.DefaultUserServer && j.User != "" {
			target = j
		}
	}
	chat, err := w.chatID(target)
	if err != nil {
		return "", "", err
	}
	if err := w.ensureChat(chat, target); err != nil {
		return "", "", err
	}
	if err := w.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: m.ClientID,
		MadeMs: now.UnixMilli(), Request: body}); err != nil {
		return "", "", err
	}
	w.changed()
	w.enqueue(sendJob{id: id, madeMs: now.UnixMilli(), deadline: now.Add(sendWait)})
	return id, chat, nil
}

// repeatTo answers a send already kept under the caller's key: done is true
// when it is one to answer again (going, gone, or a new chat refused for
// good); a key whose send failed otherwise makes a new send.
func (w *WhatsApp) repeatTo(client string) (id, chat string, done bool, err error) {
	if client == "" {
		return "", "", false, nil
	}
	o, ok, err := w.st.OutgoingClient(Name, client)
	if err != nil || !ok {
		return "", "", false, err
	}
	if o.Kind != outMessage {
		return "", "", false, errBadRequest
	}
	if o.State == store.OutFailed {
		var req sendRequest
		_ = json.Unmarshal(o.Request, &req)
		if !req.NewChat || (o.Fail != string(wire.NotOnNetwork) && o.Fail != string(wire.Refused) && o.Fail != string(wire.Unclear)) {
			return "", "", false, nil
		}
	}
	if held, ok := w.st.ChatFor(Name, o.ChatID); ok {
		chat = held
	}
	return o.ID, chat, true, nil
}

// knownOnWhatsApp reports whether the device already knows the number is
// on WhatsApp: it is in his contact store, or a LID maps to it.
func (w *WhatsApp) knownOnWhatsApp(ctx context.Context, jid types.JID) bool {
	dev := w.device()
	if dev == nil {
		return false
	}
	if dev.Contacts != nil {
		if ci, err := dev.Contacts.GetContact(ctx, jid); err == nil && ci.Found {
			return true
		}
	}
	return !w.known(jid).IsEmpty()
}

// newChatsClock is the adapter's clock as it is when used, so a test's
// clock, set after New, is the one new chats run on.
type newChatsClock struct{ w *WhatsApp }

func (c newChatsClock) Now() time.Time { return c.w.clock.Now() }

func (c newChatsClock) AfterFunc(d time.Duration, f func()) func() bool {
	return c.w.clock.AfterFunc(d, f)
}

func (c newChatsClock) Sleep(ctx context.Context, d time.Duration) error {
	return c.w.clock.Sleep(ctx, d)
}
