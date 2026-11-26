package whatsapp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
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
// already knows the number (its contact store, a LID mapped to it, or the
// form WhatsApp gave it before), one IsOnWhatsApp of that one number. That
// is the only lookup, and it happens only inside a counted new chat he sent;
// WhatsApp refusing the lookup itself holds new chats a day as well. A
// number not on WhatsApp is a send failed notOnNetwork, with nothing sent
// and no chat made. A chat is never made empty: the new chat's chat row is
// stored only once WhatsApp has accepted its first message, and a first
// message that fails stays only as its failed send. A new chat that
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
// file). It answers the chat's ID once the store holds the chat, else ""
// (a new chat, until its first message is accepted). A repeat under the
// caller's key (m.ClientID) answers the send it made, whatever chat it went
// to, and asks nothing: a send or a new chat still going or gone, or a new
// chat refused because the number is not on WhatsApp or WhatsApp turned it
// down. New chats go one at a time (ncmu), so a repeat that comes while the
// first is still being checked waits for it and answers it.
func (w *WhatsApp) SendTo(ctx context.Context, m network.Outgoing) (string, string, error) {
	if m.To == nil || m.ChatID != "" {
		return "", "", errBadRequest
	}
	jid, err := recipientJID(*m.To)
	if err != nil {
		return "", "", err
	}
	w.ncmu.Lock()
	defer w.ncmu.Unlock()
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
	target, known := w.knownOnWhatsApp(ctx, jid)
	if !known {
		resp, err := cli.IsOnWhatsApp(ctx, []string{"+" + jid.User})
		if err != nil {
			if unclearSend(err) {
				return "", "", errUnclear
			}
			// WhatsApp turned the lookup down: a pushback like any other.
			kind := wire.Refused
			if errors.Is(err, whatsmeow.ErrIQRateOverLimit) {
				kind = wire.RateLimited
			}
			w.newChats.Hold(kind)
			return "", "", &wire.Error{Kind: kind}
		}
		if len(resp) != 1 || !resp[0].IsIn {
			// Kept failed under the caller's key, so a repeat asks nothing.
			if err := w.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: jid.String(), Kind: outMessage, Client: m.ClientID,
				State: store.OutFailed, Fail: string(wire.NotOnNetwork), MadeMs: now.UnixMilli(), Request: body}); err != nil {
				return w.racedTo(m.ClientID, err)
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
	if target != jid {
		// WhatsApp's own form of the number: the typed one leads to the
		// same chat, so it is found, not checked or counted, next time.
		if err := w.st.Alias(Name, jid.String(), chat); err != nil {
			return "", "", err
		}
	}
	// No chat row yet: it is stored once WhatsApp accepts this message.
	if err := w.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: m.ClientID,
		MadeMs: now.UnixMilli(), Request: body}); err != nil {
		return w.racedTo(m.ClientID, err)
	}
	w.enqueue(sendJob{id: id, madeMs: now.UnixMilli(), deadline: now.Add(sendWait)})
	return id, "", nil
}

// racedTo answers a send whose row could not be kept: the one another call
// with the caller's key kept first (as Send does), else the error.
func (w *WhatsApp) racedTo(client string, err error) (string, string, error) {
	if id, chat, done, rerr := w.repeatTo(client); rerr == nil && done {
		return id, chat, nil
	}
	return "", "", err
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
// on WhatsApp, and as which JID: it is in his contact store, a LID maps to
// it, or the store leads it to WhatsApp's own form of it (aliased by an
// earlier new chat).
func (w *WhatsApp) knownOnWhatsApp(ctx context.Context, jid types.JID) (types.JID, bool) {
	if to, ok := w.st.AliasTarget(Name, jid.String()); ok {
		if j, err := types.ParseJID(to); err == nil && j.Server == types.DefaultUserServer && j.User != "" {
			return j, true
		}
	}
	dev := w.device()
	if dev == nil {
		return jid, false
	}
	if dev.Contacts != nil {
		if ci, err := dev.Contacts.GetContact(ctx, jid); err == nil && ci.Found {
			return jid, true
		}
	}
	return jid, !w.known(jid).IsEmpty()
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
