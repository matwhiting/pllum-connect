package messenger

import (
	"context"
	"encoding/json"
	_ "image/jpeg" // photo sizes, for an encrypted photo's message
	_ "image/png"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	watypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// React sends his reaction to a held line, or with an empty emoji removes
// his: an unencrypted thread's line by a SendReactionTask on its thread,
// an encrypted chat's by a consumer reaction naming the line's key. A
// transaction the outbox already holds sends nothing, unless it failed
// before Meta had it. A line only Beeper's copy holds is notOnNetwork.
func (m *Messenger) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	if err := m.writeGate(); err != nil {
		return err
	}
	if strings.HasPrefix(messageID, "beeper:") {
		return errNotOnNetwork
	}
	chat, err := m.writableChat(chatID)
	if err != nil {
		return err
	}
	prev, held, err := m.st.OutgoingTxn(Name, transactionID)
	if err != nil {
		return err
	}
	if held && prev.State != store.OutFailed {
		return nil
	}
	l, ok, err := m.st.Line(Name, chat, messageID)
	if err != nil {
		return err
	}
	if !ok {
		return errNotFound
	}
	if isMetaLine(l.ID) {
		if _, ok := m.threadOfChat(chat); !ok {
			return errNotOnNetwork
		}
	} else if !isEncryptedChat(chat) {
		return errNotOnNetwork
	}
	o := prev
	if held {
		if err := m.st.SetOutgoingState(Name, o.ID, store.OutSending, ""); err != nil {
			return err
		}
	} else {
		req := sendRequest{Target: messageID, Emoji: emoji}
		if !isMetaLine(l.ID) {
			req.WAID = string(newMessageID())
		}
		body, err := json.Marshal(req)
		if err != nil {
			return errInternal
		}
		o = store.Outgoing{ID: "reaction:" + itoa(methods.GenerateEpochID()), ChatID: chat, Kind: outReaction,
			Txn: transactionID, MadeMs: m.clock.Now().UnixMilli(), Request: body}
		if err := m.st.PutOutgoing(Name, o); err != nil {
			if _, raced, _ := m.st.OutgoingTxn(Name, transactionID); raced {
				return nil // another call with this transaction kept it first, and sends it
			}
			return err
		}
	}
	return m.react(o, emoji, l)
}

// react sends one kept reaction, with the emoji asked for now (a failed
// one tried again may be another), and settles it.
func (m *Messenger) react(o store.Outgoing, emoji string, l store.Line) error {
	var req sendRequest
	if json.Unmarshal(o.Request, &req) != nil {
		m.fail(o.ID, wire.Internal)
		return errInternal
	}
	ctx, cancel := m.writeCtx()
	defer cancel()
	gen, w := m.writer()
	if w == nil {
		m.fail(o.ID, wire.Refused)
		return errNotConnected
	}
	self, fbid := m.self()
	var ran bool
	var raw error
	if isMetaLine(l.ID) {
		key, _ := m.threadOfChat(o.ChatID)
		_, ran, raw, err := m.execute(ctx, gen, w, &socket.SendReactionTask{ThreadKey: key, TimestampMs: m.clock.Now().UnixMilli(),
			MessageID: l.ID, ActorID: fbid, Reaction: emoji, SyncGroup: 1, SendAttribution: table.MESSENGER_INBOX_IN_THREAD})
		if err != nil {
			return m.reactFailed(o, ran, raw, err)
		}
	} else {
		jid, perr := watypes.ParseJID(o.ChatID)
		if perr != nil || req.WAID == "" {
			m.fail(o.ID, wire.Internal)
			return errInternal
		}
		key := &waCommon.MessageKey{RemoteJID: proto.String(jid.String()), ID: proto.String(l.ID)}
		if l.FromMe {
			key.FromMe = proto.Bool(true)
		}
		if jid.Server == watypes.GroupServer {
			key.Participant = proto.String(m.senderJID(l).String())
		}
		app := consumerApp(&waConsumerApplication.ConsumerApplication_Content{
			Content: &waConsumerApplication.ConsumerApplication_Content_ReactionMessage{ReactionMessage: &waConsumerApplication.ConsumerApplication_ReactionMessage{
				Key: key, Text: proto.String(emoji), SenderTimestampMS: proto.Int64(m.clock.Now().UnixMilli())}}})
		var err error
		ran, raw, err = m.ask(ctx, gen, func(ctx context.Context) error {
			_, err := w.SendFBMessage(ctx, jid, app, nil, whatsmeow.SendRequestExtra{ID: watypes.MessageID(req.WAID)})
			return err
		})
		if err != nil {
			return m.reactFailed(o, ran, raw, err)
		}
	}
	_ = m.st.SetReaction(Name, o.ChatID, l.ID, store.Reaction{Emoji: emoji, SenderID: self, FromMe: true})
	m.settle(o.ID, "")
	m.changed()
	return nil
}

// reactFailed is a reaction that did not go: one that may have reached
// Meta stays sending (unclear); any other failed.
func (m *Messenger) reactFailed(o store.Outgoing, ran bool, raw, err error) error {
	if ran && unsure(raw) {
		return errUnclear
	}
	m.fail(o.ID, wire.Refused)
	return writeErr(ran, raw, err)
}

// MarkRead tells Meta he has read the chat up to and including through (the
// newest incoming line when empty): the unencrypted thread's lines (a
// group's, or an encrypted one-to-one's older thread) by one
// ThreadMarkReadTask at the newest of them, the encrypted lines by
// whatsmeow's MarkRead, one per sender in a group (none in a one-to-one),
// as the connector sends them. Nothing newer is marked; his marker moves
// either way.
func (m *Messenger) MarkRead(ctx context.Context, chatID, through string) error {
	if err := m.writeGate(); err != nil {
		return err
	}
	chat, err := m.writableChat(chatID)
	if err != nil {
		return err
	}
	var upTo store.Line
	var ok bool
	if through == "" {
		if upTo, ok, err = m.st.NewestIncoming(Name, chat); err == nil && !ok {
			return nil // nothing to read
		}
	} else if upTo, ok, err = m.st.Line(Name, chat, through); err == nil && !ok {
		return errNotFound
	}
	if err != nil {
		return err
	}
	lines, err := m.st.Unread(Name, chat, upTo.TimeMs)
	if err != nil {
		return err
	}
	encrypted := isEncryptedChat(chat)
	var metaAt int64
	var senders []string
	ids := map[string][]watypes.MessageID{}
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l.ID, "beeper:"):
		case isMetaLine(l.ID):
			metaAt = max(metaAt, l.TimeMs)
		case encrypted:
			by := ""
			if strings.HasSuffix(chat, groupSuffix) {
				by = l.SenderID
			}
			if _, seen := ids[by]; !seen {
				senders = append(senders, by)
			}
			ids[by] = append(ids[by], watypes.MessageID(l.ID))
		}
	}
	key, threaded := m.threadOfChat(chat)
	if (metaAt > 0 && threaded) || len(senders) > 0 {
		gen, w := m.writer()
		if w == nil {
			return errNotConnected
		}
		// Each receipt has a budget of its own, as each waits its turn.
		if metaAt > 0 && threaded {
			wctx, cancel := m.writeCtx()
			_, ran, raw, err := m.execute(wctx, gen, w, &socket.ThreadMarkReadTask{ThreadId: key, LastReadWatermarkTs: metaAt, SyncGroup: 1})
			cancel()
			if err != nil {
				return writeErr(ran, raw, err)
			}
		}
		if len(senders) > 0 {
			jid, err := watypes.ParseJID(chat)
			if err != nil {
				return errInternal
			}
			now := m.clock.Now()
			for _, by := range senders {
				var sender watypes.JID
				if by != "" {
					sender = watypes.NewJID(by, watypes.MessengerServer)
				}
				wctx, cancel := m.writeCtx()
				ran, raw, err := m.ask(wctx, gen, func(ctx context.Context) error {
					return w.MarkRead(ctx, ids[by], now, jid, sender)
				})
				cancel()
				if err != nil {
					return writeErr(ran, raw, err)
				}
			}
		}
	}
	ms := upTo.TimeMs
	if err := m.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &ms, ReadForwardOnly: true}); err != nil {
		return err
	}
	m.changed()
	return nil
}

// MarkUnread shows the chat's newest incoming line unread. The helper may
// make no such write to Messenger, so it is kept here only and nothing is
// asked of Meta.
func (m *Messenger) MarkUnread(ctx context.Context, chatID string) error {
	chat, err := m.writableChat(chatID)
	if err != nil {
		return err
	}
	in, ok, err := m.st.NewestIncoming(Name, chat)
	if err != nil {
		return err
	}
	if ok {
		before := in.TimeMs - 1
		if err := m.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &before}); err != nil {
			return err
		}
	}
	m.changed()
	return nil
}

// SetMuted mutes the chat for ever (MuteExpireTimeMS -1) or unmutes it (0)
// with a MuteThreadTask on its unencrypted thread: a group's own, or the
// thread an encrypted one-to-one is linked to. An encrypted chat with no
// such thread is muted here only, and nothing is asked.
func (m *Messenger) SetMuted(ctx context.Context, chatID string, muted bool) error {
	chat, err := m.writableChat(chatID)
	if err != nil {
		return err
	}
	if key, ok := m.threadOfChat(chat); ok {
		if err := m.writeGate(); err != nil {
			return err
		}
		wctx, cancel := m.writeCtx()
		defer cancel()
		gen, w := m.writer()
		if w == nil {
			return errNotConnected
		}
		expire := int64(0)
		if muted {
			expire = -1
		}
		if _, ran, raw, err := m.execute(wctx, gen, w, &socket.MuteThreadTask{ThreadKey: key, MuteExpireTimeMS: expire, SyncGroup: 1}); err != nil {
			return writeErr(ran, raw, err)
		}
	}
	if err := m.st.PutChat(Name, store.ChatInfo{ID: chat, Muted: &muted}); err != nil {
		return err
	}
	m.changed()
	return nil
}
