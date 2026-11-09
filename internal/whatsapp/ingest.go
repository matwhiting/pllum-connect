package whatsapp

import (
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// Each event is written to the store before its handler returns, so
// whatsmeow (with SynchronousAck) tells WhatsApp it arrived only once it is
// kept.
//
// How a WhatsApp message becomes one of the protocol's seven kinds:
//
//	Conversation, ExtendedTextMessage   text
//	ImageMessage                        photo (caption as text)
//	VideoMessage, PtvMessage            video (caption as text)
//	AudioMessage, push-to-talk          voiceNote
//	AudioMessage, otherwise             file
//	StickerMessage (and Lottie)         sticker
//	DocumentMessage                     file (caption as text)
//	polls                               text "[poll]"
//	locations, live locations           text "[location]"
//	contact cards                       text "[contact]"
//	calls, call logs, scheduled calls   text "[call]"
//	anything view-once                  text "[view once]"
//	ProtocolMessage REVOKE              the target line deleted
//	ProtocolMessage MESSAGE_EDIT        the target line's text, edited
//	ProtocolMessage HISTORY_SYNC_...    a pending_history row
//	any other protocol message          a hidden notice
//	ReactionMessage                     the target line's reactions
//	anything else with content          a hidden notice
//	key distribution, votes, pins …     nothing
//
// Photos, videos, voice notes, stickers and documents keep the message's
// bytes (raw) to download the file later.

const (
	labelPoll     = "[poll]"
	labelLocation = "[location]"
	labelContact  = "[contact]"
	labelCall     = "[call]"
	labelViewOnce = "[view once]"
)

// body is what a message shows.
type body struct {
	kind    string
	text    string
	media   []store.Media
	keepRaw bool
}

// bodyOf reads what m shows; ok is false when it shows nothing this adapter
// knows.
func bodyOf(m *waE2E.Message, viewOnce bool) (b body, ok bool) {
	if m == nil {
		return body{}, false
	}
	if viewOnce {
		return body{kind: "text", text: labelViewOnce}, true
	}
	switch {
	case m.Conversation != nil:
		return body{kind: "text", text: m.GetConversation()}, true
	case m.ExtendedTextMessage != nil:
		return body{kind: "text", text: m.GetExtendedTextMessage().GetText()}, true
	case m.ImageMessage != nil:
		x := m.GetImageMessage()
		return body{kind: "photo", text: x.GetCaption(), keepRaw: true,
			media: []store.Media{{Kind: "photo", Width: int(x.GetWidth()), Height: int(x.GetHeight()), HasFile: true}}}, true
	case m.VideoMessage != nil || m.PtvMessage != nil:
		x := m.GetVideoMessage()
		if x == nil {
			x = m.GetPtvMessage()
		}
		return body{kind: "video", text: x.GetCaption(), keepRaw: true,
			media: []store.Media{{Kind: "video", Width: int(x.GetWidth()), Height: int(x.GetHeight()), HasFile: true}}}, true
	case m.AudioMessage != nil:
		kind := "file"
		if m.GetAudioMessage().GetPTT() {
			kind = "voiceNote"
		}
		return body{kind: kind, keepRaw: true, media: []store.Media{{Kind: kind}}}, true
	case m.StickerMessage != nil:
		x := m.GetStickerMessage()
		return body{kind: "sticker", keepRaw: true,
			media: []store.Media{{Kind: "sticker", Width: int(x.GetWidth()), Height: int(x.GetHeight()), HasFile: true}}}, true
	case m.DocumentMessage != nil:
		return body{kind: "file", text: m.GetDocumentMessage().GetCaption(), keepRaw: true,
			media: []store.Media{{Kind: "file"}}}, true
	case m.PollCreationMessage != nil || m.PollCreationMessageV2 != nil || m.PollCreationMessageV3 != nil ||
		m.PollCreationMessageV4 != nil || m.PollCreationMessageV5 != nil || m.PollCreationMessageV6 != nil:
		return body{kind: "text", text: labelPoll}, true
	case m.LocationMessage != nil || m.LiveLocationMessage != nil:
		return body{kind: "text", text: labelLocation}, true
	case m.ContactMessage != nil || m.ContactsArrayMessage != nil:
		return body{kind: "text", text: labelContact}, true
	case m.Call != nil || m.CallLogMesssage != nil || m.BcallMessage != nil || m.ScheduledCallCreationMessage != nil:
		return body{kind: "text", text: labelCall}, true
	}
	return body{}, false
}

// plumbing are message fields that carry no line: keys, votes, pins and
// other bookkeeping between clients.
var plumbing = map[protoreflect.Name]bool{
	"senderKeyDistributionMessage":               true,
	"fastRatchetKeySenderKeyDistributionMessage": true,
	"messageContextInfo":                         true,
	"pollUpdateMessage":                          true,
	"encReactionMessage":                         true,
	"encEventResponseMessage":                    true,
	"encCommentMessage":                          true,
	"keepInChatMessage":                          true,
	"pinInChatMessage":                           true,
	"stickerSyncRmrMessage":                      true,
	"groupRootKeyShare":                          true,
	"rootSecretDistributeMessage":                true,
	"deviceSentMessage":                          true,
}

// onlyPlumbing reports whether m holds nothing but bookkeeping.
func onlyPlumbing(m *waE2E.Message) bool {
	only := true
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if !plumbing[fd.Name()] {
			only = false
			return false
		}
		return true
	})
	return only
}

// contextInfo is the ContextInfo of whichever part of m carries one.
func contextInfo(m *waE2E.Message) *waE2E.ContextInfo {
	var found *waE2E.ContextInfo
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		sub := v.Message()
		cf := sub.Descriptor().Fields().ByName("contextInfo")
		if cf == nil || cf.Kind() != protoreflect.MessageKind || !sub.Has(cf) {
			return true
		}
		if ci, ok := sub.Get(cf).Message().Interface().(*waE2E.ContextInfo); ok {
			found = ci
			return false
		}
		return true
	})
	return found
}

// onMessage writes one message, from the live connection or a history sync.
func (w *WhatsApp) onMessage(evt *events.Message, source string) error {
	_, err := w.ingest(evt, source)
	return err
}

// prep is what one message event writes, worked out before anything is
// written, so a history sync can move the chat's read marker before its
// lines land (they would show unread in between).
type prep struct {
	puts      []store.Message // in order: a whole line before news about it
	reactions []reaction
	line      bool  // a line (not a reaction or news about another line)
	incoming  int64 // the time of an incoming line that shows, else 0
}

type reaction struct {
	chatID, msgID string
	r             store.Reaction
}

// ingest writes one message event into the store.
func (w *WhatsApp) ingest(evt *events.Message, source string) (prep, error) {
	m := evt.Message
	if pm := m.GetProtocolMessage(); pm.GetHistorySyncNotification() != nil {
		if source != store.SourceLive || !evt.Info.IsFromMe {
			return prep{}, nil
		}
		blob, err := proto.Marshal(pm.GetHistorySyncNotification())
		if err != nil {
			return prep{}, errInternal
		}
		if err := w.st.PutPendingHistory(Name, blob); err != nil {
			return prep{}, err
		}
		w.kickHistory()
		return prep{}, nil
	}
	p, err := w.prepare(evt, source)
	if err != nil {
		return prep{}, err
	}
	return p, w.apply(p)
}

// apply writes what prepare worked out.
func (w *WhatsApp) apply(p prep) error {
	for _, m := range p.puts {
		if _, err := w.st.PutMessage(Name, m); err != nil {
			return err
		}
	}
	for _, r := range p.reactions {
		if err := w.st.SetReaction(Name, r.chatID, r.msgID, r.r); err != nil {
			return err
		}
	}
	return nil
}

// prepare works out what one message event writes. It resolves (and so may
// make) the chat and its aliases, but writes no line.
func (w *WhatsApp) prepare(evt *events.Message, source string) (prep, error) {
	info := evt.Info
	m := evt.Message
	if !allowedChat(info.Chat) || info.ID == "" {
		return prep{}, nil
	}
	if m != nil && onlyPlumbing(m) && evt.SourceWebMsg == nil {
		return prep{}, nil
	}
	if pm := m.GetProtocolMessage(); pm != nil && info.Category == "peer" {
		return prep{}, nil // between his own devices
	}

	chatID, err := w.chatID(info.Chat)
	if err != nil {
		return prep{}, err
	}
	if err := w.ensureChat(chatID, info.Chat); err != nil {
		return prep{}, err
	}
	ms := info.Timestamp.UnixMilli()
	line := store.Message{
		ID: info.ID, ChatID: chatID, SenderID: w.senderID(info.MessageSource), FromMe: info.IsFromMe,
		TimeMs: ms, Source: source,
	}
	if !info.IsFromMe {
		line.SenderName = info.PushName
	}
	hidden, shown := true, false
	news := func(m store.Message) prep { return prep{puts: []store.Message{m}} }

	// A history message with no content is a stub: a revoked line, a missed
	// call, or a notice of some change.
	if web := evt.SourceWebMsg; web != nil && m == nil {
		switch stub := web.GetMessageStubType(); {
		case stub == waWeb.WebMessageInfo_REVOKE:
			// First seen already revoked: the whole line, then the revoke.
			line.Kind, line.Hidden = "text", &shown
			p := whole(line)
			p.puts = append(p.puts, store.Message{ID: info.ID, ChatID: chatID, Deleted: true, Source: source})
			p.incoming = 0
			return p, nil
		case isCallStub(stub):
			line.Kind, line.Text, line.Hidden = "text", labelCall, &shown
			return whole(line), nil
		case stub != 0:
			line.Kind, line.Hidden = "notice", &hidden
			return whole(line), nil
		}
		return prep{}, nil
	}
	if m == nil {
		return prep{}, nil
	}

	// A history message that is itself an edit comes out of ParseWebMessage
	// under its target's ID, with the edited content.
	if web := evt.SourceWebMsg; web != nil && webIsEdit(web) {
		b, _ := bodyOf(m, false)
		return news(store.Message{ID: info.ID, ChatID: chatID, Text: b.text, Edited: true, Source: source}), nil
	}

	if pm := m.GetProtocolMessage(); pm != nil {
		target := pm.GetKey().GetID()
		switch pm.GetType() {
		case waE2E.ProtocolMessage_REVOKE:
			if target == "" {
				return prep{}, nil
			}
			return news(store.Message{ID: target, ChatID: chatID, Deleted: true, Source: source}), nil
		case waE2E.ProtocolMessage_MESSAGE_EDIT:
			if target == "" {
				return prep{}, nil
			}
			b, _ := bodyOf(pm.GetEditedMessage(), false)
			return news(store.Message{ID: target, ChatID: chatID, Text: b.text, Edited: true, Source: source}), nil
		default:
			line.Kind, line.Hidden = "notice", &hidden
			return whole(line), nil
		}
	}

	if r := m.GetReactionMessage(); r != nil {
		target := r.GetKey().GetID()
		if target == "" {
			return prep{}, nil
		}
		return prep{reactions: []reaction{{chatID: chatID, msgID: target,
			r: store.Reaction{Emoji: r.GetText(), SenderID: line.SenderID, FromMe: info.IsFromMe}}}}, nil
	}

	b, ok := bodyOf(m, evt.IsViewOnce)
	if !ok {
		line.Kind, line.Hidden = "notice", &hidden
		return whole(line), nil
	}
	line.Kind, line.Text, line.Media, line.Hidden = b.kind, b.text, b.media, &shown
	line.ReplyTo = contextInfo(m).GetStanzaID()
	if b.keepRaw {
		raw, err := proto.Marshal(m)
		if err != nil {
			return prep{}, errInternal
		}
		line.Raw = raw
	}
	p := whole(line)
	if web := evt.SourceWebMsg; web != nil {
		if info.IsFromMe {
			p.puts[0].SeenMs = seenOf(web, ms)
		}
		p.reactions = w.webReactions(chatID, info, web)
	}
	return p, nil
}

// whole is the prep for writing a whole line.
func whole(line store.Message) prep {
	p := prep{puts: []store.Message{line}, line: true}
	if !line.FromMe && line.Hidden != nil && !*line.Hidden {
		p.incoming = line.TimeMs
	}
	return p
}

// webIsEdit reports whether a history message is an edit of another.
func webIsEdit(web *waWeb.WebMessageInfo) bool {
	evt := (&events.Message{RawMessage: web.GetMessage()}).UnwrapRaw()
	return evt.Message.GetProtocolMessage().GetType() == waE2E.ProtocolMessage_MESSAGE_EDIT &&
		evt.Message.GetProtocolMessage().GetEditedMessage() != nil
}

// isCallStub reports whether a history stub is a call.
func isCallStub(stub waWeb.WebMessageInfo_StubType) bool {
	switch stub {
	case waWeb.WebMessageInfo_CALL_MISSED_VOICE, waWeb.WebMessageInfo_CALL_MISSED_VIDEO,
		waWeb.WebMessageInfo_CALL_MISSED_GROUP_VOICE, waWeb.WebMessageInfo_CALL_MISSED_GROUP_VIDEO:
		return true
	}
	return false
}

// seenOf is when the other side read or played his history line: the
// latest such receipt, or the line's own time when WhatsApp says only that
// it was read.
func seenOf(web *waWeb.WebMessageInfo, ms int64) int64 {
	var seen int64
	for _, r := range web.GetUserReceipt() {
		seen = max(seen, r.GetReadTimestamp()*1000, r.GetPlayedTimestamp()*1000)
	}
	if seen == 0 && web.GetStatus() >= waWeb.WebMessageInfo_READ {
		seen = ms
	}
	return seen
}

// webReactions are the reactions a history message carries.
func (w *WhatsApp) webReactions(chatID string, info types.MessageInfo, web *waWeb.WebMessageInfo) []reaction {
	var out []reaction
	for _, r := range web.GetReactions() {
		key := r.GetKey()
		var sender string
		fromMe := key.GetFromMe()
		switch {
		case fromMe:
			sender = w.self()
		case key.GetParticipant() != "":
			if j, err := types.ParseJID(key.GetParticipant()); err == nil {
				sender = w.personID(j)
			}
		default:
			sender = w.personID(info.Chat)
		}
		if sender == "" {
			continue
		}
		out = append(out, reaction{chatID: chatID, msgID: info.ID,
			r: store.Reaction{Emoji: r.GetText(), SenderID: sender, FromMe: fromMe}})
	}
	return out
}

// onReceipt marks his lines seen when the other side reads or plays them,
// and moves his read marker when he reads on another device.
func (w *WhatsApp) onReceipt(e *events.Receipt) error {
	if !allowedChat(e.Chat) {
		return nil
	}
	self := e.Type == types.ReceiptTypeReadSelf || e.Type == types.ReceiptTypePlayedSelf
	read := e.Type == types.ReceiptTypeRead || e.Type == types.ReceiptTypePlayed
	if !self && !read {
		return nil
	}
	chatID, ok := w.heldChat(e.Chat)
	if !ok {
		return nil
	}
	ms := e.Timestamp.UnixMilli()
	if read && !e.IsFromMe {
		for _, id := range e.MessageIDs {
			if _, err := w.st.PutMessage(Name, store.Message{ID: id, ChatID: chatID, SeenMs: ms}); err != nil {
				return err
			}
		}
		return nil
	}
	// He read these on another device: everything up to the newest of them.
	var through int64
	for _, id := range e.MessageIDs {
		if t, ok := w.st.LineTime(Name, chatID, id); ok {
			through = max(through, t)
		}
	}
	if through == 0 {
		through = ms
	}
	return w.st.PutChat(Name, store.ChatInfo{ID: chatID, ReadThroughMs: &through, ReadForwardOnly: true})
}

// msOf reads a WhatsApp time that may be in seconds or milliseconds.
func msOf(t int64) int64 {
	if t > 0 && t < 1e12 {
		return t * 1000
	}
	return t
}

// onMute shows a mute set on his phone. A mute that has already ended
// shows unmuted.
func (w *WhatsApp) onMute(e *events.Mute) error {
	chatID, ok := w.heldChat(e.JID)
	if !ok {
		return nil
	}
	muted := mutedUntil(e.Action.GetMuted(), e.Action.GetMuteEndTimestamp(), w.clock.Now().UnixMilli())
	return w.st.PutChat(Name, store.ChatInfo{ID: chatID, Muted: &muted})
}

// mutedUntil reports whether a mute ending at end (seconds or ms; -1 or 0
// for ever) still holds at now.
func mutedUntil(muted bool, end, now int64) bool {
	if !muted {
		return false
	}
	return end <= 0 || msOf(end) > now
}

// onMarkChatAsRead moves his read marker when he marks a chat read (or
// unread) on another device.
func (w *WhatsApp) onMarkChatAsRead(e *events.MarkChatAsRead) error {
	chatID, ok := w.heldChat(e.JID)
	if !ok {
		return nil
	}
	last := msOf(e.Action.GetMessageRange().GetLastMessageTimestamp())
	if last <= 0 {
		last = e.Timestamp.UnixMilli()
	}
	if last <= 0 {
		return nil
	}
	if e.Action.GetRead() {
		return w.st.PutChat(Name, store.ChatInfo{ID: chatID, ReadThroughMs: &last, ReadForwardOnly: true})
	}
	// Marked unread: the newest line shows unread again.
	before := last - 1
	return w.st.PutChat(Name, store.ChatInfo{ID: chatID, ReadThroughMs: &before})
}

// refreshTitles gives each person's chat the name WhatsApp now has for them.
func (w *WhatsApp) refreshTitles(jids ...types.JID) error {
	done := map[string]bool{}
	for _, j := range jids {
		if j.IsEmpty() || norm(j).Server == types.GroupServer {
			continue
		}
		chatID, ok := w.heldChat(j)
		if !ok || done[chatID] {
			continue
		}
		done[chatID] = true
		if t := w.titleOf(j); t != "" {
			if err := w.st.PutChat(Name, store.ChatInfo{ID: chatID, Title: &t}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *WhatsApp) onContact(e *events.Contact) error {
	jids := []types.JID{e.JID}
	for _, s := range []string{e.Action.GetPnJID(), e.Action.GetLidJID()} {
		if j, err := types.ParseJID(s); err == nil && s != "" {
			jids = append(jids, j)
		}
	}
	return w.refreshTitles(jids...)
}

// onGroupInfo shows a group's new name.
func (w *WhatsApp) onGroupInfo(e *events.GroupInfo) error {
	if e.Name == nil || e.Name.Name == "" {
		return nil
	}
	chatID, ok := w.heldChat(e.JID)
	if !ok {
		return nil
	}
	name := e.Name.Name
	return w.st.PutChat(Name, store.ChatInfo{ID: chatID, Kind: "group", Title: &name})
}
