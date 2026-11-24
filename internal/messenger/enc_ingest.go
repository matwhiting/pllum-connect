package messenger

import (
	"cmp"
	"encoding/json"
	"strconv"
	"strings"

	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMultiDevice"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// The encrypted chats' lines are whatsmeow's (events.FBMessage, live or
// restored from his backup), written as step 2's WhatsApp ingest writes
// its own. A consumer application's content becomes:
//
//	MessageText, ExtendedTextMessage    text
//	ImageMessage                        photo (caption as text)
//	VideoMessage                        video (caption as text)
//	AudioMessage, push-to-talk          voiceNote
//	AudioMessage, otherwise             file
//	StickerMessage                      sticker
//	DocumentMessage                     file
//	ViewOnceMessage                     text "[view once]"
//	LocationMessage, LiveLocation...    text "[location]"
//	ContactMessage, ContactsArray...    text "[contact]"
//	PollCreationMessage                 text "[poll]"
//	EditMessage                         the target line's text, edited
//	ReactionMessage                     the target line's reactions ("" removes his)
//	ApplicationData Revoke              the target line deleted
//	anything else with content          a hidden notice
//	poll votes, signals, sub-protocols  nothing
//
// An Armadillo message (shares, galleries, view-once media) shows its text
// where it has some, a gallery as photos, and otherwise a hidden notice.
// Photos, videos, voice notes, stickers and files keep the message's
// application (and, restored, the backup's attachment addresses) as raw,
// to fetch the file later. A line is whatsmeow's message ID, so a line
// restored from the backup and also received live is one row.

const (
	labelPoll     = "[poll]"
	labelLocation = "[location]"
	labelContact  = "[contact]"
	labelCall     = "[call]"
	labelViewOnce = "[view once]"
)

// encRaw is what an encrypted line keeps to fetch its media later.
type encRaw struct {
	App   []byte            `json:"fbApplication"`   // the MessageApplication, marshalled
	Paths map[string]string `json:"paths,omitempty"` // a restored line's attachment object IDs → their addresses
}

// encChatOf is the chat of an encrypted chat's JID: "<FBID>@msgr" for a
// one-to-one, "<id>@g.us" for a group; ok is false for any other.
func encChatOf(j watypes.JID) (chat string, group, ok bool) {
	if j.User == "" {
		return "", false, false
	}
	switch j.Server {
	case watypes.MessengerServer:
		return j.User + oneToOneSuffix, false, true
	case watypes.GroupServer:
		return j.User + groupSuffix, true, true
	}
	return "", false, false
}

// fbSender is the participant ID of an encrypted line's sender (their
// FBID), their FBID, and whether it is him.
func (m *Messenger) fbSender(j watypes.JID, fromMe bool) (id string, fbid int64, self bool) {
	fbid, _ = strconv.ParseInt(j.User, 10, 64)
	if fbid == 0 && fromMe {
		m.mu.Lock()
		fbid = m.fbid
		m.mu.Unlock()
	}
	id, self = m.personOf(fbid)
	return id, fbid, self || fromMe
}

// hasEncryptedSide reports whether a chat has an encrypted side: every
// encrypted group, and a one-to-one once its encrypted thread is known.
func (m *Messenger) hasEncryptedSide(chat string) bool {
	return strings.HasSuffix(chat, groupSuffix) || m.value("encrypted."+chat) != ""
}

// markEncrypted records that a chat has an encrypted side. A chat whose
// older thread was complete is no longer, until its backup is read to the
// start too (B1 concern 6).
func (m *Messenger) markEncrypted(chat string) {
	if m.value("encrypted."+chat) != "" {
		return
	}
	_ = m.keep("encrypted."+chat, "1")
	if _, ok := m.st.ChatFor(Name, chat); ok && m.value("encDone."+chat) == "" {
		complete := false
		_ = m.st.PutChat(Name, store.ChatInfo{ID: chat, HistoryComplete: &complete})
	}
}

// encryptedChatFor is the chat an encrypted JID's lines land in: the chat
// the store holds for it (an older thread's alias already leads there), or
// a new one, a one-to-one with the person and him, or a group.
func (m *Messenger) encryptedChatFor(j watypes.JID) (string, bool, error) {
	chat, group, ok := encChatOf(j)
	if !ok {
		return "", false, nil
	}
	if got, ok := m.st.ChatFor(Name, chat); ok {
		m.markEncrypted(got)
		return got, true, nil
	}
	info := store.ChatInfo{ID: chat, Kind: "single"}
	if group {
		info.Kind = "group"
	} else if fbid, err := strconv.ParseInt(j.User, 10, 64); err == nil && fbid != 0 {
		name := m.nameOf(fbid)
		if name != "" {
			info.Title = &name
		}
		if who, _ := m.personOf(fbid); who != "" {
			info.Participants = m.withSelf([]store.Participant{{ID: who, Name: name}})
		}
	}
	if err := m.st.PutChat(Name, info); err != nil {
		return "", false, err
	}
	m.markEncrypted(chat)
	return chat, true, nil
}

// encWrites is what one encrypted message writes: lines (a whole line
// before news about it), reactions, and the IDs of reactions, edits and
// unsends applied to other lines. A message under one of those IDs never
// comes back as a line of its own, so an "undecryptable" or "unavailable"
// line kept under it (onUndecryptable) is settled: hidden.
type encWrites struct {
	puts      []store.Message
	reactions []encReaction
	settle    []string
}

type encReaction struct {
	msgID string
	r     store.Reaction
}

// onFBMessage writes one encrypted message received live. It reports
// whether anything was written.
func (m *Messenger) onFBMessage(evt *events.FBMessage) (bool, error) {
	if evt == nil || evt.Info.ID == "" {
		return false, nil
	}
	if m.testPrep != nil {
		m.testPrep(evt)
	}
	chat, ok, err := m.encryptedChatFor(evt.Info.Chat)
	if err != nil || !ok {
		return false, err
	}
	_, changed, err := m.applyEnc(chat, m.encPrep(chat, evt, store.SourceLive, nil))
	if err != nil {
		return false, err
	}
	return changed, nil
}

// applyEnc writes what encPrep worked out, in one batch, then settles his
// sends its lines carry. added counts the new rows; changed reports whether
// anything Pllum shows changed: a new row, a row whose state differs after
// the write (a hidden notice or an edit's early row taken over by its line,
// a new text, kind, edit or revoke), or a reaction. Only a line sent again
// unchanged changes nothing (reviews M1, N1).
func (m *Messenger) applyEnc(chat string, w encWrites) (added int, changed bool, err error) {
	var echoes []string
	var refused error
	err = m.st.Batch(Name, func(b *store.Batch) error {
		added, changed, refused = m.applyEncIn(b, chat, w, &echoes)
		return nil // what was written before a refused write is kept
	})
	if err != nil {
		return 0, false, err
	}
	m.echoedEncryptedAll(echoes)
	return added, changed, refused
}

// echoedEncryptedAll settles the encrypted sends his lines carry the IDs
// of (send.go), once the lines are kept.
func (m *Messenger) echoedEncryptedAll(ids []string) {
	for _, id := range ids {
		m.echoedEncrypted(id)
	}
}

// applyEncIn writes what encPrep worked out into a batch, as applyEnc
// describes, and adds the IDs of his lines to echoes: their sends are
// settled once the batch is kept, since settling is a write of its own. The
// first write refused ends it, with the error, and what it wrote before
// stays in the batch, as one at a time it stayed in the store.
func (m *Messenger) applyEncIn(b *store.Batch, chat string, w encWrites, echoes *[]string) (added int, changed bool, err error) {
	for _, l := range w.puts {
		p, err := b.Put(l)
		if err != nil {
			return added, changed, err
		}
		if l.FromMe {
			*echoes = append(*echoes, l.ID) // his send coming back settles it (send.go)
		}
		if p.Added {
			added++
		}
		changed = changed || p.Changed
	}
	for _, r := range w.reactions {
		if err := b.SetReaction(chat, r.msgID, r.r); err != nil {
			return added, changed, err
		}
		changed = true
	}
	for _, id := range w.settle {
		settled, err := b.SettlePlaceholder(chat, id)
		if err != nil {
			return added, changed, err
		}
		changed = changed || settled
	}
	return added, changed, nil
}

// encPrep works out what one encrypted message writes into chat. paths are
// a restored line's attachment addresses, kept with its raw.
func (m *Messenger) encPrep(chat string, evt *events.FBMessage, source string, paths map[string]string) encWrites {
	info := evt.Info
	sender, fbid, fromMe := m.fbSender(info.Sender, info.IsFromMe)
	line := store.Message{ID: info.ID, ChatID: chat, SenderID: sender, FromMe: fromMe,
		TimeMs: max(info.Timestamp.UnixMilli(), 0), Source: source}
	if !fromMe {
		line.SenderName = m.nameOf(fbid)
	}
	hidden, shown := true, false
	notice := func() encWrites {
		line.Kind, line.Hidden = "notice", &hidden
		return encWrites{puts: []store.Message{line}}
	}
	news := func(n store.Message) encWrites {
		n.ChatID, n.Source = chat, source
		return encWrites{puts: []store.Message{n}, settle: settleOwn(info.ID, n.ID)}
	}

	var b body
	switch msg := evt.Message.(type) {
	case *waConsumerApplication.ConsumerApplication:
		switch p := msg.GetPayload().GetPayload().(type) {
		case *waConsumerApplication.ConsumerApplication_Payload_Content:
			c := p.Content
			if e := c.GetEditMessage(); e != nil {
				if target := e.GetKey().GetID(); target != "" {
					return news(store.Message{ID: target, Text: e.GetMessage().GetText(), Edited: true})
				}
				return encWrites{}
			}
			if r := c.GetReactionMessage(); r != nil {
				if target := r.GetKey().GetID(); target != "" && sender != "" {
					return encWrites{reactions: []encReaction{{msgID: target,
						r: store.Reaction{Emoji: r.GetText(), SenderID: sender, FromMe: fromMe}}},
						settle: settleOwn(info.ID, target)}
				}
				return encWrites{}
			}
			var ok, skip bool
			b, ok, skip = consumerBody(c)
			if skip {
				return encWrites{}
			}
			if !ok {
				return notice()
			}
		case *waConsumerApplication.ConsumerApplication_Payload_ApplicationData:
			if r := p.ApplicationData.GetRevoke(); r != nil {
				if target := r.GetKey().GetID(); target != "" {
					return news(store.Message{ID: target, Deleted: true})
				}
			}
			return encWrites{}
		default:
			return encWrites{} // signals and sub-protocols: between clients
		}
	case *waArmadilloApplication.Armadillo:
		c := msg.GetPayload().GetContent()
		if c == nil {
			return encWrites{}
		}
		var ok bool
		if b, ok = armadilloBody(c); !ok {
			return notice()
		}
	case *waMultiDevice.MultiDevice:
		return encWrites{} // between his own devices
	case nil:
		return notice() // a change to the chat (a disappearing timer, an admin notice)
	default:
		if _, call := msg.(*armadillo.Unsupported_Voip); call {
			line.Kind, line.Text, line.Hidden = "text", labelCall, &shown
			return encWrites{puts: []store.Message{line}}
		}
		return notice()
	}

	line.Kind, line.Text, line.Media, line.Hidden = b.kind, b.text, b.media, &shown
	line.ReplyTo = evt.FBApplication.GetMetadata().GetQuotedMessage().GetStanzaID()
	if b.keepRaw && evt.FBApplication != nil {
		if app, err := proto.Marshal(evt.FBApplication); err == nil {
			if raw, err := json.Marshal(encRaw{App: app, Paths: paths}); err == nil {
				line.Raw = raw
			}
		}
	}
	return encWrites{puts: []store.Message{line}}
}

// settleOwn is the message to settle when news under id changes the line
// target: id, unless it names that line itself.
func settleOwn(id, target string) []string {
	if id == "" || id == target {
		return nil
	}
	return []string{id}
}

// consumerBody reads what a consumer application's content shows; ok is
// false for content this adapter shows as a hidden notice, skip true for
// content that is no line (a poll vote).
func consumerBody(c *waConsumerApplication.ConsumerApplication_Content) (b body, ok, skip bool) {
	media := func(kind, caption string) body {
		return body{kind: kind, text: caption, keepRaw: true, media: []store.Media{{Kind: kind, HasFile: true}}}
	}
	switch {
	case c.GetMessageText() != nil:
		return body{kind: "text", text: c.GetMessageText().GetText()}, true, false
	case c.GetExtendedTextMessage() != nil:
		return body{kind: "text", text: c.GetExtendedTextMessage().GetText().GetText()}, true, false
	case c.GetImageMessage() != nil:
		return media("photo", c.GetImageMessage().GetCaption().GetText()), true, false
	case c.GetVideoMessage() != nil:
		return media("video", c.GetVideoMessage().GetCaption().GetText()), true, false
	case c.GetAudioMessage() != nil:
		if c.GetAudioMessage().GetPTT() {
			return media("voiceNote", ""), true, false
		}
		return media("file", ""), true, false
	case c.GetStickerMessage() != nil:
		return media("sticker", ""), true, false
	case c.GetDocumentMessage() != nil:
		return media("file", ""), true, false
	case c.GetViewOnceMessage() != nil:
		return body{kind: "text", text: labelViewOnce}, true, false
	case c.GetLocationMessage() != nil || c.GetLiveLocationMessage() != nil:
		return body{kind: "text", text: labelLocation}, true, false
	case c.GetContactMessage() != nil || c.GetContactsArrayMessage() != nil:
		return body{kind: "text", text: labelContact}, true, false
	case c.GetPollCreationMessage() != nil:
		return body{kind: "text", text: labelPoll}, true, false
	case c.GetPollUpdateMessage() != nil:
		return body{}, false, true
	}
	return body{}, false, false
}

// armadilloBody reads what an Armadillo message shows; ok is false for one
// shown as a hidden notice.
func armadilloBody(c *waArmadilloApplication.Armadillo_Content) (body, bool) {
	switch {
	case c.GetExtendedContentMessage() != nil:
		x := c.GetExtendedContentMessage()
		if t := cmp.Or(x.GetMessageText(), x.GetTitleText()); t != "" {
			return body{kind: "text", text: t}, true
		}
	case c.GetImageGalleryMessage() != nil:
		var media []store.Media
		for range c.GetImageGalleryMessage().GetImages() {
			media = append(media, store.Media{Kind: "photo", HasFile: true})
		}
		if len(media) > 0 {
			return body{kind: "photo", media: media, keepRaw: true}, true
		}
	case c.GetRavenMessage() != nil || c.GetRavenMessageMsgr() != nil:
		return body{kind: "text", text: labelViewOnce}, true
	case c.GetNoteReplyMessage().GetTextContent() != nil:
		return body{kind: "text", text: c.GetNoteReplyMessage().GetTextContent().GetText()}, true
	}
	return body{}, false
}

// onEncReceipt marks his lines seen when the other side reads or plays
// them (seen_ms), and moves his read marker when he reads on another
// device (read_through_ms): to the newest line he read, else the receipt's
// time.
func (m *Messenger) onEncReceipt(e *events.Receipt) (bool, error) {
	chatID, _, ok := encChatOf(e.Chat)
	if !ok {
		return false, nil
	}
	self := e.Type == watypes.ReceiptTypeReadSelf || e.Type == watypes.ReceiptTypePlayedSelf
	read := e.Type == watypes.ReceiptTypeRead || e.Type == watypes.ReceiptTypePlayed
	if !self && !read {
		return false, nil
	}
	chat, ok := m.st.ChatFor(Name, chatID)
	if !ok {
		return false, nil
	}
	ms := e.Timestamp.UnixMilli()
	if read && !e.IsFromMe {
		for _, id := range e.MessageIDs {
			if _, err := m.st.PutMessage(Name, store.Message{ID: id, ChatID: chat, SeenMs: ms}); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	var through int64
	for _, id := range e.MessageIDs {
		if t, ok := m.st.LineTime(Name, chat, id); ok {
			through = max(through, t)
		}
	}
	if through == 0 {
		through = ms
	}
	return true, m.readThrough(chat, through)
}

// onUndecryptable keeps a message whose encryption could not be undone as
// an "undecryptable" line under its ID, or, when Meta says it was sent
// with no copy for this device, an "unavailable" one: shown (Pllum says
// which), and when it comes again readable, it lands in that row. One
// already held is left as it is. When it was a reaction, an edit or an
// unsend, it comes again (live, or as a supplemental from his backup) only
// as news about another line, and that settles it (applyEnc): hidden.
func (m *Messenger) onUndecryptable(e *events.UndecryptableMessage) (bool, error) {
	if e.Info.ID == "" {
		return false, nil
	}
	chat, ok, err := m.encryptedChatFor(e.Info.Chat)
	if err != nil || !ok {
		return false, err
	}
	if _, held := m.st.LineTime(Name, chat, e.Info.ID); held {
		return false, nil
	}
	sender, _, fromMe := m.fbSender(e.Info.Sender, e.Info.IsFromMe)
	shown, kind := false, "undecryptable"
	if e.IsUnavailable {
		kind = "unavailable"
	}
	_, err = m.st.PutMessage(Name, store.Message{ID: e.Info.ID, ChatID: chat, SenderID: sender, FromMe: fromMe,
		TimeMs: max(e.Info.Timestamp.UnixMilli(), 0), Kind: kind, Hidden: &shown, Source: store.SourceLive})
	if err == nil && !fromMe {
		// his phone keeps it in his backup: its chat's newest page brings it (catchup.go)
		m.fillLater(chat)
	}
	return err == nil, err
}
