package linkedin

import (
	"cmp"
	"encoding/json"
	"strings"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// How a LinkedIn message becomes one of the protocol's lines:
//
//	DEFAULT                          its text (Body.Text), with what it carries
//	EDITED                           the same line, its new text, edited
//	RECALLED                         the same line, deleted
//	SYSTEM                           a hidden notice
//	VectorImage, ExternalMedia       a photo
//	Video                            a video
//	Audio                            a voice note
//	File                             a file (an InMail's attachment included)
//	ForwardedMessage                 "[forwarded]", then the forwarded text
//	HostURNData with no text         "[shared post]"
//	RepliedMessageContent            replyTo the original's URN
//	nothing at all                   a hidden notice, never an empty line
//
// A line with media keeps its render content (its JSON, with the CDN
// addresses) as raw, to fetch the file later. Reactions come as one count per
// emoji, with no sender, fromMe when he is among them.

const (
	labelForwarded  = "[forwarded]"
	labelSharedPost = "[shared post]"
)

// linePrefix starts every LinkedIn line's ID (a message URN), telling them
// from Beeper's copies in the same chat.
const linePrefix = "urn:li:"

// urnString is a URN as text, "" for none (an empty URN prints ":").
func urnString(u linkedingo.URN) string {
	if u.IsEmpty() {
		return ""
	}
	return u.String()
}

// nameOf is a participant's name: a person's first and last, an
// organisation's.
func nameOf(p linkedingo.MessagingParticipant) string {
	switch {
	case p.ParticipantType.Member != nil:
		m := p.ParticipantType.Member
		return strings.TrimSpace(m.FirstName.Text + " " + m.LastName.Text)
	case p.ParticipantType.Organization != nil:
		return strings.TrimSpace(p.ParticipantType.Organization.Name.Text)
	}
	return ""
}

// handleOf is a member's public profile ID, from the profile URL the
// conversation already carries; an organisation has none. A URL that names
// the member by their profile ID (id) instead gives none: that is an ID,
// not a public one.
func handleOf(p linkedingo.MessagingParticipant, id string) string {
	m := p.ParticipantType.Member
	if m == nil {
		return ""
	}
	if h := publicID(m.ProfileURL); h != id {
		return h
	}
	return ""
}

// chatInfo is what a conversation says of its chat. One without participants
// (a realtime event's partial copy) says nothing of its kind, people or name.
func (l *LinkedIn) chatInfo(c linkedingo.Conversation) store.ChatInfo {
	info := store.ChatInfo{ID: urnString(c.EntityURN)}
	if !c.LastActivityAt.IsZero() {
		info.LastActivityMs = c.LastActivityAt.UnixMilli()
	}
	self := l.self()
	title := c.Title
	if len(c.ConversationParticipants) > 0 {
		info.Kind = "single"
		if c.GroupChat {
			info.Kind = "group"
		}
		none := ""
		info.Phone = &none
		info.Participants = []store.Participant{}
		for _, p := range c.ConversationParticipants {
			id := profileID(p.EntityURN)
			if id == "" {
				continue
			}
			info.Participants = append(info.Participants, store.Participant{ID: id, Name: nameOf(p), IsSelf: id == self,
				Handle: handleOf(p, id)})
			if title == "" && !c.GroupChat && id != self {
				title = nameOf(p)
			}
		}
	}
	if title != "" {
		info.Title = &title
	}
	switch {
	case !c.LastReadAt.IsZero():
		at := c.LastReadAt.UnixMilli()
		info.ReadThroughMs = &at
	case c.Read && !c.LastActivityAt.IsZero():
		at := c.LastActivityAt.UnixMilli()
		info.ReadThroughMs = &at
	}
	return info
}

// storeConversation writes a conversation's chat, and reports whether it has
// activity newer than the store had for it (or is new): whether what came in
// it needs fetching. Spam is left out.
func (l *LinkedIn) storeConversation(c linkedingo.Conversation) (changed bool) {
	id := urnString(c.EntityURN)
	if !isConversation(id) || skipped(c) {
		return false
	}
	prev, had, err := l.st.Chat(Name, id)
	if err != nil {
		return false
	}
	if l.st.PutChat(Name, l.chatInfo(c)) != nil {
		return false
	}
	if !had {
		return true
	}
	return !c.LastActivityAt.IsZero() && c.LastActivityAt.UnixMilli() > prev.LastActivityMs
}

// readOnly makes a chat LinkedIn deleted read-only; its lines stay.
func (l *LinkedIn) readOnly(chat string) {
	if !isConversation(chat) {
		return
	}
	if _, ok, err := l.st.Chat(Name, chat); err != nil || !ok {
		return // never stored (spam, or never listed): nothing to keep readable
	}
	yes := true
	if l.st.PutChat(Name, store.ChatInfo{ID: chat, ReadOnly: &yes}) == nil {
		l.changed()
	}
}

// chatOfMessage is the conversation a message is in: the one it names, else
// the chat holding it already.
func (l *LinkedIn) chatOfMessage(m linkedingo.Message) string {
	return chatOfMessageBy(m, func(id string) (string, bool) { return l.st.ChatOfLine(Name, id) })
}

// chatOfMessageBy is chatOfMessage, asking chatOfLine for the chat holding
// a line (in a batch, the batch's own, which sees its lines).
func chatOfMessageBy(m linkedingo.Message, chatOfLine func(id string) (string, bool)) string {
	if id := cmp.Or(urnString(m.Conversation.EntityURN), urnString(m.ConversationURN)); isConversation(id) {
		return id
	}
	if chat, ok := chatOfLine(urnString(m.EntityURN)); ok {
		return chat
	}
	return ""
}

// storeMessages writes messages as lines, in one batch (review C1), and
// reports whether any was a line the store already held (so a page reached
// what was stored). The same URN is always the same row, from a page or the
// stream. A line the store refuses is passed over, as it always was; err is
// a batch the store lost, none of whose lines are kept, so whoever pages
// must not move past it.
func (l *LinkedIn) storeMessages(msgs []linkedingo.Message, source string) (heldAny bool, err error) {
	if len(msgs) == 0 {
		return false, nil
	}
	err = l.st.Batch(Name, func(b *store.Batch) error {
		for _, m := range msgs {
			id := urnString(m.EntityURN)
			chat := chatOfMessageBy(m, b.ChatOfLine)
			if id == "" || chat == "" {
				continue
			}
			st, ok := b.LineState(chat, id)
			held := ok && !st.Early
			heldAny = heldAny || held
			l.storeMessageIn(b, m, chat, id, source, held)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return heldAny, nil
}

// storeMessage writes one message in a batch of its own (storeMessageIn).
func (l *LinkedIn) storeMessage(m linkedingo.Message, chat, id, source string, held bool) {
	_ = l.st.Batch(Name, func(b *store.Batch) error {
		l.storeMessageIn(b, m, chat, id, source, held)
		return nil
	})
}

// storeMessageIn writes one message into a batch: a whole line, then an edit or a recall
// over it (a line first seen edited or recalled is written whole first, so
// it has its time and side), and its reaction counts.
func (l *LinkedIn) storeMessageIn(batch *store.Batch, m linkedingo.Message, chat, id, source string, held bool) {
	b := bodyOf(m)
	self := l.self()
	sender := profileID(m.Sender.EntityURN)
	whole := store.Message{
		ID:         id,
		ChatID:     chat,
		SenderID:   sender,
		SenderName: nameOf(m.Sender),
		FromMe:     sender != "" && sender == self,
		Text:       b.text,
		Kind:       b.kind,
		Hidden:     &b.hidden,
		ReplyTo:    b.replyTo,
		Media:      b.media,
		Source:     source,
		Raw:        b.raw,
	}
	if !m.DeliveredAt.IsZero() {
		whole.TimeMs = m.DeliveredAt.UnixMilli()
	}
	switch m.MessageBodyRenderFormat {
	case linkedingo.MessageBodyRenderFormatRecalled:
		if !held {
			whole.Text, whole.Kind, whole.Media, whole.Raw = "", "text", nil, nil
			hidden := false
			whole.Hidden = &hidden
			_, _ = batch.PutMessage(whole)
		}
		_, _ = batch.PutMessage(store.Message{ID: id, ChatID: chat, TimeMs: whole.TimeMs, Deleted: true, Source: source})
	case linkedingo.MessageBodyRenderFormatEdited:
		if !held {
			_, _ = batch.PutMessage(whole)
		}
		_, _ = batch.PutMessage(store.Message{ID: id, ChatID: chat, Text: b.text, Edited: true, Source: source})
	case linkedingo.MessageBodyRenderFormatSystem:
		whole.Kind, whole.Media, whole.Raw, whole.ReplyTo = "notice", nil, nil, ""
		hidden := true
		whole.Hidden = &hidden
		_, _ = batch.PutMessage(whole)
	default:
		_, _ = batch.PutMessage(whole)
	}
	// A page's copy is the message whole, so its counts replace what is held,
	// none included; the stream's carries them only sometimes.
	if source == store.SourceSync || len(m.ReactionSummaries) > 0 {
		summary := make([]store.Reaction, 0, len(m.ReactionSummaries))
		for _, r := range m.ReactionSummaries {
			if r.Emoji != "" && r.Count > 0 {
				summary = append(summary, store.Reaction{Emoji: r.Emoji, FromMe: r.ViewerReacted})
			}
		}
		_ = batch.SetReactionSummary(chat, id, summary)
	}
}

// body is what a message shows.
type body struct {
	kind    string
	text    string
	hidden  bool
	replyTo string
	media   []store.Media
	raw     []byte
}

// bodyOf reads what m shows (before its render format: an edit or recall
// is applied over it).
func bodyOf(m linkedingo.Message) body {
	b := body{text: m.Body.Text}
	forwarded := false
	for _, rc := range m.RenderContent {
		switch {
		case rc.VectorImage != nil:
			a := rc.VectorImage.GetLargestArtifact()
			b.media = append(b.media, store.Media{Kind: "photo", Width: a.Width, Height: a.Height, HasFile: rc.VectorImage.RootURL != ""})
		case rc.ExternalMedia != nil:
			x := rc.ExternalMedia.Media
			b.media = append(b.media, store.Media{Kind: "photo", Width: x.OriginalWidth, Height: x.OriginalHeight, HasFile: x.URL != ""})
		case rc.Video != nil:
			b.media = append(b.media, store.Media{Kind: "video"})
		case rc.Audio != nil:
			b.media = append(b.media, store.Media{Kind: "voiceNote"})
		case rc.File != nil:
			b.media = append(b.media, store.Media{Kind: "file"})
		case rc.ForwardedMessage != nil:
			forwarded = true
			if t := rc.ForwardedMessage.ForwardedBody.Text; t != "" {
				b.text = t
			}
		case rc.HostURNData != nil:
			if b.text == "" {
				b.text = labelSharedPost
			}
		case rc.RepliedMessageContent != nil:
			b.replyTo = urnString(rc.RepliedMessageContent.OriginalMessage.EntityURN)
		}
	}
	if forwarded {
		if b.text == "" {
			b.text = labelForwarded
		} else {
			b.text = labelForwarded + "\n" + b.text
		}
	}
	switch {
	case len(b.media) > 0:
		b.kind = b.media[0].Kind
		if raw, err := json.Marshal(m.RenderContent); err == nil {
			b.raw = raw
		}
	case b.text != "":
		b.kind = "text"
	default:
		b.kind, b.hidden = "notice", true
	}
	return b
}

// storeReaction applies a realtime reaction count: an emoji with a count is
// there (his when he is among them), one at none is gone.
func (l *LinkedIn) storeReaction(r linkedingo.RealtimeReactionSummary) {
	id := urnString(r.Message.EntityURN)
	chat := l.chatOfMessage(r.Message)
	if id == "" || chat == "" || r.ReactionSummary.Emoji == "" {
		return
	}
	present := r.ReactionSummary.Count > 0
	_ = l.st.SetSummaryEmoji(Name, chat, id, r.ReactionSummary.Emoji, present, present && r.ReactionSummary.ViewerReacted)
}

// storeSeen applies a seen receipt: someone else's marks his lines up to the
// message seen at its time; his own (read on another device) moves his read
// marker there.
func (l *LinkedIn) storeSeen(r linkedingo.SeenReceipt) {
	id := urnString(r.Message.EntityURN)
	chat := l.chatOfMessage(r.Message)
	if id == "" || chat == "" {
		return
	}
	at, ok := l.st.LineTime(Name, chat, id)
	if !ok {
		return
	}
	if who := profileID(r.SeenByParticipant.EntityURN); who != "" && who == l.self() {
		_ = l.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &at, ReadForwardOnly: true})
		return
	}
	seen := at
	if !r.SeenAt.IsZero() {
		seen = r.SeenAt.UnixMilli()
	}
	_ = l.st.SeenThroughAt(Name, chat, at, seen)
}
