package instagram

import (
	"cmp"
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// How an Instagram message becomes one of the protocol's kinds:
//
//	text                               text (TextBody)
//	image, view-once image             photo
//	video, view-once video             video
//	photos and videos together         photo or video, as the first is
//	voice note                         voiceNote
//	sticker, avatar or AI sticker      sticker
//	GIF                                sticker when Meta calls it one, else video
//	music sticker                      sticker
//	shared post, reel or story         text "[post]", "[reel]", "[story]"
//	shared link                        text: the URL (or the text sent with it)
//	call                               text "[call]"
//	any other notice                   a hidden notice
//	AI answers                         text
//	anything else                      text if it has any, else a hidden notice
//
// Text sent with a share follows the label on a new line. Photos, videos,
// voice notes and stickers keep the message's content (its JSON, with the
// CDN addresses) as raw, to fetch the file later.

const (
	labelPost  = "[post]"
	labelReel  = "[reel]"
	labelStory = "[story]"
	labelCall  = "[call]"
)

// body is what a message shows.
type body struct {
	kind    string
	text    string
	media   []store.Media
	hidden  bool
	keepRaw bool
}

// bodyOf reads what m shows.
func bodyOf(m *slidetypes.Message) body {
	switch c := m.Content.Content.(type) {
	case *slidetypes.MessageContentText:
		return body{kind: "text", text: cmp.Or(m.TextBody, c.TextBody)}
	case *slidetypes.MessageContentAdminText:
		if isCall(m.ContentType) {
			return body{kind: "text", text: labelCall}
		}
		return notice(adminText(c, m.IGDSnippet))
	case *slidetypes.MessageContentImage:
		return mediaBody(m.TextBody, attachments("photo", c.Attachments))
	case *slidetypes.MessageContentRavenImage:
		return mediaBody(m.TextBody, attachments("photo", []*slidetypes.Attachment{c.Attachment}))
	case *slidetypes.MessageContentVideo:
		return mediaBody(m.TextBody, attachments("video", c.Videos))
	case *slidetypes.MessageContentRavenVideo:
		return mediaBody(m.TextBody, attachments("video", []*slidetypes.Attachment{c.Attachment}))
	case *slidetypes.MessageContentMultiMedia:
		return mediaBody(m.TextBody, attachments("", c.Attachments))
	case *slidetypes.MessageContentAudio:
		media := make([]store.Media, 0, len(c.AudioAttachments))
		for range c.AudioAttachments {
			media = append(media, store.Media{Kind: "voiceNote"})
		}
		if len(media) == 0 {
			media = []store.Media{{Kind: "voiceNote"}}
		}
		return mediaBody(m.TextBody, media)
	case *slidetypes.MessageContentSticker:
		return mediaBody("", []store.Media{{Kind: "sticker", Width: c.PreviewWidth, Height: c.PreviewHeight, HasFile: c.PreviewURL != ""}})
	case *slidetypes.MessageContentAnimatedMedia:
		media := make([]store.Media, 0, len(c.AnimatedMedia))
		for _, a := range c.AnimatedMedia {
			if a == nil {
				continue
			}
			if a.IsSticker || a.AttachmentMP4URL == "" {
				media = append(media, store.Media{Kind: "sticker", Width: a.PreviewWidth, Height: a.PreviewHeight, HasFile: a.AttachmentWebpURL != "" || a.PreviewCDNURL != ""})
			} else {
				media = append(media, store.Media{Kind: "video", Width: a.PreviewWidth, Height: a.PreviewHeight})
			}
		}
		if len(media) == 0 {
			media = []store.Media{{Kind: "sticker"}}
		}
		return mediaBody(m.TextBody, media)
	case *slidetypes.MessageContentMusicSticker:
		return mediaBody("", []store.Media{{Kind: "sticker"}})
	case *slidetypes.MessageContentXMA:
		return shareOf(c)
	case *slidetypes.MessageContentAIRichResponse:
		return body{kind: "text", text: cmp.Or(c.UnifiedResponse, m.TextBody)}
	case *slidetypes.MessageContentAISearchResponse:
		return body{kind: "text", text: cmp.Or(c.MessageTextBody, m.TextBody)}
	}
	if m.TextBody != "" {
		return body{kind: "text", text: m.TextBody}
	}
	return notice(m.IGDSnippet)
}

func notice(text string) body { return body{kind: "notice", text: text, hidden: true} }

// isCall reports whether a notice's content type is a call's.
func isCall(contentType string) bool { return strings.Contains(strings.ToUpper(contentType), "CALL") }

// adminText is a notice's text: its fragments, else Meta's snippet.
func adminText(c *slidetypes.MessageContentAdminText, snippet string) string {
	var b strings.Builder
	for _, f := range c.TextFragments {
		b.WriteString(f.Plaintext)
	}
	return cmp.Or(b.String(), snippet)
}

// attachments are photos and videos; kind "" reads each one's own (a video
// has a DASH manifest).
func attachments(kind string, list []*slidetypes.Attachment) []store.Media {
	out := make([]store.Media, 0, len(list))
	for _, a := range list {
		k := kind
		if a == nil {
			out = append(out, store.Media{Kind: cmp.Or(k, "photo")})
			continue
		}
		if k == "" {
			k = "photo"
			if a.DashManifest != "" {
				k = "video"
			}
		}
		out = append(out, store.Media{Kind: k, Width: a.PreviewWidth, Height: a.PreviewHeight,
			HasFile: k == "photo" && (a.AttachmentCDNURL != "" || a.PreviewCDNURL != "")})
	}
	if len(out) == 0 {
		out = []store.Media{{Kind: cmp.Or(kind, "photo")}}
	}
	return out
}

func mediaBody(text string, media []store.Media) body {
	return body{kind: media[0].Kind, text: text, media: media, keepRaw: true}
}

// shareOf is a shared post, reel, story or link: a label (or the link), with
// any text sent with it after.
func shareOf(c *slidetypes.MessageContentXMA) body {
	if c.XMATextBody != "" && (c.XMA == nil || c.XMA.TargetID == "") {
		// A message with a link in it: its text holds the link.
		return body{kind: "text", text: c.XMATextBody}
	}
	label := shareLabel(c.XMA)
	text := label
	if c.XMATextBody != "" {
		text = strings.TrimSpace(label + "\n" + c.XMATextBody)
	}
	if text == "" {
		return notice("")
	}
	return body{kind: "text", text: text}
}

// shareLabel names what a share points at by its address.
func shareLabel(x *slidetypes.XMAContent) string {
	if x == nil {
		return ""
	}
	target := x.TargetURL
	var path string
	if u, err := url.Parse(target); err == nil && u != nil {
		path = u.Path
		// Meta wraps outside links in a redirect.
		if (u.Host == "l.facebook.com" || u.Host == "l.instagram.com") && u.Query().Get("u") != "" {
			target, path = u.Query().Get("u"), ""
		}
	}
	switch {
	case strings.HasPrefix(path, "/stories/"):
		return labelStory
	case strings.HasPrefix(path, "/reel/"), strings.HasPrefix(path, "/reels/"):
		return labelReel
	case strings.HasPrefix(path, "/p/"), strings.HasPrefix(path, "/tv/"):
		return labelPost
	case target != "":
		return target
	case x.TargetID != "":
		return labelPost
	}
	return ""
}

// lineOf is the line m makes in chat.
func (g *Instagram) lineOf(chat string, m *slidetypes.Message, source string) store.Message {
	b := bodyOf(m)
	hidden := b.hidden
	senderID, fromMe := g.personOf(m.SenderFBID)
	line := store.Message{
		ID:       cmp.Or(m.ID, m.MessageID),
		ChatID:   chat,
		SenderID: senderID,
		FromMe:   fromMe,
		TimeMs:   m.TimestampMS.UnixMilli(),
		Text:     b.text,
		Kind:     b.kind,
		Hidden:   &hidden,
		ReplyTo:  m.RepliedToMessageID,
		Media:    b.media,
		Source:   source,
	}
	if s := m.Sender; s != nil && !fromMe {
		g.learnUser(&s.UserDict)
		line.SenderName = cmp.Or(s.Name, nameOf(&s.UserDict))
		if m.SenderFBID == 0 {
			line.SenderID = cmp.Or(s.UserDict.ID, s.IGID)
		}
	}
	if b.keepRaw {
		if raw, err := json.Marshal(&m.Content); err == nil {
			line.Raw = raw
		}
	}
	if line.TimeMs < 0 {
		line.TimeMs = 0
	}
	return line
}

// storeMessage writes one message, its edit and its reactions. A message
// Meta shows as edited is written whole, then as the edit.
func (g *Instagram) storeMessage(chat string, m *slidetypes.Message, source string, asNotice bool) (added bool, err error) {
	if m == nil {
		return false, nil
	}
	line := g.lineOf(chat, m, source)
	if line.ID == "" {
		return false, nil
	}
	if asNotice && line.Text != labelCall {
		hidden := true
		line.Kind, line.Hidden, line.Media, line.Raw = "notice", &hidden, nil, nil
	}
	if added, err = g.st.PutMessage(Name, line); err != nil {
		return false, err
	}
	if len(m.SlideEditHistory) > 0 && line.Kind == "text" {
		if _, err := g.st.PutMessage(Name, store.Message{ID: line.ID, ChatID: chat, Text: line.Text, Edited: true}); err != nil {
			return added, err
		}
	}
	for _, r := range m.Reactions {
		if r == nil || r.Reaction == "" {
			continue
		}
		if err := g.setReaction(chat, line.ID, r); err != nil {
			return added, err
		}
	}
	return added, nil
}

// storeMessages writes a page of messages and counts the lines it added.
func (g *Instagram) storeMessages(chat string, edges []slidetypes.Node[*slidetypes.Message], source string) (int, error) {
	n := 0
	for _, e := range edges {
		added, err := g.storeMessage(chat, e.Node, source, false)
		if err != nil {
			return n, err
		}
		if added {
			n++
		}
	}
	return n, nil
}

// setReaction keeps one reaction, and which reaction message made it, for an
// unsend that names only that.
func (g *Instagram) setReaction(chat, msgID string, r *slidetypes.Reaction) error {
	sender, fromMe := g.personOf(r.SenderFBID)
	if err := g.st.SetReaction(Name, chat, msgID, store.Reaction{Emoji: r.Reaction, SenderID: sender, FromMe: fromMe}); err != nil {
		return err
	}
	if r.LogMessageID != "" {
		_ = g.keep("reaction."+r.LogMessageID, msgID+"\n"+sender)
	}
	return nil
}

// storeThread writes what a thread says of its chat: kind, title, people,
// mute, his read marker, their receipts and his unread mark, and activity.
func (g *Instagram) storeThread(t *slidetypes.ThreadInfo) (chat string, ok bool, err error) {
	chat, ok = g.place(t)
	if !ok {
		return "", false, nil
	}
	info := store.ChatInfo{
		ID:             chat,
		Kind:           "single",
		LastActivityMs: t.LastActivityTimestampMS.UnixMilli(),
		Participants:   g.participants(t),
		Muted:          t.IsMuted,
	}
	var title string
	switch {
	case t.IsGroup || len(t.Users) > 1:
		info.Kind, title = "group", t.ThreadTitle
	case len(t.Users) == 1:
		title = nameOf(t.Users[0])
	default:
		title = nameOf(t.Viewer)
	}
	info.Title = &title
	viewer := g.viewer()
	for _, rr := range t.SlideReadReceipts {
		if rr.ParticipantFBID == viewer && viewer != 0 {
			ms := rr.WatermarkTimestampMS.UnixMilli()
			if ms > 0 {
				info.ReadThroughMs, info.ReadForwardOnly = &ms, true
			}
		}
	}
	if err := g.st.PutChat(Name, info); err != nil {
		return chat, true, err
	}
	for _, rr := range t.SlideReadReceipts {
		if rr.ParticipantFBID != viewer {
			if ms := rr.WatermarkTimestampMS.UnixMilli(); ms > 0 {
				if err := g.st.SeenThrough(Name, chat, ms); err != nil {
					return chat, true, err
				}
			}
		}
	}
	if t.MarkedAsUnread {
		if err := g.markUnreadHere(chat); err != nil {
			return chat, true, err
		}
	}
	return chat, true, nil
}

// listed is one thread as an inbox listing showed it.
type listed struct {
	chat     string
	igChatID string
	activity int64 // its last activity, as listed
	held     int64 // the chat's last activity before the listing; 0 for a chat new here
}

// storeListing writes the threads of an inbox page, in order.
func (g *Instagram) storeListing(edges []slidetypes.Node[slidetypes.WrappedThreadInfo]) []listed {
	out := make([]listed, 0, len(edges))
	for _, e := range edges {
		t := e.Node.AsIGDirectThread
		if t == nil || t.ThreadKey == 0 {
			continue
		}
		var held int64
		if c, ok, err := g.st.Chat(Name, chatIDOf(t.ThreadKey)); err == nil && ok {
			held = c.LastActivityMs
		}
		chat, ok, err := g.storeThread(t)
		if err != nil || !ok {
			continue
		}
		out = append(out, listed{chat: chat, igChatID: t.ID, activity: t.LastActivityTimestampMS.UnixMilli(), held: held})
	}
	return out
}

// markUnreadHere sets his unread mark on a chat in the store only: the
// newest line from them reads as unread.
func (g *Instagram) markUnreadHere(chat string) error {
	in, ok, err := g.st.NewestIncoming(Name, chat)
	if err != nil || !ok {
		return err
	}
	before := in.TimeMs - 1
	return g.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &before})
}

// readThrough moves his read marker forward to ms.
func (g *Instagram) readThrough(chat string, ms int64) error {
	if ms <= 0 {
		return nil
	}
	return g.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &ms, ReadForwardOnly: true})
}

// onDelta writes one change from the socket. A new message in a thread
// never listed fetches that thread first (through the pace); any other
// change to one is dropped.
func (g *Instagram) onDelta(ctx context.Context, gen int, d *slidetypes.Delta) {
	igChatID := d.ThreadIGID
	create := false
	switch e := d.Data.(type) {
	case *slidetypes.NewMessageEvent:
		create = true
		if igChatID == "" && e.Message != nil {
			igChatID = e.Message.ThreadFBID
		}
	case *slidetypes.AdminMessageEvent:
		create = true
		if igChatID == "" && e.Message != nil {
			igChatID = e.Message.ThreadFBID
		}
	case *slidetypes.CreateReactionEvent, *slidetypes.DeleteReactionEvent,
		*slidetypes.EditMessageEvent, *slidetypes.DeleteMessageEvent,
		*slidetypes.MarkReadEvent, *slidetypes.MarkUnreadEvent, *slidetypes.ReadReceiptEvent,
		*slidetypes.MuteThreadEvent, *slidetypes.UpdateThreadNameEvent:
	default:
		return
	}
	chat, ok := g.chatOfThread(igChatID)
	if !ok && create {
		chat, ok = g.fetchThread(ctx, gen, igChatID)
	}
	if !ok {
		return
	}
	if err := g.applyDelta(chat, d.Data); err == nil {
		g.changed()
	}
}

func (g *Instagram) applyDelta(chat string, data slidetypes.DeltaEvent) error {
	switch e := data.(type) {
	case *slidetypes.NewMessageEvent:
		_, err := g.storeMessage(chat, e.Message, store.SourceLive, false)
		return err
	case *slidetypes.AdminMessageEvent:
		_, err := g.storeMessage(chat, e.Message, store.SourceLive, true)
		return err
	case *slidetypes.CreateReactionEvent:
		if e.MessageID == "" {
			return nil
		}
		return g.setReaction(chat, e.MessageID, &e.Reaction)
	case *slidetypes.DeleteReactionEvent:
		target := e.MessageID
		var sender string
		if e.Reaction.SenderFBID != 0 {
			sender, _ = g.personOf(e.Reaction.SenderFBID)
		} else {
			// Only the reaction's own message is named: look it up.
			kept := g.value("reaction." + e.Reaction.LogMessageID)
			var ok bool
			if target, sender, ok = strings.Cut(kept, "\n"); !ok || e.Reaction.LogMessageID == "" {
				return nil
			}
		}
		if target == "" {
			return nil
		}
		if e.Reaction.LogMessageID != "" {
			_ = g.keep("reaction."+e.Reaction.LogMessageID, "")
		}
		return g.st.SetReaction(Name, chat, target, store.Reaction{SenderID: sender})
	case *slidetypes.EditMessageEvent:
		if e.MessageID == "" {
			return nil
		}
		_, err := g.st.PutMessage(Name, store.Message{ID: e.MessageID, ChatID: chat, Text: e.TextBody, Edited: true})
		return err
	case *slidetypes.DeleteMessageEvent:
		if e.MessageID == "" {
			return nil
		}
		_, err := g.st.PutMessage(Name, store.Message{ID: e.MessageID, ChatID: chat, Deleted: true})
		return err
	case *slidetypes.MarkReadEvent:
		return g.readThrough(chat, e.ReadTimestampMS.UnixMilli())
	case *slidetypes.ReadReceiptEvent:
		ms := e.ReadReceipt.WatermarkTimestampMS.UnixMilli()
		if ms <= 0 {
			return nil
		}
		if v := g.viewer(); v != 0 && e.ReadReceipt.ParticipantFBID == v {
			return g.readThrough(chat, ms) // he read it elsewhere
		}
		return g.st.SeenThrough(Name, chat, ms)
	case *slidetypes.MarkUnreadEvent:
		if !e.MarkedAsUnread {
			return nil
		}
		return g.markUnreadHere(chat)
	case *slidetypes.MuteThreadEvent:
		muted := e.IsMutedNow
		return g.st.PutChat(Name, store.ChatInfo{ID: chat, Muted: &muted})
	case *slidetypes.UpdateThreadNameEvent:
		if c, ok, err := g.st.Chat(Name, chat); err != nil || !ok || c.Kind != "group" {
			return err
		}
		title := e.ThreadName
		return g.st.PutChat(Name, store.ChatInfo{ID: chat, Title: &title})
	}
	return nil
}

// fetchThread lists one thread Meta has not listed here yet (a new
// conversation), with its newest messages; ok is false when it cannot.
func (g *Instagram) fetchThread(ctx context.Context, gen int, igChatID string) (string, bool) {
	_, cli, _ := g.running()
	if igChatID == "" || cli == nil {
		return "", false
	}
	var resp *slidetypes.ThreadInfoResponse
	err := g.request(ctx, gen, func(ctx context.Context) (err error) {
		resp, err = cli.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(igChatID))
		return err
	})
	if err != nil || resp == nil || resp.ThreadInfo.AsIGDirectThread == nil {
		return "", false
	}
	t := resp.ThreadInfo.AsIGDirectThread
	chat, ok := g.place(t)
	if !ok {
		return "", false
	}
	if msgs := t.SlideMessages; msgs != nil {
		if _, err := g.storeMessages(chat, msgs.Edges, store.SourceSync); err != nil {
			return "", false
		}
		g.pageDone(chat, msgs.PageInfo, true)
	}
	if _, _, err := g.storeThread(t); err != nil {
		return "", false
	}
	return chat, true
}
