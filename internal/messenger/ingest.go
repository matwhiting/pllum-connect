package messenger

import (
	"cmp"
	"encoding/json"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMultiDevice"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// How a Messenger line (an unencrypted thread's, from the messages page, a
// page of threads or history, or the main socket) becomes one of the
// protocol's kinds:
//
//	text                               text
//	image, ephemeral image             photo
//	video, ephemeral video, GIF        video
//	audio, sound bite                  voiceNote
//	file                               file
//	sticker (also a sticker ID only)   sticker
//	shared link or post (XMA)          text: the text sent with it, else the link, else its title
//	admin notice (joins, calls, …)     a hidden notice
//	anything else                      text if it has any, else a hidden notice
//
// Photos, videos, voice notes, files and stickers keep what the library
// gave for them (its attachment rows, with the CDN addresses) as raw, to
// fetch the file later. The encrypted chats' lines are whatsmeow's: see
// encPrep, below.

// folderSpam is the folder of filtered message requests: listed and
// counted, never a chat, as the library's own connector leaves them.
const folderSpam = "spam"

// body is what a line shows.
type body struct {
	kind    string
	text    string
	media   []store.Media
	hidden  bool
	keepRaw bool
}

// wrapped is one line with its attachments.
type wrapped struct {
	*table.LSInsertMessage
	upsert   bool // a page's line (LSUpsertMessage), not a new one
	Blobs    []*table.LSInsertBlobAttachment
	Atts     []*table.LSInsertAttachment
	XMAs     []*table.LSInsertXmaAttachment
	Stickers []*table.LSInsertStickerAttachment
}

// rawOf is what a line keeps to fetch its media later.
func (w *wrapped) rawOf() []byte {
	b, err := json.Marshal(struct {
		Blobs    []*table.LSInsertBlobAttachment    `json:"blobs,omitempty"`
		Atts     []*table.LSInsertAttachment        `json:"attachments,omitempty"`
		Stickers []*table.LSInsertStickerAttachment `json:"stickers,omitempty"`
		Sticker  int64                              `json:"stickerID,omitempty"`
	}{w.Blobs, w.Atts, w.Stickers, w.StickerId})
	if err != nil {
		return nil
	}
	return b
}

// lines are a table's lines, each with its attachments.
func lines(tbl *table.LSTable) []*wrapped {
	byID := make(map[string]*wrapped, len(tbl.LSUpsertMessage)+len(tbl.LSInsertMessage))
	var out []*wrapped
	add := func(w *wrapped) {
		if w.MessageId == "" {
			return
		}
		if _, dup := byID[w.MessageId]; dup {
			return
		}
		byID[w.MessageId] = w
		out = append(out, w)
	}
	for _, u := range tbl.LSUpsertMessage {
		if u != nil {
			add(&wrapped{LSInsertMessage: u.ToInsert(), upsert: true})
		}
	}
	for _, i := range tbl.LSInsertMessage {
		if i != nil {
			add(&wrapped{LSInsertMessage: i})
		}
	}
	for _, a := range tbl.LSInsertBlobAttachment {
		if w := byID[a.MessageId]; w != nil {
			w.Blobs = append(w.Blobs, a)
		}
	}
	for _, a := range tbl.LSInsertAttachment {
		if w := byID[a.MessageId]; w != nil {
			w.Atts = append(w.Atts, a)
		}
	}
	for _, a := range tbl.LSInsertXmaAttachment {
		if w := byID[a.MessageId]; w != nil {
			w.XMAs = append(w.XMAs, a)
		}
	}
	for _, a := range tbl.LSInsertStickerAttachment {
		if w := byID[a.MessageId]; w != nil {
			w.Stickers = append(w.Stickers, a)
		}
	}
	return out
}

// mediaOf is one attachment's kind and size; ok is false for a kind that
// is no file (a share).
func mediaOf(t table.AttachmentType, w, h int64, hasFile bool) (store.Media, bool) {
	kind := ""
	switch t {
	case table.AttachmentTypeImage, table.AttachmentTypeEphemeralImage:
		kind = "photo"
	case table.AttachmentTypeVideo, table.AttachmentTypeEphemeralVideo, table.AttachmentTypeAnimatedImage:
		kind = "video"
	case table.AttachmentTypeAudio, table.AttachmentTypeSoundBite:
		kind = "voiceNote"
	case table.AttachmentTypeSticker, table.AttachmentTypeSelfieSticker, table.AttachmentTypeThirdPartySticker:
		kind = "sticker"
	case table.AttachmentTypeXMA, table.AttachmentTypeNone:
		return store.Media{}, false
	default:
		kind = "file"
	}
	return store.Media{Kind: kind, Width: int(w), Height: int(h), HasFile: hasFile && kind == "photo"}, true
}

// bodyOf reads what a line shows.
func bodyOf(w *wrapped) body {
	if w.IsAdminMessage {
		return body{kind: "notice", text: w.Text, hidden: true}
	}
	var media []store.Media
	for _, a := range w.Blobs {
		if x, ok := mediaOf(a.AttachmentType, a.PreviewWidth, a.PreviewHeight, a.PreviewUrl != "" || a.PlayableUrl != ""); ok {
			media = append(media, x)
		}
	}
	for _, a := range w.Atts {
		if x, ok := mediaOf(a.AttachmentType, a.PreviewWidth, a.PreviewHeight, a.PlayableUrl != ""); ok {
			media = append(media, x)
		}
	}
	for _, s := range w.Stickers {
		media = append(media, store.Media{Kind: "sticker", Width: int(s.PreviewWidth), Height: int(s.PreviewHeight), HasFile: s.PlayableUrl != ""})
	}
	if len(media) == 0 && w.StickerId != 0 {
		media = []store.Media{{Kind: "sticker"}}
	}
	if len(media) > 0 {
		return body{kind: media[0].Kind, text: w.Text, media: media, keepRaw: true}
	}
	if len(w.XMAs) > 0 {
		x := w.XMAs[0]
		if t := cmp.Or(w.Text, x.ActionUrl, x.TitleText); t != "" {
			return body{kind: "text", text: t}
		}
		return body{kind: "notice", hidden: true}
	}
	if w.Text != "" {
		return body{kind: "text", text: w.Text}
	}
	return body{kind: "notice", hidden: true}
}

// threadRow is what a listing row says of a thread.
type threadRow struct {
	key, parent, group int64
	tt                 table.ThreadType
	name, folder       string
	lastMs, readMs     int64
	muteMs             int64
}

// tableCtx is what one table says about where its threads belong, for the
// lines, reactions and receipts in it that name only a thread key.
type tableCtx struct {
	rows   map[int64][]threadRow
	hybrid map[int64]string // an older thread key → its encrypted chat
}

// placeKey is the chat a thread key's lines and changes belong to: the
// encrypted chat a pointer names (in this table, or kept from an earlier
// one), the chat its row in this table places it in, or the chat the store
// already holds for it. ok is false for a thread
// that is no chat (spam, a folder) or was never placed.
func (m *Messenger) placeKey(tc *tableCtx, key int64) (string, bool) {
	if key == 0 {
		return "", false
	}
	if c, ok := tc.hybrid[key]; ok {
		return c, true
	}
	if c, ok := m.encryptedAlias(key); ok {
		return c, true
	}
	if rows := tc.rows[key]; len(rows) > 0 {
		// An unencrypted row first: the main socket's lines are its.
		for _, enc := range []bool{false, true} {
			for _, r := range rows {
				if r.tt.IsWhatsApp() != enc || r.folder == folderSpam {
					continue
				}
				if p, ok := placeThread(key, r.tt); ok {
					return p.chat, true
				}
			}
		}
		return "", false
	}
	if c, ok := m.chatOfKey(key); ok {
		return c, true
	}
	for _, c := range []string{keyID(key) + oneToOneSuffix, keyID(key) + groupSuffix} {
		if got, ok := m.st.ChatFor(Name, c); ok {
			return got, true
		}
	}
	return "", false
}

// ingest writes what a table says into the store: its threads (as chats,
// or aliases of the encrypted chats), their people, lines, reactions, read
// receipts and mutes, and how far each thread's history reaches. Lines from
// a page are synced; source is what a new line from the socket is. It
// counts the table for the gate, wakes any FetchOlder waiting on a thread
// it answers, and tells Pllum when something changed.
func (m *Messenger) ingest(tbl *table.LSTable, source string) {
	if tbl == nil {
		return
	}
	m.addTable(tbl)
	tc := &tableCtx{rows: map[int64][]threadRow{}, hybrid: map[int64]string{}}
	changed := false

	for _, c := range tbl.LSDeleteThenInsertContact {
		m.learnName(c.Id, c.Name)
	}
	for _, c := range tbl.LSVerifyContactRowExists {
		m.learnName(c.ContactId, c.Name)
	}
	for _, t := range tbl.LSDeleteThenInsertThread {
		tc.rows[t.ThreadKey] = append(tc.rows[t.ThreadKey], threadRow{key: t.ThreadKey, parent: t.ParentThreadKey, group: t.SyncGroup,
			tt: t.ThreadType, name: t.ThreadName, folder: t.FolderName, lastMs: t.LastActivityTimestampMs,
			readMs: t.LastReadWatermarkTimestampMs, muteMs: t.MuteExpireTimeMs})
	}
	for _, t := range tbl.LSUpdateOrInsertThread {
		tc.rows[t.ThreadKey] = append(tc.rows[t.ThreadKey], threadRow{key: t.ThreadKey, parent: t.ParentThreadKey, group: t.SyncGroup,
			tt: t.ThreadType, name: t.ThreadName, folder: t.FolderName, lastMs: t.LastActivityTimestampMs,
			readMs: t.LastReadWatermarkTimestampMs, muteMs: t.MuteExpireTimeMs})
	}

	// Pointers from an older thread to an encrypted one, first, so the
	// older thread's rows and lines land in the encrypted chat.
	for _, p := range tbl.LSVerifyHybridThreadExists {
		if p.ThreadKey == 0 || p.ThreadJID == 0 {
			continue
		}
		chat := encryptedChat(p.ThreadJID, p.IsGroupThread || p.ThreadType == table.ENCRYPTED_OVER_WA_GROUP)
		tc.hybrid[p.ThreadKey] = chat
	}
	for _, p := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
		if p.ThreadKey == 0 || p.ThreadJID == 0 {
			continue
		}
		if group, ok := m.pointerIsGroup(tc, p.ThreadKey, p.ThreadJID); ok {
			tc.hybrid[p.ThreadKey] = encryptedChat(p.ThreadJID, group)
		}
	}
	for key, chat := range tc.hybrid {
		m.markEncrypted(chat)
		if m.alias(keyID(key), chat, true) {
			changed = true
		}
	}

	for _, rows := range tc.rows {
		for _, r := range rows {
			if m.writeThread(tc, r) {
				changed = true
			}
		}
	}
	for _, u := range tbl.LSSyncUpdateThreadName {
		if chat, ok := m.placeKey(tc, u.ThreadKey); ok && !isEncryptedChat(chat) {
			title := u.ThreadName
			if c, ok, _ := m.st.Chat(Name, chat); ok && c.Kind == "group" {
				_ = m.st.PutChat(Name, store.ChatInfo{ID: chat, Title: &title})
				changed = true
			}
		}
	}
	if m.members(tc, tbl) {
		changed = true
	}

	added, got := map[int64]int{}, map[int64]int{}
	for _, w := range lines(tbl) {
		chat, ok := m.placeKey(tc, w.ThreadKey)
		if !ok {
			continue
		}
		got[w.ThreadKey]++
		src := source
		if w.upsert {
			src = store.SourceSync
		}
		n, c, err := m.storeLine(chat, w, src)
		if err != nil {
			continue
		}
		added[w.ThreadKey] += n
		if c {
			changed = true // a line held already, sent again, changes nothing
		}
	}
	for _, d := range tbl.LSDeleteMessage {
		if chat, ok := m.placeKey(tc, d.ThreadKey); ok && d.MessageId != "" {
			if _, err := m.st.PutMessage(Name, store.Message{ID: d.MessageId, ChatID: chat, Deleted: true}); err == nil {
				changed = true
			}
		}
	}
	for _, e := range tbl.LSEditMessage {
		if chat := m.lineChat(e.MessageID); chat != "" {
			if _, err := m.st.PutMessage(Name, store.Message{ID: e.MessageID, ChatID: chat, Text: e.Text, Edited: true}); err == nil {
				changed = true
			}
		}
	}
	for _, r := range tbl.LSUpsertReaction {
		chat, ok := m.placeKey(tc, r.ThreadKey)
		if !ok || r.MessageId == "" || r.Reaction == "" {
			continue
		}
		sender, fromMe := m.personOf(r.ActorId)
		if m.st.SetReaction(Name, chat, r.MessageId, store.Reaction{Emoji: r.Reaction, SenderID: sender, FromMe: fromMe}) == nil {
			changed = true
		}
	}
	for _, r := range tbl.LSDeleteReaction {
		chat, ok := m.placeKey(tc, r.ThreadKey)
		if !ok || r.MessageId == "" {
			continue
		}
		sender, _ := m.personOf(r.ActorId)
		if m.st.SetReaction(Name, chat, r.MessageId, store.Reaction{SenderID: sender}) == nil {
			changed = true
		}
	}
	for _, r := range tbl.LSUpdateReadReceipt {
		chat, ok := m.placeKey(tc, r.ThreadKey)
		if !ok || r.ReadWatermarkTimestampMs <= 0 {
			continue
		}
		if _, self := m.personOf(r.ContactId); self {
			_ = m.readThrough(chat, r.ReadWatermarkTimestampMs) // he read it elsewhere
		} else {
			_ = m.st.SeenThrough(Name, chat, r.ReadWatermarkTimestampMs)
		}
		changed = true
	}
	for _, r := range tbl.LSMarkThreadRead {
		if chat, ok := m.placeKey(tc, r.ThreadKey); ok {
			_ = m.readThrough(chat, r.LastReadWatermarkTimestampMs)
			changed = true
		}
	}
	for _, r := range tbl.LSMarkThreadReadV2 {
		if chat, ok := m.placeKey(tc, r.ThreadKey); ok {
			_ = m.readThrough(chat, r.LastReadWatermarkTimestampMs)
			changed = true
		}
	}
	for _, u := range tbl.LSUpdateThreadMuteSetting {
		if chat, ok := m.placeKey(tc, u.ThreadKey); ok {
			muted, end := muteOf(u.MuteExpireTimeMS)
			_ = m.st.PutChat(Name, store.ChatInfo{ID: chat, Muted: &muted, MuteEndMs: end})
			changed = true
		}
	}

	// How far each thread's history reaches, and the FetchOlder waiting on
	// it.
	for _, r := range tbl.LSInsertNewMessageRange {
		chat, ok := m.placeKey(tc, r.ThreadKey)
		if !ok {
			continue
		}
		m.reached(chat, r.MinTimestampMs, r.MinMessageId, r.HasMoreBefore)
		m.answer(r.ThreadKey, rangeNote{lines: got[r.ThreadKey], added: added[r.ThreadKey], minMs: r.MinTimestampMs, minID: r.MinMessageId, more: r.HasMoreBefore})
	}
	for _, r := range tbl.LSUpdateExistingMessageRange {
		m.answer(r.ThreadKey, rangeNote{lines: got[r.ThreadKey], added: added[r.ThreadKey], existing: true, ts: r.TimestampMS, b2: r.UnknownBool2, b3: r.UnknownBool3})
	}
	if changed {
		m.changed()
	}
}

// pointerIsGroup says whether a pointer from an older thread to an
// encrypted one, which names no type, points at a group: by the encrypted
// thread's row in this table, the older thread's, the chats the store
// holds, or (a one-to-one's older thread key is the person's FBID, which is
// also the encrypted chat's) the two keys being one. ok is false when
// nothing says.
func (m *Messenger) pointerIsGroup(tc *tableCtx, key, jid int64) (group, ok bool) {
	for _, r := range tc.rows[jid] {
		if r.tt.IsWhatsApp() {
			return r.tt == table.ENCRYPTED_OVER_WA_GROUP, true
		}
	}
	for _, r := range tc.rows[key] {
		if !r.tt.IsWhatsApp() && !notAChat(r.tt) {
			return !r.tt.IsOneToOne(), true
		}
	}
	if _, ok := m.st.ChatFor(Name, encryptedChat(jid, true)); ok {
		return true, true
	}
	if _, ok := m.st.ChatFor(Name, encryptedChat(jid, false)); ok {
		return false, true
	}
	if key == jid {
		return false, true
	}
	return false, false
}

// muteOf is a mute's state and end from Meta's expiry: below zero is for
// ever, zero is none, anything else ends then.
func muteOf(expireMs int64) (muted bool, endMs int64) {
	switch {
	case expireMs < 0:
		return true, 0
	case expireMs == 0:
		return false, 0
	}
	return true, expireMs
}

// writeThread writes what one listing row says of its chat: kind, title,
// people (a one-to-one's), activity, his read marker and the mute. A
// thread that is no chat (spam, a folder) is skipped. An older thread moved
// aside when its chat became encrypted (the cutover folders) adds its
// history but never its mute or read marker, which are the encrypted
// chat's.
func (m *Messenger) writeThread(tc *tableCtx, r threadRow) bool {
	if r.folder == folderSpam {
		return false
	}
	p, ok := placeThread(r.key, r.tt)
	if !ok {
		return false
	}
	person := r.key // a one-to-one's thread key is the other person's FBID
	c, mapped := tc.hybrid[r.key]
	if !mapped {
		// A pointer from an earlier table: the row stays with it.
		c, mapped = m.encryptedAlias(r.key)
		mapped = mapped && c != p.chat
	}
	if mapped && !p.enc {
		p.chat, p.alias = c, keyID(r.key)
		p.kind = "single"
		if strings.HasSuffix(c, groupSuffix) {
			p.kind = "group"
		}
		// An older thread under another key: the person is the encrypted
		// chat's.
		person, _ = strconv.ParseInt(strings.TrimSuffix(c, oneToOneSuffix), 10, 64)
	}
	cutover := strings.HasPrefix(r.folder, cutoverFolder)
	if p.alias != "" {
		m.alias(p.alias, p.chat, false)
	}
	if p.enc || cutover {
		// An older thread moved aside: its chat has an encrypted side.
		m.markEncrypted(p.chat)
	}
	info := store.ChatInfo{ID: p.chat, Kind: p.kind, LastActivityMs: r.lastMs}
	title := r.name
	if p.kind == "single" {
		title = cmp.Or(m.nameOf(person), r.name)
		if who, _ := m.personOf(person); who != "" && r.tt.IsOneToOne() {
			info.Participants = m.withSelf([]store.Participant{{ID: who, Name: m.nameOf(person)}})
		}
	}
	if title != "" {
		info.Title = &title
	}
	if !cutover {
		if r.readMs > 0 {
			ms := r.readMs
			info.ReadThroughMs, info.ReadForwardOnly = &ms, true
		}
		muted, end := muteOf(r.muteMs)
		info.Muted, info.MuteEndMs = &muted, end
	}
	return m.st.PutChat(Name, info) == nil
}

// withSelf is a list of people with him added (once, marked), when his
// FBID is known.
func (m *Messenger) withSelf(people []store.Participant) []store.Participant {
	m.mu.Lock()
	self := m.fbid
	m.mu.Unlock()
	if self == 0 {
		return people
	}
	id := keyID(self)
	out := make([]store.Participant, 0, len(people)+1)
	for _, p := range people {
		if p.ID == id {
			continue
		}
		out = append(out, p)
	}
	return append(out, store.Participant{ID: id, IsSelf: true})
}

// members writes the people a table adds to or removes from its groups,
// merged with those the store holds.
func (m *Messenger) members(tc *tableCtx, tbl *table.LSTable) bool {
	if len(tbl.LSAddParticipantIdToGroupThread) == 0 && len(tbl.LSRemoveParticipantFromThread) == 0 {
		return false
	}
	type change struct {
		add    []store.Participant
		remove map[string]bool
	}
	byChat := map[string]*change{}
	var order []string
	get := func(key int64) *change {
		chat, ok := m.placeKey(tc, key)
		if !ok {
			return nil
		}
		c := byChat[chat]
		if c == nil {
			c = &change{remove: map[string]bool{}}
			byChat[chat] = c
			order = append(order, chat)
		}
		return c
	}
	for _, a := range tbl.LSAddParticipantIdToGroupThread {
		if c := get(a.ThreadKey); c != nil && a.ContactId != 0 {
			c.add = append(c.add, store.Participant{ID: keyID(a.ContactId), Name: cmp.Or(m.nameOf(a.ContactId), a.Nickname)})
			delete(c.remove, keyID(a.ContactId))
		}
	}
	for _, r := range tbl.LSRemoveParticipantFromThread {
		if c := get(r.ThreadKey); c != nil && r.ParticipantId != 0 {
			c.remove[keyID(r.ParticipantId)] = true
		}
	}
	wrote := false
	for _, chat := range order {
		c := byChat[chat]
		var people []store.Participant
		seen := map[string]int{}
		if held, ok, _ := m.st.Chat(Name, chat); ok {
			for _, p := range held.Participants {
				seen[p.ID] = len(people)
				people = append(people, p)
			}
		}
		for _, p := range c.add {
			if i, ok := seen[p.ID]; ok {
				if p.Name != "" {
					people[i].Name = p.Name
				}
				continue
			}
			seen[p.ID] = len(people)
			people = append(people, p)
		}
		kept := people[:0]
		for _, p := range people {
			if !c.remove[p.ID] {
				kept = append(kept, p)
			}
		}
		if m.st.PutChat(Name, store.ChatInfo{ID: chat, Participants: m.withSelf(kept)}) == nil {
			wrote = true
		}
	}
	return wrote
}

// storeLine writes one line (and, for a line Meta shows as edited, the
// edit): 1 when it made a new row; changed when anything Pllum shows
// changed: a new row, or a row whose state differs after the write (an
// early row from its edit or revoke taken over by the line, a new text,
// kind, edit or revoke). The same line sent again changes nothing (review
// C1; fix round 3).
func (m *Messenger) storeLine(chat string, w *wrapped, source string) (n int, changed bool, err error) {
	b := bodyOf(w)
	hidden := b.hidden
	sender, fromMe := m.personOf(w.SenderId)
	line := store.Message{
		ID:         w.MessageId,
		ChatID:     chat,
		SenderID:   sender,
		SenderName: m.nameOf(w.SenderId),
		FromMe:     fromMe,
		TimeMs:     max(w.TimestampMs, 0),
		Text:       b.text,
		Kind:       b.kind,
		Hidden:     &hidden,
		Media:      b.media,
		Source:     source,
	}
	if fromMe {
		line.SenderName = ""
	}
	if w.ReplySourceId != "" && w.ReplySourceType == 1 {
		line.ReplyTo = w.ReplySourceId
	}
	if b.keepRaw {
		line.Raw = w.rawOf()
	}
	before, held := m.st.LineState(Name, chat, w.MessageId)
	added, err := m.st.PutMessage(Name, line)
	if err != nil {
		return 0, false, err
	}
	m.seenLine(w.MessageId, chat)
	if w.IsUnsent {
		_, _ = m.st.PutMessage(Name, store.Message{ID: w.MessageId, ChatID: chat, Deleted: true})
	} else if w.EditCount > 0 && b.kind == "text" {
		_, _ = m.st.PutMessage(Name, store.Message{ID: w.MessageId, ChatID: chat, Text: b.text, Edited: true})
	}
	if added {
		return 1, true, nil
	}
	after, _ := m.st.LineState(Name, chat, w.MessageId)
	return 0, !held || after != before, nil
}

// readThrough moves his read marker forward to ms.
func (m *Messenger) readThrough(chat string, ms int64) error {
	if ms <= 0 {
		return nil
	}
	return m.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &ms, ReadForwardOnly: true})
}

// recentLines is how many line IDs → chats are kept for edits, which name
// only the line.
const recentLines = 4096

// seenLine keeps which chat a line is in, for an edit that names only the
// line; the oldest go first.
func (m *Messenger) seenLine(id, chat string) {
	m.lmu.Lock()
	defer m.lmu.Unlock()
	if _, ok := m.recent[id]; ok {
		return
	}
	m.recent[id] = chat
	m.recentOrder = append(m.recentOrder, id)
	if len(m.recentOrder) > recentLines {
		delete(m.recent, m.recentOrder[0])
		m.recentOrder = m.recentOrder[1:]
	}
}

// lineChat is the chat of a line seen this run, or "".
func (m *Messenger) lineChat(id string) string {
	m.lmu.Lock()
	defer m.lmu.Unlock()
	return m.recent[id]
}

// cursorOf is where a chat's older history goes on: the time and ID of the
// oldest line Meta has given; ok is false when none has.
func (m *Messenger) cursorOf(chat string) (ms int64, id string, ok bool) {
	t, id, found := strings.Cut(m.value("cursor."+chat), "\n")
	if !found {
		return 0, "", false
	}
	ms, err := strconv.ParseInt(t, 10, 64)
	return ms, id, err == nil
}

// reached records a range of a chat's history Meta has given: the cursor
// moves back to its start when that is older than the one held, and a range
// that reaches the start (nothing before it) completes the history.
func (m *Messenger) reached(chat string, minMs int64, minID string, more bool) {
	if minMs <= 0 {
		return
	}
	held, _, ok := m.cursorOf(chat)
	if !ok || minMs < held {
		_ = m.keep("cursor."+chat, strconv.FormatInt(minMs, 10)+"\n"+minID)
		_ = m.keep("stalled."+chat, "") // it moved: no longer held
	}
	if !more && (!ok || minMs <= held) {
		m.olderDone(chat)
	}
}

// olderDone records that a chat's unencrypted thread has nothing older.
func (m *Messenger) olderDone(chat string) {
	_ = m.keep("olderDone."+chat, "1")
	m.maybeComplete(chat)
}

// maybeComplete marks a chat historyComplete once every side of it has
// nothing older: its unencrypted thread, if it has one, and its encrypted
// backup, if it has an encrypted side (history.go).
func (m *Messenger) maybeComplete(chat string) {
	if m.hasEncryptedSide(chat) && m.value("encDone."+chat) == "" {
		return
	}
	if _, ok := m.threadOfChat(chat); ok && m.value("olderDone."+chat) == "" {
		return
	}
	complete := true
	_ = m.st.PutChat(Name, store.ChatInfo{ID: chat, HistoryComplete: &complete})
}

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
// before news about it) and reactions.
type encWrites struct {
	puts      []store.Message
	reactions []encReaction
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

// applyEnc writes what encPrep worked out. added counts the new rows;
// changed reports whether anything Pllum shows changed: a new row, a row
// whose state differs after the write (a hidden notice or an edit's early
// row taken over by its line, a new text, kind, edit or revoke), or a
// reaction. Only a line sent again unchanged changes nothing (reviews M1,
// N1).
func (m *Messenger) applyEnc(chat string, w encWrites) (added int, changed bool, err error) {
	for _, l := range w.puts {
		before, held := m.st.LineState(Name, l.ChatID, l.ID)
		got, err := m.st.PutMessage(Name, l)
		if err != nil {
			return added, changed, err
		}
		if got {
			added++
			changed = true
			continue
		}
		if after, _ := m.st.LineState(Name, l.ChatID, l.ID); !held || after != before {
			changed = true
		}
	}
	for _, r := range w.reactions {
		if err := m.st.SetReaction(Name, chat, r.msgID, r.r); err != nil {
			return added, changed, err
		}
		changed = true
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
		return encWrites{puts: []store.Message{n}}
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
						r: store.Reaction{Emoji: r.GetText(), SenderID: sender, FromMe: fromMe}}}}
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
// a hidden notice under its ID: when it comes again readable, it lands in
// that row and shows. One already held is left as it is.
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
	hidden := true
	_, err = m.st.PutMessage(Name, store.Message{ID: e.Info.ID, ChatID: chat, SenderID: sender, FromMe: fromMe,
		TimeMs: max(e.Info.Timestamp.UnixMilli(), 0), Kind: "notice", Hidden: &hidden, Source: store.SourceLive})
	return err == nil, err
}
