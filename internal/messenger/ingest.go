package messenger

import (
	"cmp"
	"encoding/json"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"

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
// fetch the file later. The encrypted chats' lines are whatsmeow's (B2).

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
// encrypted chat a pointer names, the chat its row in this table places it
// in, or the chat the store already holds for it. ok is false for a thread
// that is no chat (spam, a folder) or was never placed.
func (m *Messenger) placeKey(tc *tableCtx, key int64) (string, bool) {
	if key == 0 {
		return "", false
	}
	if c, ok := tc.hybrid[key]; ok {
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
		_ = m.keep("encrypted."+chat, "1")
		m.alias(keyID(key), chat)
		changed = true
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
		n, err := m.storeLine(chat, w, src)
		if err != nil {
			continue
		}
		added[w.ThreadKey] += n
		changed = true
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
		m.answer(r.ThreadKey, rangeNote{lines: got[r.ThreadKey], added: added[r.ThreadKey], existing: true, b2: r.UnknownBool2, b3: r.UnknownBool3})
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
	if c, mapped := tc.hybrid[r.key]; mapped && !p.enc {
		p.chat, p.alias = c, keyID(r.key)
		p.kind = "single"
		if strings.HasSuffix(c, groupSuffix) {
			p.kind = "group"
		}
		// An older thread under another key: the person is the encrypted
		// chat's.
		person, _ = strconv.ParseInt(strings.TrimSuffix(c, oneToOneSuffix), 10, 64)
	}
	if p.alias != "" {
		m.alias(p.alias, p.chat)
	}
	if p.enc {
		_ = m.keep("encrypted."+p.chat, "1")
	}
	cutover := strings.HasPrefix(r.folder, cutoverFolder)
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
// edit): 1 when it made a new row.
func (m *Messenger) storeLine(chat string, w *wrapped, source string) (int, error) {
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
	added, err := m.st.PutMessage(Name, line)
	if err != nil {
		return 0, err
	}
	m.seenLine(w.MessageId, chat)
	if w.IsUnsent {
		_, _ = m.st.PutMessage(Name, store.Message{ID: w.MessageId, ChatID: chat, Deleted: true})
	} else if w.EditCount > 0 && b.kind == "text" {
		_, _ = m.st.PutMessage(Name, store.Message{ID: w.MessageId, ChatID: chat, Text: b.text, Edited: true})
	}
	if added {
		return 1, nil
	}
	return 0, nil
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
	}
	if !more && (!ok || minMs <= held) {
		m.olderDone(chat)
	}
}

// olderDone records that a chat's unencrypted thread has nothing older. A
// chat without an encrypted side is then historyComplete; an encrypted
// chat's history goes on in its encrypted backup, which is B2's to read.
func (m *Messenger) olderDone(chat string) {
	_ = m.keep("olderDone."+chat, "1")
	if m.value("encrypted."+chat) != "" || isEncryptedChat(chat) && strings.HasSuffix(chat, groupSuffix) {
		return
	}
	complete := true
	_ = m.st.PutChat(Name, store.ChatInfo{ID: chat, HistoryComplete: &complete})
}
