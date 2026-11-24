package messenger

import (
	"cmp"
	"context"
	"encoding/json"
	_ "image/jpeg" // photo sizes, for an encrypted photo's message
	_ "image/png"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	watypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// sendRequest is what the outbox keeps of a write.
type sendRequest struct {
	Text     string `json:"text,omitempty"`
	ReplyTo  string `json:"replyTo,omitempty"`
	File     string `json:"file,omitempty"`
	FileName string `json:"fileName,omitempty"`
	Kind     string `json:"kind,omitempty"` // text, photo, video or file
	Mimetype string `json:"mimetype,omitempty"`
	OTID     string `json:"otid,omitempty"`     // a send on an unencrypted thread: its last message's offline threading ID
	FileOTID string `json:"fileOTID,omitempty"` // a file sent after its text there: the file's own
	FileID   string `json:"fileID,omitempty"`   // a file sent after its text in an encrypted chat: the file's message ID
	Target   string `json:"target,omitempty"`   // the line a reaction is to
	Emoji    string `json:"emoji,omitempty"`
	WAID     string `json:"waID,omitempty"` // an encrypted chat's reaction: the message ID it goes under
}

// sendProgress is how far a send got, kept in its row's message before
// the next step.
type sendProgress struct {
	Attachment int64  `json:"attachment,omitempty"` // the unencrypted upload's attachment ID
	Upload     []byte `json:"upload,omitempty"`     // the encrypted upload: its WAMediaTransport, marshalled
	Width      int    `json:"width,omitempty"`      // a photo's or video's size, for the encrypted message
	Height     int    `json:"height,omitempty"`
	TextSent   bool   `json:"textSent,omitempty"` // the text of a text-and-file went
	TextLineID string `json:"textLineID,omitempty"`
	Asking     bool   `json:"asking,omitempty"` // a message is asked and its answer not yet kept
}

// progressOf is a row's progress.
func progressOf(o store.Outgoing) sendProgress {
	var p sendProgress
	if len(o.Message) > 0 {
		_ = json.Unmarshal(o.Message, &p)
	}
	return p
}

// uploaded reports whether a send's file is uploaded already.
func (p sendProgress) uploaded() bool { return p.Attachment != 0 || len(p.Upload) > 0 }

// keepProgress keeps a send's progress in its row.
func (m *Messenger) keepProgress(id string, p sendProgress) error {
	if m.testKeep != nil {
		if err := m.testKeep(p); err != nil {
			return err
		}
	}
	b, err := json.Marshal(p)
	if err != nil {
		return errInternal
	}
	return m.st.SetOutgoingMessage(Name, id, b)
}

// otidFor makes the offline threading ID of a send. Its logic is the
// connector's getOTID (mautrix-meta v0.2610.0, pkg/connector/
// handlematrix.go), which the helper may not import: the caller's key when
// it is a positive integer, else a new epoch ID.
func otidFor(key string) int64 {
	if key != "" {
		otid, err := strconv.ParseInt(key, 10, 64)
		if err == nil && otid > 0 {
			return otid
		}
	}
	return methods.GenerateEpochID()
}

// newMessageID makes an encrypted message's ID as whatsmeow's
// GenerateMessageID does for a Messenger client.
func newMessageID() watypes.MessageID {
	return watypes.MessageID(strconv.FormatInt(whatsmeow.GenerateFacebookMessageID(), 10))
}

// Send keeps the message in the outbox under its ID (its offline threading
// ID in an unencrypted chat, its whatsmeow message ID in an encrypted one),
// returns that ID at once, and sends it in the background. A send repeated
// with the caller's key (o.ClientID) of one kept and not failed gives that
// send's ID and sends nothing more. One that failed after its upload or its
// text went is the same send, and goes on with what it has; any other
// failed send is done, and the key makes a new send.
func (m *Messenger) Send(ctx context.Context, o network.Outgoing) (string, error) {
	if o.ClientID != "" {
		if chat, ok := m.st.ChatFor(Name, o.ChatID); ok {
			prev, held, err := m.keyed(o.ClientID, chat)
			if err != nil {
				return "", err
			}
			if held && prev.State == store.OutFailed {
				return m.resume(prev)
			}
			if held {
				return prev.ID, nil
			}
		}
	}
	if err := m.writeGate(); err != nil {
		return "", err
	}
	if o.Text == "" && o.File == "" {
		return "", errBadRequest
	}
	chat, err := m.writableChat(o.ChatID)
	if err != nil {
		return "", err
	}
	// An encrypted group, or a one-to-one with an encrypted side, sends
	// through whatsmeow; any other chat on its unencrypted thread (a
	// one-to-one with only its older thread is "<FBID>@msgr" too).
	encrypted := m.hasEncryptedSide(chat)
	if !encrypted {
		if _, ok := m.threadOfChat(chat); !ok {
			return "", errNotOnNetwork
		}
	}
	req := sendRequest{Text: o.Text, Kind: "text"}
	if !strings.HasPrefix(o.ReplyTo, "beeper:") {
		req.ReplyTo = o.ReplyTo // a line only Beeper's copy holds has no ID Meta knows
	}
	if o.File != "" {
		name := cmp.Or(o.FileName, filepath.Base(o.File))
		kind, mimetype, err := fileKind(o.File, name)
		if err != nil {
			return "", err
		}
		req.File, req.FileName, req.Kind, req.Mimetype = o.File, name, kind, mimetype
	}
	twoMessages := o.File != "" && o.Text != ""
	var id string
	if encrypted {
		id = string(newMessageID())
		if twoMessages {
			req.FileID = string(newMessageID())
		}
	} else {
		otid := otidFor(o.ClientID)
		if _, taken, err := m.st.Outgoing(Name, otidPrefix+itoa(otid)); err != nil {
			return "", err
		} else if taken {
			otid = methods.GenerateEpochID() // the key's number is a done send's
		}
		req.OTID = itoa(otid)
		id = otidPrefix + req.OTID
		if twoMessages {
			req.FileOTID = itoa(methods.GenerateEpochID())
		}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", errInternal
	}
	if err := m.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: o.ClientID,
		MadeMs: m.clock.Now().UnixMilli(), Request: body}); err != nil {
		if raced, held, rerr := m.keyed(o.ClientID, chat); rerr == nil && held {
			return raced.ID, nil // another call with this key kept it first, and sends it
		}
		return "", err
	}
	m.sends.outbox.Enqueue(id)
	return id, nil
}

// SendFor is the ID of the send kept under the caller's key (a failed one
// included), or "" for none.
func (m *Messenger) SendFor(clientID string) (string, error) {
	o, ok, err := m.st.OutgoingClient(Name, clientID)
	if err != nil || !ok {
		return "", err
	}
	return o.ID, nil
}

// keyed is the send the caller's key still holds: one sending, sent or
// unknown, or one that failed after its upload or its text went. Any other
// failed send frees the key. A key naming a send to another chat is a bad
// request.
func (m *Messenger) keyed(client, chat string) (store.Outgoing, bool, error) {
	o, ok, err := m.st.OutgoingClient(Name, client)
	if err != nil || !ok {
		return store.Outgoing{}, false, err
	}
	if p := progressOf(o); o.State == store.OutFailed && !p.uploaded() && !p.TextSent {
		return store.Outgoing{}, false, nil
	}
	if o.ChatID != chat || o.Kind != outMessage {
		return store.Outgoing{}, false, errBadRequest
	}
	return o, true, nil
}

// resume sends again what is left of a failed send that holds its upload
// or whose text went: under the same row and ID, reusing the upload, the
// file alone after its text. Once in line it is not put in line again.
func (m *Messenger) resume(o store.Outgoing) (string, error) {
	if err := m.writeGate(); err != nil {
		return "", err
	}
	m.sends.resume.Lock()
	cur, ok, err := m.st.Outgoing(Name, o.ID)
	if err != nil || !ok {
		m.sends.resume.Unlock()
		return "", cmp.Or[error](err, errNotFound)
	}
	if cur.State != store.OutFailed {
		m.sends.resume.Unlock()
		return cur.ID, nil // already going again
	}
	p := progressOf(cur)
	p.Asking = false
	if err := m.keepProgress(cur.ID, p); err != nil {
		m.sends.resume.Unlock()
		return "", err
	}
	if err := m.st.SetOutgoingState(Name, cur.ID, store.OutSending, ""); err != nil {
		m.sends.resume.Unlock()
		return "", err
	}
	m.sends.resume.Unlock()
	m.sends.outbox.Enqueue(cur.ID)
	return cur.ID, nil
}

// SendState is what became of a send: sending, sent, failed with its kind,
// or unknown (cut off by a restart, or never held).
func (m *Messenger) SendState(messageID string) network.SendState {
	o, ok, err := m.st.Outgoing(Name, messageID)
	if err != nil || !ok {
		return network.SendState{State: network.SendUnknown}
	}
	switch o.State {
	case store.OutSent:
		return network.SendState{State: network.SendSent}
	case store.OutFailed:
		return network.SendState{State: network.SendFailed, Kind: wire.Kind(o.Fail)}
	case store.OutUnknown:
		return network.SendState{State: network.SendUnknown}
	}
	return network.SendState{State: network.SendSending}
}

// forgetCutOff settles every write still sending when the helper stopped.
// One that may have reached Meta (a reaction, or a message being asked) is
// unknown and never sent again. A send that never asked a message (its
// turn never came, or only its upload went) failed: refused, or unclear
// when its text went; its key sends it again with what it has.
func (m *Messenger) forgetCutOff() {
	rows, err := m.st.OutgoingSending(Name)
	if err != nil {
		return
	}
	for _, o := range rows {
		p := progressOf(o)
		switch {
		case o.Kind != outMessage || p.Asking:
			_ = m.st.SetOutgoingState(Name, o.ID, store.OutUnknown, "")
		case p.TextSent:
			_ = m.st.SetOutgoingState(Name, o.ID, store.OutFailed, string(wire.Unclear))
		default:
			_ = m.st.SetOutgoingState(Name, o.ID, store.OutFailed, string(wire.Refused))
		}
	}
}

// beginSends starts the goroutine that sends what is in line (the
// outbox's Begin): counted in the network's work, under the network's
// context; false once it has begun stopping.
func (m *Messenger) beginSends() (context.Context, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ok := m.started && !m.stopped
	if ok {
		m.work.Add(1)
	}
	return m.ctx, ok
}

// failStopped fails a send whose turn came after the helper began
// stopping: nothing of it was asked now, so refused, or unclear when its
// text went in an earlier try (as forgetCutOff has it).
func (m *Messenger) failStopped(id string) {
	kind := wire.Refused
	if o, ok, err := m.st.Outgoing(Name, id); err == nil && ok && progressOf(o).TextSent {
		kind = wire.Unclear
	}
	m.fail(id, kind)
}

// fail marks a write still sending failed with kind; one already settled
// (by its echo) stays as it is.
func (m *Messenger) fail(id string, kind wire.Kind) {
	if o, ok, err := m.st.Outgoing(Name, id); err == nil && ok && o.State != store.OutSending {
		return
	}
	_ = m.st.SetOutgoingState(Name, id, store.OutFailed, string(kind))
}

// settle marks a write sent, keeping the ID of the line it became.
func (m *Messenger) settle(id, lineID string) {
	if lineID != "" {
		_ = m.st.SetOutgoingLine(Name, id, lineID)
	}
	_ = m.st.SetOutgoingState(Name, id, store.OutSent, "")
}

// deliver sends one send, if it is still sending: its file uploaded first
// (unless an earlier try uploaded it), then its text, then the file, each
// a message of its own, as Instagram's writes send them; on the chat's
// unencrypted thread when the send was made with an offline threading ID,
// else through the encrypted side. The text carries the reply. Each
// request has its own budget (writeWait). A message whose answer may have
// reached Meta leaves the send sending.
func (m *Messenger) deliver(id string) {
	o, ok, err := m.st.Outgoing(Name, id)
	if err != nil || !ok || o.State != store.OutSending {
		return
	}
	var req sendRequest
	if json.Unmarshal(o.Request, &req) != nil {
		m.fail(id, wire.Internal)
		return
	}
	gen, w := m.writer()
	if w == nil {
		m.fail(id, wire.Refused)
		return
	}
	s := &sender{m: m, gen: gen, w: w, o: o, req: req, p: progressOf(o)}
	if s.encrypted() {
		if s.jid, err = watypes.ParseJID(o.ChatID); err != nil {
			m.fail(id, wire.Internal)
			return
		}
	} else if s.thread, ok = m.threadOfChat(o.ChatID); !ok {
		m.fail(id, wire.Internal)
		return
	}
	reply, _ := s.replyLine()
	var data []byte
	if req.File != "" {
		data, err = readSendFile(req)
		if err != nil && !s.p.uploaded() {
			m.fail(id, wire.KindOf(err))
			return
		}
		if !s.p.uploaded() {
			if err := s.upload(data); err != nil {
				m.fail(id, wire.Refused) // an upload is not a message: nothing was sent
				return
			}
		}
	}
	if req.Text != "" && !s.p.TextSent {
		lineID, ms, out := s.message(false, reply)
		switch out {
		case sentMaybe:
			m.unsettled(id, req) // it may have gone
			return
		case sentRefused:
			m.fail(id, wire.Refused)
			return
		}
		m.storeSent(o.ChatID, lineID, "text", req.Text, reply.ID, nil, ms)
		if req.File == "" {
			m.settle(id, lineID)
			m.changed()
			return
		}
		s.p.TextSent, s.p.TextLineID, s.p.Asking = true, lineID, false
		if m.keepProgress(id, s.p) != nil {
			m.forget(id) // the text went but that is not kept: unknown, never sent again
			return
		}
		m.changed()
	}
	if s.p.TextSent {
		reply = store.Line{} // the reply went with the text
	}
	lineID, ms, out := s.message(true, reply)
	switch out {
	case sentMaybe:
		m.unsettled(id, req) // it may have gone
		return
	case sentRefused:
		if s.p.TextSent {
			m.fail(id, wire.Unclear) // its text went, the file did not: the key's next send is the file alone
		} else {
			m.fail(id, wire.Refused)
		}
		return
	}
	m.storeSent(o.ChatID, lineID, req.Kind, "", reply.ID, data, ms)
	m.settle(id, cmp.Or(s.p.TextLineID, lineID)) // the send's line is its text's, as its ID is
	m.changed()
}

// unsettled ends a delivery whose message may have reached Meta. A send
// of one message stays sending: its echo settles it. A text with a file
// is unknown at once, never sent again: nothing is in flight, and no echo
// settles it (the text's says nothing of the file), so Pllum goes by the
// chat's lines, as for any unknown send.
func (m *Messenger) unsettled(id string, req sendRequest) {
	if req.File != "" && req.Text != "" {
		m.forget(id)
	}
}

// forget makes a send still sending unknown: it may have reached Meta, and
// is never sent again.
func (m *Messenger) forget(id string) {
	if o, ok, err := m.st.Outgoing(Name, id); err == nil && ok && o.State == store.OutSending {
		_ = m.st.SetOutgoingState(Name, id, store.OutUnknown, "")
	}
}

// sender is one delivery of a send.
type sender struct {
	m      *Messenger
	gen    int
	w      msgrWriter
	o      store.Outgoing
	req    sendRequest
	p      sendProgress
	thread int64       // the unencrypted thread it goes on, or
	jid    watypes.JID // the encrypted chat it goes to
}

// encrypted reports whether the send goes through the encrypted side.
func (s *sender) encrypted() bool { return s.req.OTID == "" }

// sendOutcome is what became of one message asked.
type sendOutcome int

const (
	sentOK      sendOutcome = iota // Meta took it
	sentMaybe                      // it may have reached Meta: never asked again
	sentRefused                    // nothing reached Meta, or Meta said no
)

// replyLine is the line the send replies to, when it is held and on the
// side the send goes to.
func (s *sender) replyLine() (store.Line, bool) {
	if s.req.ReplyTo == "" || isMetaLine(s.req.ReplyTo) == s.encrypted() {
		return store.Line{}, false
	}
	l, ok, err := s.m.st.Line(Name, s.o.ChatID, s.req.ReplyTo)
	if err != nil || !ok {
		return store.Line{}, false
	}
	return l, true
}

// asking keeps in the row that a message is being asked. It runs inside
// the request, after the pace let it go, just before the client is asked,
// so a stop while the send waits its turn leaves it resendable.
func (s *sender) asking() error {
	s.p.Asking = true
	if s.m.keepProgress(s.o.ID, s.p) != nil {
		s.p.Asking = false
		return errNotAsked // nothing is asked: the message is refused
	}
	return nil
}

// errNotAsked is a message that was not asked because its "being asked"
// could not be kept: refused, so the send is never left sending.
var errNotAsked = &wire.Error{Kind: wire.Refused}

// message asks Meta one message of the send, the file's or the text's,
// keeping in the row, as its request leaves, that it is being asked (so a
// stop during it is unknown at the next start, never sent again).
func (s *sender) message(file bool, reply store.Line) (lineID string, ms int64, out sendOutcome) {
	if s.encrypted() {
		id := s.o.ID
		if file && s.req.FileID != "" {
			id = s.req.FileID
		}
		app, err := s.content(file)
		if err != nil {
			return "", 0, sentRefused
		}
		md := &waMsgApplication.MessageApplication_Metadata{}
		if reply.ID != "" {
			md.QuotedMessage = &waMsgApplication.MessageApplication_Metadata_QuotedMessage{
				StanzaID: proto.String(reply.ID), Participant: proto.String(s.m.senderJID(reply).String())}
		}
		return s.sendFB(id, app, md)
	}
	otid := s.req.OTID
	if file && s.req.FileOTID != "" {
		otid = s.req.FileOTID
	}
	n, err := strconv.ParseInt(otid, 10, 64)
	if err != nil {
		return "", 0, sentRefused
	}
	task := &socket.SendMessageTask{ThreadId: s.thread, Otid: n, Source: table.MESSENGER_INBOX_IN_THREAD,
		InitiatingSource: table.FACEBOOK_INBOX, SendType: table.TEXT, SyncGroup: 1}
	if file {
		task.SendType, task.AttachmentFBIds = table.MEDIA, []int64{s.p.Attachment}
	} else {
		task.Text = s.req.Text
	}
	if reply.ID != "" {
		who, _ := strconv.ParseInt(reply.SenderID, 10, 64)
		task.ReplyMetaData = &socket.ReplyMetaData{ReplyMessageId: reply.ID, ReplySourceType: 1, ReplyType: 0, ReplySender: who}
	}
	return s.sendTask(task, otid)
}

// content is an encrypted message's content: the text, or the uploaded
// file as a photo, video or document (no caption: its text went before it).
func (s *sender) content(file bool) (*waConsumerApplication.ConsumerApplication, error) {
	if !file {
		return consumerApp(&waConsumerApplication.ConsumerApplication_Content{
			Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{MessageText: &waCommon.MessageText{Text: proto.String(s.req.Text)}}}), nil
	}
	var t waMediaTransport.WAMediaTransport
	if err := proto.Unmarshal(s.p.Upload, &t); err != nil {
		return nil, err
	}
	var c waConsumerApplication.ConsumerApplication_Content
	var err error
	width, height := proto.Uint32(uint32(s.p.Width)), proto.Uint32(uint32(s.p.Height))
	switch s.req.Kind {
	case "photo":
		img := &waConsumerApplication.ConsumerApplication_ImageMessage{Caption: &waCommon.MessageText{}}
		err = img.Set(&waMediaTransport.ImageTransport{
			Integral:  &waMediaTransport.ImageTransport_Integral{Transport: &t},
			Ancillary: &waMediaTransport.ImageTransport_Ancillary{Height: height, Width: width}})
		c.Content = &waConsumerApplication.ConsumerApplication_Content_ImageMessage{ImageMessage: img}
	case "video":
		video := &waConsumerApplication.ConsumerApplication_VideoMessage{Caption: &waCommon.MessageText{}}
		err = video.Set(&waMediaTransport.VideoTransport{
			Integral:  &waMediaTransport.VideoTransport_Integral{Transport: &t},
			Ancillary: &waMediaTransport.VideoTransport_Ancillary{Height: height, Width: width}})
		c.Content = &waConsumerApplication.ConsumerApplication_Content_VideoMessage{VideoMessage: video}
	default:
		doc := &waConsumerApplication.ConsumerApplication_DocumentMessage{FileName: proto.String(s.req.FileName)}
		err = doc.Set(&waMediaTransport.DocumentTransport{
			Integral:  &waMediaTransport.DocumentTransport_Integral{Transport: &t},
			Ancillary: &waMediaTransport.DocumentTransport_Ancillary{}})
		c.Content = &waConsumerApplication.ConsumerApplication_Content_DocumentMessage{DocumentMessage: doc}
	}
	if err != nil {
		return nil, err
	}
	return consumerApp(&c), nil
}

// sendFB asks whatsmeow to send one encrypted message under id, which is
// then its line's ID.
func (s *sender) sendFB(id string, app *waConsumerApplication.ConsumerApplication, md *waMsgApplication.MessageApplication_Metadata) (string, int64, sendOutcome) {
	ctx, cancel := s.m.writeCtx()
	defer cancel()
	var resp whatsmeow.SendResponse
	ran, raw, err := s.m.ask(ctx, s.gen, func(ctx context.Context) (err error) {
		if err := s.asking(); err != nil {
			return err // nothing asked
		}
		resp, err = s.w.SendFBMessage(ctx, s.jid, app, md, whatsmeow.SendRequestExtra{ID: watypes.MessageID(id)})
		return err
	})
	if err != nil {
		if ran && unsure(raw) {
			return "", 0, sentMaybe
		}
		return "", 0, sentRefused
	}
	ms := resp.Timestamp.UnixMilli()
	if resp.Timestamp.IsZero() || ms <= 0 {
		ms = s.m.clock.Now().UnixMilli()
	}
	return id, ms, sentOK
}

// sendTask asks Meta one SendMessageTask and reads its answer: the message
// ID its optimistic message became (sent), a failure (refused), or neither
// (its echo settles it).
func (s *sender) sendTask(task *socket.SendMessageTask, otid string) (string, int64, sendOutcome) {
	ctx, cancel := s.m.writeCtx()
	defer cancel()
	resp, ran, raw, err := s.m.executeThen(ctx, s.gen, s.w, task, s.asking)
	if err != nil {
		if ran && unsure(raw) {
			return "", 0, sentMaybe
		}
		return "", 0, sentRefused
	}
	lineID, refused := sentOf(resp, otid)
	switch {
	case lineID != "":
		return lineID, s.m.sentAt(lineID), sentOK
	case refused:
		return "", 0, sentRefused
	}
	return "", 0, sentMaybe
}

// sentOf reads Meta's answer to a send: the message ID its optimistic
// message became, or refused when Meta marked it failed.
func sentOf(resp *table.LSTable, otid string) (lineID string, refused bool) {
	if resp == nil {
		return "", false
	}
	for _, r := range resp.LSReplaceOptimsiticMessage {
		if r.OfflineThreadingId == otid && r.MessageId != "" {
			return r.MessageId, false
		}
	}
	for _, f := range resp.LSMarkOptimisticMessageFailed {
		if f.OTID == otid {
			return "", true
		}
	}
	for _, f := range resp.LSHandleFailedTask {
		if f.OTID == otid {
			return "", true
		}
	}
	return "", false
}

// sentAt is a sent message's time: the one its Meta message ID carries,
// when that is within an hour of now (as the connector checks it), else
// now.
func (m *Messenger) sentAt(lineID string) int64 {
	now := m.clock.Now()
	if ms, err := methods.ParseMessageID(lineID); err == nil {
		if t := time.UnixMilli(ms); t.After(now.Add(-time.Hour)) && t.Before(now.Add(time.Hour)) {
			return ms
		}
	}
	return now.UnixMilli()
}

// consumerApp wraps a consumer application's content.
func consumerApp(c *waConsumerApplication.ConsumerApplication_Content) *waConsumerApplication.ConsumerApplication {
	return &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: c}}}
}

// storeSent stores the line a message Meta took became, his, under its ID.
// A photo he sent is kept so it opens at once.
func (m *Messenger) storeSent(chat, lineID, kind, text, reply string, photo []byte, ms int64) {
	sender, _ := m.self()
	shown := false
	line := store.Message{ID: lineID, ChatID: chat, SenderID: sender, FromMe: true, TimeMs: ms,
		Text: text, Kind: kind, Hidden: &shown, ReplyTo: reply, Source: store.SourceLive}
	switch kind {
	case "photo":
		line.Media = []store.Media{{Kind: "photo", HasFile: true}}
	case "video", "file":
		line.Media = []store.Media{{Kind: kind}}
	}
	if _, err := m.st.PutMessage(Name, line); err == nil && kind == "photo" && photo != nil {
		m.media.Keep(Name, chat, lineID, photo)
	}
}

// echoed settles the unencrypted send whose offline threading ID his
// message's echo (or Meta's replacement of it) carries: whatever became of
// it here (sending, unknown, even failed), Meta has it, as the line lineID.
func (m *Messenger) echoed(otid, lineID string) {
	if otid == "" || lineID == "" {
		return
	}
	o, ok, err := m.st.Outgoing(Name, otidPrefix+otid)
	if err != nil || !ok || o.Kind != outMessage || o.State == store.OutSent || withFile(o) {
		return
	}
	m.settle(o.ID, lineID)
}

// echoedEncrypted settles the encrypted send a line of his carries the ID
// of (his own message coming back, from another device or his restored
// history).
func (m *Messenger) echoedEncrypted(id string) {
	if id == "" || strings.Contains(id, ":") {
		return
	}
	o, ok, err := m.st.Outgoing(Name, id)
	if err != nil || !ok || o.Kind != outMessage || o.State == store.OutSent || withFile(o) {
		return
	}
	m.settle(o.ID, id)
}

// withFile reports whether a send is a text with a file: its text's echo
// says nothing of its file, so only its delivery settles it.
func withFile(o store.Outgoing) bool {
	var req sendRequest
	return json.Unmarshal(o.Request, &req) == nil && req.File != "" && req.Text != ""
}
