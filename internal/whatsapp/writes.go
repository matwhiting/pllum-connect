package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	_ "image/jpeg" // a photo's size
	_ "image/png"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// waWriter is every write the adapter makes to WhatsApp, with the two calls
// that go with them (making a message ID, downloading a file), and nothing
// else: a message, reply, file or reaction (SendMessage after Upload), a read
// receipt when he reads (MarkRead), a mute or a "marked unread" (SendAppState
// with appstate.BuildMute or BuildMarkChatAsRead), a request that his phone
// upload a photo again (SendMediaRetryReceipt), and unlinking on his
// Disconnect (Logout). Never presence, typing, edits, deletes, archive, pins,
// groups, profile, blocking, Status or Channels. *whatsmeow.Client satisfies
// it.
type waWriter interface {
	GenerateMessageID() types.MessageID
	SendMessage(ctx context.Context, to types.JID, message *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)
	Upload(ctx context.Context, plaintext []byte, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
	MarkRead(ctx context.Context, ids []types.MessageID, timestamp time.Time, chat, sender types.JID, receiptTypeExtra ...types.ReceiptType) error
	SendAppState(ctx context.Context, patch appstate.PatchInfo) error
	Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error)
	SendMediaRetryReceipt(ctx context.Context, message *types.MessageInfo, mediaKey []byte) error
	Logout(ctx context.Context) error
}

var _ waWriter = (*whatsmeow.Client)(nil)

const (
	// sendWait is how long a send made while offline waits for the
	// connection before it fails.
	sendWait = 60 * time.Second
	// The largest file sent as a photo or video; anything larger, or of
	// another type, goes as a document up to fileLimit.
	photoLimit = 16 << 20
	videoLimit = 16 << 20
	fileLimit  = 100 << 20
)

// What the outbox sends.
const (
	outMessage  = "message"
	outReaction = "reaction"
)

var (
	errBadRequest   = &wire.Error{Kind: wire.BadRequest}
	errNotConnected = &wire.Error{Kind: wire.NotConnected}
	errOffline      = &wire.Error{Kind: wire.Offline}
	errNotFound     = &wire.Error{Kind: wire.NotFound}
	errNotOnNetwork = &wire.Error{Kind: wire.NotOnNetwork}
	errRefused      = &wire.Error{Kind: wire.Refused}
	errUnclear      = &wire.Error{Kind: wire.Unclear}
	errTooLarge     = &wire.Error{Kind: wire.TooLarge}
)

// sendRequest is what the outbox keeps of a write, enough to build it again
// after a restart.
type sendRequest struct {
	Text     string `json:"text,omitempty"`
	ReplyTo  string `json:"replyTo,omitempty"`
	File     string `json:"file,omitempty"`
	FileName string `json:"fileName,omitempty"`
	Kind     string `json:"kind,omitempty"` // the line's kind: text, photo, video or file
	Mimetype string `json:"mimetype,omitempty"`
	Target   string `json:"target,omitempty"` // the line a reaction is to
	Emoji    string `json:"emoji,omitempty"`
}

// sendJob is one send waiting for its turn.
type sendJob struct {
	id       string
	madeMs   int64
	deadline time.Time // zero: it waits for a connection however long it takes
	resend   bool      // tried before (or in an earlier run) and never acknowledged
}

// writeGate is nil when writes can go now; offline when he is signed in but
// not connected; else notConnected.
func (w *WhatsApp) writeGate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch w.state {
	case network.CatchingUp, network.Live:
		if w.cli != nil {
			return nil
		}
	case network.Offline, network.Connecting:
		return errOffline
	}
	return errNotConnected
}

// writableChat is the held chat a write names, as its ID and JID.
func (w *WhatsApp) writableChat(chatID string) (string, types.JID, error) {
	if strings.HasPrefix(chatID, "unplaced:") {
		return "", types.EmptyJID, errNotOnNetwork
	}
	held, ok := w.st.ChatFor(Name, chatID)
	if !ok {
		return "", types.EmptyJID, errNotFound
	}
	jid, err := types.ParseJID(held)
	if err != nil || !allowedChat(jid) {
		return "", types.EmptyJID, errNotOnNetwork
	}
	return held, norm(jid), nil
}

// serverRefused reports whether WhatsApp's server itself turned a message
// down: it answered the send with an error, said the recipient is not on
// WhatsApp, or (for a group send, which fetches the members first) said he is
// not in the group or that it does not exist. Only these make a send failed and refused; every other failure
// (the socket, a timeout, an upload, the helper stopping) may have left the
// message with WhatsApp or never let it go, and the send goes again under
// its own ID on the next connection.
func serverRefused(err error) bool {
	return errors.Is(err, whatsmeow.ErrServerReturnedError) || errors.Is(err, whatsmeow.ErrRecipientNotOnWhatsApp) ||
		errors.Is(err, whatsmeow.ErrNotInGroup) || errors.Is(err, whatsmeow.ErrGroupNotFound)
}

// unclearSend reports whether a write that is not a send (a receipt, an app
// state patch, unlinking) failed in a way that leaves it unknown whether
// WhatsApp has it: no answer in time, the connection cut off while waiting,
// or the helper stopping.
func unclearSend(err error) bool {
	if errors.Is(err, whatsmeow.ErrMessageTimedOut) || errors.Is(err, whatsmeow.ErrIQTimedOut) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var disc *whatsmeow.DisconnectedError
	return errors.As(err, &disc)
}

// writeErr is the kind a failed write is sent as.
func writeErr(err error) error {
	if unclearSend(err) {
		return errUnclear
	}
	return errRefused
}

// Send keeps the message in the outbox under an ID made for it, returns the
// ID at once, and sends it in the background: when connected, in the order
// sends were made; while offline, once the connection is back within a
// minute, else it fails with offline. A send repeated with the caller's key
// (m.ClientID) of one kept as sending or sent gives that send's ID and sends
// nothing more: the caller lost the first answer. One whose send failed is
// done, so the key makes a new send.
func (w *WhatsApp) Send(ctx context.Context, m network.Outgoing) (string, error) {
	// a repeat is answered before anything else, so one asked again while
	// the connection is down still gets the send it made
	if m.ClientID != "" {
		if chat, ok := w.st.ChatFor(Name, m.ChatID); ok {
			if id, held, err := w.sentFor(m.ClientID, chat); err != nil || held {
				return id, err
			}
		}
	}
	if err := w.writeGate(); err != nil && err != errOffline {
		return "", err
	}
	if m.Text == "" && m.File == "" {
		return "", errBadRequest
	}
	chat, _, err := w.writableChat(m.ChatID)
	if err != nil {
		return "", err
	}
	req := sendRequest{Text: m.Text, ReplyTo: m.ReplyTo, Kind: "text"}
	if m.File != "" {
		name := m.FileName
		if name == "" {
			name = filepath.Base(m.File)
		}
		kind, mimetype, err := fileKind(m.File, name)
		if err != nil {
			return "", err
		}
		req.File, req.FileName, req.Kind, req.Mimetype = m.File, name, kind, mimetype
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", errInternal
	}
	cli := w.client()
	if cli == nil {
		return "", errNotConnected
	}
	now := w.clock.Now()
	id := cli.GenerateMessageID()
	if err := w.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: m.ClientID,
		MadeMs: now.UnixMilli(), Request: body}); err != nil {
		if raced, held, rerr := w.sentFor(m.ClientID, chat); rerr == nil && held {
			return raced, nil // another call with this key kept it first, and sends it
		}
		return "", err
	}
	w.enqueue(sendJob{id: id, madeMs: now.UnixMilli(), deadline: now.Add(sendWait)})
	return id, nil
}

// SendFor is the message ID of the send kept under the caller's key (a
// failed one included), or "" for none. A store that can't be read is an
// error, so the caller hears "can't say", never "none".
func (w *WhatsApp) SendFor(clientID string) (string, error) {
	o, ok, err := w.st.OutgoingClient(Name, clientID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return o.ID, nil
}

// sentFor is the send already kept under the caller's key, when one is
// sending or sent. A key naming a send to another chat is a bad request.
func (w *WhatsApp) sentFor(client, chat string) (string, bool, error) {
	o, ok, err := w.st.OutgoingClient(Name, client)
	if err != nil || !ok || o.State == store.OutFailed {
		return "", false, err
	}
	if o.ChatID != chat || o.Kind != outMessage {
		return "", false, errBadRequest
	}
	return o.ID, true, nil
}

// fileKind is how a file goes: a JPEG or PNG photo or an MP4 video up to
// 16 MB, else a document up to 100 MB.
func fileKind(path, name string) (kind, mimetype string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", errNotFound
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		return "", "", errBadRequest
	}
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	sniffed := http.DetectContentType(head[:n])
	if isQuickTime(head[:n]) {
		sniffed = "video/quicktime"
	}
	size := fi.Size()
	switch {
	case (sniffed == "image/jpeg" || sniffed == "image/png") && size <= photoLimit:
		return "photo", sniffed, nil
	case (sniffed == "video/mp4" || sniffed == "video/quicktime") && size <= videoLimit:
		return "video", sniffed, nil
	case size <= fileLimit:
		return "file", documentType(name, path, sniffed), nil
	}
	return "", "", errTooLarge
}

// isQuickTime reports whether head starts a QuickTime movie: its first box
// is ftyp with the brand "qt  " (which Go's sniffing doesn't name).
func isQuickTime(head []byte) bool {
	return len(head) >= 12 && string(head[4:8]) == "ftyp" && string(head[8:12]) == "qt  "
}

// documentType is a document's MIME type: its name's, else its content's.
func documentType(name, path, sniffed string) string {
	for _, ext := range []string{filepath.Ext(name), filepath.Ext(path)} {
		if ext == "" {
			continue
		}
		if t, _, err := mime.ParseMediaType(mime.TypeByExtension(ext)); err == nil && t != "" {
			return t
		}
	}
	if t, _, err := mime.ParseMediaType(sniffed); err == nil && t != "" {
		return t
	}
	return "application/octet-stream"
}

// enqueue puts a send in line and wakes the sender.
func (w *WhatsApp) enqueue(j sendJob) {
	w.smu.Lock()
	w.queue = append(w.queue, j)
	w.smu.Unlock()
	w.kickSend()
}

func (w *WhatsApp) kickSend() {
	select {
	case w.sendKick <- struct{}{}:
	default:
	}
}

// requeueSending puts every send still sending back in line, oldest first,
// except those already in line or being tried now. It runs at start and
// each time the state enters catchingUp or live.
func (w *WhatsApp) requeueSending() {
	rows, err := w.st.OutgoingSending(Name)
	if err != nil {
		w.requeue.Store(true) // tried again on the next wake-up
		return
	}
	w.smu.Lock()
	defer w.smu.Unlock()
	for _, o := range rows {
		w.queueAgain(o)
	}
}

// queueAgain puts a send back in line, in made order, unless it is already
// in line or being tried; w.smu is held.
func (w *WhatsApp) queueAgain(o store.Outgoing) {
	if w.inflight[o.ID] {
		return
	}
	for _, j := range w.queue {
		if j.id == o.ID {
			return
		}
	}
	w.queue = append(w.queue, sendJob{id: o.ID, madeMs: o.MadeMs, resend: true})
	sort.SliceStable(w.queue, func(a, b int) bool { return w.queue[a].madeMs < w.queue[b].madeMs })
}

// sendLoop sends one waiting send at a time, in order, while connected, and
// fails a send that has waited its minute for a connection.
func (w *WhatsApp) sendLoop(ctx context.Context) {
	defer w.worker.Done()
	var stopTimer func() bool
	defer func() {
		if stopTimer != nil {
			stopTimer()
		}
	}()
	for ctx.Err() == nil {
		if w.requeue.Swap(false) {
			w.requeueSending()
		}
		if j, ok := w.nextJob(); ok {
			w.deliver(ctx, j)
			continue
		}
		next := w.expire()
		if stopTimer != nil {
			stopTimer()
			stopTimer = nil
		}
		if !next.IsZero() {
			stopTimer = w.clock.AfterFunc(next.Sub(w.clock.Now()), w.kickSend)
		}
		select {
		case <-ctx.Done():
		case <-w.sendKick:
		}
	}
}

// nextJob takes the oldest waiting send, when the connection is up.
func (w *WhatsApp) nextJob() (sendJob, bool) {
	if !w.online() {
		return sendJob{}, false
	}
	w.smu.Lock()
	defer w.smu.Unlock()
	if len(w.queue) == 0 {
		return sendJob{}, false
	}
	j := w.queue[0]
	w.queue = w.queue[1:]
	return j, true
}

// expire fails each waiting send whose minute is up, and gives the moment
// the next one's is (zero for none). Only a send never tried has a minute.
func (w *WhatsApp) expire() time.Time {
	now := w.clock.Now()
	var next time.Time
	var gone []string
	w.smu.Lock()
	keep := w.queue[:0]
	for _, j := range w.queue {
		switch {
		case j.deadline.IsZero():
			keep = append(keep, j)
		case !now.Before(j.deadline):
			gone = append(gone, j.id)
		default:
			keep = append(keep, j)
			if next.IsZero() || j.deadline.Before(next) {
				next = j.deadline
			}
		}
	}
	w.queue = keep
	w.smu.Unlock()
	for _, id := range gone {
		w.fail(id, wire.Offline)
	}
	return next
}

// fail marks a send failed with kind.
func (w *WhatsApp) fail(id string, kind wire.Kind) {
	_ = w.st.SetOutgoingState(Name, id, store.OutFailed, string(kind))
}

// deliver sends one waiting send, if it is still sending.
func (w *WhatsApp) deliver(ctx context.Context, j sendJob) {
	o, ok, err := w.st.Outgoing(Name, j.id)
	if err != nil || !ok || o.State != store.OutSending {
		return
	}
	_ = w.attempt(ctx, o, j.resend)
}

// attempt tries one send, never two of the same at once. A send left
// sending waits for the next time the state enters catchingUp or live; if
// that happened while this try ran (its requeue skipped the send being
// tried), it goes back in line now.
func (w *WhatsApp) attempt(ctx context.Context, o store.Outgoing, resend bool) error {
	w.smu.Lock()
	if w.inflight[o.ID] {
		w.smu.Unlock()
		return errUnclear
	}
	w.inflight[o.ID] = true
	w.smu.Unlock()
	gen := w.onlineGen.Load()
	err := w.sendOut(ctx, o, resend)
	w.smu.Lock()
	delete(w.inflight, o.ID)
	w.smu.Unlock()
	if ctx.Err() == nil && w.onlineGen.Load() != gen {
		if cur, ok, rerr := w.st.Outgoing(Name, o.ID); rerr == nil && ok && cur.State == store.OutSending {
			w.smu.Lock()
			w.queueAgain(cur)
			w.smu.Unlock()
			w.kickSend()
		}
	}
	return err
}

// sendOut sends one outbox row under its own ID and settles it:
//   - acknowledged: the line (or reaction) is stored, then the row is sent;
//   - WhatsApp's server turned it down (serverRefused): failed, refused. For
//     a row that may already have gone on an earlier try (resend, and its
//     message built), unclear instead;
//   - its file is gone or has grown past the limit before it was ever sent:
//     failed with notFound or tooLarge (nothing went, and it never can);
//   - anything else (no client, an upload, the socket, a timeout, the
//     helper stopping, a store write): it stays sending, and goes again
//     under the same ID the next time the state enters catchingUp or live,
//     or at the next start.
//
// The built message is kept before it is sent, so a resend sends exactly it.
func (w *WhatsApp) sendOut(ctx context.Context, o store.Outgoing, resend bool) error {
	cli := w.client()
	if cli == nil {
		return errUnclear
	}
	mayHaveGone := resend && len(o.Message) > 0
	var req sendRequest
	if err := json.Unmarshal(o.Request, &req); err != nil {
		w.fail(o.ID, wire.Internal)
		return errInternal
	}
	jid, err := types.ParseJID(o.ChatID)
	if err != nil {
		w.fail(o.ID, wire.NotOnNetwork)
		return errNotOnNetwork
	}
	var m *waE2E.Message
	var file []byte // the photo uploaded, kept so his own photo opens at once
	if len(o.Message) > 0 {
		m = &waE2E.Message{}
		if err := proto.Unmarshal(o.Message, m); err != nil {
			w.fail(o.ID, wire.Internal)
			return errInternal
		}
	} else {
		if m, file, err = w.build(ctx, cli, o.ChatID, req); err != nil {
			if err == errNotFound || err == errTooLarge {
				w.fail(o.ID, wire.KindOf(err))
				return err
			}
			return errUnclear // nothing sent; built (and uploaded) again next time
		}
		b, err := proto.Marshal(m)
		if err != nil {
			w.fail(o.ID, wire.Internal)
			return errInternal
		}
		if err := w.st.SetOutgoingMessage(Name, o.ID, b); err != nil {
			return errUnclear // nothing sent
		}
	}

	resp, err := cli.SendMessage(ctx, jid, m, whatsmeow.SendRequestExtra{ID: o.ID})
	if err != nil {
		if !serverRefused(err) {
			return errUnclear
		}
		kind := wire.Refused
		if mayHaveGone {
			kind = wire.Unclear
		}
		w.fail(o.ID, kind)
		return &wire.Error{Kind: kind}
	}

	ts := resp.Timestamp
	if ts.IsZero() {
		ts = w.clock.Now()
	}
	if o.Kind == outReaction {
		err = w.st.SetReaction(Name, o.ChatID, req.Target,
			store.Reaction{Emoji: req.Emoji, SenderID: w.self(), FromMe: true})
	} else {
		_, err = w.st.PutMessage(Name, w.sentLine(o, req, m, ts))
	}
	if err != nil {
		return errUnclear // sent, not stored: it stays sending, and the resend is kept once
	}
	if err := w.st.SetOutgoingState(Name, o.ID, store.OutSent, ""); err != nil {
		return errUnclear
	}
	if file != nil && req.Kind == "photo" {
		w.keepMedia(o.ChatID, o.ID, file)
	}
	w.changed()
	return nil
}

// build makes the WhatsApp message for a send: text (with its quote when it
// is a reply), or the file uploaded with the text as its caption.
func (w *WhatsApp) build(ctx context.Context, cli waClient, chat string, req sendRequest) (*waE2E.Message, []byte, error) {
	ci := w.quote(chat, req.ReplyTo)
	if req.File == "" {
		if ci == nil {
			return &waE2E.Message{Conversation: proto.String(req.Text)}, nil, nil
		}
		return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String(req.Text), ContextInfo: ci}}, nil, nil
	}
	data, err := os.ReadFile(req.File)
	if err != nil {
		return nil, nil, errNotFound
	}
	limit, kind := int64(fileLimit), whatsmeow.MediaDocument
	switch req.Kind {
	case "photo":
		limit, kind = photoLimit, whatsmeow.MediaImage
	case "video":
		limit, kind = videoLimit, whatsmeow.MediaVideo
	}
	if int64(len(data)) > limit {
		return nil, nil, errTooLarge
	}
	up, err := cli.Upload(ctx, data, kind)
	if err != nil {
		return nil, nil, errUnclear
	}
	var caption *string
	if req.Text != "" {
		caption = proto.String(req.Text)
	}
	switch req.Kind {
	case "photo":
		img := &waE2E.ImageMessage{
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
			Mimetype: proto.String(req.Mimetype), FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256,
			FileLength: proto.Uint64(up.FileLength), Caption: caption, ContextInfo: ci,
		}
		if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
			img.Width, img.Height = proto.Uint32(uint32(cfg.Width)), proto.Uint32(uint32(cfg.Height))
		}
		return &waE2E.Message{ImageMessage: img}, data, nil
	case "video":
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
			Mimetype: proto.String(req.Mimetype), FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256,
			FileLength: proto.Uint64(up.FileLength), Caption: caption, ContextInfo: ci,
		}}, nil, nil
	}
	doc := &waE2E.DocumentMessage{
		URL: proto.String(up.URL), DirectPath: proto.String(up.DirectPath), MediaKey: up.MediaKey,
		Mimetype: proto.String(req.Mimetype), FileEncSHA256: up.FileEncSHA256, FileSHA256: up.FileSHA256,
		FileLength: proto.Uint64(up.FileLength), FileName: proto.String(req.FileName),
		Title: proto.String(req.FileName), Caption: caption, ContextInfo: ci,
	}
	if caption != nil {
		// As WhatsApp's own apps send a captioned document.
		return &waE2E.Message{DocumentWithCaptionMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{DocumentMessage: doc}}}, nil, nil
	}
	return &waE2E.Message{DocumentMessage: doc}, nil, nil
}

// quote is a reply's context: the stored original's ID, its sender (him for
// his own lines) and its content. A reply to a line only Beeper's copy holds,
// or to one the store does not hold, goes without a quote.
func (w *WhatsApp) quote(chat, replyTo string) *waE2E.ContextInfo {
	if replyTo == "" || strings.HasPrefix(replyTo, "beeper:") {
		return nil
	}
	l, ok, err := w.st.Line(Name, chat, replyTo)
	if err != nil || !ok {
		return nil
	}
	quoted := &waE2E.Message{}
	if len(l.Raw) == 0 || proto.Unmarshal(l.Raw, quoted) != nil {
		quoted = &waE2E.Message{Conversation: proto.String(l.Text)}
	}
	participant := l.SenderID
	if l.FromMe {
		participant = w.self()
	}
	return &waE2E.ContextInfo{StanzaID: proto.String(l.ID), Participant: proto.String(participant), QuotedMessage: quoted}
}

// unwrapSent is the message as the line shows it: a captioned document's
// inner message.
func unwrapSent(m *waE2E.Message) *waE2E.Message {
	if inner := m.GetDocumentWithCaptionMessage().GetMessage(); inner != nil {
		return inner
	}
	return m
}

// sentLine is the line a sent message makes: his, at WhatsApp's time.
func (w *WhatsApp) sentLine(o store.Outgoing, req sendRequest, m *waE2E.Message, ts time.Time) store.Message {
	shown := false
	inner := unwrapSent(m)
	line := store.Message{
		ID: o.ID, ChatID: o.ChatID, SenderID: w.self(), FromMe: true, TimeMs: ts.UnixMilli(),
		Text: req.Text, Kind: req.Kind, Hidden: &shown, ReplyTo: contextInfo(inner).GetStanzaID(),
		Source: store.SourceLive,
	}
	switch req.Kind {
	case "photo":
		img := inner.GetImageMessage()
		line.Media = []store.Media{{Kind: "photo", Width: int(img.GetWidth()), Height: int(img.GetHeight()), HasFile: true}}
	case "video":
		line.Media = []store.Media{{Kind: "video"}} // no thumbnail is made for it
	case "file":
		line.Media = []store.Media{{Kind: "file"}}
	}
	if req.Kind != "text" {
		line.Raw, _ = proto.Marshal(inner)
	}
	return line
}

// SendState is what became of a send: sending (also while unclear), sent,
// failed with its kind, or unknown for an ID the outbox never held.
func (w *WhatsApp) SendState(messageID string) network.SendState {
	o, ok, err := w.st.Outgoing(Name, messageID)
	if err != nil || !ok {
		return network.SendState{State: network.SendUnknown}
	}
	switch o.State {
	case store.OutSent:
		return network.SendState{State: network.SendSent}
	case store.OutFailed:
		return network.SendState{State: network.SendFailed, Kind: wire.Kind(o.Fail)}
	}
	return network.SendState{State: network.SendSending}
}

// React sends his reaction (or, with an empty emoji, removes it) to a held
// line, keyed by the line's sender. A transaction the outbox already holds
// sends nothing, unless it failed before WhatsApp had it.
func (w *WhatsApp) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	gate := w.writeGate()
	if gate != nil && gate != errOffline {
		return gate
	}
	if strings.HasPrefix(messageID, "beeper:") {
		return errNotOnNetwork
	}
	chat, jid, err := w.writableChat(chatID)
	if err != nil {
		return err
	}
	prev, held, err := w.st.OutgoingTxn(Name, transactionID)
	if err != nil {
		return err
	}
	if held && prev.State != store.OutFailed {
		return nil
	}
	target, ok, err := w.st.Line(Name, chat, messageID)
	if err != nil {
		return err
	}
	if !ok {
		return errNotFound
	}
	if gate != nil {
		return gate
	}
	if held {
		// Refused before it went: the same reaction, under the same ID.
		if err := w.st.SetOutgoingState(Name, prev.ID, store.OutSending, ""); err != nil {
			return err
		}
		return w.attempt(w.bg(), prev, false)
	}
	cli := w.client()
	if cli == nil {
		return errNotConnected
	}
	sender := types.EmptyJID
	if target.FromMe {
		sender, _ = types.ParseJID(w.self())
	} else if s, err := types.ParseJID(target.SenderID); err == nil {
		sender = s
	}
	msg, err := proto.Marshal(cli.BuildReaction(jid, sender, messageID, emoji))
	if err != nil {
		return errInternal
	}
	req, err := json.Marshal(sendRequest{Target: messageID, Emoji: emoji})
	if err != nil {
		return errInternal
	}
	o := store.Outgoing{ID: cli.GenerateMessageID(), ChatID: chat, Kind: outReaction, Txn: transactionID,
		MadeMs: w.clock.Now().UnixMilli(), Request: req, Message: msg}
	if err := w.st.PutOutgoing(Name, o); err != nil {
		if _, raced, _ := w.st.OutgoingTxn(Name, transactionID); raced {
			return nil // another call with this transaction kept it first, and sends it
		}
		return err
	}
	return w.attempt(w.bg(), o, false)
}

// MarkRead sends read receipts for the chat's unread lines up to and
// including through (the newest incoming line when empty), one receipt per
// sender, then moves his read marker there. Nothing newer is marked.
func (w *WhatsApp) MarkRead(ctx context.Context, chatID, through string) error {
	if err := w.writeGate(); err != nil {
		return err
	}
	chat, jid, err := w.writableChat(chatID)
	if err != nil {
		return err
	}
	var upTo store.Line
	var ok bool
	if through == "" {
		upTo, ok, err = w.st.NewestIncoming(Name, chat)
		if err == nil && !ok {
			return nil // nothing to read
		}
	} else {
		upTo, ok, err = w.st.Line(Name, chat, through)
		if err == nil && !ok {
			return errNotFound
		}
	}
	if err != nil {
		return err
	}
	lines, err := w.st.Unread(Name, chat, upTo.TimeMs)
	if err != nil {
		return err
	}
	var senders []string
	ids := map[string][]types.MessageID{}
	for _, l := range lines {
		if strings.HasPrefix(l.ID, "beeper:") {
			continue
		}
		if _, seen := ids[l.SenderID]; !seen {
			senders = append(senders, l.SenderID)
		}
		ids[l.SenderID] = append(ids[l.SenderID], l.ID)
	}
	cli := w.client()
	if cli == nil {
		return errNotConnected
	}
	now := w.clock.Now()
	for _, s := range senders {
		sender, _ := types.ParseJID(s)
		if err := cli.MarkRead(w.bg(), ids[s], now, jid, sender); err != nil {
			return writeErr(err)
		}
	}
	ms := upTo.TimeMs
	if err := w.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &ms, ReadForwardOnly: true}); err != nil {
		return err
	}
	w.changed()
	return nil
}

// MarkUnread marks the chat unread on WhatsApp (an app state patch naming
// its last line), and shows its newest incoming line unread.
func (w *WhatsApp) MarkUnread(ctx context.Context, chatID string) error {
	if err := w.writeGate(); err != nil {
		return err
	}
	chat, jid, err := w.writableChat(chatID)
	if err != nil {
		return err
	}
	c, ok, err := w.st.Chat(Name, chat)
	if err != nil {
		return err
	}
	if !ok || c.LastMessageID == "" {
		return nil // nothing to mark
	}
	last, ok, err := w.st.Line(Name, chat, c.LastMessageID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	var key *waCommon.MessageKey
	if !strings.HasPrefix(last.ID, "beeper:") {
		key = &waCommon.MessageKey{RemoteJID: proto.String(jid.String()), FromMe: proto.Bool(last.FromMe), ID: proto.String(last.ID)}
		if !last.FromMe && jid.Server == types.GroupServer {
			key.Participant = proto.String(last.SenderID)
		}
	}
	cli := w.client()
	if cli == nil {
		return errNotConnected
	}
	patch := appstate.BuildMarkChatAsRead(jid, false, time.UnixMilli(last.TimeMs), key)
	if err := cli.SendAppState(w.bg(), patch); err != nil {
		return writeErr(err)
	}
	if in, ok, err := w.st.NewestIncoming(Name, chat); err == nil && ok {
		before := in.TimeMs - 1
		if err := w.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &before}); err != nil {
			return err
		}
	}
	w.changed()
	return nil
}

// SetMuted mutes the chat for ever, or unmutes it.
func (w *WhatsApp) SetMuted(ctx context.Context, chatID string, muted bool) error {
	if err := w.writeGate(); err != nil {
		return err
	}
	chat, jid, err := w.writableChat(chatID)
	if err != nil {
		return err
	}
	cli := w.client()
	if cli == nil {
		return errNotConnected
	}
	if err := cli.SendAppState(w.bg(), appstate.BuildMute(jid, muted, 0)); err != nil {
		return writeErr(err)
	}
	if err := w.st.PutChat(Name, store.ChatInfo{ID: chat, Muted: &muted}); err != nil {
		return err
	}
	w.armMuteTimer() // a mute for ever or an unmute leaves no end to wait for
	w.changed()
	return nil
}

// Logout is his Disconnect: WhatsApp is told to unlink Pllum (whatsmeow then
// deletes the device), the linked marker goes so a restart shows Not
// connected rather than Reconnect needed, his kept photos (plain files in
// connect/media) are deleted, and every send still waiting fails. The chats
// stay in the store. Linked but offline, nothing changes and the answer is
// offline: WhatsApp would still list Pllum. Once WhatsApp has agreed, a
// failure to delete whatsmeow's own copy of the device still counts as
// logged out.
func (w *WhatsApp) Logout(ctx context.Context) error {
	w.CancelLogin()
	w.mu.Lock()
	if !w.started || w.stopped {
		w.mu.Unlock()
		return errInternal
	}
	cli, dev := w.cli, w.dev
	linked := dev != nil && dev.ID != nil && !dev.Deleted
	tell := linked && cli != nil && w.state != network.ReconnectNeeded
	online := w.state == network.CatchingUp || w.state == network.Live
	w.mu.Unlock()

	if tell {
		if !online {
			return errOffline
		}
		if err := cli.Logout(w.bg()); err != nil {
			if !unlinkedAnyway(err) {
				return writeErr(err)
			}
			_ = dev.Delete(context.WithoutCancel(w.bg())) // once more; Start copes if it stays
		}
	} else if linked {
		// WhatsApp already unlinked it: forget it here too.
		_ = dev.Delete(context.WithoutCancel(w.bg()))
	}
	if err := os.Remove(w.linkedMarker()); err != nil && !os.IsNotExist(err) {
		return errInternal
	}
	w.mu.Lock()
	if w.cli == cli {
		w.cli = nil
	}
	if w.stopCatchUp != nil {
		w.stopCatchUp()
		w.stopCatchUp = nil
	}
	w.setState(network.NotConnected, "")
	w.mu.Unlock()
	if cli != nil {
		cli.Disconnect()
	}
	w.failWaiting()
	w.forgetMedia()
	return nil
}

// unlinkedAnyway reports whether whatsmeow's Logout failed only after
// WhatsApp agreed to unlink, deleting its own store (client.go, Logout: the
// request's failure is "error sending logout request", this one "error
// deleting data from store").
func unlinkedAnyway(err error) bool {
	return strings.HasPrefix(err.Error(), "error deleting data from store")
}

// failWaiting fails every send still sending: notConnected for one never
// tried, unclear for one that may have gone.
func (w *WhatsApp) failWaiting() {
	w.smu.Lock()
	w.queue = nil
	w.smu.Unlock()
	rows, err := w.st.OutgoingSending(Name)
	if err != nil {
		return
	}
	for _, o := range rows {
		kind := wire.NotConnected
		if len(o.Message) > 0 {
			kind = wire.Unclear
		}
		w.fail(o.ID, kind)
	}
}
