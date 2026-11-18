package messenger

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow"
	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	watypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// msgrWriter is every write the adapter makes to Messenger, and nothing
// else: the main socket's ExecuteTasks, handed only a SendMessageTask, a
// SendReactionTask, a ThreadMarkReadTask or a MuteThreadTask (execute
// refuses any other before it is called, and metaWriter again); whatsmeow's
// SendFBMessage and MarkRead for the encrypted chats; DownloadFB, an
// encrypted file's download; and Upload, a file for an unencrypted chat
// (meta.Upload: the library's mercury upload, asked once).
type msgrWriter interface {
	ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error)
	SendFBMessage(ctx context.Context, to watypes.JID, message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)
	MarkRead(ctx context.Context, ids []watypes.MessageID, timestamp time.Time, chat, sender watypes.JID, receiptTypeExtra ...watypes.ReceiptType) error
	DownloadFB(ctx context.Context, transport *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error)
	Upload(ctx context.Context, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error)
}

// metaWriter is the real clients' writes: messagix's for the main socket
// and the upload, whatsmeow's for the encrypted chats (nil until the
// encrypted side is set up).
type metaWriter struct {
	c  *messagix.Client
	wa *whatsmeow.Client
}

var _ msgrWriter = metaWriter{}

var errNotAllowed = &wire.Error{Kind: wire.Refused}

// ExecuteTasks hands Meta one write task of the four allowed; any other,
// or more than one, is refused here and never reaches the client.
func (w metaWriter) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if len(tasks) != 1 || !allowedWrite(tasks[0]) {
		return nil, errNotAllowed
	}
	if w.c == nil {
		return nil, messagix.ErrClientIsNil
	}
	return w.c.ExecuteTasks(ctx, tasks...)
}

func (w metaWriter) SendFBMessage(ctx context.Context, to watypes.JID, message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	if w.wa == nil {
		return whatsmeow.SendResponse{}, whatsmeow.ErrClientIsNil
	}
	return w.wa.SendFBMessage(ctx, to, message, metadata, extra...)
}

func (w metaWriter) MarkRead(ctx context.Context, ids []watypes.MessageID, timestamp time.Time, chat, sender watypes.JID, receiptTypeExtra ...watypes.ReceiptType) error {
	if w.wa == nil {
		return whatsmeow.ErrClientIsNil
	}
	return w.wa.MarkRead(ctx, ids, timestamp, chat, sender, receiptTypeExtra...)
}

func (w metaWriter) DownloadFB(ctx context.Context, transport *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error) {
	if w.wa == nil {
		return nil, whatsmeow.ErrClientIsNil
	}
	return w.wa.DownloadFB(ctx, transport, mediaType)
}

func (w metaWriter) Upload(ctx context.Context, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error) {
	if w.c == nil {
		return nil, messagix.ErrClientIsNil
	}
	return meta.Upload(ctx, w.c, threadID, media)
}

// allowedWrite reports whether a task is one of the four writes the
// adapter may make.
func allowedWrite(task socket.Task) bool {
	switch task.(type) {
	case *socket.SendMessageTask, *socket.SendReactionTask, *socket.ThreadMarkReadTask, *socket.MuteThreadTask:
		return true
	}
	return false
}

// writerFor is the writer of the real clients; nil for any other.
func writerFor(cli msgrClient, e e2eeClient) msgrWriter {
	rc, ok := cli.(realClient)
	if !ok {
		return nil
	}
	w := metaWriter{c: rc.Client}
	if wc, ok := e.(waClient); ok {
		w.wa = wc.Client
	}
	return w
}

// execute asks Meta one write task through the pace. A task that is not
// one of the four writes is refused before anything is asked. ran reports
// whether the request reached the client (raw is then its own error).
func (m *Messenger) execute(ctx context.Context, gen int, w msgrWriter, task socket.Task) (resp *table.LSTable, ran bool, raw, err error) {
	if task == nil || !allowedWrite(task) {
		return nil, false, nil, errNotAllowed
	}
	ran, raw, err = m.ask(ctx, gen, func(ctx context.Context) (err error) {
		resp, err = w.ExecuteTasks(ctx, task)
		return err
	})
	return resp, ran, raw, err
}

// ask makes one request through the pace. ran reports whether it reached
// the client (raw is then the client's own error); when it did not, err is
// why (the pace's block, a wait cut short, or the run ended).
func (m *Messenger) ask(ctx context.Context, gen int, fn func(ctx context.Context) error) (ran bool, raw, err error) {
	err = m.request(ctx, gen, func(ctx context.Context) error {
		ran = true
		raw = fn(ctx)
		return raw
	})
	return ran, raw, err
}

// Messenger's writes. Every one is a request to Meta and goes through the
// pace (and so is refused, with the block's kind, while Meta has the
// session stopped), as do the reads they need (an upload, a file's fetch).
// No write is ever asked again after Meta has answered it, or when it may
// have reached Meta: only the outbox row's state moves.
//
// An unencrypted chat's send (a group's) is kept in the outbox under
// "otid:<offline threading ID>", the ID its echo carries, and goes as one
// SendMessageTask (a file uploaded first, its caption the task's text, as
// the library's own converter sends it). Meta's answer, or the echo on the
// main socket, gives Meta's message ID, which the line is stored under and
// sendStatus reports as lineID. An encrypted chat's send is made with a
// whatsmeow message ID (as GenerateMessageID makes it in Messenger mode)
// passed to SendFBMessage, so the line's ID is the send's own. Sends go in
// the background, one at a time in the order made.
//
//	Meta answered with the message          sent (lineID)
//	nothing reached Meta, or Meta said no   failed, refused
//	no answer in time, or no readable one   sending; at the next start unknown
//
// A send still sending at start becomes unknown and is never sent again; a
// later echo, or the line coming back under its ID, still settles it.
// ExecuteTasks goes over the main socket as one stream, which the library
// never sends again; whatsmeow sends an encrypted message again only after
// its socket drops mid-send, under the same message ID, which the other
// side drops as a copy.

const (
	// otidPrefix begins the ID of a send to an unencrypted chat: the
	// offline threading ID it goes with, which its echo carries.
	otidPrefix  = "otid:"
	outMessage  = "message"
	outReaction = "reaction"
	// writeWait is the longest a write waits for its turn and its answer.
	writeWait = 60 * time.Second
	// The largest photo and video sent.
	photoLimit = 16 << 20
	videoLimit = 16 << 20
)

var (
	errNotOnNetwork = &wire.Error{Kind: wire.NotOnNetwork}
	errRefused      = &wire.Error{Kind: wire.Refused}
	errTooLarge     = &wire.Error{Kind: wire.TooLarge}
)

// sendRequest is what the outbox keeps of a write.
type sendRequest struct {
	Text     string `json:"text,omitempty"`
	ReplyTo  string `json:"replyTo,omitempty"`
	File     string `json:"file,omitempty"`
	FileName string `json:"fileName,omitempty"`
	Kind     string `json:"kind,omitempty"` // text, photo or video
	Mimetype string `json:"mimetype,omitempty"`
	OTID     string `json:"otid,omitempty"`   // an unencrypted chat's send: its offline threading ID
	Target   string `json:"target,omitempty"` // the line a reaction is to
	Emoji    string `json:"emoji,omitempty"`
	WAID     string `json:"waID,omitempty"` // an encrypted chat's reaction: the message ID it goes under
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

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// isMetaLine reports whether a line is an unencrypted thread's (Meta's
// message IDs begin "mid."), not an encrypted chat's.
func isMetaLine(id string) bool { return strings.HasPrefix(id, "mid.") }

// writeGate is nil when a write may be asked now: the pace allows a request
// (else its block's kind) and he is signed in (live, or offline while a
// socket reconnects; else notConnected).
func (m *Messenger) writeGate() error {
	if err := m.blocked(); err != nil {
		return err
	}
	m.mu.Lock()
	s := m.state
	m.mu.Unlock()
	switch s {
	case network.Live, network.Offline:
		return nil
	}
	return errNotConnected
}

// writableChat is the held chat a write names: an encrypted chat, or an
// unencrypted thread's (its thread key).
func (m *Messenger) writableChat(chatID string) (string, error) {
	if strings.Contains(chatID, "unplaced:") {
		return "", errNotOnNetwork
	}
	chat, ok := m.st.ChatFor(Name, chatID)
	if !ok {
		return "", errNotFound
	}
	if isEncryptedChat(chat) {
		if _, err := watypes.ParseJID(chat); err != nil {
			return "", errNotOnNetwork
		}
		return chat, nil
	}
	if key, err := strconv.ParseInt(chat, 10, 64); err != nil || key <= 0 {
		return "", errNotOnNetwork
	}
	return chat, nil
}

// writeCtx is a write's context: the network's, for at most writeWait.
func (m *Messenger) writeCtx() (context.Context, context.CancelFunc) {
	m.mu.Lock()
	base := m.ctx
	m.mu.Unlock()
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, writeWait)
}

// writer is the current run's number and writer; the writer is nil when no
// client is signed in.
func (m *Messenger) writer() (int, msgrWriter) {
	m.mu.Lock()
	gen, cli, e := m.gen, m.cli, m.e2ee
	m.mu.Unlock()
	if cli == nil || m.newWriter == nil {
		return gen, nil
	}
	return gen, m.newWriter(cli, e)
}

// self is his participant ID and FBID ("" and 0 before the messages page).
func (m *Messenger) self() (string, int64) {
	m.mu.Lock()
	fbid := m.fbid
	m.mu.Unlock()
	id, _ := m.personOf(fbid)
	return id, fbid
}

// senderJID is the JID of a line's sender, as an encrypted chat names a
// person: their FBID on the Messenger server.
func (m *Messenger) senderJID(l store.Line) watypes.JID {
	who := l.SenderID
	if who == "" && l.FromMe {
		who, _ = m.self()
	}
	return watypes.NewJID(who, watypes.MessengerServer)
}

// unsure reports whether a write that failed may still have reached Meta,
// so must never be sent again: no answer in time, the helper stopping
// while it waited, the connection dropping mid-send, or an answer that
// could not be read or understood. Only a socket that was never open or
// never dialled, Meta saying no (an error answer, a checkpoint, an ended
// session, a ban, a rate limit), a write refused here, or a request never
// asked (the caller's ran is false) is safe to send again.
func unsure(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return false
	}
	switch {
	case errors.Is(err, dgw.ErrSocketNotOpen), errors.Is(err, dgw.ErrDial),
		errors.Is(err, whatsmeow.ErrNotConnected), errors.Is(err, whatsmeow.ErrNotLoggedIn),
		errors.Is(err, whatsmeow.ErrClientIsNil), errors.Is(err, whatsmeow.ErrServerReturnedError):
		return false
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled),
		errors.Is(err, whatsmeow.ErrMessageTimedOut), errors.Is(err, whatsmeow.ErrIQTimedOut):
		return true
	}
	var er *types.ErrorResponse
	var we *wire.Error
	if errors.As(err, &er) || errors.As(err, &we) {
		return false
	}
	switch kindOf(err) {
	case wire.Checkpoint, meta.ReconnectNeeded, meta.Banned, wire.RateLimited:
		return false
	}
	return true
}

// writeErr is the error a write that is not a send returns: why it was
// never asked, unclear when it may have reached Meta, Meta pushing back
// (checkpoint, rateLimited, notConnected), else refused.
func writeErr(ran bool, raw, err error) error {
	switch {
	case !ran:
		return err
	case unsure(raw):
		return errUnclear
	}
	switch kindOf(raw) {
	case wire.Checkpoint, meta.ReconnectNeeded, meta.Banned, wire.RateLimited:
		return err
	}
	return errRefused
}

// Send keeps the message in the outbox under its ID (its offline threading
// ID in an unencrypted chat, its whatsmeow message ID in an encrypted one),
// returns that ID at once, and sends it in the background. A send repeated
// with the caller's key (o.ClientID) of one kept and not failed gives that
// send's ID and sends nothing more; a failed one is done, and the key makes
// a new send. A file goes only on an unencrypted thread: the encrypted
// side has no upload the helper may make.
func (m *Messenger) Send(ctx context.Context, o network.Outgoing) (string, error) {
	if o.ClientID != "" {
		if chat, ok := m.st.ChatFor(Name, o.ChatID); ok {
			prev, held, err := m.keyed(o.ClientID, chat)
			if err != nil {
				return "", err
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
		if encrypted {
			return "", errUnsupported
		}
		kind, mimetype, err := fileKind(o.File)
		if err != nil {
			return "", err
		}
		req.File, req.FileName, req.Kind, req.Mimetype = o.File, cmp.Or(o.FileName, filepath.Base(o.File)), kind, mimetype
	}
	var id string
	if encrypted {
		id = string(newMessageID())
	} else {
		otid := otidFor(o.ClientID)
		if _, taken, err := m.st.Outgoing(Name, otidPrefix+itoa(otid)); err != nil {
			return "", err
		} else if taken {
			otid = methods.GenerateEpochID() // the key's number is a done send's
		}
		req.OTID = itoa(otid)
		id = otidPrefix + req.OTID
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", errInternal
	}
	if err := m.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: o.ClientID,
		MadeMs: m.now().UnixMilli(), Request: body}); err != nil {
		if raced, held, rerr := m.keyed(o.ClientID, chat); rerr == nil && held {
			return raced.ID, nil // another call with this key kept it first, and sends it
		}
		return "", err
	}
	m.enqueue(id)
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
// unknown. A failed send frees the key. A key naming a send to another
// chat is a bad request.
func (m *Messenger) keyed(client, chat string) (store.Outgoing, bool, error) {
	o, ok, err := m.st.OutgoingClient(Name, client)
	if err != nil || !ok || o.State == store.OutFailed {
		return store.Outgoing{}, false, err
	}
	if o.ChatID != chat || o.Kind != outMessage {
		return store.Outgoing{}, false, errBadRequest
	}
	return o, true, nil
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

// forgetCutOff makes every write still sending unknown: it may have reached
// Meta before the helper stopped, so it is never sent again.
func (m *Messenger) forgetCutOff() {
	rows, err := m.st.OutgoingSending(Name)
	if err != nil {
		return
	}
	for _, o := range rows {
		_ = m.st.SetOutgoingState(Name, o.ID, store.OutUnknown, "")
	}
}

// fileKind is how a file goes: a JPEG or PNG photo, or an MP4 or QuickTime
// video, each up to 16 MB (Instagram's rule). Anything else is
// unsupported.
func fileKind(path string) (kind, mimetype string, err error) {
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
	if n >= 12 && string(head[4:8]) == "ftyp" && string(head[8:12]) == "qt  " {
		sniffed = "video/quicktime"
	}
	switch sniffed {
	case "image/jpeg", "image/png":
		if fi.Size() > photoLimit {
			return "", "", errTooLarge
		}
		return "photo", sniffed, nil
	case "video/mp4", "video/quicktime":
		if fi.Size() > videoLimit {
			return "", "", errTooLarge
		}
		return "video", sniffed, nil
	}
	return "", "", errUnsupported
}

// readSendFile reads a send's file again, as it is now.
func readSendFile(req sendRequest) ([]byte, error) {
	data, err := os.ReadFile(req.File)
	if err != nil {
		return nil, errNotFound
	}
	limit := int64(photoLimit)
	if req.Kind == "video" {
		limit = videoLimit
	}
	if int64(len(data)) > limit {
		return nil, errTooLarge
	}
	return data, nil
}

// enqueue puts a send in line; one goroutine sends them, in order.
func (m *Messenger) enqueue(id string) {
	m.busy.Add(1)
	m.smu.Lock()
	m.queue = append(m.queue, id)
	if m.draining {
		m.smu.Unlock()
		return
	}
	m.draining = true
	m.smu.Unlock()
	m.mu.Lock()
	ok := m.started && !m.stopped
	if ok {
		m.work.Add(1)
	}
	ctx := m.ctx
	m.mu.Unlock()
	if !ok {
		m.drain(nil)
		return
	}
	go func() {
		defer m.work.Done()
		m.drain(ctx)
	}()
}

// drain sends what is in line until none is left. A send whose turn comes
// after the helper began stopping never went: refused.
func (m *Messenger) drain(ctx context.Context) {
	for {
		m.smu.Lock()
		if len(m.queue) == 0 {
			m.draining = false
			m.smu.Unlock()
			return
		}
		id := m.queue[0]
		m.queue = m.queue[1:]
		m.smu.Unlock()
		if ctx == nil || ctx.Err() != nil {
			m.fail(id, wire.Refused)
		} else {
			m.deliver(id)
		}
		m.busy.Add(-1)
	}
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

// deliver sends one send, if it is still sending: on the chat's
// unencrypted thread when it was made with an offline threading ID, else
// through the encrypted side.
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
	ctx, cancel := m.writeCtx()
	defer cancel()
	gen, w := m.writer()
	if w == nil {
		m.fail(id, wire.Refused)
		return
	}
	if req.OTID == "" {
		m.deliverEncrypted(ctx, gen, w, o, req)
	} else {
		m.deliverUnencrypted(ctx, gen, w, o, req)
	}
}

// deliverUnencrypted sends a message on a chat's unencrypted thread (a
// group's own, or a one-to-one's older thread): the file uploaded first (so a failed upload sends nothing), then one
// SendMessageTask, as the connector's message converter builds it, with
// no read receipt beside it.
func (m *Messenger) deliverUnencrypted(ctx context.Context, gen int, w msgrWriter, o store.Outgoing, req sendRequest) {
	key, ok := m.threadOfChat(o.ChatID)
	otid, err := strconv.ParseInt(req.OTID, 10, 64)
	if !ok || err != nil {
		m.fail(o.ID, wire.Internal)
		return
	}
	task := &socket.SendMessageTask{ThreadId: key, Otid: otid, Source: table.MESSENGER_INBOX_IN_THREAD,
		InitiatingSource: table.FACEBOOK_INBOX, SendType: table.TEXT, SyncGroup: 1, Text: req.Text}
	var data []byte
	if req.File != "" {
		if data, err = readSendFile(req); err != nil {
			m.fail(o.ID, wire.KindOf(err))
			return
		}
		var resp *types.MercuryUploadResponse
		if _, _, err := m.ask(ctx, gen, func(ctx context.Context) (err error) {
			resp, err = w.Upload(ctx, key, &httpclient.MercuryUploadMedia{Filename: req.FileName, MimeType: req.Mimetype, MediaData: data})
			return err
		}); err != nil {
			m.fail(o.ID, wire.Refused) // an upload is not a message: nothing was sent
			return
		}
		var fbid int64
		if resp != nil && resp.Payload.RealMetadata != nil {
			fbid = resp.Payload.RealMetadata.GetFbId()
		}
		if fbid == 0 {
			m.fail(o.ID, wire.Refused)
			return
		}
		task.SendType, task.AttachmentFBIds = table.MEDIA, []int64{fbid}
	}
	reply := ""
	if req.ReplyTo != "" && isMetaLine(req.ReplyTo) {
		if l, ok, _ := m.st.Line(Name, o.ChatID, req.ReplyTo); ok {
			sender, _ := strconv.ParseInt(l.SenderID, 10, 64)
			task.ReplyMetaData = &socket.ReplyMetaData{ReplyMessageId: l.ID, ReplySourceType: 1, ReplyType: 0, ReplySender: sender}
			reply = l.ID
		}
	}
	resp, ran, raw, err := m.execute(ctx, gen, w, task)
	if err != nil {
		if ran && unsure(raw) {
			return // it stays sending
		}
		m.fail(o.ID, wire.Refused)
		return
	}
	lineID, refused := sentOf(resp, req.OTID)
	switch {
	case lineID != "":
		m.storeSent(o.ChatID, lineID, req, reply, data, m.sentAt(lineID))
		m.settle(o.ID, lineID)
		m.changed()
	case refused:
		m.fail(o.ID, wire.Refused)
	default:
		// Meta answered without the message: its echo settles it, or at
		// the next start it is unknown.
	}
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
	now := m.now()
	if ms, err := methods.ParseMessageID(lineID); err == nil {
		if t := time.UnixMilli(ms); t.After(now.Add(-time.Hour)) && t.Before(now.Add(time.Hour)) {
			return ms
		}
	}
	return now.UnixMilli()
}

// deliverEncrypted sends an encrypted chat's text under the row's ID, so
// the line is stored (and comes back) under it. A reply quotes its line by
// ID and sender.
func (m *Messenger) deliverEncrypted(ctx context.Context, gen int, w msgrWriter, o store.Outgoing, req sendRequest) {
	jid, err := watypes.ParseJID(o.ChatID)
	if err != nil {
		m.fail(o.ID, wire.Internal)
		return
	}
	app := consumerApp(&waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{MessageText: &waCommon.MessageText{Text: proto.String(req.Text)}}})
	md := &waMsgApplication.MessageApplication_Metadata{}
	reply := ""
	if req.ReplyTo != "" && !isMetaLine(req.ReplyTo) {
		if l, ok, _ := m.st.Line(Name, o.ChatID, req.ReplyTo); ok {
			md.QuotedMessage = &waMsgApplication.MessageApplication_Metadata_QuotedMessage{
				StanzaID: proto.String(l.ID), Participant: proto.String(m.senderJID(l).String())}
			reply = l.ID
		}
	}
	var resp whatsmeow.SendResponse
	ran, raw, err := m.ask(ctx, gen, func(ctx context.Context) (err error) {
		resp, err = w.SendFBMessage(ctx, jid, app, md, whatsmeow.SendRequestExtra{ID: watypes.MessageID(o.ID)})
		return err
	})
	if err != nil {
		if ran && unsure(raw) {
			return // it stays sending
		}
		m.fail(o.ID, wire.Refused)
		return
	}
	ms := resp.Timestamp.UnixMilli()
	if resp.Timestamp.IsZero() || ms <= 0 {
		ms = m.now().UnixMilli()
	}
	m.storeSent(o.ChatID, o.ID, req, reply, nil, ms)
	m.settle(o.ID, o.ID)
	m.changed()
}

// consumerApp wraps a consumer application's content.
func consumerApp(c *waConsumerApplication.ConsumerApplication_Content) *waConsumerApplication.ConsumerApplication {
	return &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: c}}}
}

// storeSent stores the line a message Meta took became, his, under its ID.
// A photo he sent is kept so it opens at once.
func (m *Messenger) storeSent(chat, lineID string, req sendRequest, reply string, photo []byte, ms int64) {
	sender, _ := m.self()
	shown := false
	line := store.Message{ID: lineID, ChatID: chat, SenderID: sender, FromMe: true, TimeMs: ms,
		Text: req.Text, Kind: req.Kind, Hidden: &shown, ReplyTo: reply, Source: store.SourceLive}
	switch req.Kind {
	case "photo":
		line.Media = []store.Media{{Kind: "photo", HasFile: true}}
	case "video":
		line.Media = []store.Media{{Kind: "video"}}
	}
	if _, err := m.st.PutMessage(Name, line); err == nil && req.Kind == "photo" && photo != nil {
		m.keepMedia(chat, lineID, photo)
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
	if err != nil || !ok || o.Kind != outMessage || (o.State == store.OutSent && o.LineID == lineID) {
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
	if err != nil || !ok || o.Kind != outMessage || o.State == store.OutSent {
		return
	}
	m.settle(o.ID, id)
}

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
			Txn: transactionID, MadeMs: m.now().UnixMilli(), Request: body}
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
		_, ran, raw, err := m.execute(ctx, gen, w, &socket.SendReactionTask{ThreadKey: key, TimestampMs: m.now().UnixMilli(),
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
				Key: key, Text: proto.String(emoji), SenderTimestampMS: proto.Int64(m.now().UnixMilli())}}})
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
		wctx, cancel := m.writeCtx()
		defer cancel()
		gen, w := m.writer()
		if w == nil {
			return errNotConnected
		}
		if metaAt > 0 && threaded {
			if _, ran, raw, err := m.execute(wctx, gen, w, &socket.ThreadMarkReadTask{ThreadId: key, LastReadWatermarkTs: metaAt, SyncGroup: 1}); err != nil {
				return writeErr(ran, raw, err)
			}
		}
		if len(senders) > 0 {
			jid, err := watypes.ParseJID(chat)
			if err != nil {
				return errInternal
			}
			now := m.now()
			for _, by := range senders {
				var sender watypes.JID
				if by != "" {
					sender = watypes.NewJID(by, watypes.MessengerServer)
				}
				if ran, raw, err := m.ask(wctx, gen, func(ctx context.Context) error {
					return w.MarkRead(ctx, ids[by], now, jid, sender)
				}); err != nil {
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
