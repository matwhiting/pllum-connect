package messenger

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image"
	_ "image/jpeg" // photo sizes, for an encrypted photo's message
	_ "image/png"
	"io"
	"mime"
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
// encrypted file's download; Upload, a file for an unencrypted chat
// (meta.Upload: the library's mercury upload, asked once); and UploadFB, a
// file for an encrypted chat (whatsmeow's Upload, one request to one media
// host, as the connector's reuploadMediaToWhatsApp uploads it).
type msgrWriter interface {
	ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error)
	SendFBMessage(ctx context.Context, to watypes.JID, message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error)
	MarkRead(ctx context.Context, ids []watypes.MessageID, timestamp time.Time, chat, sender watypes.JID, receiptTypeExtra ...watypes.ReceiptType) error
	DownloadFB(ctx context.Context, transport *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error)
	Upload(ctx context.Context, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error)
	UploadFB(ctx context.Context, plaintext []byte, mediaType whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
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

func (w metaWriter) UploadFB(ctx context.Context, plaintext []byte, mediaType whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	if w.wa == nil {
		return whatsmeow.UploadResponse{}, whatsmeow.ErrClientIsNil
	}
	return w.wa.Upload(ctx, plaintext, mediaType)
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
	return m.executeThen(ctx, gen, w, task, nil)
}

// executeThen is execute with first run just before the task leaves, after
// the pace; if first fails, nothing is asked.
func (m *Messenger) executeThen(ctx context.Context, gen int, w msgrWriter, task socket.Task, first func() error) (resp *table.LSTable, ran bool, raw, err error) {
	if task == nil || !allowedWrite(task) {
		return nil, false, nil, errNotAllowed
	}
	ran, raw, err = m.ask(ctx, gen, func(ctx context.Context) (err error) {
		if first != nil {
			if err := first(); err != nil {
				return err
			}
		}
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
// No message is ever asked again after Meta has answered it, or when it
// may have reached Meta: only the outbox row's state moves.
//
// A chat's unencrypted thread (a group's, or a one-to-one's older thread)
// takes SendMessageTasks, each under an offline threading ID its echo
// carries; Meta's answer or the echo gives Meta's message ID, which the
// line is stored under. An encrypted chat takes SendFBMessage under a
// whatsmeow message ID (as GenerateMessageID makes it in Messenger mode),
// which is the line's ID. A send is kept in the outbox under the ID of its
// text ("otid:<n>", or the whatsmeow ID), or of its file when it has no
// text: text with a file is two messages, the text then the file, as
// Instagram's writes send them, the file's own ID kept in the request.
// Sends go in the background, one at a time in the order made.
//
//	Meta answered with every message        sent (lineID: the text's, else the file's)
//	nothing reached Meta, or Meta said no   failed, refused
//	  (after the text of a text-and-file went: unclear)
//	no answer in time, or no readable one   sending; at the next start unknown
//	  (a text with a file: unknown at once)
//
// What a send has done is kept in its row before the next step (its
// progress): the upload, so a send tried again reuses it and never uploads
// twice; the text's message, so the file alone goes after; and, as every
// message's request leaves, that it is being asked. At start a send whose
// message was being asked becomes unknown and is never sent again (a later
// echo, or the line coming back under its ID, still settles it); one that
// never asked a message (its turn never came, or only its upload went) is
// failed, refused, and its key sends it again with what it has (unclear,
// when its text went). The echo of a text whose file is still to go
// settles nothing. ExecuteTasks goes over the main socket as one
// stream, which the library never sends again; whatsmeow sends an
// encrypted message again only after its socket drops mid-send, under the
// same message ID, which the other side drops as a copy.

const (
	// otidPrefix begins the ID of a send to an unencrypted chat: the
	// offline threading ID it goes with, which its echo carries.
	otidPrefix  = "otid:"
	outMessage  = "message"
	outReaction = "reaction"
	// writeWait is the longest a write waits for its turn and its answer.
	writeWait = 60 * time.Second
	// The largest photo and video sent (WhatsApp's and Instagram's), and
	// the largest file (Messenger's attachment limit).
	photoLimit = 16 << 20
	videoLimit = 16 << 20
	fileLimit  = 25 << 20
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

// writeCtx is one request's context: the network's, for at most
// m.writeWait (writeWait). Every request of a write has its own.
func (m *Messenger) writeCtx() (context.Context, context.CancelFunc) {
	m.mu.Lock()
	base := m.ctx
	m.mu.Unlock()
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, cmp.Or(m.writeWait, writeWait))
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
	m.smu.Lock()
	cur, ok, err := m.st.Outgoing(Name, o.ID)
	if err != nil || !ok {
		m.smu.Unlock()
		return "", cmp.Or[error](err, errNotFound)
	}
	if cur.State != store.OutFailed {
		m.smu.Unlock()
		return cur.ID, nil // already going again
	}
	p := progressOf(cur)
	p.Asking = false
	if err := m.keepProgress(cur.ID, p); err != nil {
		m.smu.Unlock()
		return "", err
	}
	if err := m.st.SetOutgoingState(Name, cur.ID, store.OutSending, ""); err != nil {
		m.smu.Unlock()
		return "", err
	}
	m.smu.Unlock()
	m.enqueue(cur.ID)
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

// fileKind is how a file goes: a JPEG or PNG photo or an MP4 or QuickTime
// video up to 16 MB, else a file (a document) up to 25 MB: WhatsApp's
// rule (step 2), with Messenger's attachment limit.
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
	if n >= 12 && string(head[4:8]) == "ftyp" && string(head[8:12]) == "qt  " {
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

// documentType is a document's MIME type: its name's, else its content's
// (WhatsApp's, step 2).
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

// readSendFile reads a send's file again, as it is now.
func readSendFile(req sendRequest) ([]byte, error) {
	data, err := os.ReadFile(req.File)
	if err != nil {
		return nil, errNotFound
	}
	limit := int64(fileLimit)
	switch req.Kind {
	case "photo":
		limit = photoLimit
	case "video":
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
			m.failStopped(id)
		} else {
			m.deliver(id)
		}
		m.busy.Add(-1)
	}
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

// upload uploads the send's file once, with a budget of its own, and keeps
// what Meta gave for it in the row before any message goes: the
// attachment ID on an unencrypted thread (meta.Upload), else the encrypted
// file's transport (UploadFB), built as the connector's
// reuploadMediaToWhatsApp builds it, with a thumbnail size Messenger's
// apps need: a photo's or video's own (clamped to 400), else 400×400.
func (s *sender) upload(data []byte) error {
	ctx, cancel := s.m.writeCtx()
	defer cancel()
	if s.encrypted() {
		var up whatsmeow.UploadResponse
		if _, _, err := s.m.ask(ctx, s.gen, func(ctx context.Context) (err error) {
			up, err = s.w.UploadFB(ctx, data, mediaTypeOf(s.req.Kind))
			return err
		}); err != nil {
			return err
		}
		if up.DirectPath == "" {
			return errRefused
		}
		width, height := mediaSize(s.req.Kind, data)
		tw, th := clampTo400(width, height)
		if tw <= 0 || th <= 0 {
			tw, th = 400, 400
		}
		if width <= 0 || height <= 0 {
			width, height = tw, th // never 0×0
		}
		mimetype := s.req.Mimetype
		b, err := proto.Marshal(&waMediaTransport.WAMediaTransport{
			Integral: &waMediaTransport.WAMediaTransport_Integral{
				FileSHA256: up.FileSHA256, MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256,
				DirectPath: proto.String(up.DirectPath), MediaKeyTimestamp: proto.Int64(s.m.now().Unix()),
			},
			Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{
				FileLength: proto.Uint64(uint64(len(data))), Mimetype: &mimetype,
				// Messenger's apps show a file only with a thumbnail size (the connector's note).
				Thumbnail: &waMediaTransport.WAMediaTransport_Ancillary_Thumbnail{
					ThumbnailWidth: proto.Uint32(uint32(tw)), ThumbnailHeight: proto.Uint32(uint32(th))},
				ObjectID: proto.String(up.ObjectID),
			},
		})
		if err != nil {
			return errInternal
		}
		s.p.Upload, s.p.Width, s.p.Height = b, width, height
	} else {
		var resp *types.MercuryUploadResponse
		if _, _, err := s.m.ask(ctx, s.gen, func(ctx context.Context) (err error) {
			resp, err = s.w.Upload(ctx, s.thread, &httpclient.MercuryUploadMedia{Filename: s.req.FileName, MimeType: s.req.Mimetype, MediaData: data})
			return err
		}); err != nil {
			return err
		}
		var fbid int64
		if resp != nil && resp.Payload.RealMetadata != nil {
			fbid = resp.Payload.RealMetadata.GetFbId()
		}
		if fbid == 0 {
			return errRefused
		}
		s.p.Attachment = fbid
	}
	return s.m.keepProgress(s.o.ID, s.p)
}

// mediaSize is a photo's or video's size, read from the file, or 0, 0.
func mediaSize(kind string, data []byte) (int, int) {
	switch kind {
	case "photo":
		if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
			return cfg.Width, cfg.Height
		}
	case "video":
		return mp4Size(data)
	}
	return 0, 0
}

// mp4Size is an MP4 or QuickTime video's size: the first track header
// (moov/trak/tkhd) with one, or 0, 0. A track header's width and height
// are 16.16 fixed-point numbers at its end (ISO/IEC 14496-12 §8.3.2).
func mp4Size(data []byte) (int, int) {
	var walk func(b []byte) (int, int)
	walk = func(b []byte) (int, int) {
		for len(b) >= 8 {
			size, head := uint64(binary.BigEndian.Uint32(b)), uint64(8)
			typ := string(b[4:8])
			switch size {
			case 0:
				size = uint64(len(b))
			case 1:
				if len(b) < 16 {
					return 0, 0
				}
				size, head = binary.BigEndian.Uint64(b[8:16]), 16
			}
			if size < head || size > uint64(len(b)) {
				return 0, 0
			}
			body := b[head:size]
			switch typ {
			case "moov", "trak":
				if w, h := walk(body); w > 0 && h > 0 {
					return w, h
				}
			case "tkhd":
				at := 4 + 72 // version 0: after version and flags, 72 bytes to the width
				if len(body) > 0 && body[0] == 1 {
					at = 4 + 84
				}
				if len(body) >= at+8 {
					w, h := int(binary.BigEndian.Uint32(body[at:])>>16), int(binary.BigEndian.Uint32(body[at+4:])>>16)
					if w > 0 && h > 0 {
						return w, h
					}
				}
			}
			b = b[size:]
		}
		return 0, 0
	}
	return walk(data)
}

// mediaTypeOf is the media type an encrypted file is uploaded as.
func mediaTypeOf(kind string) whatsmeow.MediaType {
	switch kind {
	case "photo":
		return whatsmeow.MediaImage
	case "video":
		return whatsmeow.MediaVideo
	}
	return whatsmeow.MediaDocument
}

// clampTo400 is a thumbnail's size: the connector's clampTo400
// (msgconv/to-whatsapp.go), copied.
func clampTo400(w, h int) (int, int) {
	if w > 400 {
		h = h * 400 / w
		w = 400
	}
	if h > 400 {
		w = w * 400 / h
		h = 400
	}
	return w, h
}

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
		ms = s.m.now().UnixMilli()
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
	now := m.now()
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
			now := m.now()
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
