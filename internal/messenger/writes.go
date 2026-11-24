package messenger

import (
	"cmp"
	"context"
	"errors"
	_ "image/jpeg" // photo sizes, for an encrypted photo's message
	_ "image/png"
	"net"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow"
	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	watypes "go.mau.fi/whatsmeow/types"

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
	gen, cli, e := m.runs.Gen, m.cli, m.e2ee
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
