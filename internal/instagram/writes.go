package instagram

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

	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/util/jsontime"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Instagram's writes. Every one is a request to Meta and goes through the
// pace (and so is refused, with the block's kind, while Meta has the
// session stopped), as do the reads they need: FetchThreadID, the upload,
// the media fetch and an expired photo's refresh. No write is ever asked
// again after Meta has answered it, or when it may have reached Meta: only
// the outbox row's state moves.
//
// A send is kept in the outbox under "otid:<offline threading ID>", the ID
// Meta's echo of it carries, and that ID is the answer to `send`. It goes
// in the background, one at a time in the order made. Meta's answer (or
// its echo, whichever comes first) stores the line under Meta's message ID
// and keeps that ID as the row's line ID, which sendStatus reports.
//
//	Meta answered with the message          sent, lineID
//	nothing reached Meta, or Meta said no   failed, refused
//	  (after the text of a text-and-file went: unclear)
//	no answer in time, or no readable one   sending; at the next start unknown
//
// A send still sending at start becomes unknown and is never sent again:
// that Meta drops a second send with the same threading ID is unproven. An
// echo carrying its threading ID still settles it.
//
// The library would also send again by itself; what it does here:
//   - its HTTP layer re-POSTs a request up to five more times after a
//     transport failure or a 5xx. The two send mutations are marked NoRetry
//     (init, below), so a send is POSTed once. Reactions, read receipts and
//     mutes keep the retries: sent twice, they come to the same.
//   - on Meta's "please reload the page" answer (1357004), every mutation
//     here reloads the inbox (ReloadIndex, at most once per 15 minutes) and
//     is asked once more, outside the pace. Meta refused the first ask, so a
//     message still goes once. Turning this off would mean calling
//     GetHTTP().MakeGraphQLRequest directly; it is left as the library has it.
//   - its media upload repeats itself on an answer it cannot read, with
//     sleeps the context cannot cut short. Upload (below) is a one-shot
//     copy of it instead.

// igWriter is every write the adapter makes to Instagram, and nothing else:
// a message (SendMessage), a photo or video (SendMedia after Upload), a
// reaction (SendReaction), a read receipt when he reads (MarkRead, then
// MarkReadValidation), and a mute (MuteThread). Never edits, unsends,
// deletes, typing, pins, groups, blocking, message requests, push or
// sign-in. Upload is an adapter over GetHTTP().SendMercuryUploadRequest;
// the rest are *instameow.Client's own.
type igWriter interface {
	SendMessage(ctx context.Context, req *slidetypes.SendTextRequest) (*slidetypes.SendTextResponse, error)
	SendMedia(ctx context.Context, req *slidetypes.SendMediaRequest) (*slidetypes.SendMediaResponse, error)
	SendReaction(ctx context.Context, req *slidetypes.CreateReactionRequest) (*slidetypes.SendReactionResponse, error)
	MarkRead(ctx context.Context, req *slidetypes.MarkReadRequest) (*slidetypes.MarkReadResponse, error)
	MarkReadValidation(ctx context.Context, req *slidetypes.MarkReadRequest) (*slidetypes.MarkReadValidationResponse, error)
	MuteThread(ctx context.Context, req *slidetypes.MuteThreadRequest) (*slidetypes.MuteThreadResponse, error)
	Upload(ctx context.Context, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error)
}

// metaClient is the real client's writes (and its media downloads, in
// media.go).
type metaClient struct{ c *instameow.Client }

var (
	_ igWriter     = metaClient{}
	_ igDownloader = metaClient{}
)

func (m metaClient) SendMessage(ctx context.Context, req *slidetypes.SendTextRequest) (*slidetypes.SendTextResponse, error) {
	return m.c.SendMessage(ctx, req)
}

func (m metaClient) SendMedia(ctx context.Context, req *slidetypes.SendMediaRequest) (*slidetypes.SendMediaResponse, error) {
	return m.c.SendMedia(ctx, req)
}

func (m metaClient) SendReaction(ctx context.Context, req *slidetypes.CreateReactionRequest) (*slidetypes.SendReactionResponse, error) {
	return m.c.SendReaction(ctx, req)
}

func (m metaClient) MarkRead(ctx context.Context, req *slidetypes.MarkReadRequest) (*slidetypes.MarkReadResponse, error) {
	return m.c.MarkRead(ctx, req)
}

func (m metaClient) MarkReadValidation(ctx context.Context, req *slidetypes.MarkReadRequest) (*slidetypes.MarkReadValidationResponse, error) {
	return m.c.MarkReadValidation(ctx, req)
}

func (m metaClient) MuteThread(ctx context.Context, req *slidetypes.MuteThreadRequest) (*slidetypes.MuteThreadResponse, error) {
	return m.c.MuteThread(ctx, req)
}

// Upload uploads a file for a thread, asked once: meta.Upload, the
// library's SendMercuryUploadRequest without its retry loop.
func (m metaClient) Upload(ctx context.Context, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error) {
	return meta.Upload(ctx, m.c, threadID, media)
}

// writerFor is the writer of a client: the real one's through metaClient,
// a test's fake as it is.
func writerFor(c igClient) igWriter {
	switch c := c.(type) {
	case *instameow.Client:
		return metaClient{c}
	case igWriter:
		return c
	}
	return nil
}

// The send mutations are asked once: the library's retries would POST the
// same message again after a slow answer or a 5xx, and that Meta drops the
// copy (same offline threading ID) is unproven. GraphQLDocs is a map of
// values, so each doc is copied, flagged and stored back.
func init() {
	for _, name := range []string{"IGDirectTextSendMutation", "IGDirectMediaSendMutation"} {
		doc := graphql.GraphQLDocs[name]
		doc.NoRetry = true
		graphql.GraphQLDocs[name] = doc
	}
}

const (
	outMessage  = "message"
	outReaction = "reaction"
	otidPrefix  = "otid:"
	// writeWait is the longest a write waits for its turn and its answer.
	writeWait = 60 * time.Second
	// The largest photo and video sent.
	photoLimit = 16 << 20
	videoLimit = 16 << 20
)

var (
	errNotOnNetwork = &wire.Error{Kind: wire.NotOnNetwork}
	errRefused      = &wire.Error{Kind: wire.Refused}
	errUnclear      = &wire.Error{Kind: wire.Unclear}
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
	OTID     string `json:"otid,omitempty"`     // the threading ID of the row's message (the file's, with one)
	TextOTID string `json:"textOTID,omitempty"` // a text sent before its file: the text's own
	Target   string `json:"target,omitempty"`   // the line a reaction is to
	Emoji    string `json:"emoji,omitempty"`
}

// sendProgress is how far a send got, kept in the outbox row's message
// before the next step: a text-and-file send whose text went never sends
// the text again.
type sendProgress struct {
	TextSent   bool   `json:"textSent,omitempty"`
	TextLineID string `json:"textLineID,omitempty"`
}

// progressOf is a row's progress.
func progressOf(o store.Outgoing) sendProgress {
	var p sendProgress
	if len(o.Message) > 0 {
		_ = json.Unmarshal(o.Message, &p)
	}
	return p
}

// otidFor makes the offline threading ID of a write. Its logic is
// igconnector's getOTID (mautrix-meta v0.2610.0, pkg/igconnector/
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

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// writeGate is nil when a write may be asked now: the pace allows a request
// (else its block's kind) and he is signed in (live, catching up or
// offline; else notConnected).
func (g *Instagram) writeGate() error {
	if err := g.gate(); err != nil {
		return err
	}
	g.mu.Lock()
	s := g.state
	g.mu.Unlock()
	switch s {
	case network.Live, network.CatchingUp, network.Offline:
		return nil
	}
	return errNotConnected
}

// writableChat is the held chat a write names, and its thread key.
func (g *Instagram) writableChat(chatID string) (string, int64, error) {
	if strings.Contains(chatID, "unplaced:") {
		return "", 0, errNotOnNetwork
	}
	chat, ok := g.st.ChatFor(Name, chatID)
	if !ok {
		return "", 0, errNotFound
	}
	key, err := strconv.ParseInt(chat, 10, 64)
	if err != nil || key <= 0 {
		return "", 0, errNotOnNetwork
	}
	return chat, key, nil
}

// writeCtx is a write's context: the network's, for at most writeWait.
func (g *Instagram) writeCtx() (context.Context, context.CancelFunc) {
	g.mu.Lock()
	base := g.ctx
	g.mu.Unlock()
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, writeWait)
}

// writer is the current run's number, client and writer; the writer is nil
// when no client is signed in.
func (g *Instagram) writer() (int, igClient, igWriter) {
	gen, cli, _ := g.running()
	if cli == nil {
		return gen, nil, nil
	}
	return gen, cli, g.newWriter(cli)
}

// ask makes one request through the pace. ran reports whether it reached
// the client (raw is then the client's own error); when it did not, err is
// why (the pace's block, a wait cut short, or the run ended).
func (g *Instagram) ask(ctx context.Context, gen int, fn func(ctx context.Context) error) (ran bool, raw, err error) {
	err = g.request(ctx, gen, func(ctx context.Context) error {
		ran = true
		raw = fn(ctx)
		return raw
	})
	return ran, raw, err
}

// unsure reports whether a write that failed may still have reached Meta,
// so must never be sent again: a connection that broke after it was made
// (the body may have gone), a 5xx, no answer in time, the helper stopping
// while it waited, or an answer that could not be read or understood. Only
// a connection never made (a dial failure), Meta saying no (an error
// answer, a 4xx, a checkpoint, an ended session, a ban, a rate limit), or a
// request never asked (the caller's ran is false) is safe to send again.
func unsure(err error) bool {
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return false // nothing reached Meta
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, httpclient.ErrResponseReadFailed) || errors.Is(err, httpclient.ErrRequestFailed) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	if code := statusOf(err); code != 0 {
		return code >= 500
	}
	var er *types.ErrorResponse
	var ge *types.GraphQLError
	if errors.As(err, &er) || errors.As(err, &ge) {
		return false
	}
	switch kindOf(err) {
	case wire.Checkpoint, meta.ReconnectNeeded, meta.Banned, wire.RateLimited:
		return false
	}
	return true
}

// statusOf is the HTTP status of the library's "unexpected status" error
// ("server returned unexpected HTTP status 503", as the library formats it
// in httpclient.makeRequestOnce), or 0.
func statusOf(err error) int {
	if !errors.Is(err, httpclient.ErrUnexpectedError) {
		return 0
	}
	if errors.Is(err, httpclient.ErrRateLimited) {
		return http.StatusTooManyRequests
	}
	msg := err.Error()
	prefix := httpclient.ErrUnexpectedError.Error() + " "
	i := strings.LastIndex(msg, prefix)
	if i < 0 {
		return 0
	}
	rest := msg[i+len(prefix):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	code, _ := strconv.Atoi(rest[:end])
	return code
}

// writeErr is the error a write that is not a send returns: why it was
// never asked, unclear when it may have reached Meta, Meta pushing back
// (checkpoint, rateLimited), else refused.
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

// threadIDs are the chat's two Instagram thread IDs: the short one the
// sends, reactions, mutes and pages take, and the long one read receipts
// take. FetchThreadID is asked once per chat (by its thread key, as
// igconnector does); the answer is kept as "route.<chat>".
func (g *Instagram) threadIDs(ctx context.Context, gen int, cli igClient, chat string) (short, long string, err error) {
	if s, l, ok := strings.Cut(g.value("route."+chat), "\n"); ok && s != "" && l != "" {
		return s, l, nil
	}
	key, err := strconv.ParseInt(chat, 10, 64)
	if err != nil || key <= 0 {
		return "", "", errNotOnNetwork
	}
	var ids *instameow.ThreadIGIDs
	if err := g.request(ctx, gen, func(ctx context.Context) (err error) {
		ids, err = cli.FetchThreadID(ctx, key)
		return err
	}); err != nil {
		return "", "", err
	}
	if ids == nil || ids.ShortID == "" || ids.LongID == "" {
		return "", "", errNotFound
	}
	_ = g.keep("route."+chat, ids.ShortID+"\n"+ids.LongID)
	if _, ok := g.chatOfThread(ids.ShortID); !ok {
		_ = g.keep("thread."+ids.ShortID, chat) // its echoes name it so
	}
	return ids.ShortID, ids.LongID, nil
}

// Send keeps the message in the outbox under its offline threading ID,
// returns that ID at once, and sends it in the background. A send repeated
// with the caller's key (m.ClientID) of one kept and not failed gives that
// send's ID and sends nothing more. One that failed after its text went
// (a text-and-file send) is the same send, and goes on with the file alone.
// Any other failed send is done, and the key makes a new send.
func (g *Instagram) Send(ctx context.Context, m network.Outgoing) (string, error) {
	if m.ClientID != "" {
		if chat, ok := g.st.ChatFor(Name, m.ChatID); ok {
			o, held, err := g.keyed(m.ClientID, chat)
			if err != nil {
				return "", err
			}
			if held && o.State == store.OutFailed {
				return g.resume(o)
			}
			if held {
				return o.ID, nil
			}
		}
	}
	if err := g.writeGate(); err != nil {
		return "", err
	}
	if m.Text == "" && m.File == "" {
		return "", errBadRequest
	}
	chat, _, err := g.writableChat(m.ChatID)
	if err != nil {
		return "", err
	}
	req := sendRequest{Text: m.Text, Kind: "text"}
	if !strings.HasPrefix(m.ReplyTo, "beeper:") {
		req.ReplyTo = m.ReplyTo // a line only Beeper's copy holds has no ID Meta knows
	}
	if m.File != "" {
		name := cmp.Or(m.FileName, filepath.Base(m.File))
		kind, mimetype, err := fileKind(m.File)
		if err != nil {
			return "", err
		}
		req.File, req.FileName, req.Kind, req.Mimetype = m.File, name, kind, mimetype
		if req.Text != "" {
			req.TextOTID = itoa(methods.GenerateEpochID()) // made first: the text goes first
		}
	}
	otid := otidFor(m.ClientID)
	if _, taken, err := g.st.Outgoing(Name, otidPrefix+itoa(otid)); err != nil {
		return "", err
	} else if taken {
		otid = methods.GenerateEpochID() // the key's number is a done send's
	}
	req.OTID = itoa(otid)
	body, err := json.Marshal(req)
	if err != nil {
		return "", errInternal
	}
	id := otidPrefix + req.OTID
	if err := g.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: m.ClientID,
		MadeMs: g.clock.Now().UnixMilli(), Request: body}); err != nil {
		if raced, held, rerr := g.keyed(m.ClientID, chat); rerr == nil && held {
			return raced.ID, nil // another call with this key kept it first, and sends it
		}
		return "", err
	}
	g.enqueue(id)
	return id, nil
}

// SendFor is the ID of the send kept under the caller's key (a failed one
// included), or "" for none.
func (g *Instagram) SendFor(clientID string) (string, error) {
	o, ok, err := g.st.OutgoingClient(Name, clientID)
	if err != nil || !ok {
		return "", err
	}
	return o.ID, nil
}

// keyed is the send the caller's key still holds: one sending, sent or
// unknown, or one that failed after part of it went (its text). A failed
// send that sent nothing frees the key. A key naming a send to another chat
// is a bad request.
func (g *Instagram) keyed(client, chat string) (store.Outgoing, bool, error) {
	o, ok, err := g.st.OutgoingClient(Name, client)
	if err != nil || !ok {
		return store.Outgoing{}, false, err
	}
	if o.State == store.OutFailed && !progressOf(o).TextSent {
		return store.Outgoing{}, false, nil
	}
	if o.ChatID != chat || o.Kind != outMessage {
		return store.Outgoing{}, false, errBadRequest
	}
	return o, true, nil
}

// resume sends again what is left of a send that failed after its text
// went: the file, under the same row and ID. Once in line it is not put in
// line again.
func (g *Instagram) resume(o store.Outgoing) (string, error) {
	if err := g.writeGate(); err != nil {
		return "", err
	}
	g.smu.Lock()
	cur, ok, err := g.st.Outgoing(Name, o.ID)
	if err != nil || !ok {
		g.smu.Unlock()
		return "", cmp.Or[error](err, errNotFound)
	}
	if cur.State != store.OutFailed {
		g.smu.Unlock()
		return cur.ID, nil // already going again
	}
	if err := g.st.SetOutgoingState(Name, cur.ID, store.OutSending, ""); err != nil {
		g.smu.Unlock()
		return "", err
	}
	g.smu.Unlock()
	g.enqueue(cur.ID)
	return cur.ID, nil
}

// SendState is what became of a send: sending, sent, failed with its kind,
// or unknown (cut off by a restart, or never held).
func (g *Instagram) SendState(messageID string) network.SendState {
	o, ok, err := g.st.Outgoing(Name, messageID)
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
func (g *Instagram) forgetCutOff() {
	rows, err := g.st.OutgoingSending(Name)
	if err != nil {
		return
	}
	for _, o := range rows {
		_ = g.st.SetOutgoingState(Name, o.ID, store.OutUnknown, "")
	}
}

// fileKind is how a file goes: a JPEG or PNG photo, or an MP4 or QuickTime
// video, each up to 16 MB. Anything else is unsupported.
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

// enqueue puts a send in line; one goroutine sends them, in order.
func (g *Instagram) enqueue(id string) {
	g.busy.Add(1)
	g.smu.Lock()
	g.queue = append(g.queue, id)
	if g.draining {
		g.smu.Unlock()
		return
	}
	g.draining = true
	g.smu.Unlock()
	g.mu.Lock()
	ok := g.started && !g.stopped
	if ok {
		g.work.Add(1)
	}
	ctx := g.ctx
	g.mu.Unlock()
	if !ok {
		g.drain(nil)
		return
	}
	go func() {
		defer g.work.Done()
		g.drain(ctx)
	}()
}

// drain sends what is in line until none is left. A send whose turn comes
// after the helper began stopping never went: refused.
func (g *Instagram) drain(ctx context.Context) {
	for {
		g.smu.Lock()
		if len(g.queue) == 0 {
			g.draining = false
			g.smu.Unlock()
			return
		}
		id := g.queue[0]
		g.queue = g.queue[1:]
		g.smu.Unlock()
		if ctx == nil || ctx.Err() != nil {
			g.fail(id, wire.Refused)
		} else {
			g.deliver(id)
		}
		g.busy.Add(-1)
	}
}

// fail marks a write failed with kind.
func (g *Instagram) fail(id string, kind wire.Kind) {
	_ = g.st.SetOutgoingState(Name, id, store.OutFailed, string(kind))
}

// settle marks a write sent, keeping the ID of the line it became.
func (g *Instagram) settle(id, lineID string) {
	if lineID != "" {
		_ = g.st.SetOutgoingLine(Name, id, lineID)
	}
	_ = g.st.SetOutgoingState(Name, id, store.OutSent, "")
}

// deliver sends one send, if it is still sending: the thread's IDs, the
// file uploaded (before any message, so a failed upload sends nothing), the
// text as its own message when there is a file too, then the message.
func (g *Instagram) deliver(id string) {
	o, ok, err := g.st.Outgoing(Name, id)
	if err != nil || !ok || o.State != store.OutSending {
		return
	}
	var req sendRequest
	if json.Unmarshal(o.Request, &req) != nil || req.OTID == "" {
		g.fail(id, wire.Internal)
		return
	}
	ctx, cancel := g.writeCtx()
	defer cancel()
	gen, cli, w := g.writer()
	if w == nil {
		g.fail(id, wire.Refused)
		return
	}
	key, _ := strconv.ParseInt(o.ChatID, 10, 64)
	short, _, err := g.threadIDs(ctx, gen, cli, o.ChatID)
	if err != nil {
		g.fail(id, wire.Refused)
		return
	}

	var data []byte
	var attachment string
	if req.File != "" {
		if data, err = readSendFile(req); err != nil {
			g.fail(id, wire.KindOf(err))
			return
		}
		var resp *types.MercuryUploadResponse
		if _, _, err := g.ask(ctx, gen, func(ctx context.Context) (err error) {
			resp, err = w.Upload(ctx, key, &httpclient.MercuryUploadMedia{Filename: req.FileName, MimeType: req.Mimetype, MediaData: data})
			return err
		}); err != nil {
			g.fail(id, wire.Refused) // an upload is not a message: nothing was sent
			return
		}
		var fbid int64
		if resp != nil && resp.Payload.RealMetadata != nil {
			fbid = resp.Payload.RealMetadata.GetFbId()
		}
		if fbid == 0 {
			g.fail(id, wire.Refused)
			return
		}
		attachment = itoa(fbid)
	}

	reply := req.ReplyTo
	progress := progressOf(o)
	textSent := progress.TextSent
	if req.TextOTID != "" && !textSent {
		msg, ok, maybe := g.sendOne(ctx, gen, func(ctx context.Context) (slidetypes.SentMessage, error) {
			return sendText(ctx, w, short, req.TextOTID, req.Text, reply)
		})
		if !ok {
			if !maybe {
				g.fail(id, wire.Refused)
			}
			return // maybe: it stays sending
		}
		progress = sendProgress{TextSent: true, TextLineID: g.storeSent(o.ChatID, msg, req.Text, "text", reply, nil)}
		b, _ := json.Marshal(progress)
		if err := g.st.SetOutgoingMessage(Name, id, b); err != nil {
			return // the text went but that is not kept: it stays sending, never sent again
		}
		textSent = true
	}
	if textSent {
		reply = "" // the reply went with the text
	}

	var msg slidetypes.SentMessage
	var maybe bool
	if req.File != "" {
		msg, ok, maybe = g.sendOne(ctx, gen, func(ctx context.Context) (slidetypes.SentMessage, error) {
			r := &slidetypes.SendMediaRequest{AttachmentFBID: attachment, ThreadID: short, OfflineThreadingID: req.OTID}
			if reply != "" {
				r.ReplyToMessageID = &reply
			}
			resp, err := w.SendMedia(ctx, r)
			if err != nil || resp == nil {
				return slidetypes.SentMessage{}, cmp.Or[error](err, errInternal)
			}
			return resp.GetMessage(), nil
		})
	} else {
		msg, ok, maybe = g.sendOne(ctx, gen, func(ctx context.Context) (slidetypes.SentMessage, error) {
			return sendText(ctx, w, short, req.OTID, req.Text, reply)
		})
	}
	switch {
	case ok:
	case maybe:
		return // it stays sending
	case textSent:
		g.fail(id, wire.Unclear) // its text went, the file did not: the key's next send is the file alone
		return
	default:
		g.fail(id, wire.Refused)
		return
	}
	text := req.Text
	if req.File != "" {
		text = ""
	}
	lineID := g.storeSent(o.ChatID, msg, text, req.Kind, reply, data)
	g.settle(id, lineID)
	g.changed()
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

// sendOne asks Meta to send one message. ok: Meta answered with it.
// Otherwise maybe reports whether it may have reached Meta all the same.
func (g *Instagram) sendOne(ctx context.Context, gen int, fn func(ctx context.Context) (slidetypes.SentMessage, error)) (msg slidetypes.SentMessage, ok, maybe bool) {
	ran, raw, err := g.ask(ctx, gen, func(ctx context.Context) (err error) {
		msg, err = fn(ctx)
		return err
	})
	if err == nil {
		return msg, true, false
	}
	return msg, false, ran && unsure(raw)
}

// sendText is one text message, as igconnector's message converter builds
// it (pkg/msgconv/igconv/from-matrix.go): to the thread's short ID, with
// the web inbox's attribution and no mentions.
func sendText(ctx context.Context, w igWriter, thread, otid, text, reply string) (slidetypes.SentMessage, error) {
	attribution := "igd_web_chat_tab:in_thread"
	req := &slidetypes.SendTextRequest{
		IGThreadIGID:       &thread,
		OfflineThreadingID: otid,
		Text:               slidetypes.SensitiveString{Value: text},
		SendAttribution:    &attribution,
	}
	if reply != "" {
		req.ReplyToMessageID = &reply
	}
	resp, err := w.SendMessage(ctx, req)
	if err != nil || resp == nil {
		return slidetypes.SentMessage{}, cmp.Or[error](err, errInternal)
	}
	return resp.GetMessage(), nil
}

// storeSent stores the line a message Meta took became, his, under Meta's
// message ID, and is that ID ("" when Meta gave none: its echo will). A
// photo he sent is kept so it opens at once.
func (g *Instagram) storeSent(chat string, msg slidetypes.SentMessage, text, kind, reply string, photo []byte) string {
	lineID := cmp.Or(msg.ID, msg.MessageID)
	if lineID == "" {
		return ""
	}
	ts := msg.TimestampMS.UnixMilli()
	if ts <= 0 {
		ts = g.clock.Now().UnixMilli()
	}
	sender, _ := g.personOf(g.viewer())
	shown := false
	line := store.Message{
		ID: lineID, ChatID: chat, SenderID: cmp.Or(sender, g.self()), FromMe: true, TimeMs: ts,
		Text: text, Kind: kind, Hidden: &shown, ReplyTo: reply, Source: store.SourceLive,
	}
	switch kind {
	case "photo":
		line.Media = []store.Media{{Kind: "photo", HasFile: true}}
	case "video":
		line.Media = []store.Media{{Kind: "video"}}
	}
	if _, err := g.st.PutMessage(Name, line); err == nil && kind == "photo" && photo != nil {
		g.keepMedia(chat, lineID, photo)
	}
	return lineID
}

// echoed settles the send his own message's echo carries the threading ID
// of: whatever became of it here (sending, unknown, even failed), Meta has
// it, as the line lineID.
func (g *Instagram) echoed(m *slidetypes.Message, lineID string) {
	if m == nil || m.OfflineThreadingID == "" || lineID == "" {
		return
	}
	if _, mine := g.personOf(m.SenderFBID); !mine {
		return
	}
	o, ok, err := g.st.Outgoing(Name, otidPrefix+m.OfflineThreadingID)
	if err != nil || !ok || o.Kind != outMessage || (o.State == store.OutSent && o.LineID == lineID) {
		return
	}
	g.settle(o.ID, lineID)
}

// React sends his reaction to a held line (ReactionStatusCreated), or, with
// an empty emoji, removes his (ReactionStatusDeleted, naming it). A
// transaction the outbox already holds sends nothing, unless it failed
// before Meta had it. A line only Beeper's copy holds is notOnNetwork.
func (g *Instagram) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	if err := g.writeGate(); err != nil {
		return err
	}
	if strings.HasPrefix(messageID, "beeper:") {
		return errNotOnNetwork
	}
	chat, _, err := g.writableChat(chatID)
	if err != nil {
		return err
	}
	prev, held, err := g.st.OutgoingTxn(Name, transactionID)
	if err != nil {
		return err
	}
	if held && prev.State != store.OutFailed {
		return nil
	}
	if _, ok, err := g.st.Line(Name, chat, messageID); err != nil {
		return err
	} else if !ok {
		return errNotFound
	}
	o := prev
	if held {
		if err := g.st.SetOutgoingState(Name, o.ID, store.OutSending, ""); err != nil {
			return err
		}
	} else {
		body, err := json.Marshal(sendRequest{Target: messageID, Emoji: emoji})
		if err != nil {
			return errInternal
		}
		o = store.Outgoing{ID: "reaction:" + itoa(methods.GenerateEpochID()), ChatID: chat, Kind: outReaction,
			Txn: transactionID, MadeMs: g.clock.Now().UnixMilli(), Request: body}
		if err := g.st.PutOutgoing(Name, o); err != nil {
			if _, raced, _ := g.st.OutgoingTxn(Name, transactionID); raced {
				return nil // another call with this transaction kept it first, and sends it
			}
			return err
		}
	}
	return g.react(o, emoji)
}

// react sends one kept reaction, with the emoji asked for now (a failed
// one tried again may be another), and settles it.
func (g *Instagram) react(o store.Outgoing, emoji string) error {
	var req sendRequest
	if json.Unmarshal(o.Request, &req) != nil {
		g.fail(o.ID, wire.Internal)
		return errInternal
	}
	ctx, cancel := g.writeCtx()
	defer cancel()
	status := slidetypes.ReactionStatusCreated
	chosen := emoji
	if emoji == "" {
		own, ok, err := g.st.OwnReaction(Name, o.ChatID, req.Target)
		if err != nil {
			g.fail(o.ID, wire.Refused)
			return err
		}
		if !ok {
			g.settle(o.ID, "") // he has none to remove: nothing is asked
			return nil
		}
		status, emoji = slidetypes.ReactionStatusDeleted, own
	}
	gen, cli, w := g.writer()
	if w == nil {
		g.fail(o.ID, wire.Refused)
		return errNotConnected
	}
	short, _, err := g.threadIDs(ctx, gen, cli, o.ChatID)
	if err != nil {
		g.fail(o.ID, wire.Refused)
		return err
	}
	ran, raw, err := g.ask(ctx, gen, func(ctx context.Context) error {
		_, err := w.SendReaction(ctx, &slidetypes.CreateReactionRequest{Input: slidetypes.ReactionInput{
			Emoji: emoji, ItemID: "", MessageID: req.Target, ReactionStatus: status, ThreadID: short,
		}})
		return err
	})
	if err != nil {
		if ran && unsure(raw) {
			return errUnclear // it stays sending
		}
		g.fail(o.ID, wire.Refused)
		return writeErr(ran, raw, err)
	}
	sender, _ := g.personOf(g.viewer())
	_ = g.st.SetReaction(Name, o.ChatID, req.Target, store.Reaction{Emoji: chosen, SenderID: sender, FromMe: true})
	g.settle(o.ID, "")
	g.changed()
	return nil
}

// MarkRead tells Meta he has read the chat up to and including through (the
// newest incoming line when empty): MarkRead, then MarkReadValidation, both
// naming the newest unread line Meta knows at or before it. Nothing newer
// is marked. With no such line nothing is asked; his marker moves either
// way.
func (g *Instagram) MarkRead(ctx context.Context, chatID, through string) error {
	if err := g.writeGate(); err != nil {
		return err
	}
	chat, _, err := g.writableChat(chatID)
	if err != nil {
		return err
	}
	var upTo store.Line
	var ok bool
	if through == "" {
		if upTo, ok, err = g.st.NewestIncoming(Name, chat); err == nil && !ok {
			return nil // nothing to read
		}
	} else if upTo, ok, err = g.st.Line(Name, chat, through); err == nil && !ok {
		return errNotFound
	}
	if err != nil {
		return err
	}
	lines, err := g.st.Unread(Name, chat, upTo.TimeMs)
	if err != nil {
		return err
	}
	var target *store.Line
	for i := len(lines) - 1; i >= 0; i-- {
		if !strings.HasPrefix(lines[i].ID, "beeper:") {
			target = &lines[i]
			break
		}
	}
	if target != nil {
		wctx, cancel := g.writeCtx()
		defer cancel()
		gen, cli, w := g.writer()
		if w == nil {
			return errNotConnected
		}
		_, long, err := g.threadIDs(wctx, gen, cli, chat)
		if err != nil {
			return err
		}
		md := slidetypes.MarkReadMetadata{IGThreadIGID: long}
		item := ""
		if ran, raw, err := g.ask(wctx, gen, func(ctx context.Context) error {
			_, err := w.MarkRead(ctx, &slidetypes.MarkReadRequest{Metadata: md, Data: slidetypes.MarkReadData{MessageID: target.ID, ItemID: &item}})
			return err
		}); err != nil {
			return writeErr(ran, raw, err)
		}
		if ran, raw, err := g.ask(wctx, gen, func(ctx context.Context) error {
			_, err := w.MarkReadValidation(ctx, &slidetypes.MarkReadRequest{Metadata: md, Data: slidetypes.MarkReadData{
				MessageID: target.ID, MessageTimestampMS: jsontime.UnixMilliString{Time: time.UnixMilli(target.TimeMs)}}})
			return err
		}); err != nil {
			return writeErr(ran, raw, err)
		}
	}
	ms := upTo.TimeMs
	if err := g.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &ms, ReadForwardOnly: true}); err != nil {
		return err
	}
	g.changed()
	return nil
}

// MarkUnread shows the chat's newest incoming line unread. Instagram has no
// such write, so it is kept here only and nothing is asked of Meta.
func (g *Instagram) MarkUnread(ctx context.Context, chatID string) error {
	chat, _, err := g.writableChat(chatID)
	if err != nil {
		return err
	}
	if err := g.markUnreadHere(chat); err != nil {
		return err
	}
	g.changed()
	return nil
}

// SetMuted mutes the chat for ever (MuteSeconds -1) or unmutes it (0).
func (g *Instagram) SetMuted(ctx context.Context, chatID string, muted bool) error {
	if err := g.writeGate(); err != nil {
		return err
	}
	chat, _, err := g.writableChat(chatID)
	if err != nil {
		return err
	}
	wctx, cancel := g.writeCtx()
	defer cancel()
	gen, cli, w := g.writer()
	if w == nil {
		return errNotConnected
	}
	short, _, err := g.threadIDs(wctx, gen, cli, chat)
	if err != nil {
		return err
	}
	seconds := 0
	if muted {
		seconds = -1
	}
	if ran, raw, err := g.ask(wctx, gen, func(ctx context.Context) error {
		_, err := w.MuteThread(ctx, &slidetypes.MuteThreadRequest{ThreadID: short, MuteSeconds: seconds, OfflineThreadingID: itoa(otidFor(""))})
		return err
	}); err != nil {
		return writeErr(ran, raw, err)
	}
	if err := g.st.PutChat(Name, store.ChatInfo{ID: chat, Muted: &muted}); err != nil {
		return err
	}
	g.changed()
	return nil
}
