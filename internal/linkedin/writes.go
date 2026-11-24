package linkedin

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"
	"go.mau.fi/util/jsontime"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// LinkedIn's writes: a send (with its upload), a reaction, a mark read or
// unread, and a mute, which is Pllum's own and never reaches LinkedIn. Each
// one he asks for goes at once, never held back by the background's pace,
// and never waits on or wakes the background work. Every request goes
// through request, so none goes during a pause, and LinkedIn pushing back on
// one (a 403, 429 or 999) pauses everything, as a read's would. A write is
// asked only while he is signed in: live, catching up or offline (else
// notConnected), and not paused (else rateLimited).
//
// A send is kept in the outbox under "li-pending:<origin token>", a new UUID
// that is also the originToken LinkedIn is given, and that ID is the answer
// to `send`. It goes in the background, one at a time in the order made: its
// file first (UploadMedia), then the message (SendMessage), which is asked
// once, ever. The patched library never retries it (PLLUM-CHANGES.md, change
// 2), and nothing here asks again for that origin token, after an error or a
// restart: LinkedIn is told not to dedupe by it, so a second ask could be a
// second message.
//
//	LinkedIn answered with the message     sent, lineID = its URN
//	a 400, 404 or 422                      failed, refused
//	a 403, 429 or 999 (a pause)            failed, rateLimited
//	nothing was asked (paused, signed out,
//	  the upload failed, the file gone)    failed, with why
//	anything else after it was asked (the
//	  network, a 502 to 504, no answer)    unknown
//
// Pllum reads unknown as still sending, and its outbox settles it by the
// chat's lines. A send still sending at start is unknown too: the helper died
// while it was being asked.

const (
	outMessage  = "message"
	outReaction = "reaction"
	// pendingPrefix starts a send's ID: "li-pending:<origin token>".
	pendingPrefix  = "li-pending:"
	reactionPrefix = "li-reaction:"
	// fileLimit is the largest file sent.
	fileLimit = 20 << 20
	// writeWait is the longest a write waits for its answer.
	writeWait = 60 * time.Second
	// participantPrefix makes a profile ID a messaging participant's URN.
	participantPrefix = "urn:li:msg_messagingParticipant:urn:li:fsd_profile"
)

var (
	errNotFound     = &wire.Error{Kind: wire.NotFound}
	errNotOnNetwork = &wire.Error{Kind: wire.NotOnNetwork}
	errRefused      = &wire.Error{Kind: wire.Refused}
	errTooLarge     = &wire.Error{Kind: wire.TooLarge}
)

// sendRequest is what the outbox keeps of a write.
type sendRequest struct {
	Text        string `json:"text,omitempty"`
	ReplyTo     string `json:"replyTo,omitempty"`
	File        string `json:"file,omitempty"`
	FileName    string `json:"fileName,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	OriginToken string `json:"originToken,omitempty"`
	Target      string `json:"target,omitempty"` // the line a reaction is to
	Emoji       string `json:"emoji,omitempty"`
}

// writeGate is the run a write may be asked on now: he is signed in (live,
// catching up or offline, with a client; else notConnected) and LinkedIn is
// not paused (else rateLimited).
func (l *LinkedIn) writeGate() (gen int, cli liClient, err error) {
	l.mu.Lock()
	s, gen, cli := l.state, l.runs.Gen, l.cli
	ok := l.started && !l.stopped
	l.mu.Unlock()
	switch s {
	case network.Live, network.CatchingUp, network.Offline:
	default:
		return 0, nil, errNotConnected
	}
	if _, blocked := l.pause.blocked(); blocked {
		return 0, nil, errRateLimited
	}
	if cli == nil || !ok {
		return 0, nil, errNotConnected
	}
	return gen, cli, nil
}

// writableChat is the held LinkedIn conversation a write to LinkedIn names.
// A chat only Beeper's copy holds, or one LinkedIn deleted, is not on
// LinkedIn.
func (l *LinkedIn) writableChat(chatID string) (string, error) {
	if strings.Contains(chatID, "unplaced:") {
		return "", errNotOnNetwork
	}
	chat, ok := l.st.ChatFor(Name, chatID)
	if !ok {
		return "", errNotFound
	}
	if !isConversation(chat) {
		return "", errNotOnNetwork
	}
	if c, ok, err := l.st.Chat(Name, chat); err != nil {
		return "", err
	} else if ok && c.ReadOnly {
		return "", errNotOnNetwork
	}
	return chat, nil
}

// writeCtx is a write's context: ctx, silent, for at most writeWait.
func writeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(silent(ctx), writeWait)
}

// ask makes one request of run gen he asked for (never paced). ran reports
// whether it reached the client (raw is then the client's own error); when
// it did not, err is why (a pause, or the run ended).
func (l *LinkedIn) ask(ctx context.Context, gen int, fn func(ctx context.Context) error) (ran bool, raw, err error) {
	err = l.request(ctx, gen, false, func(ctx context.Context) error {
		ran = true
		raw = fn(ctx)
		return raw
	})
	return ran, raw, err
}

// statusOf is LinkedIn's answer's status in err, or 0.
func statusOf(err error) int {
	var re *linkedingo.ResponseError
	if errors.As(err, &re) && re != nil {
		return re.StatusCode
	}
	return 0
}

// refusedStatus reports whether LinkedIn's answer says it took nothing: a
// 400, 404 or 422.
func refusedStatus(raw error) bool {
	switch statusOf(raw) {
	case 400, 404, 422:
		return true
	}
	return false
}

// failKind is the kind a write that failed with nothing changed ends with:
// why it was never asked, rateLimited for a pause, refused for LinkedIn's
// 4xx, else the request's kind (offline, timeout, notConnected, internal).
func failKind(ran bool, raw, err error) wire.Kind {
	kind := wire.KindOf(err)
	switch {
	case !ran, kind == wire.RateLimited:
		return kind
	case statusOf(raw) >= 400 && statusOf(raw) < 500:
		return wire.Refused
	}
	return kind
}

// writeErr is the error a write that failed returns.
func writeErr(ran bool, raw, err error) error {
	return &wire.Error{Kind: failKind(ran, raw, err)}
}

// Send keeps the message in the outbox under "li-pending:<origin token>",
// returns that ID at once, and sends it in the background. A send repeated
// with the caller's key (m.ClientID) of one kept and not failed gives that
// send's ID and sends nothing more; a failed one is done, and the key makes
// a new send. A file over 20 MB is tooLarge before anything is kept.
func (l *LinkedIn) Send(ctx context.Context, m network.Outgoing) (string, error) {
	if m.ClientID != "" {
		if chat, ok := l.st.ChatFor(Name, m.ChatID); ok {
			o, held, err := l.keyed(m.ClientID, chat)
			if err != nil {
				return "", err
			}
			if held {
				return o.ID, nil
			}
		}
	}
	if _, _, err := l.writeGate(); err != nil {
		return "", err
	}
	if m.Text == "" && m.File == "" {
		return "", errBadRequest
	}
	chat, err := l.writableChat(m.ChatID)
	if err != nil {
		return "", err
	}
	req := sendRequest{Text: m.Text, OriginToken: uuid.NewString()}
	if strings.HasPrefix(m.ReplyTo, linePrefix) {
		req.ReplyTo = m.ReplyTo // a line only Beeper's copy holds has no URN LinkedIn knows
	}
	if m.File != "" {
		req.File = m.File
		req.FileName = cmp.Or(m.FileName, filepath.Base(m.File))
		if req.ContentType, err = fileType(m.File, req.FileName); err != nil {
			return "", err
		}
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", errInternal
	}
	id := pendingPrefix + req.OriginToken
	if err := l.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: m.ClientID,
		MadeMs: l.clock.Now().UnixMilli(), Request: body}); err != nil {
		if raced, held, rerr := l.keyed(m.ClientID, chat); rerr == nil && held {
			return raced.ID, nil // another call with this key kept it first, and sends it
		}
		return "", err
	}
	l.outbox.Enqueue(id)
	return id, nil
}

// keyed is the send the caller's key still holds: one sending, sent or
// unknown. A failed send frees the key. A key naming a send to another chat
// is a bad request.
func (l *LinkedIn) keyed(client, chat string) (store.Outgoing, bool, error) {
	if client == "" {
		return store.Outgoing{}, false, nil
	}
	o, ok, err := l.st.OutgoingClient(Name, client)
	if err != nil || !ok || o.State == store.OutFailed {
		return store.Outgoing{}, false, err
	}
	if o.ChatID != chat || o.Kind != outMessage {
		return store.Outgoing{}, false, errBadRequest
	}
	return o, true, nil
}

// SendFor is the ID of the send kept under the caller's key (a failed one
// included), or "" for none.
func (l *LinkedIn) SendFor(clientID string) (string, error) {
	o, ok, err := l.st.OutgoingClient(Name, clientID)
	if err != nil || !ok {
		return "", err
	}
	return o.ID, nil
}

// SendState is what became of a send: sending, sent, failed with its kind,
// or unknown (asked with no answer, cut off by a restart, or never held).
// Its lineID is the server's to read from the outbox.
func (l *LinkedIn) SendState(messageID string) network.SendState {
	o, ok, err := l.st.Outgoing(Name, messageID)
	if err != nil || !ok {
		return network.SendState{State: network.SendUnknown}
	}
	switch o.State {
	case store.OutSent:
		return network.SendState{State: network.SendSent}
	case store.OutFailed:
		return network.SendState{State: network.SendFailed, Kind: wire.Kind(o.Fail)}
	case store.OutSending:
		return network.SendState{State: network.SendSending}
	}
	return network.SendState{State: network.SendUnknown}
}

// forgetCutOff makes every write still sending unknown: it may have reached
// LinkedIn before the helper stopped, so it is never sent again.
func (l *LinkedIn) forgetCutOff() {
	rows, err := l.st.OutgoingSending(Name)
	if err != nil {
		return
	}
	for _, o := range rows {
		_ = l.st.SetOutgoingState(Name, o.ID, store.OutUnknown, "")
	}
}

// fileType is how a file goes: its content type, a JPEG or PNG by its bytes,
// anything else by its name's extension (else its bytes). Over 20 MB is
// tooLarge.
func fileType(path, name string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errNotFound
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() == 0 {
		return "", errBadRequest
	}
	if fi.Size() > fileLimit {
		return "", errTooLarge
	}
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	sniffed := bare(http.DetectContentType(head[:n]))
	if isPhoto(sniffed) {
		return sniffed, nil
	}
	if t := bare(mime.TypeByExtension(filepath.Ext(name))); t != "" {
		return t, nil
	}
	return sniffed, nil
}

// bare is a content type without its parameters.
func bare(ct string) string {
	if t, _, err := mime.ParseMediaType(ct); err == nil {
		return t
	}
	return ""
}

// isPhoto reports whether a content type goes as a photo.
func isPhoto(ct string) bool { return ct == "image/jpeg" || ct == "image/png" }

// uploadType is how LinkedIn takes a file of content type ct.
func uploadType(ct string) linkedingo.MediaUploadType {
	if isPhoto(ct) {
		return linkedingo.MediaUploadTypePhotoAttachment
	}
	return linkedingo.MediaUploadTypeFileAttachment
}

// readSendFile reads a send's file as it is now.
func readSendFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errNotFound
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, fileLimit+1))
	if err != nil {
		return nil, errNotFound
	}
	if len(data) > fileLimit {
		return nil, errTooLarge
	}
	return data, nil
}

// beginSends starts the goroutine that sends what is in line (the
// outbox's Begin), beside (never through) the background work: counted in
// the network's work, under the network's context; false once it has
// begun stopping (each send in line then fails notConnected: it was never
// asked).
func (l *LinkedIn) beginSends() (context.Context, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ok := l.started && !l.stopped
	if ok {
		l.work.Add(1)
	}
	return l.ctx, ok
}

// fail marks a write failed with kind.
func (l *LinkedIn) fail(id string, kind wire.Kind) {
	_ = l.st.SetOutgoingState(Name, id, store.OutFailed, string(kind))
}

// settle marks a write sent, keeping the ID of the line it became.
func (l *LinkedIn) settle(id, lineID string) {
	if lineID != "" {
		_ = l.st.SetOutgoingLine(Name, id, lineID)
	}
	_ = l.st.SetOutgoingState(Name, id, store.OutSent, "")
}

// running is the run in force and its client (nil when none).
func (l *LinkedIn) running() (int, liClient) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.runs.Gen, l.cli
}

// deliver sends one send, if it is still sending: the file uploaded first
// (a failed upload sends nothing), then the message, asked once.
func (l *LinkedIn) deliver(base context.Context, id string) {
	o, ok, err := l.st.Outgoing(Name, id)
	if err != nil || !ok || o.State != store.OutSending {
		return
	}
	var req sendRequest
	if json.Unmarshal(o.Request, &req) != nil || req.OriginToken == "" {
		l.fail(id, wire.Internal)
		return
	}
	gen, cli := l.running()
	if cli == nil {
		kind := wire.NotConnected
		if _, blocked := l.pause.blocked(); blocked {
			kind = wire.RateLimited
		}
		l.fail(id, kind)
		return
	}
	ctx, cancel := writeCtx(base)
	defer cancel()

	var rc []linkedingo.SendRenderContent
	var data []byte
	if req.File != "" {
		if data, err = readSendFile(req.File); err != nil {
			l.fail(id, wire.KindOf(err))
			return
		}
		var asset linkedingo.URN
		ran, raw, err := l.ask(ctx, gen, func(ctx context.Context) (err error) {
			asset, err = cli.UploadMedia(ctx, uploadType(req.ContentType), req.FileName, req.ContentType, len(data), bytes.NewReader(data))
			return err
		})
		if err != nil {
			l.fail(id, failKind(ran, raw, err)) // an upload is not a message: nothing was sent
			return
		}
		if asset.IsEmpty() {
			l.fail(id, wire.Refused)
			return
		}
		rc = append(rc, linkedingo.SendRenderContent{File: &linkedingo.SendFile{
			AssetURN: asset, ByteSize: len(data), MediaType: req.ContentType, Name: req.FileName,
		}})
	}
	if req.ReplyTo != "" {
		if r, ok := l.repliedTo(o.ChatID, req.ReplyTo); ok {
			rc = append(rc, r)
		}
	}

	var resp *linkedingo.MessageSentResponse
	ran, raw, err := l.ask(ctx, gen, func(ctx context.Context) (err error) {
		resp, err = cli.SendMessage(ctx, linkedingo.NewURN(o.ChatID), linkedingo.SendMessageBody{Text: req.Text}, rc, req.OriginToken)
		return err
	})
	switch {
	case err == nil:
	case !ran:
		l.fail(id, wire.KindOf(err)) // never asked: a pause, or the run ended
		return
	case wire.KindOf(err) == wire.RateLimited:
		l.fail(id, wire.RateLimited) // LinkedIn pushed back, and the pause has begun
		return
	case refusedStatus(raw):
		l.fail(id, wire.Refused)
		return
	default:
		// It may have reached LinkedIn: never asked again.
		_ = l.st.SetOutgoingState(Name, id, store.OutUnknown, "")
		return
	}
	lineID := l.storeSent(o.ChatID, resp, req, data)
	l.settle(id, lineID)
	l.changed()
}

// repliedTo is the original a reply carries, built from the stored line.
func (l *LinkedIn) repliedTo(chat, msgID string) (linkedingo.SendRenderContent, bool) {
	line, ok, err := l.st.Line(Name, chat, msgID)
	if err != nil || !ok {
		return linkedingo.SendRenderContent{}, false
	}
	sender := line.SenderID
	if sender == "" && line.FromMe {
		sender = l.self()
	}
	if sender == "" {
		return linkedingo.SendRenderContent{}, false
	}
	return linkedingo.SendRenderContent{RepliedMessageContent: &linkedingo.SendRepliedMessage{
		OriginalSenderURN:  linkedingo.NewURN(sender).WithPrefix(participantPrefix),
		OriginalSendAt:     jsontime.UM(time.UnixMilli(line.TimeMs)),
		OriginalMessageURN: linkedingo.NewURN(line.ID),
		MessageBody:        linkedingo.AttributedText{Text: line.Text},
	}}, true
}

// storeSent stores the message LinkedIn answered a send with, his, live,
// under its URN, and is that URN ("" when the answer has none). When his
// own message came on the stream first, that line is kept as it is: it is
// the same row. A photo he sent is kept so it opens without a download.
func (l *LinkedIn) storeSent(chat string, resp *linkedingo.MessageSentResponse, req sendRequest, data []byte) string {
	if resp == nil {
		return ""
	}
	m := resp.Data
	id := urnString(m.EntityURN)
	if id == "" {
		return ""
	}
	if st, ok := l.st.LineState(Name, chat, id); !ok || st.Early {
		if m.Sender.EntityURN.IsEmpty() {
			m.Sender = linkedingo.MessagingParticipant{EntityURN: linkedingo.NewURN(l.self()).WithPrefix(participantPrefix)}
		}
		if m.Body.Text == "" && len(m.RenderContent) == 0 {
			m.Body.Text = req.Text
		}
		if m.DeliveredAt.IsZero() {
			m.DeliveredAt = jsontime.UM(l.clock.Now())
		}
		l.storeMessage(m, chat, id, store.SourceLive, false)
	}
	if data != nil && isPhoto(req.ContentType) {
		l.media.Keep(Name, chat, id, data)
	}
	return id
}

// React sends his reaction to a held line, or, with an empty emoji, removes
// his, naming the emoji his stored reaction has (none: nothing is asked). A
// transaction the outbox already holds sends nothing, unless it failed. A
// line only Beeper's copy holds is notOnNetwork.
func (l *LinkedIn) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	gen, cli, err := l.writeGate()
	if err != nil {
		return err
	}
	if !strings.HasPrefix(messageID, linePrefix) {
		return errNotOnNetwork
	}
	chat, err := l.writableChat(chatID)
	if err != nil {
		return err
	}
	prev, held, err := l.st.OutgoingTxn(Name, transactionID)
	if err != nil {
		return err
	}
	if held && prev.State != store.OutFailed {
		return nil
	}
	if _, ok, err := l.st.Line(Name, chat, messageID); err != nil {
		return err
	} else if !ok {
		return errNotFound
	}
	remove := emoji == ""
	if remove {
		own, ok, err := l.st.OwnReaction(Name, chat, messageID)
		if err != nil {
			return err
		}
		if !ok {
			return nil // he has none to remove
		}
		emoji = own
	}
	o := prev
	if held {
		if err := l.st.SetOutgoingState(Name, o.ID, store.OutSending, ""); err != nil {
			return err
		}
	} else {
		body, err := json.Marshal(sendRequest{Target: messageID, Emoji: emoji})
		if err != nil {
			return errInternal
		}
		o = store.Outgoing{ID: reactionPrefix + uuid.NewString(), ChatID: chat, Kind: outReaction,
			Txn: transactionID, MadeMs: l.clock.Now().UnixMilli(), Request: body}
		if err := l.st.PutOutgoing(Name, o); err != nil {
			if _, raced, _ := l.st.OutgoingTxn(Name, transactionID); raced {
				return nil // another call with this transaction kept it first, and sends it
			}
			return err
		}
	}
	wctx, cancel := writeCtx(ctx)
	defer cancel()
	target := linkedingo.NewURN(messageID)
	ran, raw, err := l.ask(wctx, gen, func(ctx context.Context) error {
		if remove {
			return cli.RemoveReaction(ctx, target, emoji)
		}
		return cli.SendReaction(ctx, target, emoji)
	})
	if err != nil {
		kind := failKind(ran, raw, err)
		l.fail(o.ID, kind)
		return &wire.Error{Kind: kind}
	}
	// Removed: the count's own update, if others reacted the same, puts it
	// back without him.
	_ = l.st.SetSummaryEmoji(Name, chat, messageID, emoji, !remove, !remove)
	l.settle(o.ID, "")
	l.changed()
	return nil
}

// MarkRead marks the chat read on LinkedIn, which marks a whole chat only:
// so when through is given and a newer incoming line that shows is stored,
// nothing is asked and nothing moves. Otherwise MarkConversationRead, and his
// read marker moves to through (the newest incoming line when empty).
func (l *LinkedIn) MarkRead(ctx context.Context, chatID, through string) error {
	gen, cli, err := l.writeGate()
	if err != nil {
		return err
	}
	chat, err := l.writableChat(chatID)
	if err != nil {
		return err
	}
	newest, hasNewest, err := l.st.NewestIncoming(Name, chat)
	if err != nil {
		return err
	}
	upTo, have := newest, hasNewest
	if through != "" {
		line, ok, err := l.st.Line(Name, chat, through)
		if err != nil {
			return err
		}
		if !ok {
			return errNotFound
		}
		if hasNewest && newest.TimeMs > line.TimeMs {
			return nil // marking the chat would mark the newer line read too
		}
		upTo, have = line, true
	}
	wctx, cancel := writeCtx(ctx)
	defer cancel()
	if ran, raw, err := l.ask(wctx, gen, func(ctx context.Context) error {
		_, err := cli.MarkConversationRead(ctx, linkedingo.NewURN(chat))
		return err
	}); err != nil {
		return writeErr(ran, raw, err)
	}
	if have && upTo.TimeMs > 0 {
		ms := upTo.TimeMs
		if err := l.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &ms, ReadForwardOnly: true}); err != nil {
			return err
		}
	}
	l.changed()
	return nil
}

// MarkUnread marks the chat unread on LinkedIn (MarkConversationUnread), and
// shows its newest incoming line unread.
func (l *LinkedIn) MarkUnread(ctx context.Context, chatID string) error {
	gen, cli, err := l.writeGate()
	if err != nil {
		return err
	}
	chat, err := l.writableChat(chatID)
	if err != nil {
		return err
	}
	wctx, cancel := writeCtx(ctx)
	defer cancel()
	if ran, raw, err := l.ask(wctx, gen, func(ctx context.Context) error {
		_, err := cli.MarkConversationUnread(ctx, linkedingo.NewURN(chat))
		return err
	}); err != nil {
		return writeErr(ran, raw, err)
	}
	if in, ok, err := l.st.NewestIncoming(Name, chat); err == nil && ok {
		before := in.TimeMs - 1
		if err := l.st.PutChat(Name, store.ChatInfo{ID: chat, ReadThroughMs: &before}); err != nil {
			return err
		}
	}
	l.changed()
	return nil
}

// SetMuted mutes or unmutes a held chat in the store only. LinkedIn has no
// mute the library can set (through Beeper his LinkedIn mutes were Beeper's
// own), so nothing is asked of LinkedIn, signed in or not.
func (l *LinkedIn) SetMuted(ctx context.Context, chatID string, muted bool) error {
	chat, ok := l.st.ChatFor(Name, chatID)
	if !ok {
		return errNotFound
	}
	if err := l.st.PutChat(Name, store.ChatInfo{ID: chat, Muted: &muted}); err != nil {
		return err
	}
	l.changed()
	return nil
}
