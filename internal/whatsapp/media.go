package whatsapp

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A photo or sticker is downloaded once, and a video's thumbnail (carried in
// the message) written once, to the media folder every network shares
// (package mediacache: sealed, 0600 in a 0700 folder). Voice notes and documents
// have no file to show. A
// file gone from WhatsApp's servers (404 or 410) is asked of his phone once;
// with no answer in 30 s it is mediaGone. The folder is kept under 500 MB,
// the least recently used files going first.

const mediaRetryWait = 30 * time.Second

var (
	errMediaGone   = &wire.Error{Kind: wire.MediaGone}
	errRateLimited = &wire.Error{Kind: wire.RateLimited}
	errTimeout     = &wire.Error{Kind: wire.Timeout}
)

// Media is the path of a line's photo, sticker or video thumbnail, fetched
// the first time it is asked for. A file already kept is given even while
// signed out or offline.
func (w *WhatsApp) Media(ctx context.Context, chatID, messageID string) (string, error) {
	if strings.HasPrefix(messageID, "beeper:") || strings.HasPrefix(chatID, "unplaced:") {
		return "", errNotOnNetwork
	}
	chat := chatID
	if held, ok := w.st.ChatFor(Name, chatID); ok {
		chat = held
	}
	path := w.media.Path(Name, chat, messageID)
	unlock := w.media.Lock(path)
	defer unlock()
	if w.media.Kept(path) {
		return path, nil
	}
	gate := w.writeGate()
	if gate != nil && gate != errOffline {
		return "", gate
	}
	l, ok, err := w.st.Line(Name, chat, messageID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errNotFound
	}
	if len(l.Raw) == 0 {
		return "", errUnsupported
	}
	var m waE2E.Message
	if err := proto.Unmarshal(l.Raw, &m); err != nil {
		return "", errInternal
	}
	var part whatsmeow.DownloadableMessage
	switch {
	case m.GetImageMessage() != nil:
		part = m.GetImageMessage()
	case m.GetStickerMessage() != nil:
		part = m.GetStickerMessage()
	case m.GetVideoMessage() != nil || m.GetPtvMessage() != nil:
		v := m.GetVideoMessage()
		if v == nil {
			v = m.GetPtvMessage()
		}
		if len(v.GetJPEGThumbnail()) == 0 {
			return "", errMediaGone
		}
		return path, w.media.Write(path, v.GetJPEGThumbnail())
	default:
		return "", errUnsupported
	}
	if gate != nil {
		return "", gate
	}
	cli := w.client()
	if cli == nil {
		return "", errNotConnected
	}
	data, err := w.download(ctx, cli, chat, l, part)
	if err != nil {
		return "", err
	}
	return path, w.media.Write(path, data)
}

// download fetches a file; one gone from WhatsApp's servers is asked of his
// phone once, and fetched from where the phone puts it.
func (w *WhatsApp) download(ctx context.Context, cli waClient, chat string, l store.Line, part whatsmeow.DownloadableMessage) ([]byte, error) {
	data, err := cli.Download(ctx, part)
	if err == nil {
		return data, nil
	}
	if !gone(err) {
		return nil, mediaErr(err)
	}
	jid, err := types.ParseJID(chat)
	if err != nil {
		return nil, errMediaGone
	}
	info := &types.MessageInfo{ID: l.ID, MessageSource: types.MessageSource{
		Chat: jid, IsFromMe: l.FromMe, IsGroup: jid.Server == types.GroupServer}}
	if info.IsGroup && !l.FromMe {
		info.Sender, _ = types.ParseJID(l.SenderID)
	}
	answer := w.awaitRetry(l.ID)
	defer w.dropRetry(l.ID, answer)
	expired := make(chan struct{})
	stop := w.clock.AfterFunc(mediaRetryWait, func() { close(expired) })
	defer stop()
	if err := cli.SendMediaRetryReceipt(ctx, info, part.GetMediaKey()); err != nil {
		if historyTransient(err) {
			return nil, errOffline
		}
		return nil, errMediaGone
	}
	var evt *events.MediaRetry
	select {
	case evt = <-answer:
	case <-expired:
		return nil, errMediaGone
	case <-ctx.Done():
		return nil, errTimeout
	}
	notif, err := whatsmeow.DecryptMediaRetryNotification(evt, part.GetMediaKey())
	if err != nil || notif.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS || notif.GetDirectPath() == "" {
		return nil, errMediaGone
	}
	data, err = cli.Download(ctx, withDirectPath(part, notif.GetDirectPath()))
	if err != nil {
		if gone(err) {
			return nil, errMediaGone
		}
		return nil, mediaErr(err)
	}
	return data, nil
}

// gone reports whether WhatsApp's servers no longer hold a file.
func gone(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) || errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// mediaErr is the kind a failed download is sent as.
func mediaErr(err error) error {
	var herr whatsmeow.DownloadHTTPError
	isHTTP := errors.As(err, &herr) && herr.Response != nil
	switch {
	case isHTTP && herr.StatusCode == 429:
		return errRateLimited
	case historyGone(err):
		return errMediaGone
	case errors.Is(err, context.DeadlineExceeded):
		return errTimeout
	case historyTransient(err):
		return errOffline
	case isHTTP:
		return errMediaGone
	}
	return errInternal
}

// withDirectPath is part at the path his phone uploaded it to again.
func withDirectPath(part whatsmeow.DownloadableMessage, path string) whatsmeow.DownloadableMessage {
	switch p := part.(type) {
	case *waE2E.ImageMessage:
		c := proto.Clone(p).(*waE2E.ImageMessage)
		c.DirectPath = proto.String(path)
		return c
	case *waE2E.StickerMessage:
		c := proto.Clone(p).(*waE2E.StickerMessage)
		c.DirectPath = proto.String(path)
		return c
	}
	return part
}

// awaitRetry is where his phone's answer to a media retry for id arrives.
func (w *WhatsApp) awaitRetry(id string) chan *events.MediaRetry {
	ch := make(chan *events.MediaRetry, 1)
	w.rmu.Lock()
	w.mretry[id] = ch
	w.rmu.Unlock()
	return ch
}

func (w *WhatsApp) dropRetry(id string, ch chan *events.MediaRetry) {
	w.rmu.Lock()
	if w.mretry[id] == ch {
		delete(w.mretry, id)
	}
	w.rmu.Unlock()
}

// mediaRetried hands his phone's answer to the fetch waiting for it.
func (w *WhatsApp) mediaRetried(e *events.MediaRetry) {
	w.rmu.Lock()
	ch := w.mretry[e.MessageID]
	w.rmu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- e:
	default:
	}
}
