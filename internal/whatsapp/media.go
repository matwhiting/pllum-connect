package whatsapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
// the message) written once, to connect/media/<sha256 of network|chat|message>,
// 0600 in a 0700 folder. Voice notes and documents have no file to show. A
// file gone from WhatsApp's servers (404 or 410) is asked of his phone once;
// with no answer in 30 s it is mediaGone. The folder is kept under 500 MB,
// the least recently used files going first.

const (
	mediaCap       = 500 << 20
	mediaRetryWait = 30 * time.Second
)

var (
	errMediaGone   = &wire.Error{Kind: wire.MediaGone}
	errRateLimited = &wire.Error{Kind: wire.RateLimited}
	errTimeout     = &wire.Error{Kind: wire.Timeout}
)

func (w *WhatsApp) mediaDir() string { return filepath.Join(w.dir, "media") }

// mediaPath is where a line's file is kept.
func (w *WhatsApp) mediaPath(chat, msgID string) string {
	sum := sha256.Sum256([]byte(Name + "|" + chat + "|" + msgID))
	return filepath.Join(w.mediaDir(), hex.EncodeToString(sum[:]))
}

// mediaLock is the lock for one file, so it is fetched once at a time.
func (w *WhatsApp) mediaLock(path string) *sync.Mutex {
	w.mmu.Lock()
	defer w.mmu.Unlock()
	l, ok := w.mlocks[path]
	if !ok {
		l = &sync.Mutex{}
		w.mlocks[path] = l
	}
	return l
}

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
	path := w.mediaPath(chat, messageID)
	lock := w.mediaLock(path)
	lock.Lock()
	defer lock.Unlock()
	if kept(path) {
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
		return path, w.writeMedia(path, v.GetJPEGThumbnail())
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
	return path, w.writeMedia(path, data)
}

// kept reports whether a file is kept, marking it just used.
func kept(path string) bool {
	if fi, err := os.Stat(path); err != nil || !fi.Mode().IsRegular() {
		return false
	}
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	return true
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
	w.mmu.Lock()
	w.mretry[id] = ch
	w.mmu.Unlock()
	return ch
}

func (w *WhatsApp) dropRetry(id string, ch chan *events.MediaRetry) {
	w.mmu.Lock()
	if w.mretry[id] == ch {
		delete(w.mretry, id)
	}
	w.mmu.Unlock()
}

// mediaRetried hands his phone's answer to the fetch waiting for it.
func (w *WhatsApp) mediaRetried(e *events.MediaRetry) {
	w.mmu.Lock()
	ch := w.mretry[e.MessageID]
	w.mmu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- e:
	default:
	}
}

// keepMedia keeps a file he sent, so it opens without a download.
func (w *WhatsApp) keepMedia(chat, msgID string, data []byte) {
	path := w.mediaPath(chat, msgID)
	lock := w.mediaLock(path)
	lock.Lock()
	defer lock.Unlock()
	_ = w.writeMedia(path, data)
}

// writeMedia writes a file whole (never half), 0600, then trims the folder.
func (w *WhatsApp) writeMedia(path string, data []byte) error {
	dir := w.mediaDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return errInternal
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return errInternal
	}
	f, err := os.CreateTemp(dir, ".part-*")
	if err != nil {
		return errInternal
	}
	tmp := f.Name()
	_, werr := f.Write(data)
	cerr := f.Close()
	if werr != nil || cerr != nil || os.Chmod(tmp, 0o600) != nil || os.Rename(tmp, path) != nil {
		os.Remove(tmp)
		return errInternal
	}
	w.trimMedia(path)
	return nil
}

// trimMedia removes the least recently used files until the folder holds
// at most mediaCap bytes; keep (the file just written) stays.
func (w *WhatsApp) trimMedia(keep string) {
	entries, err := os.ReadDir(w.mediaDir())
	if err != nil {
		return
	}
	type file struct {
		path string
		size int64
		at   time.Time
	}
	var files []file
	var total int64
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || !e.Type().IsRegular() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, file{filepath.Join(w.mediaDir(), e.Name()), fi.Size(), fi.ModTime()})
		total += fi.Size()
	}
	if total <= w.mediaCap {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if !files[i].at.Equal(files[j].at) {
			return files[i].at.Before(files[j].at)
		}
		return files[i].path < files[j].path
	})
	for _, f := range files {
		if total <= w.mediaCap {
			return
		}
		if f.path == keep {
			continue
		}
		if os.Remove(f.path) == nil {
			total -= f.size
		}
	}
}
