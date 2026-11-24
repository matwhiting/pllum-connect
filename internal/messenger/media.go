package messenger

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/sealed"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A photo or sticker is fetched once, through the pace, to
// connect/media/<sha256 of network|chat|message>, sealed, 0600 in a 0700 folder, as
// WhatsApp's and Instagram's are (the folder is shared and kept under 500
// MB, the least recently used files going first): an unencrypted thread's
// from the CDN address its line keeps (the signed-in client's own HTTP
// client, no cookies: the address is signed), an encrypted chat's with
// whatsmeow's DownloadFB from the transport its message application keeps
// (a restored line's address from the backup taking the place of the
// message's own). An address the CDN or WhatsApp's servers no longer take
// (403, 404, 410) is mediaGone; nothing is asked again for it. Videos,
// voice notes and files have no file to show.

const (
	mediaCap = 500 << 20
	// mediaLimit is the largest file fetched.
	mediaLimit = 32 << 20
)

var errMediaGone = &wire.Error{Kind: wire.MediaGone}

// msgrDownloader fetches an unencrypted chat's file from Meta's CDN. It is
// a read, and goes through the pace like every request.
type msgrDownloader interface {
	Download(ctx context.Context, url string) ([]byte, error)
}

// cdnClient is the real client's CDN downloads.
type cdnClient struct{ c *messagix.Client }

var _ msgrDownloader = cdnClient{}

// downloaderFor is the downloader of the real client; nil for any other.
func downloaderFor(cli msgrClient) msgrDownloader {
	if rc, ok := cli.(realClient); ok {
		return cdnClient{rc.Client}
	}
	return nil
}

// statusError is a CDN's answer other than 200 (and 429, which is
// httpclient.ErrRateLimited, so the pace backs off).
type statusError int

func (e statusError) Error() string { return "status " + strconv.Itoa(int(e)) }

// Download fetches url with the client's HTTP client, with the headers
// mautrix-meta's media downloads send for an image (msgconv/mediadl).
func (d cdnClient) Download(ctx context.Context, url string) ([]byte, error) {
	if d.c == nil {
		return nil, messagix.ErrClientIsNil
	}
	h := d.c.GetHTTP()
	if h == nil || h.HTTP == nil {
		return nil, messagix.ErrClientIsNil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errNotFound
	}
	req.Header.Set("Accept", "image/avif,image/webp,*/*")
	req.Header.Set("Sec-Fetch-Dest", "image")
	req.Header.Set("Sec-Fetch-Mode", "no-cors")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("User-Agent", useragent.UserAgent)
	req.Header.Set("sec-ch-ua", useragent.SecCHUserAgent)
	req.Header.Set("sec-ch-ua-platform", useragent.SecCHPlatform)
	resp, err := h.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %w", httpclient.ErrRequestFailed, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, httpclient.ErrRateLimited
	case resp.StatusCode != http.StatusOK:
		return nil, statusError(resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, mediaLimit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", httpclient.ErrResponseReadFailed, err)
	}
	if len(data) > mediaLimit {
		return nil, errTooLarge
	}
	return data, nil
}

// goneFB reports whether an encrypted file's download failed because
// WhatsApp's media servers no longer have it (403, 404, 410).
func goneFB(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

func (m *Messenger) mediaDir() string { return filepath.Join(m.dir, "media") }

// mediaPath is where a line's file is kept.
func (m *Messenger) mediaPath(chat, msgID string) string {
	sum := sha256.Sum256([]byte(Name + "|" + chat + "|" + msgID))
	return filepath.Join(m.mediaDir(), hex.EncodeToString(sum[:]))
}

// mediaLock is the lock for one file, so it is fetched once at a time.
func (m *Messenger) mediaLock(path string) *sync.Mutex {
	m.mmu.Lock()
	defer m.mmu.Unlock()
	l, ok := m.mlocks[path]
	if !ok {
		l = &sync.Mutex{}
		m.mlocks[path] = l
	}
	return l
}

// Media is the path of a line's photo or sticker, fetched the first time
// it is asked for. A file already kept is given even while signed out or
// stopped by Meta; fetching one is refused like any request then.
func (m *Messenger) Media(ctx context.Context, chatID, messageID string) (string, error) {
	if strings.HasPrefix(messageID, "beeper:") || strings.Contains(chatID, "unplaced:") {
		return "", errNotOnNetwork
	}
	chat := chatID
	if held, ok := m.st.ChatFor(Name, chatID); ok {
		chat = held
	}
	path := m.mediaPath(chat, messageID)
	lock := m.mediaLock(path)
	lock.Lock()
	defer lock.Unlock()
	if kept(path) {
		return path, nil
	}
	if err := m.writeGate(); err != nil {
		return "", err
	}
	l, ok, err := m.st.Line(Name, chat, messageID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errNotFound
	}
	if l.Kind != "photo" && l.Kind != "sticker" {
		return "", errUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, writeWait)
	defer cancel()
	var data []byte
	if raw, ok := encRawOf(l.Raw); ok {
		data, err = m.fetchEncrypted(ctx, raw)
	} else {
		data, err = m.fetchCDN(ctx, l)
	}
	if err != nil {
		return "", err
	}
	return path, m.writeMedia(path, data)
}

// encRawOf is an encrypted line's raw (its message application), and
// whether the line is one.
func encRawOf(raw []byte) (encRaw, bool) {
	var r encRaw
	if len(raw) == 0 || json.Unmarshal(raw, &r) != nil || len(r.App) == 0 {
		return encRaw{}, false
	}
	return r, true
}

// fetchEncrypted downloads an encrypted line's photo or sticker with
// DownloadFB, through the pace.
func (m *Messenger) fetchEncrypted(ctx context.Context, raw encRaw) ([]byte, error) {
	var app waMsgApplication.MessageApplication
	if proto.Unmarshal(raw.App, &app) != nil {
		return nil, errUnsupported
	}
	dec, err := whatsmeow.DecodeFBApplication(&app)
	if err != nil {
		return nil, errUnsupported
	}
	transport := encTransport(dec.Message)
	if transport.GetIntegral() == nil {
		return nil, errUnsupported
	}
	if path := raw.Paths[transport.GetAncillary().GetObjectID()]; path != "" {
		transport = proto.CloneOf(transport)
		transport.Integral.DirectPath = proto.String(path)
	}
	gen, w := m.writer()
	if w == nil {
		return nil, errNotConnected
	}
	var data []byte
	ran, raw2, err := m.ask(ctx, gen, func(ctx context.Context) (err error) {
		data, err = w.DownloadFB(ctx, transport.GetIntegral(), whatsmeow.MediaImage)
		return err
	})
	if err != nil {
		if ran && goneFB(raw2) {
			return nil, errMediaGone
		}
		return nil, err
	}
	if len(data) > mediaLimit {
		return nil, errTooLarge
	}
	return data, nil
}

// encTransport is the file transport of an encrypted photo or sticker, or
// nil for anything else (both are downloaded as images, as the connector
// downloads them).
func encTransport(msg any) *waMediaTransport.WAMediaTransport {
	app, ok := msg.(*waConsumerApplication.ConsumerApplication)
	if !ok {
		return nil
	}
	c := app.GetPayload().GetContent()
	switch {
	case c.GetImageMessage() != nil:
		t, err := c.GetImageMessage().Decode()
		if err != nil {
			return nil
		}
		return t.GetIntegral().GetTransport()
	case c.GetStickerMessage() != nil:
		t, err := c.GetStickerMessage().Decode()
		if err != nil {
			return nil
		}
		return t.GetIntegral().GetTransport()
	}
	return nil
}

// fetchCDN downloads an unencrypted line's photo or sticker from the CDN
// address its line keeps, through the pace.
func (m *Messenger) fetchCDN(ctx context.Context, l store.Line) ([]byte, error) {
	url := cdnURL(l.Kind, l.Raw)
	if url == "" {
		return nil, errMediaGone
	}
	gen, cli, _ := m.running()
	if cli == nil || m.newDownloader == nil {
		return nil, errNotConnected
	}
	d := m.newDownloader(cli)
	if d == nil {
		return nil, errNotConnected
	}
	var data []byte
	ran, raw, err := m.ask(ctx, gen, func(ctx context.Context) (err error) {
		data, err = d.Download(ctx, url)
		return err
	})
	if err != nil {
		var s statusError
		if ran && errors.As(raw, &s) {
			return nil, errMediaGone // the CDN answered: no file there for us
		}
		return nil, err
	}
	return data, nil
}

// cdnURL is the address of a line's first file of its kind (the playable
// address, else the preview, as the connector picks them), or "" when its
// raw holds none.
func cdnURL(kind string, raw []byte) string {
	var r struct {
		Blobs    []*table.LSInsertBlobAttachment    `json:"blobs"`
		Atts     []*table.LSInsertAttachment        `json:"attachments"`
		Stickers []*table.LSInsertStickerAttachment `json:"stickers"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &r) != nil {
		return ""
	}
	for _, a := range r.Blobs {
		if x, ok := mediaOf(a.AttachmentType, 0, 0, false); ok && x.Kind == kind {
			if u := cmp.Or(a.PlayableUrl, a.PreviewUrl); u != "" {
				return u
			}
		}
	}
	for _, a := range r.Atts {
		if x, ok := mediaOf(a.AttachmentType, 0, 0, false); ok && x.Kind == kind {
			if u := cmp.Or(a.PlayableUrl, a.PreviewUrl); u != "" {
				return u
			}
		}
	}
	if kind == "sticker" {
		for _, s := range r.Stickers {
			if u := cmp.Or(s.PlayableUrl, s.PreviewUrl); u != "" {
				return u
			}
		}
	}
	return ""
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

// keepMedia keeps a photo he sent, so it opens without a download.
func (m *Messenger) keepMedia(chat, msgID string, data []byte) {
	path := m.mediaPath(chat, msgID)
	lock := m.mediaLock(path)
	lock.Lock()
	defer lock.Unlock()
	_ = m.writeMedia(path, data)
}

// writeMedia writes a file sealed under the files key (package sealed),
// whole (never half), 0600, then trims the folder.
func (m *Messenger) writeMedia(path string, data []byte) error {
	if err := sealed.WriteFile(m.mediaDir(), path, sealed.Key(m.key), data); err != nil {
		return errInternal
	}
	m.trimMedia(path)
	return nil
}

// trimMedia removes the least recently used files until the folder holds
// at most m.mediaCap bytes; keep (the file just written) stays.
func (m *Messenger) trimMedia(keep string) {
	entries, err := os.ReadDir(m.mediaDir())
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
		files = append(files, file{filepath.Join(m.mediaDir(), e.Name()), fi.Size(), fi.ModTime()})
		total += fi.Size()
	}
	if total <= m.mediaCap {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if !files[i].at.Equal(files[j].at) {
			return files[i].at.Before(files[j].at)
		}
		return files[i].path < files[j].path
	})
	for _, f := range files {
		if total <= m.mediaCap {
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
