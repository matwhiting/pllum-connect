package instagram

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

	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"

	"github.com/matwhiting/pllum-connect/internal/sealed"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A photo or sticker is fetched once, from the CDN address its message
// carries, through the signed-in client's own HTTP client and the pace, to
// connect/media/<sha256 of network|chat|message>, sealed, 0600 in a 0700 folder, as
// WhatsApp's are (the folder is shared and kept under 500 MB, the least
// recently used files going first). The CDN's addresses expire: one that
// answers 403, 404 or 410 is refreshed with one PaginateMessages call (the
// page of lines older than the next one, as igconnector refreshes a blob),
// and fetched again; if that fails too it is mediaGone. Videos and voice
// notes have no file to show.

const (
	mediaCap = 500 << 20
	// mediaLimit is the largest file fetched.
	mediaLimit = 32 << 20
	// refreshPage is how many lines the refresh's page holds.
	refreshPage = 20
)

var errMediaGone = &wire.Error{Kind: wire.MediaGone}

// igDownloader fetches a file from Meta's CDN. It is a read, and goes
// through the pace like every request.
type igDownloader interface {
	Download(ctx context.Context, url string) ([]byte, error)
}

// downloaderFor is the downloader of a client: the real one's through
// metaClient, a test's fake as it is.
func downloaderFor(c igClient) igDownloader {
	switch c := c.(type) {
	case *instameow.Client:
		return metaClient{c}
	case igDownloader:
		return c
	}
	return nil
}

// statusError is a CDN's answer other than 200 (and 429, which is
// httpclient.ErrRateLimited, so the pace backs off).
type statusError int

func (e statusError) Error() string { return "status " + strconv.Itoa(int(e)) }

// Download fetches url with the client's HTTP client (no cookies: the CDN
// address is signed), with the headers mautrix-meta's media downloads send
// for an image (msgconv/mediadl).
func (m metaClient) Download(ctx context.Context, url string) ([]byte, error) {
	h := m.c.GetHTTP()
	if h == nil || h.HTTP == nil {
		return nil, instameow.ErrClientIsNil
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

// expired reports whether a download failed because the CDN no longer
// takes its address.
func expired(err error) bool {
	var s statusError
	return errors.As(err, &s) && (s == 403 || s == 404 || s == 410)
}

func (g *Instagram) mediaDir() string { return filepath.Join(g.dir, "media") }

// mediaPath is where a line's file is kept.
func (g *Instagram) mediaPath(chat, msgID string) string {
	sum := sha256.Sum256([]byte(Name + "|" + chat + "|" + msgID))
	return filepath.Join(g.mediaDir(), hex.EncodeToString(sum[:]))
}

// mediaLock is the lock for one file, so it is fetched once at a time.
func (g *Instagram) mediaLock(path string) *sync.Mutex {
	g.mmu.Lock()
	defer g.mmu.Unlock()
	l, ok := g.mlocks[path]
	if !ok {
		l = &sync.Mutex{}
		g.mlocks[path] = l
	}
	return l
}

// Media is the path of a line's photo or sticker, fetched the first time
// it is asked for. A file already kept is given even while signed out or
// stopped by Meta; fetching one is refused like any request then.
func (g *Instagram) Media(ctx context.Context, chatID, messageID string) (string, error) {
	if strings.HasPrefix(messageID, "beeper:") || strings.Contains(chatID, "unplaced:") {
		return "", errNotOnNetwork
	}
	chat := chatID
	if held, ok := g.st.ChatFor(Name, chatID); ok {
		chat = held
	}
	path := g.mediaPath(chat, messageID)
	lock := g.mediaLock(path)
	lock.Lock()
	defer lock.Unlock()
	if kept(path) {
		return path, nil
	}
	if err := g.writeGate(); err != nil {
		return "", err
	}
	l, ok, err := g.st.Line(Name, chat, messageID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errNotFound
	}
	if l.Kind != "photo" && l.Kind != "sticker" {
		return "", errUnsupported
	}
	url := fileURL(l.Kind, l.Raw)
	if url == "" {
		return "", errMediaGone
	}
	ctx, cancel := context.WithTimeout(ctx, writeWait)
	defer cancel()
	gen, cli, _ := g.running()
	if cli == nil {
		return "", errNotConnected
	}
	d := g.newDownloader(cli)
	if d == nil {
		return "", errNotConnected
	}
	data, raw, err := g.fetch(ctx, gen, d, url)
	if err != nil && expired(raw) {
		fresh, rerr := g.refresh(ctx, gen, cli, chat, l)
		if rerr != nil {
			return "", rerr
		}
		data, raw, err = g.fetch(ctx, gen, d, fresh)
		if err != nil && expired(raw) {
			return "", errMediaGone
		}
	}
	if err != nil {
		return "", mediaErr(raw, err)
	}
	return path, g.writeMedia(path, data)
}

// fetch downloads url through the pace; raw is the download's own error,
// nil when it was never asked.
func (g *Instagram) fetch(ctx context.Context, gen int, d igDownloader, url string) (data []byte, raw, err error) {
	ran, raw, err := g.ask(ctx, gen, func(ctx context.Context) (err error) {
		data, err = d.Download(ctx, url)
		return err
	})
	if !ran {
		raw = nil
	}
	return data, raw, err
}

// mediaErr is the error a failed download returns: an answer from the CDN
// is mediaGone; anything else keeps its kind (offline, timeout,
// rateLimited, or why it was never asked).
func mediaErr(raw, err error) error {
	var s statusError
	if raw != nil && errors.As(raw, &s) {
		return errMediaGone
	}
	return err
}

// refresh asks Meta once for the page holding a line, for its file's
// current address: the lines older than the one after it (the newest page
// when it is the newest), as igconnector's refreshBlobMedia does.
func (g *Instagram) refresh(ctx context.Context, gen int, cli igClient, chat string, l store.Line) (string, error) {
	thread, ok := g.threadOfChat(chat)
	if !ok {
		short, _, err := g.threadIDs(ctx, gen, cli, chat)
		if err != nil {
			return "", err
		}
		thread = short
	}
	req := &slidetypes.PaginateMessagesRequest{ThreadID: thread, FirstN: refreshPage, InitialMessagePageCount: refreshPage}
	if next, ok, err := g.st.LineAfter(Name, chat, l.ID); err == nil && ok && !strings.HasPrefix(next.ID, "beeper:") {
		older := next.ID
		req.OlderThanMessageID = &older
	}
	var resp *slidetypes.PaginateMessagesResponse
	if err := g.request(ctx, gen, func(ctx context.Context) (err error) {
		resp, err = cli.PaginateMessages(ctx, req)
		return err
	}); err != nil {
		switch wire.KindOf(err) {
		case wire.Internal, wire.NotFound:
			return "", errMediaGone
		}
		return "", err
	}
	if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.Messages == nil {
		return "", errMediaGone
	}
	for _, e := range resp.ThreadInfo.AsIGDirectThread.Messages.Edges {
		m := e.Node
		if m == nil || cmp.Or(m.ID, m.MessageID) != l.ID {
			continue
		}
		raw, err := json.Marshal(&m.Content)
		if err != nil {
			return "", errMediaGone
		}
		if url := fileURL(l.Kind, raw); url != "" {
			return url, nil
		}
	}
	return "", errMediaGone
}

// rawMedia is the part of a message's content (as ingest keeps it in raw)
// that holds its file's address.
type rawMedia struct {
	Attachments   []*slidetypes.Attachment         `json:"attachments"`                     // a photo
	Ordered       []*slidetypes.Attachment         `json:"ordered_photo_video_attachments"` // photos and videos together
	Attachment    *slidetypes.Attachment           `json:"attachment"`                      // a view-once photo
	PreviewURL    string                           `json:"preview_url"`                     // a sticker
	AnimatedMedia []*slidetypes.AnimatedAttachment `json:"animated_media"`                  // an animated sticker
}

// fileURL is the address of a photo's (its first photo's) or sticker's
// file, or "" when the content holds none.
func fileURL(kind string, raw []byte) string {
	var c rawMedia
	if len(raw) == 0 || json.Unmarshal(raw, &c) != nil {
		return ""
	}
	switch kind {
	case "photo":
		all := append(append(append([]*slidetypes.Attachment{}, c.Attachments...), c.Ordered...), c.Attachment)
		for _, a := range all {
			if a == nil || a.DashManifest != "" {
				continue
			}
			if u := cmp.Or(a.AttachmentCDNURL, a.PreviewCDNURL); u != "" {
				return u
			}
		}
	case "sticker":
		if c.PreviewURL != "" {
			return c.PreviewURL
		}
		for _, a := range c.AnimatedMedia {
			if a == nil {
				continue
			}
			if u := cmp.Or(a.AttachmentWebpURL, a.PreviewCDNURL); u != "" {
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
func (g *Instagram) keepMedia(chat, msgID string, data []byte) {
	path := g.mediaPath(chat, msgID)
	lock := g.mediaLock(path)
	lock.Lock()
	defer lock.Unlock()
	_ = g.writeMedia(path, data)
}

// writeMedia writes a file sealed under the files key (package sealed),
// whole (never half), 0600, then trims the folder.
func (g *Instagram) writeMedia(path string, data []byte) error {
	if err := sealed.WriteFile(g.mediaDir(), path, sealed.Key(g.key), data); err != nil {
		return errInternal
	}
	g.trimMedia(path)
	return nil
}

// trimMedia removes the least recently used files until the folder holds
// at most g.mediaCap bytes; keep (the file just written) stays.
func (g *Instagram) trimMedia(keep string) {
	entries, err := os.ReadDir(g.mediaDir())
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
		files = append(files, file{filepath.Join(g.mediaDir(), e.Name()), fi.Size(), fi.ModTime()})
		total += fi.Size()
	}
	if total <= g.mediaCap {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if !files[i].at.Equal(files[j].at) {
			return files[i].at.Before(files[j].at)
		}
		return files[i].path < files[j].path
	})
	for _, f := range files {
		if total <= g.mediaCap {
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
