package instagram

import (
	"cmp"
	"context"
	"encoding/json"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A photo or sticker is fetched once, from the CDN address its message
// carries, through the signed-in client's own HTTP client and the pace, to
// the media folder every network shares (package mediacache: sealed, kept
// under 500 MB, the least recently used files going first). The CDN's addresses expire: one that
// answers 403, 404 or 410 is refreshed with one PaginateMessages call (the
// page of lines older than the next one, as igconnector refreshes a blob),
// and fetched again; if that fails too it is mediaGone. Videos and voice
// notes have no file to show.

const (
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

// Download fetches url with the client's HTTP client (meta.Download).
func (m metaClient) Download(ctx context.Context, url string) ([]byte, error) {
	h := m.c.GetHTTP()
	if h == nil || h.HTTP == nil {
		return nil, instameow.ErrClientIsNil
	}
	return meta.Download(ctx, h.HTTP, url)
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
	path := g.media.Path(Name, chat, messageID)
	unlock := g.media.Lock(path)
	defer unlock()
	if g.media.Kept(path) {
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
	if err != nil && meta.Expired(raw) {
		fresh, rerr := g.refresh(ctx, gen, cli, chat, l)
		if rerr != nil {
			return "", rerr
		}
		data, raw, err = g.fetch(ctx, gen, d, fresh)
		if err != nil && meta.Expired(raw) {
			return "", errMediaGone
		}
	}
	if err != nil {
		return "", mediaErr(raw, err)
	}
	return path, g.media.Write(path, data)
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
	if raw != nil && meta.Answered(raw) {
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
