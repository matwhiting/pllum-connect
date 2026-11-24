package messenger

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A photo or sticker is fetched once, through the pace, to
// the media folder every network shares (package mediacache: sealed, kept
// under 500 MB, the least recently used files going first): an unencrypted thread's
// from the CDN address its line keeps (the signed-in client's own HTTP
// client, no cookies: the address is signed), an encrypted chat's with
// whatsmeow's DownloadFB from the transport its message application keeps
// (a restored line's address from the backup taking the place of the
// message's own). An address the CDN or WhatsApp's servers no longer take
// (403, 404, 410) is mediaGone; nothing is asked again for it. Videos,
// voice notes and files have no file to show.

// mediaLimit is the largest file fetched.
const mediaLimit = meta.MediaLimit

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

// Download fetches url with the client's HTTP client (meta.Download).
func (d cdnClient) Download(ctx context.Context, url string) ([]byte, error) {
	if d.c == nil {
		return nil, messagix.ErrClientIsNil
	}
	h := d.c.GetHTTP()
	if h == nil || h.HTTP == nil {
		return nil, messagix.ErrClientIsNil
	}
	return meta.Download(ctx, h.HTTP, url)
}

// goneFB reports whether an encrypted file's download failed because
// WhatsApp's media servers no longer have it (403, 404, 410).
func goneFB(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
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
	path := m.media.Path(Name, chat, messageID)
	unlock := m.media.Lock(path)
	defer unlock()
	if m.media.Kept(path) {
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
	return path, m.media.Write(path, data)
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
		if ran && meta.Answered(raw) {
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
