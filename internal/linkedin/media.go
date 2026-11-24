package linkedin

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A photo is fetched once, when he first asks for it: its largest artifact
// (GetLargestArtifactURL; an external image's own URL) through DownloadHTTP,
// to the media folder every network shares (package mediacache: sealed,
// kept under 500 MB, the least recently used files going first). It is a request he asked for, so
// it is not paced; it never goes during a pause, and the CDN refusing it
// never pauses LinkedIn (download). The CDN's signed URLs expire: one that
// answers 401, 403, 404 or 410 has its message read again
// (GetMessagesBefore(conv, its time + 1 ms, 1)), once, for a fresh URL, and
// that is fetched; if either fails it is mediaGone. Voice notes, videos and
// files have no file to show (hasFile false), and are unsupported here.

// mediaLimit is the largest file fetched.
const mediaLimit = 32 << 20

var errMediaGone = &wire.Error{Kind: wire.MediaGone}

// Media is the path of a line's photo, fetched the first time it is asked
// for. A file already kept is given even while signed out or paused;
// fetching one is refused then, like any request.
func (l *LinkedIn) Media(ctx context.Context, chatID, messageID string) (string, error) {
	if !strings.HasPrefix(messageID, linePrefix) || strings.Contains(chatID, "unplaced:") {
		return "", errNotOnNetwork
	}
	chat := chatID
	if held, ok := l.st.ChatFor(Name, chatID); ok {
		chat = held
	}
	path := l.media.Path(Name, chat, messageID)
	unlock := l.media.Lock(path)
	defer unlock()
	if l.media.Kept(path) {
		return path, nil
	}
	line, ok, err := l.st.Line(Name, chat, messageID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errNotFound
	}
	if line.Kind != "photo" {
		return "", errUnsupported
	}
	url := photoURL(line.Raw)
	if url == "" {
		return "", errMediaGone
	}
	gen, cli, err := l.writeGate()
	if err != nil {
		return "", err
	}
	mctx, cancel := writeCtx(ctx)
	defer cancel()
	data, expired, err := l.fetchMedia(mctx, cli, url)
	if expired {
		fresh, rerr := l.refreshPhoto(mctx, gen, cli, chat, line)
		if rerr != nil {
			return "", rerr
		}
		data, expired, err = l.fetchMedia(mctx, cli, fresh)
		if expired {
			return "", errMediaGone
		}
	}
	if err != nil {
		return "", err
	}
	return path, l.media.Write(path, data)
}

// fetchMedia downloads url. expired reports whether the CDN no longer takes
// the address (401, 403, 404, 410), which a fresh one may cure.
func (l *LinkedIn) fetchMedia(ctx context.Context, cli liClient, url string) (data []byte, expired bool, err error) {
	resp, raw, err := l.downloadRaw(ctx, cli, url)
	if err != nil {
		switch statusOf(raw) {
		case 401, 403, 404, 410:
			return nil, true, err
		}
		return nil, false, err
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(io.LimitReader(resp.Body, mediaLimit+1))
	if err != nil {
		return nil, false, &wire.Error{Kind: wire.Offline}
	}
	if len(data) > mediaLimit {
		return nil, false, errTooLarge
	}
	return data, false, nil
}

// refreshPhoto reads a photo's message again, once, for its file's current
// URL: the one message before its time + 1 ms. The message as LinkedIn has
// it now is stored, so its line holds the fresh URL too.
func (l *LinkedIn) refreshPhoto(ctx context.Context, gen int, cli liClient, chat string, line store.Line) (string, error) {
	var page *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]
	if err := l.request(ctx, gen, false, func(ctx context.Context) (err error) {
		page, err = cli.GetMessagesBefore(ctx, linkedingo.NewURN(chat), time.UnixMilli(line.TimeMs+1), 1)
		return err
	}); err != nil {
		switch wire.KindOf(err) {
		case wire.RateLimited, wire.NotConnected, wire.Offline, wire.Timeout:
			return "", err
		}
		return "", errMediaGone
	}
	if page == nil {
		return "", errMediaGone
	}
	for _, m := range page.Elements {
		if urnString(m.EntityURN) != line.ID {
			continue
		}
		raw, err := json.Marshal(m.RenderContent)
		if err != nil {
			return "", errMediaGone
		}
		url := photoURL(raw)
		if url == "" {
			return "", errMediaGone
		}
		if m.Conversation.EntityURN.IsEmpty() && m.ConversationURN.IsEmpty() {
			m.ConversationURN = linkedingo.NewURN(chat)
		}
		_, _ = l.storeMessages([]linkedingo.Message{m}, store.SourceSync) // the URL is answered either way
		return url, nil
	}
	return "", errMediaGone
}

// photoURL is the URL of a photo's file, from its render content as ingest
// keeps it in raw: a vector image's largest artifact, else an external
// image's URL; "" when it holds none.
func photoURL(raw []byte) string {
	var rcs []linkedingo.RenderContent
	if len(raw) == 0 || json.Unmarshal(raw, &rcs) != nil {
		return ""
	}
	for _, rc := range rcs {
		switch {
		case rc.VectorImage != nil && rc.VectorImage.RootURL != "":
			return rc.VectorImage.GetLargestArtifactURL()
		case rc.ExternalMedia != nil && rc.ExternalMedia.Media.URL != "":
			return rc.ExternalMedia.Media.URL
		}
	}
	return ""
}
