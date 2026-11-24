package instagram

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// photoMsg is a photo from sender, its file at url (an invented address).
func photoMsg(id string, sender int64, at time.Time, url string) *slidetypes.Message {
	return &slidetypes.Message{
		ID: id, MessageID: id, SenderFBID: sender, TimestampMS: ms(at),
		Content: slidetypes.MessageContentWrapper{TypeName: "SlideMessageImageContent", Content: &slidetypes.MessageContentImage{
			Attachments: []*slidetypes.Attachment{{AttachmentFBID: "invented-att-1", AttachmentCDNURL: url, PreviewWidth: 640, PreviewHeight: 480}},
		}},
	}
}

// photoFrom stores a photo from Alice in her chat.
func (h *harness) photoFrom(id, url string, at time.Time) {
	h.t.Helper()
	if err := h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: photoMsg(id, aliceFBI, at, url)})); err != nil {
		h.t.Fatal(err)
	}
	if l, ok := h.lines(aliceChat)[id]; !ok || l.Kind != "photo" {
		h.t.Fatalf("photo %s was stored as %+v (%v)", id, l, ok)
	}
}

var photoBytes = []byte("\x89PNG invented photo bytes")

func TestAPhotoDownloadsOnce(t *testing.T) {
	h := signedIn(t)
	const url = "https://cdn.invalid/invented-p1.jpg"
	h.m.files = map[string]fakeFile{url: {data: photoBytes}}
	h.photoFrom("mid.$invented-p1", url, start)

	ctx := context.Background()
	first, err := h.g.Media(ctx, aliceChat, "mid.$invented-p1")
	if err != nil {
		t.Fatalf("Media = %v", err)
	}
	second, err := h.g.Media(ctx, aliceChat, "mid.$invented-p1")
	if err != nil || second != first {
		t.Errorf("asked again: %q, %v; want %q", second, err, first)
	}
	if n := h.m.count("Download"); n != 1 {
		t.Errorf("Download was asked %d times, want 1", n)
	}
	if got, _ := openMedia(first); !bytes.Equal(got, photoBytes) {
		t.Errorf("the file holds %q", got)
	}
	if filepath.Dir(first) != filepath.Join(h.dir, "media") {
		t.Errorf("the file is at %s, want in connect/media", first)
	}
	if fi, err := os.Stat(first); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the file's mode is %v (%v), want 0600", fi.Mode(), err)
	}
}

func TestAnExpiredPhotoIsRefreshedOnceThenGone(t *testing.T) {
	const (
		old   = "https://cdn.invalid/invented-old.jpg"
		fresh = "https://cdn.invalid/invented-fresh.jpg"
		photo = "mid.$invented-p1"
		after = "mid.$invented-a1"
	)
	setup := func(t *testing.T, freshStatus int) *harness {
		h := signedIn(t)
		h.m.files = map[string]fakeFile{old: {status: 403}, fresh: {data: photoBytes, status: freshStatus}}
		h.photoFrom(photo, old, start)
		h.incoming(after, start.Add(time.Second))
		h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
			return messagesPage(false, "", photoMsg(photo, aliceFBI, start, fresh)), nil
		}
		return h
	}

	t.Run("refreshed", func(t *testing.T) {
		h := setup(t, 200)
		before := h.m.count("PaginateMessages")
		path, err := h.g.Media(context.Background(), aliceChat, photo)
		if err != nil {
			t.Fatalf("Media = %v", err)
		}
		if got, _ := openMedia(path); !bytes.Equal(got, photoBytes) {
			t.Errorf("the file holds %q", got)
		}
		if n := h.m.count("PaginateMessages") - before; n != 1 {
			t.Errorf("PaginateMessages was asked %d times, want 1", n)
		}
		h.m.mu.Lock()
		older := h.m.olderThan[len(h.m.olderThan)-1]
		h.m.mu.Unlock()
		if older != after {
			t.Errorf("the refresh asked for lines older than %q, want the next line %s", older, after)
		}
		if args := h.m.argsOf("Download"); len(args) != 2 || args[0] != old || args[1] != fresh {
			t.Errorf("Download %v, want the old address then the fresh one", args)
		}
		gaps(t, h.m.requests())
	})

	t.Run("gone", func(t *testing.T) {
		h := setup(t, 403)
		before := h.m.count("PaginateMessages")
		if _, err := h.g.Media(context.Background(), aliceChat, photo); kindErr(err) != wire.MediaGone {
			t.Errorf("Media = %v, want mediaGone", err)
		}
		if n := h.m.count("PaginateMessages") - before; n != 1 {
			t.Errorf("PaginateMessages was asked %d times, want 1", n)
		}
		if n := h.m.count("Download"); n != 2 {
			t.Errorf("Download was asked %d times, want 2", n)
		}
	})
}
