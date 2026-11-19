package linkedin

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// photoMessage is a photo in conversation n whose largest artifact is at
// root+segment.
func photoMessage(n, id int, from linkedingo.MessagingParticipant, at time.Time, root, segment string) linkedingo.Message {
	m := text(n, id, from, at, "")
	m.RenderContent = []linkedingo.RenderContent{{VectorImage: &linkedingo.VectorImage{
		RootURL: root,
		Artifacts: []linkedingo.VectorArtifact{
			{Width: 100, Height: 100, FileIdentifyingURLPathSegment: "small-invented"},
			{Width: 800, Height: 600, FileIdentifyingURLPathSegment: segment},
			{Width: 400, Height: 300, FileIdentifyingURLPathSegment: "medium-invented"},
		},
	}}}
	return m
}

// cdn answers downloads: a URL it holds gives its bytes, any other the
// status gone.
type cdn struct {
	mu    sync.Mutex
	files map[string]string
	gone  int
}

func (c *cdn) answer(url string) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if body, ok := c.files[url]; ok {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
	return nil, statusErr(c.gone)
}

func TestAnExpiredPhotoURLIsRefreshedOnce(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	at := start.Add(-time.Hour)
	alice := person(aliceID, "Alice", "Invented")
	r.arrive(c, photoMessage(1, 5, alice, at, "https://media.invented.example/old/", "large-invented"))

	fresh := "https://media.invented.example/fresh/large-invented"
	r.f.download = (&cdn{files: map[string]string{fresh: "invented-photo-bytes"}, gone: 403}).answer
	var asked []string
	r.f.before = func(conv string, before time.Time, count int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		asked = append(asked, before.UTC().Format(time.RFC3339Nano)+" "+strings.Repeat("x", count))
		return messagePage("", photoMessage(1, 5, alice, at, "https://media.invented.example/fresh/", "large-invented"))
	}

	path, err := r.l.Media(context.Background(), chat, msgURN(5))
	if err != nil {
		t.Fatalf("Media: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "invented-photo-bytes" {
		t.Errorf("the file holds %q, want the photo", got)
	}
	var downloads []string
	for _, c := range r.f.callsNow() {
		if c.name == "DownloadHTTP" {
			downloads = append(downloads, c.arg)
		}
	}
	if len(downloads) != 2 || downloads[0] != "https://media.invented.example/old/large-invented" || downloads[1] != fresh {
		t.Errorf("downloads = %v, want the largest artifact, then its fresh URL", downloads)
	}
	if want := []string{at.Add(time.Millisecond).Format(time.RFC3339Nano) + " x"}; len(asked) != 1 || asked[0] != want[0] {
		t.Errorf("the refresh asked %v, want one message before its time + 1 ms", asked)
	}
	if _, blocked := r.l.pause.blocked(); blocked {
		t.Error("an expired photo URL paused LinkedIn")
	}

	// Saved once: the next ask is the file kept.
	n := len(r.f.callsNow())
	if again, err := r.l.Media(context.Background(), chat, msgURN(5)); err != nil || again != path {
		t.Fatalf("Media again: %q, %v", again, err)
	}
	if names := r.f.namesSince(n); len(names) != 0 {
		t.Errorf("a kept photo asked LinkedIn %v", names)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the file's mode is %v (%v), want 0600", fi.Mode().Perm(), err)
	}
}

func TestAGonePhotoIsMediaGone(t *testing.T) {
	for _, code := range []int{401, 404, 410} {
		r := newRig(t)
		c := r.live()
		chat := r.heldChat(1)
		alice := person(aliceID, "Alice", "Invented")
		r.arrive(c, photoMessage(1, 5, alice, start, "https://media.invented.example/old/", "large-invented"))
		r.f.download = (&cdn{gone: code}).answer
		r.f.before = func(conv string, before time.Time, count int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
			return messagePage("", photoMessage(1, 5, alice, start, "https://media.invented.example/fresh/", "large-invented"))
		}
		if _, err := r.l.Media(context.Background(), chat, msgURN(5)); wire.KindOf(err) != wire.MediaGone {
			t.Errorf("%d: Media = %v, want mediaGone", code, err)
		}
		if n := r.f.count("DownloadHTTP"); n != 2 {
			t.Errorf("%d: %d downloads, want 2 (the old URL, the fresh one)", code, n)
		}
		if n := r.f.count("GetMessagesBefore"); n != 1 {
			t.Errorf("%d: %d refreshes, want 1", code, n)
		}
		if _, blocked := r.l.pause.blocked(); blocked {
			t.Errorf("%d: a gone photo paused LinkedIn", code)
		}
	}

	// The message LinkedIn no longer has: gone after one refresh.
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	r.arrive(c, photoMessage(1, 5, person(aliceID, "Alice", "Invented"), start, "https://media.invented.example/old/", "large-invented"))
	r.f.download = (&cdn{gone: 410}).answer
	r.f.before = func(conv string, before time.Time, count int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("")
	}
	if _, err := r.l.Media(context.Background(), chat, msgURN(5)); wire.KindOf(err) != wire.MediaGone {
		t.Errorf("a photo whose message is gone: %v, want mediaGone", err)
	}
	if n := r.f.count("DownloadHTTP"); n != 1 {
		t.Errorf("%d downloads, want 1", n)
	}

	// Videos, voice notes and files have no file to show.
	video := text(1, 6, person(aliceID, "Alice", "Invented"), start, "")
	video.RenderContent = []linkedingo.RenderContent{{Video: &linkedingo.VideoPlayMetadata{}}}
	r.arrive(c, video)
	if line, _ := r.lineByID(chat, msgURN(6)); len(line.Media) != 1 || line.Media[0].HasFile {
		t.Errorf("a video's media is %+v, want no file", line.Media)
	}
	if _, err := r.l.Media(context.Background(), chat, msgURN(6)); wire.KindOf(err) != wire.Unsupported {
		t.Errorf("Media of a video: %v, want unsupported", err)
	}
}
