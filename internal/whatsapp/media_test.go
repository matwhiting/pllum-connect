package whatsapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every file, key, path and ID here is invented.

var mediaKey = []byte("invented-media-key-32-bytes-long")

func photoMsg(path string) *waE2E.Message {
	return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{DirectPath: proto.String(path), MediaKey: mediaKey,
		Mimetype: proto.String("image/jpeg")}}
}

// mediaFile is where a line's file is kept.
func mediaFile(r *rig, chat, id string) string {
	sum := sha256.Sum256([]byte(Name + "|" + chat + "|" + id))
	return filepath.Join(r.dir, "media", hex.EncodeToString(sum[:]))
}

func TestAPhotoDownloadsOnce(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	chat := aliceJID.String()
	c.with(func() {
		c.files["/v/invented-photo"] = []byte("invented jpeg bytes")
		c.files["/v/invented-sticker"] = []byte("invented webp bytes")
	})
	r.send(msg(aliceJID, aliceJID, "3EB0P0000000000000001", base+2000, false, photoMsg("/v/invented-photo")))
	r.send(msg(aliceJID, aliceJID, "3EB0P0000000000000002", base+3000, false, &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
		DirectPath: proto.String("/v/invented-sticker"), MediaKey: mediaKey}}))
	r.send(msg(aliceJID, aliceJID, "3EB0P0000000000000003", base+4000, false, &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
		DirectPath: proto.String("/v/invented-video"), MediaKey: mediaKey, JPEGThumbnail: []byte("invented thumbnail")}}))
	r.send(msg(aliceJID, aliceJID, "3EB0P0000000000000004", base+5000, false, &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
		DirectPath: proto.String("/v/invented-voice"), MediaKey: mediaKey, PTT: proto.Bool(true)}}))
	r.send(msg(aliceJID, aliceJID, "3EB0P0000000000000005", base+6000, false, &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		DirectPath: proto.String("/v/invented-doc"), MediaKey: mediaKey}}))

	for _, tc := range []struct {
		id, want string
	}{
		{"3EB0P0000000000000001", "invented jpeg bytes"},
		{"3EB0P0000000000000002", "invented webp bytes"},
		{"3EB0P0000000000000003", "invented thumbnail"},
	} {
		for range 2 {
			path, err := r.w.Media(ctx, chat, tc.id)
			if err != nil {
				t.Fatalf("Media(%s): %v", tc.id, err)
			}
			if path != mediaFile(r, chat, tc.id) {
				t.Fatalf("the file is at %s, want %s", path, mediaFile(r, chat, tc.id))
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != tc.want {
				t.Fatalf("the file holds %q (%v), want %q", got, err, tc.want)
			}
			if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
				t.Fatalf("the file's mode is %v", fi.Mode().Perm())
			}
		}
	}
	if fi, _ := os.Stat(filepath.Join(r.dir, "media")); fi.Mode().Perm() != 0o700 {
		t.Errorf("the media folder's mode is %v", fi.Mode().Perm())
	}
	c.mu.Lock()
	fetches := append([]string(nil), c.fetches...)
	c.mu.Unlock()
	if len(fetches) != 2 || fetches[0] != "/v/invented-photo" || fetches[1] != "/v/invented-sticker" {
		t.Fatalf("downloads %v: want the photo and the sticker once each (a video's thumbnail is in the message)", fetches)
	}
	// Voice notes and documents have no file to show.
	for _, id := range []string{"3EB0P0000000000000004", "3EB0P0000000000000005", "3EB0A0000000000000001"} {
		if _, err := r.w.Media(ctx, chat, id); wire.KindOf(err) != wire.Unsupported {
			t.Errorf("Media(%s) gave %v, want unsupported", id, err)
		}
	}
	if _, err := r.w.Media(ctx, chat, "3EB0P0000000000000099"); wire.KindOf(err) != wire.NotFound {
		t.Errorf("a line nobody holds gave %v", err)
	}
	if _, err := r.w.Media(ctx, chat, "beeper:bm-1"); wire.KindOf(err) != wire.NotOnNetwork {
		t.Errorf("a Beeper line gave %v", err)
	}
	// A kept file is there while offline too; one not yet fetched is not.
	c.dispatch(&events.Disconnected{})
	if _, err := r.w.Media(ctx, chat, "3EB0P0000000000000001"); err != nil {
		t.Errorf("a kept photo while offline: %v", err)
	}
	r.send(msg(aliceJID, aliceJID, "3EB0P0000000000000006", base+7000, false, photoMsg("/v/invented-later")))
	if _, err := r.w.Media(ctx, chat, "3EB0P0000000000000006"); wire.KindOf(err) != wire.Offline {
		t.Errorf("a photo not yet fetched while offline gave %v", err)
	}
}

// retryAnswer is his phone's answer to a media retry: the photo uploaded
// again at path.
func retryAnswer(t *testing.T, info *types.MessageInfo, key []byte, path string) *events.MediaRetry {
	t.Helper()
	plain, err := proto.Marshal(&waMmsRetry.MediaRetryNotification{StanzaID: proto.String(info.ID),
		DirectPath: proto.String(path), Result: waMmsRetry.MediaRetryNotification_SUCCESS.Enum()})
	if err != nil {
		t.Fatal(err)
	}
	iv := []byte("invented-iv!")
	ct, err := gcmutil.Encrypt(hkdfutil.SHA256(key, nil, []byte("WhatsApp Media Retry Notification"), 32), iv, plain, []byte(info.ID))
	if err != nil {
		t.Fatal(err)
	}
	return &events.MediaRetry{Ciphertext: ct, IV: iv, MessageID: info.ID, ChatID: info.Chat, FromMe: info.IsFromMe}
}

func TestAGonePhotoAsksThePhoneOnceThenIsMediaGone(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	chat := aliceJID.String()
	r.send(msg(aliceJID, aliceJID, "3EB0G0000000000000001", base+2000, false, photoMsg("/v/invented-expired")))

	done := make(chan error, 1)
	go func() {
		_, err := r.w.Media(ctx, chat, "3EB0G0000000000000001")
		done <- err
	}()
	eventually(t, "the retry receipt", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.retries) == 1
	})
	r.clock.Advance(29 * time.Second)
	select {
	case err := <-done:
		t.Fatalf("Media gave up before 30 s: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	r.clock.Advance(time.Second)
	select {
	case err := <-done:
		if wire.KindOf(err) != wire.MediaGone {
			t.Fatalf("got %v, want mediaGone", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Media did not give up after 30 s")
	}
	c.mu.Lock()
	retries, fetches := len(c.retries), len(c.fetches)
	c.mu.Unlock()
	if retries != 1 || fetches != 1 {
		t.Fatalf("%d retry receipts and %d downloads, want one each", retries, fetches)
	}
	if _, err := os.Stat(mediaFile(r, chat, "3EB0G0000000000000001")); !os.IsNotExist(err) {
		t.Fatalf("a file was kept for a gone photo")
	}

	// When the phone uploads it again, the new copy is fetched.
	r.send(msg(aliceJID, aliceJID, "3EB0G0000000000000002", base+3000, false, photoMsg("/v/invented-expired-2")))
	c.with(func() {
		c.files["/v/invented-again"] = []byte("invented jpeg, uploaded again")
		c.onRetry = func(info *types.MessageInfo, key []byte) {
			if info.Chat != aliceJID || info.IsFromMe || !bytes.Equal(key, mediaKey) {
				t.Errorf("the retry receipt named %+v", info)
			}
			go c.dispatch(retryAnswer(t, info, key, "/v/invented-again"))
		}
	})
	path, err := r.w.Media(ctx, chat, "3EB0G0000000000000002")
	if err != nil {
		t.Fatalf("Media after the phone's answer: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "invented jpeg, uploaded again" {
		t.Fatalf("the file holds %q", got)
	}
}

func TestTheMediaFolderIsTrimmedOldestFirst(t *testing.T) {
	r, c := withAlice(t)
	r.w.mediaCap = 100
	dir := filepath.Join(r.dir, "media")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	for i, name := range []string{"aaa-oldest", "bbb-older", "ccc-newer"} {
		p := filepath.Join(dir, name)
		_ = os.WriteFile(p, bytes.Repeat([]byte{'x'}, 40), 0o600)
		at := old.Add(time.Duration(i) * time.Minute)
		_ = os.Chtimes(p, at, at)
	}
	c.with(func() { c.files["/v/invented-new"] = bytes.Repeat([]byte{'y'}, 30) })
	r.send(msg(aliceJID, aliceJID, "3EB0T0000000000000001", base+2000, false, photoMsg("/v/invented-new")))
	path, err := r.w.Media(context.Background(), aliceJID.String(), "3EB0T0000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		left = append(left, e.Name())
	}
	// 40+40+40+30 = 150 > 100: the two oldest go, leaving 70.
	kept := map[string]bool{}
	for _, n := range left {
		kept[n] = true
	}
	if len(left) != 2 || !kept["ccc-newer"] || !kept[filepath.Base(path)] {
		t.Fatalf("left %v, want the new file and ccc-newer", left)
	}
}
