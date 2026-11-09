package whatsapp

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every name, number, JID, ID and text here is invented, and nothing reaches
// WhatsApp: the writes land in the fake client.

// sendOK sends m and fails the test on an error.
func (r *rig) sendOK(m network.Outgoing) string {
	r.t.Helper()
	id, err := r.w.Send(context.Background(), m)
	if err != nil {
		r.t.Fatalf("Send: %v", err)
	}
	if id == "" {
		r.t.Fatalf("Send gave no ID")
	}
	return id
}

// settled waits for a send to leave sending.
func (r *rig) settled(id string) network.SendState {
	r.t.Helper()
	var st network.SendState
	eventually(r.t, "the send to settle", func() bool {
		st = r.w.SendState(id)
		return st.State != network.SendSending
	})
	return st
}

// withAlice is a live adapter with a chat with Alice holding one line.
func withAlice(t *testing.T) (*rig, *fakeClient) {
	r := newRig(t, true)
	c := r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0A0000000000000001", base+1000, "invented hello"))
	return r, c
}

func kindOf(err error) wire.Kind { return wire.KindOf(err) }

func TestSendReturnsTheIDAtOnceAndRecordsItFirst(t *testing.T) {
	r, c := withAlice(t)
	release := make(chan struct{})
	c.with(func() { c.sendBlock = release })
	type atSend struct {
		state network.SendState
		row   bool
	}
	seen := make(chan atSend, 1)
	c.with(func() {
		c.onSend = func(sm sentMessage) {
			_, row, _ := r.st.Outgoing(Name, sm.id)
			seen <- atSend{r.w.SendState(sm.id), row}
		}
	})

	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented reply"})
	// Send has returned while WhatsApp has not answered.
	if st := r.w.SendState(id); st.State != network.SendSending {
		t.Fatalf("straight after Send the state is %+v, want sending", st)
	}
	var at atSend
	select {
	case at = <-seen:
	case <-time.After(5 * time.Second):
		t.Fatalf("the send never started")
	}
	if !at.row || at.state.State != network.SendSending {
		t.Fatalf("when the network send began the outbox row was kept %v, state %+v", at.row, at.state)
	}
	if got := c.sentMessages()[0]; got.id != id || got.to != aliceJID || got.msg.GetConversation() != "invented reply" {
		t.Fatalf("sent %+v under ID %q to %v, want the made ID %q to Alice", got.msg, got.id, got.to, id)
	}
	for _, m := range r.messages(aliceJID.String()) {
		if m.ID == id {
			t.Fatalf("the line was stored before WhatsApp acknowledged it")
		}
	}
	close(release)
	if st := r.settled(id); st.State != network.SendSent {
		t.Fatalf("after the ack the state is %+v", st)
	}
}

func TestTheSentLineHasTheSameID(t *testing.T) {
	r, c := withAlice(t)
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented answer"})
	if st := r.settled(id); st.State != network.SendSent || st.Kind != "" {
		t.Fatalf("state %+v, want sent", st)
	}
	line := r.message(aliceJID.String(), id)
	if !line.FromMe || line.Text != "invented answer" || line.Kind != "text" || line.Source != store.SourceLive ||
		line.TimeMs != base+60_000 || line.SenderID != selfJID.String() || *line.Hidden {
		t.Fatalf("the sent line is %+v", line)
	}
	if got := r.chat(aliceJID.String()); got.LastMessageID != id || !got.LastFromMe {
		t.Fatalf("the chat's last line is %q (fromMe %v), want the sent ID", got.LastMessageID, got.LastFromMe)
	}
	if n := len(c.sentMessages()); n != 1 {
		t.Fatalf("%d sends, want 1", n)
	}
	if len(r.log.named("changed")) == 0 {
		t.Errorf("no changed event after the sent line was stored")
	}
}

func TestAFailureBeforeTheServerIsRefused(t *testing.T) {
	for _, err := range []error{whatsmeow.ErrNotConnected, whatsmeow.ErrRecipientNotOnWhatsApp, whatsmeow.ErrServerReturnedError} {
		t.Run(err.Error(), func(t *testing.T) {
			r, c := withAlice(t)
			c.with(func() { c.sendErr = err })
			id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented"})
			if st := r.settled(id); st.State != network.SendFailed || st.Kind != wire.Refused {
				t.Fatalf("state %+v, want failed/refused", st)
			}
			if got := len(r.messages(aliceJID.String())); got != 1 {
				t.Fatalf("the chat holds %d lines, want only Alice's", got)
			}
		})
	}
}

func TestATimeoutStaysSending(t *testing.T) {
	r, c := withAlice(t)
	c.with(func() { c.sendErr = whatsmeow.ErrMessageTimedOut })
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented"})
	eventually(t, "the send to return", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.sendReturns == 1
	})
	time.Sleep(20 * time.Millisecond)
	if st := r.w.SendState(id); st.State != network.SendSending || st.Kind != "" {
		t.Fatalf("after a timeout the state is %+v, want sending (unclear)", st)
	}
	if got := len(r.messages(aliceJID.String())); got != 1 {
		t.Fatalf("a send that may not have gone was stored as a line")
	}
	if n := len(c.sentMessages()); n != 1 {
		t.Fatalf("an unclear send was tried %d times in the same run", n)
	}
}

func TestASendCutOffByARestartIsSentAgainWithTheSameID(t *testing.T) {
	r, c := withAlice(t)
	c.with(func() { c.sendBlock = make(chan struct{}) }) // never answered: the helper dies first
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented, sent once"})
	eventually(t, "the send to reach the network", func() bool { return len(c.sentMessages()) == 1 })

	r.restart()
	// Not connected yet: nothing goes.
	c2 := r.client()
	time.Sleep(20 * time.Millisecond)
	if n := len(c2.sentMessages()); n != 0 {
		t.Fatalf("%d sends before the connection came up", n)
	}
	if st := r.w.SendState(id); st.State != network.SendSending {
		t.Fatalf("after the restart the state is %+v, want sending", st)
	}
	c2.dispatch(&events.Connected{})
	if st := r.settled(id); st.State != network.SendSent {
		t.Fatalf("the resend ended %+v", st)
	}
	sent := c2.sentMessages()
	if len(sent) != 1 || sent[0].id != id || sent[0].msg.GetConversation() != "invented, sent once" {
		t.Fatalf("the resend was %+v, want the same message under the same ID %q", sent, id)
	}
	var n int
	for _, m := range r.messages(aliceJID.String()) {
		if m.ID == id {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the line is held %d times", n)
	}

	// A third run sends nothing more.
	r.restart()
	c3 := r.client()
	c3.dispatch(&events.Connected{})
	time.Sleep(30 * time.Millisecond)
	if got := len(c3.sentMessages()); got != 0 {
		t.Fatalf("a sent message was sent again (%d)", got)
	}
}

func TestAResendWhoseRetryIsRefusedIsUnclear(t *testing.T) {
	r, c := withAlice(t)
	c.with(func() { c.sendBlock = make(chan struct{}) })
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented"})
	eventually(t, "the send to reach the network", func() bool { return len(c.sentMessages()) == 1 })
	r.mu.Lock()
	r.onMake = func(fc *fakeClient) { fc.sendErr = whatsmeow.ErrNotConnected }
	r.mu.Unlock()
	r.restart()
	r.client().dispatch(&events.Connected{})
	// The first try may have gone: a refused retry is not "safe to retry".
	if st := r.settled(id); st.State != network.SendFailed || st.Kind != wire.Unclear {
		t.Fatalf("state %+v, want failed/unclear", st)
	}
}

func TestASendWhileOfflineWaitsAMinuteForTheConnection(t *testing.T) {
	r, c := withAlice(t)
	c.dispatch(&events.Disconnected{})
	first := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented one"})
	r.clock.Advance(30 * time.Second)
	second := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented two"})
	r.clock.Advance(30 * time.Second)
	if st := r.settled(first); st.State != network.SendFailed || st.Kind != wire.Offline {
		t.Fatalf("after 60 s offline the first send is %+v, want failed/offline", st)
	}
	if st := r.w.SendState(second); st.State != network.SendSending {
		t.Fatalf("after 30 s the second send is %+v, want sending", st)
	}
	c.dispatch(&events.KeepAliveRestored{})
	if st := r.settled(second); st.State != network.SendSent {
		t.Fatalf("after the connection came back the second send is %+v", st)
	}
	if sent := c.sentMessages(); len(sent) != 1 || sent[0].id != second {
		t.Fatalf("sent %+v, want only the second", sent)
	}
}

func TestSendsGoInTheOrderTheyWereMade(t *testing.T) {
	r, c := withAlice(t)
	c.dispatch(&events.Disconnected{})
	photo := writeJPEG(t, r.dir, 8, 6)
	var ids []string
	for _, m := range []network.Outgoing{
		{ChatID: aliceJID.String(), File: photo, Text: "invented first"},
		{ChatID: aliceJID.String(), Text: "invented second"},
		{ChatID: aliceJID.String(), Text: "invented third"},
	} {
		ids = append(ids, r.sendOK(m))
	}
	c.dispatch(&events.KeepAliveRestored{})
	for _, id := range ids {
		r.settled(id)
	}
	sent := c.sentMessages()
	if len(sent) != 3 || sent[0].id != ids[0] || sent[1].id != ids[1] || sent[2].id != ids[2] {
		t.Fatalf("the sends went in the order %v, want %v", []string{sent[0].id, sent[1].id, sent[2].id}, ids)
	}
}

// replyContext is the ContextInfo a sent text carries.
func replyContext(m *waE2E.Message) *waE2E.ContextInfo {
	return m.GetExtendedTextMessage().GetContextInfo()
}

func TestAReplyQuotesTheOriginal(t *testing.T) {
	r, c := withAlice(t)
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented yes", ReplyTo: "3EB0A0000000000000001"})
	r.settled(id)
	sent := c.sentMessages()[0].msg
	ci := replyContext(sent)
	if sent.GetExtendedTextMessage().GetText() != "invented yes" || ci.GetStanzaID() != "3EB0A0000000000000001" ||
		ci.GetParticipant() != aliceJID.String() || ci.GetQuotedMessage().GetConversation() != "invented hello" {
		t.Fatalf("the reply went as %v", sent)
	}
	if line := r.message(aliceJID.String(), id); line.ReplyTo != "3EB0A0000000000000001" {
		t.Errorf("the stored reply points at %q", line.ReplyTo)
	}

	// A reply to his own line names him; a reply to a photo quotes the photo.
	mine := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented again", ReplyTo: id})
	r.settled(mine)
	if ci := replyContext(c.sentMessages()[1].msg); ci.GetParticipant() != selfJID.String() ||
		ci.GetQuotedMessage().GetConversation() != "invented yes" {
		t.Fatalf("a reply to his own line carried %v", ci)
	}
	r.send(msg(groupJID, bobJID, "3EB0A0000000000000002", base+2000, false, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
		Caption: proto.String("invented caption"), DirectPath: proto.String("/v/invented-photo")}}))
	toPhoto := r.sendOK(network.Outgoing{ChatID: groupJID.String(), Text: "invented nice", ReplyTo: "3EB0A0000000000000002"})
	r.settled(toPhoto)
	ci = replyContext(c.sentMessages()[2].msg)
	if ci.GetParticipant() != bobJID.String() || ci.GetQuotedMessage().GetImageMessage().GetCaption() != "invented caption" {
		t.Fatalf("a reply to a group photo carried %v", ci)
	}
}

func TestAReplyToABeeperLineGoesWithoutTheQuote(t *testing.T) {
	r, c := withAlice(t)
	_, err := r.st.Import(Name, store.ImportBatch{BeeperChatID: "!invented:beeper.local", ChatID: aliceJID.String(),
		Chat: store.ImportChat{Kind: "single"},
		Messages: []store.ImportMessage{{BeeperID: "bm-1", FromMe: false, SenderID: aliceJID.String(),
			TimeMs: base - 86_400_000, Text: "invented old line", Kind: "text"}}})
	if err != nil {
		t.Fatal(err)
	}
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented late reply", ReplyTo: "beeper:bm-1"})
	r.settled(id)
	sent := c.sentMessages()[0].msg
	if sent.GetConversation() != "invented late reply" || sent.GetExtendedTextMessage() != nil {
		t.Fatalf("a reply to a Beeper line went as %v, want plain text", sent)
	}
	if line := r.message(aliceJID.String(), id); line.ReplyTo != "" {
		t.Errorf("the stored line says it replies to %q", line.ReplyTo)
	}
}

// writeJPEG writes a w×h JPEG under dir and gives its path.
func writeJPEG(t *testing.T, dir string, w, h int) string {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	img.Set(1, 1, color.RGBA{R: 200, A: 255})
	var b bytes.Buffer
	if err := jpeg.Encode(&b, img, nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "outbox", "invented.jpg")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAPhotoWithTextIsOneMessageWithACaption(t *testing.T) {
	r, c := withAlice(t)
	photo := writeJPEG(t, r.dir, 8, 6)
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented look", File: photo})
	if st := r.settled(id); st.State != network.SendSent {
		t.Fatalf("state %+v", st)
	}
	sent := c.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("%d messages sent, want one", len(sent))
	}
	img := sent[0].msg.GetImageMessage()
	if img == nil || img.GetCaption() != "invented look" || img.GetMimetype() != "image/jpeg" ||
		img.GetWidth() != 8 || img.GetHeight() != 6 || img.GetDirectPath() != "/v/up-1" || len(img.GetMediaKey()) == 0 {
		t.Fatalf("the photo went as %v", sent[0].msg)
	}
	if ups := c.uploadList(); len(ups) != 1 || ups[0].kind != whatsmeow.MediaImage {
		t.Fatalf("uploads %d, want one image", len(ups))
	}
	lines := r.messages(aliceJID.String())
	if len(lines) != 2 {
		t.Fatalf("the chat holds %d lines, want Alice's and one photo", len(lines))
	}
	line := r.message(aliceJID.String(), id)
	if line.Kind != "photo" || line.Text != "invented look" || len(line.Media) != 1 || !line.Media[0].HasFile ||
		line.Media[0].Width != 8 {
		t.Fatalf("the photo line is %+v", line)
	}
	// His own photo opens without a download.
	path, err := r.w.Media(context.Background(), aliceJID.String(), id)
	if err != nil {
		t.Fatalf("Media of his own photo: %v", err)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, c.uploadList()[0].data) {
		t.Errorf("his own photo's file is not what he sent")
	}
	if len(c.fetches) != 0 {
		t.Errorf("his own photo was downloaded")
	}

	// A PNG is a photo too; an MP4 is a video.
	var pb bytes.Buffer
	_ = png.Encode(&pb, image.NewRGBA(image.Rect(0, 0, 3, 2)))
	pngPath := filepath.Join(r.dir, "outbox", "invented.png")
	_ = os.WriteFile(pngPath, pb.Bytes(), 0o600)
	mp4Path := filepath.Join(r.dir, "outbox", "invented.mp4")
	_ = os.WriteFile(mp4Path, append([]byte("\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"), make([]byte, 64)...), 0o600)
	pid := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), File: pngPath})
	vid := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), File: mp4Path, Text: "invented clip"})
	r.settled(pid)
	r.settled(vid)
	sent = c.sentMessages()
	if sent[1].msg.GetImageMessage().GetMimetype() != "image/png" {
		t.Errorf("the PNG went as %v", sent[1].msg)
	}
	if v := sent[2].msg.GetVideoMessage(); v.GetMimetype() != "video/mp4" || v.GetCaption() != "invented clip" ||
		c.uploadList()[2].kind != whatsmeow.MediaVideo {
		t.Errorf("the MP4 went as %v", sent[2].msg)
	}
	if line := r.message(aliceJID.String(), vid); line.Kind != "video" || line.Text != "invented clip" {
		t.Errorf("the video line is %+v", line)
	}
}

func TestAnUnknownTypeIsADocument(t *testing.T) {
	r, c := withAlice(t)
	path := filepath.Join(r.dir, "outbox", "invented-notes.unknownkind")
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	_ = os.WriteFile(path, []byte("\x01\x02invented bytes of no known kind\x00"), 0o600)
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), File: path, FileName: "Invented Notes.unknownkind", Text: "invented see"})
	r.settled(id)
	sent := c.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("%d messages", len(sent))
	}
	doc := sent[0].msg.GetDocumentWithCaptionMessage().GetMessage().GetDocumentMessage()
	if doc == nil || doc.GetFileName() != "Invented Notes.unknownkind" || doc.GetCaption() != "invented see" ||
		doc.GetMimetype() != "application/octet-stream" || c.uploadList()[0].kind != whatsmeow.MediaDocument {
		t.Fatalf("the file went as %v", sent[0].msg)
	}
	if line := r.message(aliceJID.String(), id); line.Kind != "file" || line.Text != "invented see" {
		t.Fatalf("the file line is %+v", line)
	}

	// With no file name, the file's own; with no caption, a plain document.
	bare := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), File: path})
	r.settled(bare)
	if d := c.sentMessages()[1].msg.GetDocumentMessage(); d.GetFileName() != "invented-notes.unknownkind" {
		t.Fatalf("a nameless file went as %v", c.sentMessages()[1].msg)
	}
}

func TestOverTheLimitIsTooLarge(t *testing.T) {
	r, c := withAlice(t)
	sized := func(name string, head []byte, size int64) string {
		t.Helper()
		path := filepath.Join(r.dir, "outbox", name)
		_ = os.MkdirAll(filepath.Dir(path), 0o700)
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write(head)
		if err := f.Truncate(size); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return path
	}
	huge := sized("invented-huge.bin", nil, 100<<20+1)
	if _, err := r.w.Send(context.Background(), network.Outgoing{ChatID: aliceJID.String(), File: huge}); kindOf(err) != wire.TooLarge {
		t.Fatalf("a file over 100 MB gave %v, want tooLarge", err)
	}
	if sending, _ := r.st.OutgoingSending(Name); len(sending) != 0 {
		t.Fatalf("a refused file was kept in the outbox")
	}

	// A photo over 16 MB goes as a document.
	jpg, _ := os.ReadFile(writeJPEG(t, r.dir, 4, 4))
	big := sized("invented-big.jpg", jpg, 16<<20+1)
	id := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), File: big})
	r.settled(id)
	if d := c.sentMessages()[0].msg.GetDocumentMessage(); d.GetMimetype() != "image/jpeg" || d.GetFileName() != "invented-big.jpg" {
		t.Fatalf("a 16 MB+ photo went as %v", c.sentMessages()[0].msg)
	}
	if n := c.writes(); n != 2 { // one upload, one send
		t.Fatalf("%d writes, want an upload and a send", n)
	}
}

func TestReactUsesTheTargetsSender(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	r.send(textFrom(groupJID, bobJID, "3EB0A0000000000000003", base+3000, "invented group line"))
	r.send(textMine(groupJID, "3EB0A0000000000000004", base+4000, "invented own line"))

	if err := r.w.React(ctx, groupJID.String(), "3EB0A0000000000000003", "👍", "txn-1"); err != nil {
		t.Fatalf("React: %v", err)
	}
	if err := r.w.React(ctx, groupJID.String(), "3EB0A0000000000000004", "😂", "txn-2"); err != nil {
		t.Fatalf("React: %v", err)
	}
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "❤️", "txn-3"); err != nil {
		t.Fatalf("React: %v", err)
	}
	sent := c.sentMessages()
	if len(sent) != 3 {
		t.Fatalf("%d sends", len(sent))
	}
	theirs := sent[0].msg.GetReactionMessage()
	if sent[0].to != groupJID || theirs.GetText() != "👍" || theirs.GetKey().GetID() != "3EB0A0000000000000003" ||
		theirs.GetKey().GetFromMe() || theirs.GetKey().GetParticipant() != bobJID.String() ||
		theirs.GetKey().GetRemoteJID() != groupJID.String() {
		t.Fatalf("a reaction to Bob's group line went as %v", sent[0].msg)
	}
	mine := sent[1].msg.GetReactionMessage()
	if !mine.GetKey().GetFromMe() || mine.GetKey().Participant != nil {
		t.Fatalf("a reaction to his own line went as %v", sent[1].msg)
	}
	direct := sent[2].msg.GetReactionMessage()
	if direct.GetKey().GetFromMe() || direct.GetKey().Participant != nil || direct.GetKey().GetRemoteJID() != aliceJID.String() {
		t.Fatalf("a reaction in a direct chat went as %v", sent[2].msg)
	}
	if sent[0].id == "" || sent[0].id == sent[1].id {
		t.Errorf("reactions went without their own made IDs")
	}
	line := r.message(groupJID.String(), "3EB0A0000000000000003")
	if len(line.Reactions) != 1 || line.Reactions[0].Emoji != "👍" || !line.Reactions[0].FromMe ||
		line.Reactions[0].SenderID != selfJID.String() {
		t.Fatalf("the stored reactions are %+v", line.Reactions)
	}
}

func TestAnEmptyEmojiRemoves(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "👍", "txn-a"); err != nil {
		t.Fatal(err)
	}
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "", "txn-b"); err != nil {
		t.Fatal(err)
	}
	sent := c.sentMessages()
	if len(sent) != 2 || sent[1].msg.GetReactionMessage() == nil || sent[1].msg.GetReactionMessage().GetText() != "" ||
		sent[1].msg.GetReactionMessage().Text == nil {
		t.Fatalf("the removal went as %v", sent)
	}
	if line := r.message(aliceJID.String(), "3EB0A0000000000000001"); len(line.Reactions) != 0 {
		t.Fatalf("his reaction is still stored: %+v", line.Reactions)
	}
}

func TestARepeatedTransactionSendsNothing(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	for range 3 {
		if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "👍", "txn-same"); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(c.sentMessages()); n != 1 {
		t.Fatalf("a repeated transaction sent %d reactions", n)
	}

	// One refused before it went may be tried again under its transaction.
	c.with(func() { c.sendErr = whatsmeow.ErrNotConnected })
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "😮", "txn-refused"); kindOf(err) != wire.Refused {
		t.Fatalf("a refused reaction gave %v", err)
	}
	c.with(func() { c.sendErr = nil })
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "😮", "txn-refused"); err != nil {
		t.Fatal(err)
	}
	sent := c.sentMessages()
	if len(sent) != 3 || sent[1].id != sent[2].id {
		t.Fatalf("the retry of a refused reaction: %d sends, IDs %q and %q", len(sent), sent[1].id, sent[2].id)
	}
	// A timed-out one (it may have gone) is not sent again.
	c.with(func() { c.sendErr = whatsmeow.ErrMessageTimedOut })
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "🎉", "txn-unclear"); kindOf(err) != wire.Unclear {
		t.Fatalf("a timed-out reaction gave %v", err)
	}
	c.with(func() { c.sendErr = nil })
	if err := r.w.React(ctx, aliceJID.String(), "3EB0A0000000000000001", "🎉", "txn-unclear"); err != nil {
		t.Fatal(err)
	}
	if n := len(c.sentMessages()); n != 4 {
		t.Fatalf("an unclear reaction was sent again (%d sends)", n)
	}
}

func TestReactingToABeeperLineIsNotOnNetwork(t *testing.T) {
	r, c := withAlice(t)
	err := r.w.React(context.Background(), aliceJID.String(), "beeper:bm-9", "👍", "txn-b")
	if kindOf(err) != wire.NotOnNetwork {
		t.Fatalf("got %v, want notOnNetwork", err)
	}
	if c.writes() != 0 {
		t.Fatalf("something was written")
	}
	if err := r.w.React(context.Background(), aliceJID.String(), "3EB0A0000000000000099", "👍", "txn-c"); kindOf(err) != wire.NotFound {
		t.Fatalf("a line nobody holds gave %v, want notFound", err)
	}
}

func TestMarkReadStopsAtThroughAndGroupsBySender(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	g := groupJID.String()
	r.send(textFrom(groupJID, aliceJID, "3EB0M0000000000000001", base+1000, "invented a1"))
	r.send(textFrom(groupJID, bobJID, "3EB0M0000000000000002", base+2000, "invented b1"))
	r.send(textMine(groupJID, "3EB0M0000000000000003", base+3000, "invented mine"))
	r.send(textFrom(groupJID, aliceJID, "3EB0M0000000000000004", base+4000, "invented a2"))
	r.send(textFrom(groupJID, aliceJID, "3EB0M0000000000000005", base+5000, "invented a3, newer"))
	if u := r.chat(g).Unread; u != 4 {
		t.Fatalf("unread %d before", u)
	}

	if err := r.w.MarkRead(ctx, g, "3EB0M0000000000000004"); err != nil {
		t.Fatalf("MarkRead: %v", err)
	}
	got := map[string][]string{}
	for _, m := range c.marks {
		if m.chat != groupJID {
			t.Fatalf("a receipt went to %v", m.chat)
		}
		got[m.sender.String()] = append(got[m.sender.String()], m.ids...)
	}
	want := map[string][]string{
		aliceJID.String(): {"3EB0M0000000000000001", "3EB0M0000000000000004"},
		bobJID.String():   {"3EB0M0000000000000002"},
	}
	if len(c.marks) != 2 || !reflect.DeepEqual(got, want) {
		t.Fatalf("receipts %v (%d calls), want %v in one call per sender", got, len(c.marks), want)
	}
	if u := r.chat(g).Unread; u != 1 {
		t.Fatalf("unread %d after, want the newer line still unread", u)
	}

	// Null is the newest incoming line; lines already read are not marked again.
	if err := r.w.MarkRead(ctx, g, ""); err != nil {
		t.Fatal(err)
	}
	if len(c.marks) != 3 || !reflect.DeepEqual(c.marks[2].ids, []string{"3EB0M0000000000000005"}) {
		t.Fatalf("the second mark sent %+v", c.marks[2:])
	}
	if u := r.chat(g).Unread; u != 0 {
		t.Fatalf("unread %d", u)
	}
	// Direct chat: the sender is the person.
	if err := r.w.MarkRead(ctx, aliceJID.String(), ""); err != nil {
		t.Fatal(err)
	}
	if m := c.marks[3]; m.chat != aliceJID || !reflect.DeepEqual(m.ids, []string{"3EB0A0000000000000001"}) {
		t.Fatalf("a direct chat's receipt was %+v", m)
	}
	if err := r.w.MarkRead(ctx, g, "3EB0M0000000000000099"); kindOf(err) != wire.NotFound {
		t.Fatalf("an unknown through gave %v", err)
	}
}

func TestMarkUnreadAndMuteAreAppStatePatches(t *testing.T) {
	r, c := withAlice(t)
	ctx := context.Background()
	r.send(textMine(aliceJID, "3EB0A0000000000000002", base+2000, "invented mine"))
	if err := r.w.MarkRead(ctx, aliceJID.String(), ""); err != nil {
		t.Fatal(err)
	}
	if err := r.w.MarkUnread(ctx, aliceJID.String()); err != nil {
		t.Fatalf("MarkUnread: %v", err)
	}
	if err := r.w.SetMuted(ctx, aliceJID.String(), true); err != nil {
		t.Fatalf("SetMuted: %v", err)
	}
	if err := r.w.SetMuted(ctx, aliceJID.String(), false); err != nil {
		t.Fatal(err)
	}
	if len(c.patches) != 3 {
		t.Fatalf("%d patches", len(c.patches))
	}
	lastKey := &waCommon.MessageKey{RemoteJID: proto.String(aliceJID.String()), FromMe: proto.Bool(true), ID: proto.String("3EB0A0000000000000002")}
	wantUnread := appstate.BuildMarkChatAsRead(aliceJID, false, time.UnixMilli(base+2000), lastKey)
	if !samePatch(c.patches[0], wantUnread) {
		t.Fatalf("mark unread sent %+v, want %+v", c.patches[0], wantUnread)
	}
	if !samePatch(c.patches[1], appstate.BuildMute(aliceJID, true, 0)) ||
		c.patches[1].Mutations[0].Value.GetMuteAction().GetMuteEndTimestamp() != -1 {
		t.Fatalf("mute sent %+v, want a mute for ever", c.patches[1])
	}
	if !samePatch(c.patches[2], appstate.BuildMute(aliceJID, false, 0)) {
		t.Fatalf("unmute sent %+v", c.patches[2])
	}
	if got := r.chat(aliceJID.String()); got.Unread != 1 || got.Muted {
		t.Fatalf("after the patches the chat shows unread %d, muted %v; want 1 and unmuted", got.Unread, got.Muted)
	}
	if err := r.w.SetMuted(ctx, aliceJID.String(), true); err != nil {
		t.Fatal(err)
	}
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("a mute did not show")
	}
}

// samePatch compares two patches but for their time.
func samePatch(a, b appstate.PatchInfo) bool {
	if a.Type != b.Type || len(a.Mutations) != len(b.Mutations) {
		return false
	}
	for i := range a.Mutations {
		x, y := a.Mutations[i], b.Mutations[i]
		if !reflect.DeepEqual(x.Index, y.Index) || x.Version != y.Version || !proto.Equal(x.Value, y.Value) {
			return false
		}
	}
	return true
}

func TestEveryWriteWhileSignedOutIsNotConnected(t *testing.T) {
	check := func(t *testing.T, r *rig) {
		t.Helper()
		ctx := context.Background()
		chat := aliceJID.String()
		if _, err := r.w.Send(ctx, network.Outgoing{ChatID: chat, Text: "invented"}); kindOf(err) != wire.NotConnected {
			t.Errorf("Send: %v", err)
		}
		if err := r.w.React(ctx, chat, "3EB0A0000000000000001", "👍", "txn"); kindOf(err) != wire.NotConnected {
			t.Errorf("React: %v", err)
		}
		if err := r.w.MarkRead(ctx, chat, ""); kindOf(err) != wire.NotConnected {
			t.Errorf("MarkRead: %v", err)
		}
		if err := r.w.MarkUnread(ctx, chat); kindOf(err) != wire.NotConnected {
			t.Errorf("MarkUnread: %v", err)
		}
		if err := r.w.SetMuted(ctx, chat, true); kindOf(err) != wire.NotConnected {
			t.Errorf("SetMuted: %v", err)
		}
		if _, err := r.w.Media(ctx, chat, "3EB0A0000000000000002"); kindOf(err) != wire.NotConnected {
			t.Errorf("Media: %v", err)
		}
		if sending, _ := r.st.OutgoingSending(Name); len(sending) != 0 {
			t.Errorf("a send was kept while signed out")
		}
	}
	t.Run("never linked", func(t *testing.T) {
		r := newRig(t, false)
		r.start()
		check(t, r)
	})
	t.Run("unlinked by WhatsApp", func(t *testing.T) {
		r, c := withAlice(t)
		r.send(msg(aliceJID, aliceJID, "3EB0A0000000000000002", base+2000, false, &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			DirectPath: proto.String("/v/invented")}}))
		c.dispatch(&events.LoggedOut{})
		before := c.writes()
		check(t, r)
		if c.writes() != before {
			t.Errorf("a write reached the client")
		}
	})
}

func TestOnlyTheAllowedWritesExist(t *testing.T) {
	names := func(t reflect.Type) []string {
		var out []string
		for i := range t.NumMethod() {
			out = append(out, t.Method(i).Name)
		}
		sort.Strings(out)
		return out
	}
	writer := names(reflect.TypeOf((*waWriter)(nil)).Elem())
	want := []string{"Download", "GenerateMessageID", "Logout", "MarkRead", "SendAppState", "SendMediaRetryReceipt", "SendMessage", "Upload"}
	if !reflect.DeepEqual(writer, want) {
		t.Fatalf("waWriter has %v, want exactly %v", writer, want)
	}
	// The rest of what the adapter uses of the client signs in, stays
	// connected, reads history and acknowledges it, and builds a reaction for
	// SendMessage: nothing else writes.
	client := names(reflect.TypeOf((*waClient)(nil)).Elem())
	rest := []string{"AddEventHandlerWithSuccessStatus", "BuildReaction", "Connect", "Disconnect", "DownloadHistorySync",
		"GetQRChannel", "IsConnected", "ParseWebMessage", "SendProtocolMessageReceipt"}
	all := append(append([]string(nil), want...), rest...)
	sort.Strings(all)
	if !reflect.DeepEqual(client, all) {
		t.Fatalf("waClient has %v, want the writer's eight and %v", client, rest)
	}
	var _ waWriter = (*whatsmeow.Client)(nil)
}

func TestLogoutForgetsTheLinkAndKeepsTheChats(t *testing.T) {
	r, c := withAlice(t)
	if err := r.w.Logout(context.Background()); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if c.logouts != 1 {
		t.Fatalf("%d logouts", c.logouts)
	}
	if s := r.w.Status(); s.State != network.NotConnected {
		t.Fatalf("after Logout the state is %v", s.State)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "whatsapp", "linked")); !os.IsNotExist(err) {
		t.Fatalf("the linked marker is still there (%v)", err)
	}
	if got := len(r.messages(aliceJID.String())); got != 1 {
		t.Fatalf("the chat holds %d lines after Logout", got)
	}
	r.restart()
	if s := r.w.Status(); s.State != network.NotConnected {
		t.Fatalf("after a restart the state is %v, want notConnected (not reconnectNeeded)", s.State)
	}
	// Signed out, Logout has nothing to tell WhatsApp.
	if err := r.w.Logout(context.Background()); err != nil {
		t.Fatalf("a second Logout: %v", err)
	}
}

func TestASendWaitingWhenHeDisconnectsFails(t *testing.T) {
	r, c := withAlice(t)
	c.dispatch(&events.Disconnected{})
	waiting := r.sendOK(network.Outgoing{ChatID: aliceJID.String(), Text: "invented, never"})
	// Linked but offline: WhatsApp cannot be told, so nothing is forgotten.
	if err := r.w.Logout(context.Background()); kindOf(err) != wire.Offline {
		t.Fatalf("Logout while offline gave %v, want offline", err)
	}
	if st := r.w.SendState(waiting); st.State != network.SendSending {
		t.Fatalf("a refused Logout moved the send to %+v", st)
	}
	c.dispatch(&events.LoggedOut{}) // WhatsApp unlinks; then he presses Disconnect
	if err := r.w.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := r.settled(waiting); st.State != network.SendFailed || st.Kind != wire.NotConnected {
		t.Fatalf("a send waiting at Disconnect is %+v, want failed/notConnected", st)
	}
	if c.writes() != 0 {
		t.Fatalf("Disconnect after an unlink wrote to WhatsApp")
	}
}
