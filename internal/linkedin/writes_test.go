package linkedin

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// heldChat stores conversation n as a chat with Alice, as a catch-up would.
func (r *rig) heldChat(n int) string {
	r.t.Helper()
	id := convURN(n)
	if err := r.st.PutChat(Name, store.ChatInfo{ID: id, Kind: "single", LastActivityMs: start.UnixMilli()}); err != nil {
		r.t.Fatal(err)
	}
	return id
}

// arrive hands a message to the network on the realtime stream, as LinkedIn
// sends one, and waits until it is stored.
func (r *rig) arrive(c *fakeClient, m linkedingo.Message) {
	r.t.Helper()
	c.dispatch(messageEvent(m))
	chat := urnString(m.Conversation.EntityURN)
	id := urnString(m.EntityURN)
	eventually(r.t, "the message to be stored", func() bool {
		_, ok := r.st.LineState(Name, chat, id)
		return ok
	})
}

func (r *rig) send(m network.Outgoing) string {
	r.t.Helper()
	id, err := r.l.Send(context.Background(), m)
	if err != nil {
		r.t.Fatalf("Send: %v", err)
	}
	return id
}

// waitSend waits until the send id is in state (with kind, when failed).
func (r *rig) waitSend(id, state string, kind wire.Kind) {
	r.t.Helper()
	eventually(r.t, "send "+state+"/"+string(kind), func() bool {
		s := r.l.SendState(id)
		return s.State == state && s.Kind == kind
	})
	r.idle()
}

func (r *rig) outgoing(id string) store.Outgoing {
	r.t.Helper()
	o, ok, err := r.st.Outgoing(Name, id)
	if err != nil || !ok {
		r.t.Fatalf("no outgoing %q: %v", id, err)
	}
	return o
}

// lineByID is the chat's line id; ok is false when it holds none.
func (r *rig) lineByID(chat, id string) (store.Message, int) {
	r.t.Helper()
	var found store.Message
	n := 0
	for _, m := range r.lines(chat) {
		if m.ID == id {
			found = m
			n++
		}
	}
	return found, n
}

// netErr is the network failing under a request.
func netErr() error {
	return &url.Error{Op: "Post", URL: "https://invented.example/messages", Err: errors.New("invented: connection reset")}
}

func TestSendReturnsAPendingIDAtOnceAndSettlesToTheURN(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	release := make(chan struct{})
	r.f.send = func(c *fakeClient, ctx context.Context, conv string, body linkedingo.SendMessageBody, rc []linkedingo.SendRenderContent, txn string) (*linkedingo.MessageSentResponse, error) {
		<-release
		return c.f.sentNow(conv, body), nil
	}

	id := r.send(network.Outgoing{ChatID: chat, Text: "invented hello"})
	token, ok := strings.CutPrefix(id, "li-pending:")
	if !ok {
		t.Fatalf("message ID %q, want li-pending:<origin token>", id)
	}
	if _, err := uuid.Parse(token); err != nil {
		t.Fatalf("origin token %q is not a UUID", token)
	}
	if s := r.l.SendState(id); s.State != network.SendSending {
		t.Fatalf("before LinkedIn answers the send is %v, want sending", s)
	}
	eventually(t, "SendMessage to be asked", func() bool { return r.f.count("SendMessage") == 1 })
	close(release)
	r.waitSend(id, network.SendSent, "")

	sends := r.f.sendsNow()
	if len(sends) != 1 || sends[0].txn != token || sends[0].conv != chat || sends[0].body.Text != "invented hello" {
		t.Fatalf("SendMessage was asked %+v, want once to the chat with the origin token", sends)
	}
	want := msgURN(800001)
	if o := r.outgoing(id); o.LineID != want {
		t.Errorf("lineID = %q, want the sent message's URN", o.LineID)
	}
	line, n := r.lineByID(chat, want)
	if n != 1 || !line.FromMe || line.Text != "invented hello" || line.Source != store.SourceLive {
		t.Errorf("the sent line is %+v (%d rows), want his, live, with its text", line, n)
	}
}

func TestTheEchoBeforeTheAnswerIsOneLine(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	at := start.Add(time.Hour)
	r.clock.Advance(time.Hour)
	echo := text(1, 777, me(), at, "invented echo")
	r.f.send = func(c *fakeClient, ctx context.Context, conv string, body linkedingo.SendMessageBody, rc []linkedingo.SendRenderContent, txn string) (*linkedingo.MessageSentResponse, error) {
		// His own message comes on the stream before the send's answer.
		c.dispatch(messageEvent(echo))
		return &linkedingo.MessageSentResponse{Data: linkedingo.Message{
			EntityURN:   echo.EntityURN,
			Body:        echo.Body,
			DeliveredAt: echo.DeliveredAt,
		}}, nil
	}

	id := r.send(network.Outgoing{ChatID: chat, Text: "invented echo"})
	r.waitSend(id, network.SendSent, "")

	if o := r.outgoing(id); o.LineID != msgURN(777) {
		t.Errorf("lineID = %q, want the echo's URN", o.LineID)
	}
	lines := r.lines(chat)
	if len(lines) != 1 || lines[0].ID != msgURN(777) || !lines[0].FromMe {
		t.Fatalf("the chat holds %d lines (%+v), want his one line", len(lines), lines)
	}
}

func TestASendCutOffByARestartIsNeverSentAgain(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	r.f.send = func(c *fakeClient, ctx context.Context, conv string, body linkedingo.SendMessageBody, rc []linkedingo.SendRenderContent, txn string) (*linkedingo.MessageSentResponse, error) {
		<-ctx.Done() // LinkedIn has it, and the helper stops before the answer
		return nil, ctx.Err()
	}
	id := r.send(network.Outgoing{ChatID: chat, Text: "invented cut off"})
	eventually(t, "SendMessage to be asked", func() bool { return r.f.count("SendMessage") == 1 })

	// And one an earlier run kept as sending when the helper died outright.
	died := "li-pending:" + uuid.NewString()
	if err := r.st.PutOutgoing(Name, store.Outgoing{ID: died, ChatID: chat, Kind: "message", MadeMs: start.UnixMilli(),
		Request: []byte(`{"text":"invented died","originToken":"` + strings.TrimPrefix(died, "li-pending:") + `"}`)}); err != nil {
		t.Fatal(err)
	}

	r.restart()
	for _, x := range []string{id, died} {
		if s := r.l.SendState(x); s.State != network.SendUnknown {
			t.Errorf("after the restart a cut-off send is %v, want unknown", s)
		}
	}
	r.f.send = nil
	r.live()
	r.idle()
	if n := r.f.count("SendMessage"); n != 1 {
		t.Errorf("SendMessage was asked %d times, want 1: a cut-off send never goes again", n)
	}
	for _, x := range []string{id, died} {
		if s := r.l.SendState(x); s.State != network.SendUnknown {
			t.Errorf("after going live again a cut-off send is %v, want unknown", s)
		}
	}
}

func TestAFailedSendIsOneAttemptAndUnknown(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)

	r.f.setErr("SendMessage", netErr())
	first := r.send(network.Outgoing{ChatID: chat, Text: "invented one", ClientID: "client-invented-1"})
	r.waitSend(first, network.SendUnknown, "")
	if n := r.f.count("SendMessage"); n != 1 {
		t.Fatalf("a send that failed on the network made %d SendMessage calls, want 1", n)
	}

	r.f.setErr("SendMessage", statusErr(503))
	second := r.send(network.Outgoing{ChatID: chat, Text: "invented two"})
	r.waitSend(second, network.SendUnknown, "")
	if n := r.f.count("SendMessage"); n != 2 {
		t.Fatalf("a send that got a 503 made %d more SendMessage calls, want 1", n-1)
	}
	if st, k := r.l.stateNow(); st != network.Live || k != "" {
		t.Errorf("after the failed sends the state is %s/%s, want live", st, k)
	}

	// The same key again (its answer lost) is the same send: nothing goes.
	if again := r.send(network.Outgoing{ChatID: chat, Text: "invented one", ClientID: "client-invented-1"}); again != first {
		t.Errorf("the same key gave %q, want the first send's %q", again, first)
	}

	r.f.setErr("SendMessage", nil)
	r.restart()
	r.live()
	r.idle()
	if n := r.f.count("SendMessage"); n != 2 {
		t.Errorf("after a restart SendMessage was asked %d times in all, want 2", n)
	}
	for _, id := range []string{first, second} {
		if s := r.l.SendState(id); s.State != network.SendUnknown {
			t.Errorf("after a restart a failed send is %v, want unknown", s)
		}
	}
}

// pngBytes is a tiny invented PNG: its signature and a few bytes.
var pngBytes = append([]byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"), make([]byte, 64)...)

func TestAPhotoIsUploadedThenSentWithItsText(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	dir := t.TempDir()
	photo := filepath.Join(dir, "on-disk.png")
	if err := os.WriteFile(photo, pngBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	n := len(r.f.callsNow())
	id := r.send(network.Outgoing{ChatID: chat, Text: "invented caption", File: photo, FileName: "invented.png"})
	r.waitSend(id, network.SendSent, "")

	if names := r.f.namesSince(n); !slices.Equal(names, []string{"UploadMedia", "SendMessage"}) {
		t.Fatalf("calls = %v, want UploadMedia then SendMessage", names)
	}
	up := r.f.uploadsNow()[0]
	if up.kind != linkedingo.MediaUploadTypePhotoAttachment || up.filename != "invented.png" || up.contentType != "image/png" ||
		up.size != len(pngBytes) || string(up.data) != string(pngBytes) {
		t.Errorf("the upload was %s %q %s %d bytes, want the PNG as a photo attachment", up.kind, up.filename, up.contentType, up.size)
	}
	sent := r.f.sendsNow()[0]
	if sent.body.Text != "invented caption" {
		t.Errorf("the message's text is %q, want the caption", sent.body.Text)
	}
	if len(sent.rc) != 1 || sent.rc[0].File == nil {
		t.Fatalf("the message carries %+v, want the file", sent.rc)
	}
	f := sent.rc[0].File
	if f.AssetURN.String() != uploadedAsset || f.Name != "invented.png" || f.MediaType != "image/png" || f.ByteSize != len(pngBytes) {
		t.Errorf("the file is %+v, want the uploaded asset", f)
	}
	// His photo opens without a download.
	path, err := r.l.Media(context.Background(), chat, r.outgoing(id).LineID)
	if err != nil {
		t.Fatalf("Media of his sent photo: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(pngBytes) {
		t.Errorf("his sent photo's file holds %d bytes, want the PNG", len(got))
	}
	if r.f.count("DownloadHTTP") != 0 {
		t.Errorf("his own sent photo was downloaded")
	}

	// Any other file goes as a file attachment.
	pdf := filepath.Join(dir, "on-disk.pdf")
	if err := os.WriteFile(pdf, []byte("%PDF-1.4\ninvented\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	id2 := r.send(network.Outgoing{ChatID: chat, File: pdf, FileName: "invented.pdf"})
	r.waitSend(id2, network.SendSent, "")
	if up := r.f.uploadsNow()[1]; up.kind != linkedingo.MediaUploadTypeFileAttachment || up.contentType != "application/pdf" {
		t.Errorf("the PDF was uploaded as %s %s, want a file attachment", up.kind, up.contentType)
	}
}

func TestOverTwentyMegabytesIsTooLarge(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	big := filepath.Join(t.TempDir(), "big.png")
	f, err := os.Create(big)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(pngBytes); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(20<<20 + 1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	n := len(r.f.callsNow())
	_, err = r.l.Send(context.Background(), network.Outgoing{ChatID: chat, Text: "invented", File: big})
	if wire.KindOf(err) != wire.TooLarge {
		t.Fatalf("Send of a 20 MB + 1 byte file: %v, want tooLarge", err)
	}
	r.idle()
	if names := r.f.namesSince(n); len(names) != 0 {
		t.Errorf("a file too large asked LinkedIn %v", names)
	}

	// Exactly 20 MB goes.
	if err := os.Truncate(big, 20<<20); err != nil {
		t.Fatal(err)
	}
	id := r.send(network.Outgoing{ChatID: chat, File: big})
	r.waitSend(id, network.SendSent, "")
}

func TestAReplyCarriesTheOriginal(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	at := start.Add(-time.Hour)
	r.arrive(c, text(1, 5, person(aliceID, "Alice", "Invented"), at, "invented question"))

	id := r.send(network.Outgoing{ChatID: chat, Text: "invented answer", ReplyTo: msgURN(5)})
	r.waitSend(id, network.SendSent, "")
	sent := r.f.sendsNow()[0]
	if len(sent.rc) != 1 || sent.rc[0].RepliedMessageContent == nil {
		t.Fatalf("the message carries %+v, want the original", sent.rc)
	}
	rep := sent.rc[0].RepliedMessageContent
	if rep.OriginalMessageURN.String() != msgURN(5) {
		t.Errorf("original message %s, want the one replied to", rep.OriginalMessageURN)
	}
	if want := "urn:li:msg_messagingParticipant:urn:li:fsd_profile:" + aliceID; rep.OriginalSenderURN.String() != want {
		t.Errorf("original sender %s, want %s", rep.OriginalSenderURN, want)
	}
	if !rep.OriginalSendAt.Time.Equal(at) {
		t.Errorf("original time %s, want %s", rep.OriginalSendAt.Time, at)
	}
	if rep.MessageBody.Text != "invented question" {
		t.Errorf("original text %q, want the question", rep.MessageBody.Text)
	}
	if sent.body.Text != "invented answer" {
		t.Errorf("the reply's text is %q", sent.body.Text)
	}
}

func TestAReplyToABeeperLineGoesWithoutIt(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	id := r.send(network.Outgoing{ChatID: chat, Text: "invented answer", ReplyTo: "beeper:$invented-beeper-line"})
	r.waitSend(id, network.SendSent, "")
	sent := r.f.sendsNow()
	if len(sent) != 1 || len(sent[0].rc) != 0 || sent[0].body.Text != "invented answer" {
		t.Fatalf("SendMessage was asked %+v, want the text alone", sent)
	}
}

func TestA400IsRefusedAndA429PausesAndFails(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	r.arrive(c, text(1, 5, person(aliceID, "Alice", "Invented"), start, "invented question"))

	r.f.setErr("SendMessage", statusErr(400))
	refused := r.send(network.Outgoing{ChatID: chat, Text: "invented refused"})
	r.waitSend(refused, network.SendFailed, wire.Refused)
	if _, blocked := r.l.pause.blocked(); blocked {
		t.Fatal("a 400 paused LinkedIn")
	}
	if st, k := r.l.stateNow(); st != network.Live || k != "" {
		t.Fatalf("after a 400 the state is %s/%s, want live", st, k)
	}
	for _, code := range []int{404, 422} {
		r.f.setErr("SendMessage", statusErr(code))
		id := r.send(network.Outgoing{ChatID: chat, Text: "invented refused"})
		r.waitSend(id, network.SendFailed, wire.Refused)
	}

	r.f.setErr("SendMessage", statusErr(429))
	limited := r.send(network.Outgoing{ChatID: chat, Text: "invented limited"})
	r.waitSend(limited, network.SendFailed, wire.RateLimited)
	if _, blocked := r.l.pause.blocked(); !blocked {
		t.Fatal("a 429 on a send did not pause LinkedIn")
	}
	r.waitState(network.Offline, wire.RateLimited)

	// Paused: every write is refused, and nothing is asked of LinkedIn.
	n := len(r.f.callsNow())
	if _, err := r.l.Send(context.Background(), network.Outgoing{ChatID: chat, Text: "invented later"}); wire.KindOf(err) != wire.RateLimited {
		t.Errorf("Send during the pause: %v, want rateLimited", err)
	}
	if err := r.l.React(context.Background(), chat, msgURN(5), "👍", "txn-invented-1"); wire.KindOf(err) != wire.RateLimited {
		t.Errorf("React during the pause: %v, want rateLimited", err)
	}
	if err := r.l.MarkRead(context.Background(), chat, ""); wire.KindOf(err) != wire.RateLimited {
		t.Errorf("MarkRead during the pause: %v, want rateLimited", err)
	}
	if err := r.l.MarkUnread(context.Background(), chat); wire.KindOf(err) != wire.RateLimited {
		t.Errorf("MarkUnread during the pause: %v, want rateLimited", err)
	}
	r.idle()
	for _, name := range r.f.namesSince(n) {
		if name != "RealtimeDisconnect" {
			t.Errorf("%s was asked during the pause", name)
		}
	}

	// A pause status on a reaction pauses everything too.
	r2 := newRig(t)
	c2 := r2.live()
	chat2 := r2.heldChat(1)
	r2.arrive(c2, text(1, 5, person(aliceID, "Alice", "Invented"), start, "invented question"))
	r2.f.setErr("SendReaction", statusErr(999))
	if err := r2.l.React(context.Background(), chat2, msgURN(5), "👍", ""); wire.KindOf(err) != wire.RateLimited {
		t.Errorf("React answered 999: %v, want rateLimited", err)
	}
	r2.waitState(network.Offline, wire.RateLimited)
}

func TestUnreactUsesHisStoredEmoji(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	r.arrive(c, text(1, 5, person(aliceID, "Alice", "Invented"), start, "invented question"))
	if err := r.st.SetReactionSummary(Name, chat, msgURN(5), []store.Reaction{{Emoji: "🎉", FromMe: true}, {Emoji: "👍"}}); err != nil {
		t.Fatal(err)
	}
	n := len(r.f.callsNow())
	if err := r.l.React(context.Background(), chat, msgURN(5), "", "txn-invented-1"); err != nil {
		t.Fatalf("React to remove: %v", err)
	}
	calls := r.f.callsNow()[n:]
	if len(calls) != 1 || calls[0].name != "RemoveReaction" || calls[0].arg != msgURN(5)+" 🎉" {
		t.Fatalf("calls = %+v, want RemoveReaction with his 🎉", calls)
	}
	line, _ := r.lineByID(chat, msgURN(5))
	for _, x := range line.Reactions {
		if x.FromMe {
			t.Errorf("his reaction %q is still held", x.Emoji)
		}
	}

	// With none of his own, nothing is asked.
	n = len(r.f.callsNow())
	if err := r.l.React(context.Background(), chat, msgURN(5), "", "txn-invented-2"); err != nil {
		t.Fatalf("React to remove none: %v", err)
	}
	if names := r.f.namesSince(n); len(names) != 0 {
		t.Errorf("removing no reaction asked %v", names)
	}
}

func TestARepeatedTransactionSendsNothing(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	r.arrive(c, text(1, 5, person(aliceID, "Alice", "Invented"), start, "invented question"))
	for range 2 {
		if err := r.l.React(context.Background(), chat, msgURN(5), "❤️", "txn-invented-7"); err != nil {
			t.Fatalf("React: %v", err)
		}
	}
	if n := r.f.count("SendReaction"); n != 1 {
		t.Fatalf("SendReaction was asked %d times for one transaction, want 1", n)
	}
	line, _ := r.lineByID(chat, msgURN(5))
	if len(line.Reactions) != 1 || line.Reactions[0].Emoji != "❤️" || !line.Reactions[0].FromMe {
		t.Errorf("reactions = %+v, want his ❤️", line.Reactions)
	}

	// A line only Beeper's copy holds is not on LinkedIn.
	if err := r.l.React(context.Background(), chat, "beeper:$invented-beeper-line", "👍", "txn-invented-8"); wire.KindOf(err) != wire.NotOnNetwork {
		t.Errorf("React to a Beeper line: %v, want notOnNetwork", err)
	}
}

func TestMarkReadSkipsANewerIncomingMessage(t *testing.T) {
	r := newRig(t)
	c := r.live()
	chat := r.heldChat(1)
	alice := person(aliceID, "Alice", "Invented")
	r.arrive(c, text(1, 5, alice, start.Add(time.Minute), "invented one"))
	r.arrive(c, text(1, 6, alice, start.Add(2*time.Minute), "invented two"))
	if u := r.chat(chat).Unread; u != 2 {
		t.Fatalf("unread = %d before marking, want 2", u)
	}

	if err := r.l.MarkRead(context.Background(), chat, msgURN(5)); err != nil {
		t.Fatalf("MarkRead through the older line: %v", err)
	}
	if n := r.f.count("MarkConversationRead"); n != 0 {
		t.Fatalf("marking through an older line asked LinkedIn %d times; it marks the whole chat", n)
	}
	if u := r.chat(chat).Unread; u != 2 {
		t.Errorf("unread = %d after the no-op, want 2", u)
	}

	if err := r.l.MarkRead(context.Background(), chat, msgURN(6)); err != nil {
		t.Fatalf("MarkRead through the newest line: %v", err)
	}
	calls := r.f.callsNow()
	if last := calls[len(calls)-1]; last.name != "MarkConversationRead" || last.arg != chat {
		t.Fatalf("the last call is %+v, want MarkConversationRead of the chat", last)
	}
	if u := r.chat(chat).Unread; u != 0 {
		t.Errorf("unread = %d after marking, want 0", u)
	}

	if err := r.l.MarkUnread(context.Background(), chat); err != nil {
		t.Fatalf("MarkUnread: %v", err)
	}
	if n := r.f.count("MarkConversationUnread"); n != 1 {
		t.Errorf("MarkConversationUnread was asked %d times, want 1", n)
	}
	if u := r.chat(chat).Unread; u != 1 {
		t.Errorf("unread = %d after marking unread, want 1", u)
	}

	if err := r.l.MarkRead(context.Background(), chat, ""); err != nil {
		t.Fatalf("MarkRead of the whole chat: %v", err)
	}
	if n := r.f.count("MarkConversationRead"); n != 2 {
		t.Errorf("MarkConversationRead was asked %d times in all, want 2", n)
	}
	if u := r.chat(chat).Unread; u != 0 {
		t.Errorf("unread = %d after marking the whole chat, want 0", u)
	}
}

func TestMuteNeverReachesLinkedIn(t *testing.T) {
	r := newRig(t)
	r.live()
	chat := r.heldChat(1)
	n := len(r.f.callsNow())
	if err := r.l.SetMuted(context.Background(), chat, true); err != nil {
		t.Fatalf("SetMuted: %v", err)
	}
	if !r.chat(chat).Muted {
		t.Error("the chat is not muted")
	}
	if err := r.l.SetMuted(context.Background(), chat, false); err != nil {
		t.Fatalf("SetMuted false: %v", err)
	}
	if r.chat(chat).Muted {
		t.Error("the chat is still muted")
	}
	r.idle()
	if names := r.f.namesSince(n); len(names) != 0 {
		t.Errorf("muting asked LinkedIn %v", names)
	}

	// Signed out, it still mutes: it is Pllum's own.
	if err := r.l.Logout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.l.SetMuted(context.Background(), chat, true); err != nil || !r.chat(chat).Muted {
		t.Errorf("SetMuted signed out: %v, muted %v", err, r.chat(chat).Muted)
	}
	if err := r.l.SetMuted(context.Background(), convURN(99), true); wire.KindOf(err) != wire.NotFound {
		t.Errorf("SetMuted of a chat never held: %v, want notFound", err)
	}
}
