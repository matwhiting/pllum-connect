package messenger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Task B3: Messenger's writes and media. Every ID, path and text is
// invented.

// The chats the writes name: an unencrypted group, and Alice's and Bob's
// encrypted one-to-ones.
var (
	groupChat = keyID(groupOne)
	aliceChat = keyID(aliceFBID) + oneToOneSuffix
	bobChat   = keyID(bobFBID) + oneToOneSuffix
)

// sends waits for every send in line to have gone.
func (h *harness) sends(t *testing.T) {
	t.Helper()
	eventually(t, "the sends to go", func() bool { return h.m.sends.busy.Load() == 0 })
}

// send sends o and waits for it to go.
func (h *harness) send(t *testing.T, o network.Outgoing) string {
	t.Helper()
	id, err := h.m.Send(context.Background(), o)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	h.sends(t)
	return id
}

func (h *harness) outgoing(t *testing.T, id string) store.Outgoing {
	t.Helper()
	o, ok, err := h.st.Outgoing(Name, id)
	if err != nil || !ok {
		t.Fatalf("outgoing %s: ok %v, err %v", id, ok, err)
	}
	return o
}

// his are the IDs of his lines in a chat, sorted.
func (h *harness) his(t *testing.T, chat string) []string {
	t.Helper()
	var out []string
	for id, l := range h.lines(t, chat) {
		if l.FromMe {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// tasksOf are the tasks of one type ExecuteTasks was handed.
func tasksOf[T socket.Task](f *fakeMeta) []T {
	var out []T
	for _, t := range f.written() {
		if x, ok := t.(T); ok {
			out = append(out, x)
		}
	}
	return out
}

func (f *fakeMeta) sentFB() []fbSend {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fbSend{}, f.fbSends...)
}

func (f *fakeMeta) receipts() []markCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]markCall{}, f.marks...)
}

// echo is his message as the main socket brings it back, carrying the
// offline threading ID it was sent with.
func echo(key int64, id, otid string, at time.Time) *table.LSTable {
	return &table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{
		ThreadKey: key, MessageId: id, OfflineThreadingId: otid, SenderId: selfFBID, TimestampMs: at.UnixMilli(), Text: canary + " echo"}}}
}

// textOf is the text of an encrypted send.
func textOf(s fbSend) string {
	app, _ := s.msg.(*waConsumerApplication.ConsumerApplication)
	return app.GetPayload().GetContent().GetMessageText().GetText()
}

// A group's send answers at once with "otid:<n>", the offline threading ID
// it goes with, and settles to Meta's message ID: from Meta's answer, or,
// when that does not carry it, from the echo. The echo is the same line,
// not a second one; the same key again sends nothing.
func TestAGroupSendSettlesToMetasID(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	ctx := context.Background()

	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " hello", ClientID: "key-1"})
	otid, ok := strings.CutPrefix(id, otidPrefix)
	if !ok || otid == "" {
		t.Fatalf("the send's ID is %q, want otid:<n>", id)
	}
	want := "mid.$invented-sent-" + otid
	if st := h.m.SendState(id); st.State != network.SendSent {
		t.Errorf("the send is %+v, want sent", st)
	}
	if o := h.outgoing(t, id); o.LineID != want {
		t.Errorf("lineID = %q, want Meta's %s", o.LineID, want)
	}
	sent := tasksOf[*socket.SendMessageTask](h.f)
	if len(sent) != 1 {
		t.Fatalf("%d SendMessageTasks, want 1", len(sent))
	}
	if s := sent[0]; strconv.FormatInt(s.Otid, 10) != otid || s.ThreadId != groupOne || s.Text != canary+" hello" ||
		s.SendType != table.TEXT || s.SyncGroup != 1 || s.Source != table.MESSENGER_INBOX_IN_THREAD {
		t.Errorf("the task is %+v", s)
	}
	if l, ok := h.line(t, groupChat, want); !ok || !l.FromMe || l.Text != canary+" hello" {
		t.Errorf("his line is %+v (held %v)", l, ok)
	}

	// The echo is the same line.
	h.f.last().dispatch(echo(groupOne, want, otid, h.f.clock.Now()))
	if got := h.his(t, groupChat); !slices.Equal(got, []string{want}) {
		t.Errorf("his lines are %v, want only %s", got, want)
	}

	// The same key again (its answer was lost) is the same send, sent once.
	again, err := h.m.Send(ctx, network.Outgoing{ChatID: groupChat, Text: canary + " hello", ClientID: "key-1"})
	h.sends(t)
	if err != nil || again != id {
		t.Errorf("a repeated send gave %q, %v; want %s", again, err, id)
	}
	if n := h.f.count("SendMessageTask"); n != 1 {
		t.Errorf("SendMessageTask was asked %d times, want 1", n)
	}

	// Meta's answer without the ID leaves it sending; its echo settles it.
	h.f.mu.Lock()
	h.f.answer = func(*socket.SendMessageTask) *table.LSTable { return &table.LSTable{} }
	h.f.mu.Unlock()
	id2 := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " second", ClientID: "key-2"})
	if st := h.m.SendState(id2); st.State != network.SendSending {
		t.Fatalf("with no ID in the answer the send is %+v, want sending", st)
	}
	const echoed = "mid.$invented-echoed-2"
	h.f.last().dispatch(echo(groupOne, echoed, strings.TrimPrefix(id2, otidPrefix), h.f.clock.Now()))
	if st := h.m.SendState(id2); st.State != network.SendSent {
		t.Errorf("after its echo the send is %+v, want sent", st)
	}
	if o := h.outgoing(t, id2); o.LineID != echoed {
		t.Errorf("lineID = %q, want the echo's %s", o.LineID, echoed)
	}
	gaps(t, h.f.requests())
}

// An encrypted chat's send is made with whatsmeow's message ID, passed to
// SendFBMessage, so the line is stored (and comes back) under the ID the
// send answered with. A reply quotes its line by ID and sender.
func TestAnEncryptedSendKeepsItsID(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	at := h.f.clock.Now().Add(-time.Hour)
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED50", at, textContent(canary+" hi"), ""))

	id := h.send(t, network.Outgoing{ChatID: aliceChat, Text: canary + " encrypted", ReplyTo: "WAINVENTED50", ClientID: "key-e"})
	if id == "" || strings.HasPrefix(id, otidPrefix) || strings.Contains(id, ":") {
		t.Fatalf("the send's ID is %q, want a whatsmeow message ID", id)
	}
	if st := h.m.SendState(id); st.State != network.SendSent {
		t.Errorf("the send is %+v, want sent", st)
	}
	sends := h.f.sentFB()
	if len(sends) != 1 {
		t.Fatalf("SendFBMessage was asked %d times, want 1", len(sends))
	}
	s := sends[0]
	if string(s.id) != id || s.to.String() != oneToOne(aliceFBID).String() || textOf(s) != canary+" encrypted" {
		t.Errorf("SendFBMessage was asked %s to %s with %q, want %s to Alice", s.id, s.to, textOf(s), id)
	}
	if q := s.meta.GetQuotedMessage(); q.GetStanzaID() != "WAINVENTED50" || q.GetParticipant() != person(aliceFBID).ToNonAD().String() {
		t.Errorf("the reply quotes %q by %q", q.GetStanzaID(), q.GetParticipant())
	}
	if l, ok := h.line(t, aliceChat, id); !ok || !l.FromMe || l.Text != canary+" encrypted" || l.ReplyTo != "WAINVENTED50" {
		t.Errorf("his line is %+v (held %v)", l, ok)
	}

	// The same message coming back (from his phone, or his restored
	// history) is the same line.
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(selfFBID), id, h.f.clock.Now(), textContent(canary+" encrypted"), ""))
	if got := h.his(t, aliceChat); !slices.Equal(got, []string{id}) {
		t.Errorf("his lines are %v, want only %s", got, id)
	}

	again, err := h.m.Send(context.Background(), network.Outgoing{ChatID: aliceChat, Text: canary + " encrypted", ClientID: "key-e"})
	h.sends(t)
	if err != nil || again != id {
		t.Errorf("a repeated send gave %q, %v; want %s", again, err, id)
	}
	if n := h.f.count("SendFBMessage"); n != 1 {
		t.Errorf("SendFBMessage was asked %d times, want 1", n)
	}
	gaps(t, h.f.requests())
}

// Review Focus 2: a send that may have reached Meta when the helper
// stopped (no answer in time) is never sent again, by itself or by the
// same key asked again, in a group or an encrypted chat.
func TestAMetaSendCutOffByARestartIsNotSentAgain(t *testing.T) {
	for _, tc := range []struct {
		name, chat, write string
		err               error
	}{
		{"a group", groupChat, "SendMessageTask", context.DeadlineExceeded},
		{"an encrypted chat", aliceChat, "SendFBMessage", whatsmeow.ErrMessageTimedOut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "connect")
			f := newFakeMeta()
			h := newHarnessOver(t, dir, f)
			h.signIn(t)
			f.setErr(tc.write, tc.err)
			out := network.Outgoing{ChatID: tc.chat, Text: canary + " cut off", ClientID: "key-cut"}
			id := h.send(t, out)
			if st := h.m.SendState(id); st.State != network.SendSending {
				t.Fatalf("before the restart the send is %+v, want sending", st)
			}
			h.close()

			f.setErr(tc.write, nil)
			h = newHarnessOver(t, dir, f)
			if st := h.m.SendState(id); st.State != network.SendUnknown {
				t.Errorf("after the restart the send is %+v, want unknown", st)
			}
			h.signIn(t)
			again, err := h.m.Send(context.Background(), out)
			h.sends(t)
			if err != nil || again != id {
				t.Errorf("the key asked again gave %q, %v; want %s", again, err, id)
			}
			if n := f.count(tc.write); n != 1 {
				t.Errorf("%s was asked %d times, want 1", tc.write, n)
			}
			if st := h.m.SendState(id); st.State != network.SendUnknown {
				t.Errorf("the send is %+v, want still unknown", st)
			}
		})
	}
}

// methodNames are the methods of an interface type, sorted.
func methodNames(v any) []string {
	typ := reflect.TypeOf(v).Elem()
	var out []string
	for i := range typ.NumMethod() {
		out = append(out, typ.Method(i).Name)
	}
	sort.Strings(out)
	return out
}

// Only the four write tasks ever reach ExecuteTasks: any other is refused
// before the pace or the client, by the adapter and by the real writer;
// and the reads' client takes only the two reads.
func TestOnlyTheAllowedTasksAreExecuted(t *testing.T) {
	if got, want := methodNames((*msgrWriter)(nil)), []string{"DownloadFB", "ExecuteTasks", "MarkRead", "SendFBMessage", "Upload", "UploadFB"}; !slices.Equal(got, want) {
		t.Errorf("msgrWriter has %v, want exactly %v", got, want)
	}
	if got := methodNames((*msgrDownloader)(nil)); !slices.Equal(got, []string{"Download"}) {
		t.Errorf("msgrDownloader has %v, want only Download", got)
	}

	h := newHarness(t)
	h.signIn(t)
	ctx := context.Background()
	gen, _, _ := h.m.running()
	w := fakeWriter{h.f}
	from := len(h.f.requests())
	for _, task := range []socket.Task{
		&socket.DeleteMessageTask{MessageId: "mid.$invented-1"},
		&socket.EditMessageTask{MessageID: "mid.$invented-1"},
		&socket.RenameThreadTask{ThreadKey: groupOne},
		&socket.FetchThreadsTask{},
		&socket.FetchMessagesTask{ThreadKey: groupOne},
		nil,
	} {
		_, ran, _, err := h.m.execute(ctx, gen, w, task)
		if ran || wire.KindOf(err) != wire.Refused {
			t.Errorf("%T: ran %v, err %v; want refused before Meta", task, ran, err)
		}
	}
	if n := len(h.f.written()); n != 0 {
		t.Errorf("%d tasks reached ExecuteTasks, want none", n)
	}
	if n := len(h.f.requests()); n != from {
		t.Errorf("%d requests were made, want none", n-from)
	}

	for _, task := range []socket.Task{
		&socket.SendMessageTask{ThreadId: groupOne, Otid: 7000000000000001, Text: canary},
		&socket.SendReactionTask{ThreadKey: groupOne, MessageID: "mid.$invented-1"},
		&socket.ThreadMarkReadTask{ThreadId: groupOne},
		&socket.MuteThreadTask{ThreadKey: groupOne},
	} {
		if _, ran, _, err := h.m.execute(ctx, gen, w, task); !ran || err != nil {
			t.Errorf("%T: ran %v, err %v; want asked", task, ran, err)
		}
	}
	if n := len(h.f.written()); n != 4 {
		t.Errorf("%d tasks reached ExecuteTasks, want the 4", n)
	}

	// The real writer refuses before its client (a nil one here): another
	// task, or more than one at once.
	if _, err := (metaWriter{}).ExecuteTasks(ctx, &socket.DeleteMessageTask{}); wire.KindOf(err) != wire.Refused {
		t.Errorf("the real writer gave %v for a delete, want refused", err)
	}
	if _, err := (metaWriter{}).ExecuteTasks(ctx, &socket.SendMessageTask{}, &socket.ThreadMarkReadTask{}); wire.KindOf(err) != wire.Refused {
		t.Errorf("the real writer gave %v for two tasks, want refused", err)
	}
	// The reads' client takes only the two reads.
	if _, err := (realClient{}).ExecuteTasks(ctx, &socket.SendMessageTask{}); wire.KindOf(err) != wire.Refused {
		t.Errorf("the reads' client gave %v for a send, want refused", err)
	}
}

// markRead marks a chat read through a line, and fails on an error.
func (h *harness) markRead(t *testing.T, chat, through string) {
	t.Helper()
	if err := h.m.MarkRead(context.Background(), chat, through); err != nil {
		t.Fatalf("MarkRead %s: %v", through, err)
	}
}

// Read receipts name only the unread lines up to and including through:
// an encrypted chat's by whatsmeow's MarkRead, grouped by sender (none in a
// one-to-one); an unencrypted thread's (a group's, or a one-to-one's older
// thread) by one ThreadMarkReadTask at its newest such line. Nothing newer
// is marked, and his marker moves to through.
func TestMarkReadStopsAtThrough(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	base := h.f.clock.Now().Add(-time.Hour)
	at := func(i int) time.Time { return base.Add(time.Duration(i) * time.Minute) }

	// An encrypted group: Alice, Bob, Alice, Bob.
	g := encryptedGroup(encGroup)
	for i, from := range []int64{aliceFBID, bobFBID, aliceFBID, bobFBID} {
		h.deliver(t, fbMessage(g, person(from), fmt.Sprintf("WAINVENTED8%d", i+1), at(i), textContent(canary), ""))
	}
	encChat := keyID(encGroup) + groupSuffix
	h.markRead(t, encChat, "WAINVENTED83")
	got := h.f.receipts()
	if len(got) != 2 {
		t.Fatalf("%d receipts, want one per sender: %+v", len(got), got)
	}
	aliceJID := watypes.NewJID(keyID(aliceFBID), watypes.MessengerServer).String()
	bobJID := watypes.NewJID(keyID(bobFBID), watypes.MessengerServer).String()
	if r := got[0]; r.chat.String() != g.String() || r.sender.String() != aliceJID || !slices.Equal(r.ids, []watypes.MessageID{"WAINVENTED81", "WAINVENTED83"}) {
		t.Errorf("the first receipt is %v from %s in %s", r.ids, r.sender, r.chat)
	}
	if r := got[1]; r.sender.String() != bobJID || !slices.Equal(r.ids, []watypes.MessageID{"WAINVENTED82"}) {
		t.Errorf("the second receipt is %v from %s", r.ids, r.sender)
	}
	if n := len(tasksOf[*socket.ThreadMarkReadTask](h.f)); n != 0 {
		t.Errorf("an encrypted group sent %d ThreadMarkReadTasks", n)
	}
	if c := h.chat(t, encChat); c.Unread != 1 {
		t.Errorf("the group has %d unread, want 1 (the line after through)", c.Unread)
	}

	// An unencrypted group: one task, at the line through names.
	h.f.last().dispatch(history(groupOne, false,
		line("mid.$invented-g1", aliceFBID, at(0), canary),
		line("mid.$invented-g2", bobFBID, at(1), canary),
		line("mid.$invented-g3", aliceFBID, at(2), canary)))
	h.markRead(t, groupChat, "mid.$invented-g2")
	marks := tasksOf[*socket.ThreadMarkReadTask](h.f)
	if len(marks) != 1 || marks[0].ThreadId != groupOne || marks[0].LastReadWatermarkTs != at(1).UnixMilli() || marks[0].SyncGroup != 1 {
		t.Errorf("the group's read tasks are %+v, want one at the second line", marks)
	}
	if c := h.chat(t, groupChat); c.Unread != 1 {
		t.Errorf("the group has %d unread, want 1", c.Unread)
	}

	// Alice's chat: her older thread's lines and her encrypted ones.
	h.f.last().dispatch(history(aliceFBID, false,
		line("mid.$invented-a1", aliceFBID, at(0), canary),
		line("mid.$invented-a2", aliceFBID, at(1), canary)))
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED91", at(2), textContent(canary), ""))
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED92", at(3), textContent(canary), ""))
	h.markRead(t, aliceChat, "WAINVENTED91")
	marks = tasksOf[*socket.ThreadMarkReadTask](h.f)
	if len(marks) != 2 || marks[1].ThreadId != aliceFBID || marks[1].LastReadWatermarkTs != at(1).UnixMilli() {
		t.Errorf("Alice's older thread was marked %+v, want once at her second older line", marks)
	}
	got = h.f.receipts()
	if len(got) != 3 || got[2].chat.String() != oneToOne(aliceFBID).String() || !got[2].sender.IsEmpty() ||
		!slices.Equal(got[2].ids, []watypes.MessageID{"WAINVENTED91"}) {
		t.Errorf("Alice's receipts are %+v, want WAINVENTED91 alone, with no sender", got[2:])
	}
	if c := h.chat(t, aliceChat); c.Unread != 1 {
		t.Errorf("Alice's chat has %d unread, want 1", c.Unread)
	}
	gaps(t, h.f.requests())
}

// A mute goes to Meta on the chat's unencrypted thread: a group's own, or
// the thread an encrypted one-to-one is linked to. An encrypted chat with
// no thread key is muted here only, and nothing is asked.
func TestMuteWithoutAThreadKeyStaysLocal(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	const strangerFBID int64 = 2000055
	h.deliver(t, fbMessage(oneToOne(strangerFBID), person(strangerFBID), "WAINVENTED60", h.f.clock.Now(), textContent(canary), ""))
	stranger := keyID(strangerFBID) + oneToOneSuffix

	from := len(h.f.requests())
	if err := h.m.SetMuted(context.Background(), stranger, true); err != nil {
		t.Fatalf("SetMuted: %v", err)
	}
	if !h.chat(t, stranger).Muted {
		t.Errorf("the chat is not muted")
	}
	if n := len(h.f.requests()) - from; n != 0 || len(h.f.written()) != 0 {
		t.Errorf("a mute with no thread key asked Meta %d times", n)
	}

	if err := h.m.SetMuted(context.Background(), bobChat, true); err != nil {
		t.Fatalf("SetMuted Bob: %v", err)
	}
	if err := h.m.SetMuted(context.Background(), groupChat, false); err != nil {
		t.Fatalf("SetMuted group: %v", err)
	}
	mutes := tasksOf[*socket.MuteThreadTask](h.f)
	if len(mutes) != 2 || mutes[0].ThreadKey != bobFBID || mutes[0].MuteExpireTimeMS != -1 || mutes[0].SyncGroup != 1 ||
		mutes[1].ThreadKey != groupOne || mutes[1].MuteExpireTimeMS != 0 {
		t.Errorf("the mutes are %+v, want Bob's thread for ever, then the group unmuted", mutes)
	}
	if !h.chat(t, bobChat).Muted || h.chat(t, groupChat).Muted {
		t.Errorf("Bob muted %v, the group muted %v", h.chat(t, bobChat).Muted, h.chat(t, groupChat).Muted)
	}
}

// fbPhoto is an invented encrypted photo whose message application carries
// the file's transport, as whatsmeow gives it.
func fbPhoto(t *testing.T, chat, sender watypes.JID, id string, at time.Time, path, object string) *events.FBMessage {
	t.Helper()
	img := &waConsumerApplication.ConsumerApplication_ImageMessage{Caption: &waCommon.MessageText{Text: proto.String("")}}
	err := img.Set(&waMediaTransport.ImageTransport{Integral: &waMediaTransport.ImageTransport_Integral{
		Transport: &waMediaTransport.WAMediaTransport{
			Integral: &waMediaTransport.WAMediaTransport_Integral{
				DirectPath: proto.String(path), MediaKey: []byte("key-invented"), FileSHA256: []byte("sha-invented"), FileEncSHA256: []byte("enc-invented"),
			},
			Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{ObjectID: proto.String(object), Mimetype: proto.String("image/jpeg")},
		}}})
	if err != nil {
		t.Fatal(err)
	}
	app := content(&waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ImageMessage{ImageMessage: img}})
	evt := fbMessage(chat, sender, id, at, app, "")
	sub := &waMsgApplication.MessageApplication_SubProtocolPayload_ConsumerMessage{}
	if err := sub.Set(app); err != nil {
		t.Fatal(err)
	}
	evt.FBApplication.Payload = &waMsgApplication.MessageApplication_Payload{
		Content: &waMsgApplication.MessageApplication_Payload_SubProtocol{
			SubProtocol: &waMsgApplication.MessageApplication_SubProtocolPayload{SubProtocol: sub}}}
	return evt
}

// An encrypted photo is downloaded once, through whatsmeow's DownloadFB
// and the pace, to a 0600 file named for the line; asked again, it is the
// file kept.
func TestAnEncryptedPhotoDownloadsOnce(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	const path = "/v/t62.invented/photo-1"
	photo := []byte("invented photo bytes")
	h.f.mu.Lock()
	h.f.files[path] = photo
	h.f.mu.Unlock()
	h.deliver(t, fbPhoto(t, oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED70", h.f.clock.Now(), path, "object-invented-1"))

	first, err := h.m.Media(context.Background(), aliceChat, "WAINVENTED70")
	if err != nil {
		t.Fatalf("Media: %v", err)
	}
	second, err := h.m.Media(context.Background(), aliceChat, "WAINVENTED70")
	if err != nil || second != first {
		t.Fatalf("Media again = %q, %v; want %q", second, err, first)
	}
	if n := h.f.count("DownloadFB"); n != 1 {
		t.Errorf("DownloadFB was asked %d times, want 1", n)
	}
	h.f.mu.Lock()
	asked := append([]string{}, h.f.fbDownloads...)
	h.f.mu.Unlock()
	if !slices.Equal(asked, []string{path}) {
		t.Errorf("DownloadFB was asked %v, want the photo's path", asked)
	}
	data, err := openMedia(first)
	if err != nil || string(data) != string(photo) {
		t.Errorf("the file holds %q, %v", data, err)
	}
	if fi, err := os.Stat(first); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the file is %v, %v; want 0600", fi, err)
	}

	// A line restored from his backup is fetched from the address the
	// backup gives for its file, not the message's own.
	const restored = "/v/t62.invented/restored-1"
	h.f.mu.Lock()
	h.f.files[restored] = []byte("invented restored bytes")
	h.f.mu.Unlock()
	evt := fbPhoto(t, oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED71", h.f.clock.Now(), "/v/t62.invented/stale-1", "object-invented-2")
	w := h.m.encPrep(aliceChat, evt, store.SourceSync, map[string]string{"object-invented-2": restored})
	if _, _, err := h.m.applyEnc(aliceChat, w); err != nil {
		t.Fatal(err)
	}
	p, err := h.m.Media(context.Background(), aliceChat, "WAINVENTED71")
	if err != nil {
		t.Fatalf("Media of the restored photo: %v", err)
	}
	if data, _ := openMedia(p); string(data) != "invented restored bytes" {
		t.Errorf("the restored photo holds %q", data)
	}
	gaps(t, h.f.requests())
}

// A group's photo is fetched once from the CDN address its line carries.
func TestAGroupPhotoDownloadsOnceFromTheCDN(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	const url = "https://cdn.invalid/invented-photo-1.jpg"
	h.f.mu.Lock()
	h.f.files[url] = []byte("invented cdn bytes")
	h.f.mu.Unlock()
	tbl := history(groupOne, false, line("mid.$invented-p1", aliceFBID, h.f.clock.Now(), ""))
	tbl.LSInsertBlobAttachment = []*table.LSInsertBlobAttachment{{MessageId: "mid.$invented-p1", ThreadKey: groupOne,
		AttachmentType: table.AttachmentTypeImage, PreviewUrl: url}}
	h.f.last().dispatch(tbl)

	for range 2 {
		p, err := h.m.Media(context.Background(), groupChat, "mid.$invented-p1")
		if err != nil {
			t.Fatalf("Media: %v", err)
		}
		if data, _ := openMedia(p); string(data) != "invented cdn bytes" {
			t.Errorf("the file holds %q", data)
		}
	}
	if n := h.f.count("Download"); n != 1 {
		t.Errorf("the CDN was asked %d times, want 1", n)
	}
	// An address the CDN no longer takes is mediaGone, asked once.
	tbl = history(groupOne, false, line("mid.$invented-p2", aliceFBID, h.f.clock.Now(), ""))
	tbl.LSInsertBlobAttachment = []*table.LSInsertBlobAttachment{{MessageId: "mid.$invented-p2", ThreadKey: groupOne,
		AttachmentType: table.AttachmentTypeImage, PreviewUrl: "https://cdn.invalid/invented-gone.jpg"}}
	h.f.last().dispatch(tbl)
	if _, err := h.m.Media(context.Background(), groupChat, "mid.$invented-p2"); wire.KindOf(err) != wire.MediaGone {
		t.Errorf("a gone photo gave %v, want mediaGone", err)
	}
}

// writePhoto writes a small invented PNG and is its path.
func writePhoto(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "invented.png")
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 4, 3))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// His own photo, sent to a group, is uploaded once and sent as one
// message, and opens without a download.
func TestAGroupPhotoIsUploadedThenSent(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	id := h.send(t, network.Outgoing{ChatID: groupChat, File: writePhoto(t)})
	sent := tasksOf[*socket.SendMessageTask](h.f)
	if h.f.count("Upload") != 1 || len(sent) != 1 {
		t.Fatalf("%d uploads, %d sends; want one each", h.f.count("Upload"), len(sent))
	}
	if s := sent[0]; s.SendType != table.MEDIA || !slices.Equal(s.AttachmentFBIds, []int64{5550000000000001}) || s.Text != "" ||
		strconv.FormatInt(s.Otid, 10) != strings.TrimPrefix(id, otidPrefix) {
		t.Errorf("the task is %+v", s)
	}
	lineID := h.outgoing(t, id).LineID
	if l, ok := h.line(t, groupChat, lineID); !ok || l.Kind != "photo" {
		t.Errorf("his line is %+v (held %v)", l, ok)
	}
	if _, err := h.m.Media(context.Background(), groupChat, lineID); err != nil {
		t.Errorf("Media of his photo: %v", err)
	}
	if n := h.f.count("Download"); n != 0 {
		t.Errorf("his own photo was downloaded %d times", n)
	}
}

// fbFile is the image, video or document an encrypted send carries, and
// its transport's direct path ("" for a send with no file).
func fbFile(t *testing.T, s fbSend) (kind, path string) {
	t.Helper()
	kind, tr := fbTransport(t, s)
	return kind, tr.GetIntegral().GetDirectPath()
}

// fbTransport is the kind and transport of the file an encrypted send
// carries.
func fbTransport(t *testing.T, s fbSend) (string, *waMediaTransport.WAMediaTransport) {
	t.Helper()
	app, _ := s.msg.(*waConsumerApplication.ConsumerApplication)
	c := app.GetPayload().GetContent()
	switch {
	case c.GetImageMessage() != nil:
		tr, err := c.GetImageMessage().Decode()
		if err != nil {
			t.Fatal(err)
		}
		return "photo", tr.GetIntegral().GetTransport()
	case c.GetVideoMessage() != nil:
		tr, err := c.GetVideoMessage().Decode()
		if err != nil {
			t.Fatal(err)
		}
		return "video", tr.GetIntegral().GetTransport()
	case c.GetDocumentMessage() != nil:
		tr, err := c.GetDocumentMessage().Decode()
		if err != nil {
			t.Fatal(err)
		}
		return "file", tr.GetIntegral().GetTransport()
	}
	return "", nil
}

// Ruling 1 (Review Focus 2): a photo to an encrypted one-to-one is
// uploaded once with whatsmeow's upload and sent under its message ID. A
// send that did not go (the socket dropped before it) is the same send
// after a restart, and goes again with the upload it has, never a second
// one; once sent, the same key sends nothing. One cut off mid-send is
// unknown after a restart and never sent again.
func TestAnEncryptedPhotoSendsOnceWithItsUpload(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "connect")
	f := newFakeMeta()
	h := newHarnessOver(t, dir, f)
	h.signIn(t)
	out := network.Outgoing{ChatID: aliceChat, File: writePhoto(t), ClientID: "key-photo"}

	f.setErr("SendFBMessage", whatsmeow.ErrNotConnected)
	id := h.send(t, out)
	if st := h.m.SendState(id); st.State != network.SendFailed || st.Kind != wire.Refused {
		t.Fatalf("a send that never went is %+v, want failed refused", st)
	}
	if n := f.count("UploadFB"); n != 1 {
		t.Fatalf("UploadFB was asked %d times, want 1", n)
	}
	h.close()

	f.setErr("SendFBMessage", nil)
	h = newHarnessOver(t, dir, f)
	h.signIn(t)
	again, err := h.m.Send(context.Background(), out)
	h.sends(t)
	if err != nil || again != id {
		t.Fatalf("the key asked again gave %q, %v; want %s", again, err, id)
	}
	if st := h.m.SendState(id); st.State != network.SendSent {
		t.Errorf("the send is %+v, want sent", st)
	}
	if n := f.count("UploadFB"); n != 1 {
		t.Errorf("UploadFB was asked %d times, want 1: the upload is reused", n)
	}
	sends := f.sentFB()
	if len(sends) != 2 {
		t.Fatalf("SendFBMessage was asked %d times, want 2 (the one that never went, then this)", len(sends))
	}
	for i, s := range sends {
		kind, path := fbFile(t, s)
		if kind != "photo" || path != "/v/t62.invented/upload-1" || string(s.id) != id || s.to.String() != oneToOne(aliceFBID).String() {
			t.Errorf("send %d was a %q at %q under %s to %s", i, kind, path, s.id, s.to)
		}
	}
	f.mu.Lock()
	types := append([]whatsmeow.MediaType{}, f.fbUploads...)
	f.mu.Unlock()
	if !slices.Equal(types, []whatsmeow.MediaType{whatsmeow.MediaImage}) {
		t.Errorf("uploaded as %v, want an image", types)
	}
	if l, ok := h.line(t, aliceChat, id); !ok || !l.FromMe || l.Kind != "photo" {
		t.Errorf("his line is %+v (held %v)", l, ok)
	}
	if _, err := h.m.Media(context.Background(), aliceChat, id); err != nil || f.count("DownloadFB") != 0 {
		t.Errorf("his own photo: %v, after %d downloads", err, f.count("DownloadFB"))
	}

	// Sent, it is never sent again: not after a restart, not by its key.
	h.close()
	h = newHarnessOver(t, dir, f)
	h.signIn(t)
	if again, err := h.m.Send(context.Background(), out); err != nil || again != id {
		t.Errorf("the key after the send gave %q, %v", again, err)
	}
	h.sends(t)
	if f.count("UploadFB") != 1 || f.count("SendFBMessage") != 2 {
		t.Errorf("after the send: %d uploads, %d sends; want 1 and 2", f.count("UploadFB"), f.count("SendFBMessage"))
	}

	// Cut off mid-send: unknown after a restart, and never sent again.
	f.setErr("SendFBMessage", whatsmeow.ErrMessageTimedOut)
	cut := network.Outgoing{ChatID: aliceChat, File: writePhoto(t), ClientID: "key-photo-cut"}
	id2 := h.send(t, cut)
	h.close()
	f.setErr("SendFBMessage", nil)
	h = newHarnessOver(t, dir, f)
	if st := h.m.SendState(id2); st.State != network.SendUnknown {
		t.Errorf("a photo cut off mid-send is %+v after a restart, want unknown", st)
	}
	h.signIn(t)
	if again, err := h.m.Send(context.Background(), cut); err != nil || again != id2 {
		t.Errorf("its key gave %q, %v", again, err)
	}
	h.sends(t)
	if f.count("UploadFB") != 2 || f.count("SendFBMessage") != 3 {
		t.Errorf("after the cut-off: %d uploads, %d sends; want 2 and 3", f.count("UploadFB"), f.count("SendFBMessage"))
	}
	gaps(t, f.requests())
}

// A group's reaction is a SendReactionTask by him (an empty one removes
// it); an encrypted chat's a consumer reaction naming the line's key. A
// transaction already held sends nothing.
func TestReactInAGroupAndAnEncryptedChat(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	ctx := context.Background()
	h.f.last().dispatch(history(groupOne, false, line("mid.$invented-r1", aliceFBID, h.f.clock.Now(), canary)))
	h.deliver(t, fbMessage(encryptedGroup(encGroup), person(bobFBID), "WAINVENTED40", h.f.clock.Now(), textContent(canary), ""))
	encChat := keyID(encGroup) + groupSuffix

	if err := h.m.React(ctx, groupChat, "mid.$invented-r1", "❤️", "txn-1"); err != nil {
		t.Fatalf("React: %v", err)
	}
	if err := h.m.React(ctx, groupChat, "mid.$invented-r1", "❤️", "txn-1"); err != nil {
		t.Fatalf("React again: %v", err)
	}
	if err := h.m.React(ctx, groupChat, "mid.$invented-r1", "", "txn-2"); err != nil {
		t.Fatalf("React remove: %v", err)
	}
	rs := tasksOf[*socket.SendReactionTask](h.f)
	if len(rs) != 2 || rs[0].ThreadKey != groupOne || rs[0].MessageID != "mid.$invented-r1" || rs[0].Reaction != "❤️" ||
		rs[0].ActorID != selfFBID || rs[1].Reaction != "" {
		t.Errorf("the reaction tasks are %+v", rs)
	}

	if err := h.m.React(ctx, encChat, "WAINVENTED40", "👍", "txn-3"); err != nil {
		t.Fatalf("React encrypted: %v", err)
	}
	sends := h.f.sentFB()
	if len(sends) != 1 {
		t.Fatalf("%d encrypted sends, want 1", len(sends))
	}
	app, _ := sends[0].msg.(*waConsumerApplication.ConsumerApplication)
	r := app.GetPayload().GetContent().GetReactionMessage()
	if r.GetText() != "👍" || r.GetKey().GetID() != "WAINVENTED40" || r.GetKey().GetRemoteJID() != encryptedGroup(encGroup).String() ||
		r.GetKey().GetFromMe() || r.GetKey().GetParticipant() != watypes.NewJID(keyID(bobFBID), watypes.MessengerServer).String() || sends[0].id == "" {
		t.Errorf("the encrypted reaction is %v with ID %q", r, sends[0].id)
	}
	if err := h.m.React(ctx, groupChat, "beeper:invented-1", "❤️", "txn-4"); wire.KindOf(err) != wire.NotOnNetwork {
		t.Errorf("a reaction to a Beeper line gave %v, want notOnNetwork", err)
	}
}

// Messenger has no "mark unread" write: it is kept here only.
func TestMarkUnreadSendsNothing(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.f.last().dispatch(history(groupOne, false, line("mid.$invented-u1", aliceFBID, h.f.clock.Now(), canary)))
	h.markRead(t, groupChat, "")
	from := len(h.f.requests())
	if err := h.m.MarkUnread(context.Background(), groupChat); err != nil {
		t.Fatalf("MarkUnread: %v", err)
	}
	if c := h.chat(t, groupChat); c.Unread != 1 {
		t.Errorf("the chat has %d unread, want 1", c.Unread)
	}
	if n := len(h.f.requests()) - from; n != 0 {
		t.Errorf("MarkUnread made %d requests", n)
	}
}

// A checkpoint refuses every write before Meta, with its kind.
func TestACheckpointRefusesEveryWrite(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	ctx := context.Background()
	h.f.last().dispatch(history(groupOne, false, line("mid.$invented-c1", aliceFBID, h.f.clock.Now(), canary)))
	h.f.setErr("ThreadMarkReadTask", httpclient.ErrCheckpointRequired)
	if err := h.m.MarkRead(ctx, groupChat, ""); wire.KindOf(err) != wire.Checkpoint {
		t.Fatalf("the read receipt that met the checkpoint gave %v", err)
	}
	from := len(h.f.requests())
	check := func(what string, err error) {
		t.Helper()
		if k := wire.KindOf(err); k != wire.Checkpoint && k != wire.NotConnected {
			t.Errorf("%s = %v, want refused for the checkpoint", what, err)
		}
	}
	_, err := h.m.Send(ctx, network.Outgoing{ChatID: groupChat, Text: canary})
	check("Send", err)
	check("React", h.m.React(ctx, groupChat, "mid.$invented-c1", "❤️", "txn-c"))
	check("MarkRead", h.m.MarkRead(ctx, groupChat, ""))
	check("SetMuted", h.m.SetMuted(ctx, groupChat, true))
	_, err = h.m.Media(ctx, groupChat, "mid.$invented-c1")
	check("Media", err)
	if n := len(h.f.requests()) - from; n != 0 {
		t.Errorf("%d requests after the checkpoint", n)
	}
	if _, ok := h.m.pace.Blocked(); !ok {
		t.Errorf("the pace is not blocked")
	}
}

// A one-to-one that has only an older, unencrypted thread is still
// "<FBID>@msgr", but its send goes on that thread (a SendMessageTask, under
// an offline threading ID), not through the encrypted side.
func TestAnUnencryptedOneToOneSendsOnItsThread(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	const erinFBID int64 = 2000066
	at := h.f.clock.Now().Add(-time.Hour)
	tbl := history(erinFBID, false, line("mid.$invented-e1", erinFBID, at, canary))
	tbl.LSDeleteThenInsertThread = []*table.LSDeleteThenInsertThread{thread(erinFBID, table.ONE_TO_ONE, at, "inbox")}
	h.f.last().dispatch(tbl)
	erin := keyID(erinFBID) + oneToOneSuffix
	if _, ok := h.line(t, erin, "mid.$invented-e1"); !ok {
		t.Fatalf("Erin's line is not in %s (chats %v)", erin, h.chatIDs(t))
	}

	id := h.send(t, network.Outgoing{ChatID: erin, Text: canary + " old thread"})
	if !strings.HasPrefix(id, otidPrefix) {
		t.Errorf("the send's ID is %q, want otid:<n>", id)
	}
	sent := tasksOf[*socket.SendMessageTask](h.f)
	if len(sent) != 1 || sent[0].ThreadId != erinFBID {
		t.Errorf("the sends are %+v, want one on Erin's thread", sent)
	}
	if n := h.f.count("SendFBMessage"); n != 0 {
		t.Errorf("SendFBMessage was asked %d times", n)
	}
	if st := h.m.SendState(id); st.State != network.SendSent {
		t.Errorf("the send is %+v, want sent", st)
	}
	// With nothing encrypted, its receipts and mute go on that thread too.
	h.markRead(t, erin, "")
	if marks := tasksOf[*socket.ThreadMarkReadTask](h.f); len(marks) != 1 || marks[0].ThreadId != erinFBID {
		t.Errorf("the read tasks are %+v", marks)
	}
	if n := len(h.f.receipts()); n != 0 {
		t.Errorf("%d encrypted receipts", n)
	}
}

// Text with a file is two messages, the text then the file, as Instagram's
// writes send them (A3), each under an ID of its own; the send's ID and
// lineID are the text's. In a group the file is uploaded once and goes as
// its own MEDIA task; in an encrypted chat as its own SendFBMessage.
func TestTextWithAFileIsTwoSendsTextFirst(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	from := len(h.f.requests())
	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " caption", File: writePhoto(t)})
	otid := strings.TrimPrefix(id, otidPrefix)
	var names []string
	for _, c := range h.f.requests()[from:] {
		names = append(names, c.name)
	}
	if want := []string{"Upload", "SendMessageTask", "SendMessageTask"}; !slices.Equal(names, want) {
		t.Errorf("requests %v, want %v", names, want)
	}
	sent := tasksOf[*socket.SendMessageTask](h.f)
	if len(sent) != 2 {
		t.Fatalf("%d sends, want 2", len(sent))
	}
	text, file := sent[0], sent[1]
	fileOTID := strconv.FormatInt(file.Otid, 10)
	if text.SendType != table.TEXT || text.Text != canary+" caption" || strconv.FormatInt(text.Otid, 10) != otid {
		t.Errorf("the first task is %+v, want the text under the send's threading ID", text)
	}
	if file.SendType != table.MEDIA || file.Text != "" || fileOTID == otid || !slices.Equal(file.AttachmentFBIds, []int64{5550000000000001}) {
		t.Errorf("the second task is %+v, want the photo alone under its own threading ID", file)
	}
	if o := h.outgoing(t, id); o.State != store.OutSent || o.LineID != "mid.$invented-sent-"+otid {
		t.Errorf("the send is %s with lineID %q, want sent with the text's", o.State, o.LineID)
	}
	tl, tok := h.line(t, groupChat, "mid.$invented-sent-"+otid)
	pl, pok := h.line(t, groupChat, "mid.$invented-sent-"+fileOTID)
	if !tok || !pok || tl.Kind != "text" || tl.Text != canary+" caption" || pl.Kind != "photo" || pl.Text != "" {
		t.Errorf("the lines are %+v (%v) and %+v (%v)", tl, tok, pl, pok)
	}

	eid := h.send(t, network.Outgoing{ChatID: aliceChat, Text: canary + " encrypted caption", File: writePhoto(t)})
	sends := h.f.sentFB()
	if h.f.count("UploadFB") != 1 || len(sends) != 2 {
		t.Fatalf("%d uploads, %d encrypted sends; want 1 and 2", h.f.count("UploadFB"), len(sends))
	}
	if string(sends[0].id) != eid || textOf(sends[0]) != canary+" encrypted caption" {
		t.Errorf("the first encrypted send is %q under %s, want the text under %s", textOf(sends[0]), sends[0].id, eid)
	}
	if kind, _ := fbFile(t, sends[1]); kind != "photo" || string(sends[1].id) == eid {
		t.Errorf("the second encrypted send is a %q under %s, want the photo under its own ID", kind, sends[1].id)
	}
	if l, ok := h.line(t, aliceChat, string(sends[1].id)); !ok || l.Kind != "photo" {
		t.Errorf("the photo's line is %+v (held %v)", l, ok)
	}
	if o := h.outgoing(t, eid); o.State != store.OutSent || (o.LineID != "" && o.LineID != eid) {
		t.Errorf("the encrypted send is %s with lineID %q, want sent with the text's", o.State, o.LineID)
	}
	gaps(t, h.f.requests())
}

// A file whose text went and whose own message Meta turned down is failed
// unclear; its key again is the same send, and sends the file alone with
// its upload: no second upload, no second text.
func TestAFileWhoseTextWentIsNotSentWithItAgain(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.f.mu.Lock()
	h.f.answer = func(t *socket.SendMessageTask) *table.LSTable {
		otid := strconv.FormatInt(t.Otid, 10)
		if t.SendType == table.MEDIA {
			return &table.LSTable{LSMarkOptimisticMessageFailed: []*table.LSMarkOptimisticMessageFailed{{OTID: otid}}}
		}
		return &table.LSTable{LSReplaceOptimsiticMessage: []*table.LSReplaceOptimsiticMessage{{OfflineThreadingId: otid, MessageId: "mid.$invented-sent-" + otid}}}
	}
	h.f.mu.Unlock()
	out := network.Outgoing{ChatID: groupChat, Text: canary + " caption", File: writePhoto(t), ClientID: "key-half"}
	id := h.send(t, out)
	if st := h.m.SendState(id); st.State != network.SendFailed || st.Kind != wire.Unclear {
		t.Fatalf("the send is %+v, want failed unclear", st)
	}
	h.f.mu.Lock()
	h.f.answer = nil
	h.f.mu.Unlock()
	again, err := h.m.Send(context.Background(), out)
	h.sends(t)
	if err != nil || again != id {
		t.Fatalf("the key again gave %q, %v; want %s", again, err, id)
	}
	var media, texts int
	for _, s := range tasksOf[*socket.SendMessageTask](h.f) {
		if s.SendType == table.MEDIA {
			media++
		} else {
			texts++
		}
	}
	if h.f.count("Upload") != 1 || media != 2 || texts != 1 {
		t.Errorf("%d uploads, %d photos, %d texts; want 1, 2 and 1", h.f.count("Upload"), media, texts)
	}
	if o := h.outgoing(t, id); o.State != store.OutSent || o.LineID != "mid.$invented-sent-"+strings.TrimPrefix(id, otidPrefix) {
		t.Errorf("the send is %s with lineID %q, want sent with the text's", o.State, o.LineID)
	}
}

// stopAfter stops the network (as Pllum quitting does) as soon as the
// write called name has been answered, before the next request. The wait
// it returns waits for that Stop to return: a quit's process has ended
// (its device database closed) before the next one starts, and a second
// Stop (harness.close) returns at once, without waiting for the first.
func (h *harness) stopAfter(t *testing.T, name string) (wait func()) {
	t.Helper()
	var once sync.Once
	done := make(chan struct{})
	h.f.mu.Lock()
	h.f.hook = func(n string) {
		if n != name {
			return
		}
		once.Do(func() {
			go func() {
				defer close(done)
				h.m.Stop()
			}()
			eventually(t, "the stop", func() bool {
				h.m.mu.Lock()
				defer h.m.mu.Unlock()
				return h.m.stopped
			})
		})
	}
	h.f.mu.Unlock()
	return func() {
		t.Helper()
		eventually(t, "the quit's Stop to return", func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		})
	}
}

// Review C1: Pllum quits after a send's text went and before its file
// did. The send is failed unclear (the file never went), never sent, so
// Pllum never confirms it on its text alone: not when the text's echo
// arrives, not after a restart. Its key then sends the file alone, with the
// upload it has; the text never goes twice. In a group and in an encrypted
// chat.
func TestAQuitBetweenTextAndFileIsNeverConfirmed(t *testing.T) {
	for _, tc := range []struct {
		name, chat, text string
		echo             func(h *harness, id string)
	}{
		{"a group", groupChat, "SendMessageTask", func(h *harness, id string) {
			otid := strings.TrimPrefix(id, otidPrefix)
			h.f.last().dispatch(echo(groupOne, "mid.$invented-sent-"+otid, otid, h.f.clock.Now()))
		}},
		{"an encrypted chat", aliceChat, "SendFBMessage", func(h *harness, id string) {
			// to this run's handler: the stopped run's declines it
			h.f.e2ee.mu.Lock()
			handler := h.f.e2ee.handlers[len(h.f.e2ee.handlers)-1]
			h.f.e2ee.mu.Unlock()
			if !handler(fbMessage(oneToOne(aliceFBID), person(selfFBID), id, h.f.clock.Now(), textContent(canary+" caption"), "")) {
				t.Fatalf("the handler did not take the text's echo")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "connect")
			f := newFakeMeta()
			h := newHarnessOver(t, dir, f)
			h.signIn(t)
			out := network.Outgoing{ChatID: tc.chat, Text: canary + " caption", File: writePhoto(t), ClientID: "key-quit"}
			quit := h.stopAfter(t, tc.text)
			id, err := h.m.Send(context.Background(), out)
			if err != nil {
				t.Fatalf("Send: %v", err)
			}
			h.sends(t)
			if st := h.m.SendState(id); st.State != network.SendFailed || st.Kind != wire.Unclear {
				t.Errorf("after the quit the send is %+v, want failed unclear", st)
			}
			uploads := f.count("Upload") + f.count("UploadFB")
			texts := f.count(tc.text)
			if uploads != 1 || texts != 1 {
				t.Fatalf("before the restart: %d uploads, %d sends; want 1 and 1 (the text)", uploads, texts)
			}
			quit()
			h.close()

			f.mu.Lock()
			f.hook = nil
			f.mu.Unlock()
			h = newHarnessOver(t, dir, f)
			if st := h.m.SendState(id); st.State != network.SendFailed || st.Kind != wire.Unclear {
				t.Errorf("after the restart the send is %+v, want failed unclear", st)
			}
			h.signIn(t)
			tc.echo(h, id)
			if st := h.m.SendState(id); st.State != network.SendFailed {
				t.Errorf("the text's echo made the send %+v; its file never went", st)
			}
			again, err := h.m.Send(context.Background(), out)
			h.sends(t)
			if err != nil || again != id {
				t.Fatalf("the key again gave %q, %v; want %s", again, err, id)
			}
			if n := f.count("Upload") + f.count("UploadFB"); n != 1 {
				t.Errorf("%d uploads, want 1", n)
			}
			if n := f.count(tc.text); n != 2 {
				t.Errorf("%s was asked %d times, want 2 (the text, then the file alone)", tc.text, n)
			}
			if st := h.m.SendState(id); st.State != network.SendSent {
				t.Errorf("after the file went the send is %+v, want sent", st)
			}
			if tc.chat == groupChat {
				last := tasksOf[*socket.SendMessageTask](f)
				if l := last[len(last)-1]; l.SendType != table.MEDIA || l.Text != "" {
					t.Errorf("the key's send was %+v, want the file alone", l)
				}
			} else if kind, _ := fbFile(t, f.sentFB()[1]); kind != "photo" {
				t.Errorf("the key's send was a %q, want the photo alone", kind)
			}
		})
	}
}

// A helper stopped between steps (as a crash would leave it): a send whose
// upload is kept but whose message was never asked comes back failed,
// refused, and its key sends it with that upload, never a second; one whose
// text went comes back failed, unclear, and its key sends the file alone.
func TestASendStoppedBetweenStepsGoesOnWithWhatItHas(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "connect")
	f := newFakeMeta()
	h := newHarnessOver(t, dir, f)
	photo := writePhoto(t)
	put := func(id, chat, client string, req sendRequest, p sendProgress) {
		t.Helper()
		body, _ := json.Marshal(req)
		if err := h.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: chat, Kind: outMessage, Client: client, MadeMs: 1, Request: body}); err != nil {
			t.Fatal(err)
		}
		if err := h.m.keepProgress(id, p); err != nil {
			t.Fatal(err)
		}
	}
	kept, _ := proto.Marshal(&waMediaTransport.WAMediaTransport{
		Integral:  &waMediaTransport.WAMediaTransport_Integral{DirectPath: proto.String("/v/t62.invented/kept-1")},
		Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{ObjectID: proto.String("object-kept-1")}})
	const encID = "7000000000000091"
	put(encID, aliceChat, "key-kept", sendRequest{File: photo, FileName: "invented.png", Kind: "photo", Mimetype: "image/png"},
		sendProgress{Upload: kept, Width: 4, Height: 3})
	const groupID = otidPrefix + "7000000000000092"
	put(groupID, groupChat, "key-half", sendRequest{Text: canary + " before", File: photo, FileName: "invented.png", Kind: "photo",
		Mimetype: "image/png", OTID: "7000000000000092", FileOTID: "7000000000000093"},
		sendProgress{Attachment: 5550000000000009, TextSent: true, TextLineID: "mid.$invented-kept-text"})
	h.close()

	h = newHarnessOver(t, dir, f)
	if st := h.m.SendState(encID); st.State != network.SendFailed || st.Kind != wire.Refused {
		t.Errorf("the send stopped after its upload is %+v, want failed refused", st)
	}
	if st := h.m.SendState(groupID); st.State != network.SendFailed || st.Kind != wire.Unclear {
		t.Errorf("the send stopped after its text is %+v, want failed unclear", st)
	}
	h.signIn(t)
	for _, tc := range []struct{ chat, client, id string }{{aliceChat, "key-kept", encID}, {groupChat, "key-half", groupID}} {
		got, err := h.m.Send(context.Background(), network.Outgoing{ChatID: tc.chat, File: photo, Text: "x", ClientID: tc.client})
		if err != nil || got != tc.id {
			t.Errorf("the key %s gave %q, %v; want %s", tc.client, got, err, tc.id)
		}
	}
	h.sends(t)
	if f.count("UploadFB") != 0 || f.count("Upload") != 0 {
		t.Errorf("%d encrypted and %d group uploads, want none: each send has its own", f.count("UploadFB"), f.count("Upload"))
	}
	sends := f.sentFB()
	if len(sends) != 1 || string(sends[0].id) != encID {
		t.Fatalf("the encrypted sends are %+v, want the photo once under its ID", sends)
	}
	if kind, path := fbFile(t, sends[0]); kind != "photo" || path != "/v/t62.invented/kept-1" {
		t.Errorf("the photo went as a %q at %q, want the kept upload", kind, path)
	}
	tasks := tasksOf[*socket.SendMessageTask](f)
	if len(tasks) != 1 || tasks[0].SendType != table.MEDIA || tasks[0].Otid != 7000000000000093 ||
		!slices.Equal(tasks[0].AttachmentFBIds, []int64{5550000000000009}) || tasks[0].Text != "" {
		t.Errorf("the group's tasks are %+v, want its file alone", tasks)
	}
	if o := h.outgoing(t, groupID); o.State != store.OutSent || o.LineID != "mid.$invented-kept-text" {
		t.Errorf("the group's send is %s with lineID %q, want sent with its text's", o.State, o.LineID)
	}
	if st := h.m.SendState(encID); st.State != network.SendSent {
		t.Errorf("the encrypted send is %+v, want sent", st)
	}
}

// writeMP4 writes an invented MP4 whose one track is width×height.
func writeMP4(t *testing.T, width, height uint32) string {
	t.Helper()
	box := func(typ string, body []byte) []byte {
		b := binary.BigEndian.AppendUint32(nil, uint32(8+len(body)))
		return append(append(b, typ...), body...)
	}
	tkhd := make([]byte, 4+80) // version 0: 4 bytes of version and flags, then 80 of fields
	binary.BigEndian.PutUint32(tkhd[76:], width<<16)
	binary.BigEndian.PutUint32(tkhd[80:], height<<16)
	ftyp := box("ftyp", []byte("mp42\x00\x00\x00\x00"))
	data := append(ftyp, box("moov", box("trak", box("tkhd", tkhd)))...)
	path := filepath.Join(t.TempDir(), "invented.mp4")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Review I2: an encrypted video or document carries a thumbnail size, as
// Messenger's apps need to show it: a video's from its track (clamped to
// 400), else 400×400 as a photo with no size has; never 0×0.
func TestEncryptedVideosAndFilesCarryAThumbnailSize(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	doc := filepath.Join(t.TempDir(), "invented-notes.txt")
	if err := os.WriteFile(doc, []byte(canary+" notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.send(t, network.Outgoing{ChatID: aliceChat, File: writeMP4(t, 640, 360)})
	h.send(t, network.Outgoing{ChatID: aliceChat, File: doc})
	sends := h.f.sentFB()
	if len(sends) != 2 {
		t.Fatalf("%d encrypted sends, want 2", len(sends))
	}
	for i, want := range []struct {
		kind string
		w, h uint32
	}{{"video", 400, 225}, {"file", 400, 400}} {
		kind, tr := fbTransport(t, sends[i])
		th := tr.GetAncillary().GetThumbnail()
		if kind != want.kind || th.GetThumbnailWidth() != want.w || th.GetThumbnailHeight() != want.h {
			t.Errorf("send %d is a %q with a %d×%d thumbnail, want a %s at %d×%d", i, kind, th.GetThumbnailWidth(), th.GetThumbnailHeight(), want.kind, want.w, want.h)
		}
	}
	app, _ := sends[0].msg.(*waConsumerApplication.ConsumerApplication)
	v, err := app.GetPayload().GetContent().GetVideoMessage().Decode()
	if err != nil || v.GetAncillary().GetWidth() != 640 || v.GetAncillary().GetHeight() != 360 {
		t.Errorf("the video says %d×%d (%v), want 640×360", v.GetAncillary().GetWidth(), v.GetAncillary().GetHeight(), err)
	}
	h.f.mu.Lock()
	types := append([]whatsmeow.MediaType{}, h.f.fbUploads...)
	h.f.mu.Unlock()
	if !slices.Equal(types, []whatsmeow.MediaType{whatsmeow.MediaVideo, whatsmeow.MediaDocument}) {
		t.Errorf("uploaded as %v, want a video then a document", types)
	}
}

// Review M1: that a message is being asked is kept only once its request
// leaves, after the pace, so a send stopped while it waits its turn (here,
// held back by a rate limit that came after its upload) is resendable,
// never unknown.
func TestAskingIsKeptOnlyOnceTheRequestLeaves(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.f.mu.Lock()
	h.f.hook = func(name string) {
		if name == "Upload" {
			h.m.pace.Fail(wire.RateLimited)
		}
	}
	h.f.mu.Unlock()
	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " waits", File: writePhoto(t)})
	if st := h.m.SendState(id); st.State != network.SendFailed {
		t.Fatalf("a send the pace held back is %+v, want failed", st)
	}
	if p := progressOf(h.outgoing(t, id)); p.Asking {
		t.Errorf("a send never asked is kept as being asked")
	}
	if n := h.f.count("SendMessageTask"); n != 0 {
		t.Errorf("SendMessageTask was asked %d times under a rate limit", n)
	}
}

// Review I1: each request of a send (the upload, the text, the file) has a
// budget of its own. Three requests that each take most of one would
// together run out a single shared budget before the file.
func TestEachRequestOfASendHasItsOwnBudget(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.m.writeWait = 400 * time.Millisecond
	h.f.mu.Lock()
	h.f.hook = func(name string) {
		if name == "Upload" || name == "SendMessageTask" {
			time.Sleep(250 * time.Millisecond) // a slow answer, inside its request (real time: the budget is a context deadline)
		}
	}
	h.f.mu.Unlock()
	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " slow", File: writePhoto(t)})
	if st := h.m.SendState(id); st.State != network.SendSent {
		t.Errorf("the send is %+v, want sent: each request had its own second", st)
	}
	if n := h.f.count("SendMessageTask"); n != 2 {
		t.Errorf("SendMessageTask was asked %d times, want 2", n)
	}
}

// Review N1: a text-and-file send whose answer is unsure, on either of its
// two messages, is unknown as soon as its delivery ends, never left
// sending: nothing is in flight, and no echo settles it. Pllum then goes by
// the chat's lines (or offers Try again) as for any unknown send. A send of
// one message stays sending: its own echo can settle it.
func TestATextAndFileWithAnUnsureAnswerIsUnknownAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name, chat string
		set        func(f *fakeMeta)
	}{
		{"a group's text times out", groupChat, func(f *fakeMeta) { f.writeErrs["SendMessageTask"] = context.DeadlineExceeded }},
		{"a group's file is answered without its ID", groupChat, func(f *fakeMeta) {
			f.answer = func(t *socket.SendMessageTask) *table.LSTable {
				if t.SendType == table.MEDIA {
					return &table.LSTable{}
				}
				otid := strconv.FormatInt(t.Otid, 10)
				return &table.LSTable{LSReplaceOptimsiticMessage: []*table.LSReplaceOptimsiticMessage{{OfflineThreadingId: otid, MessageId: "mid.$invented-sent-" + otid}}}
			}
		}},
		{"an encrypted file times out", aliceChat, func(f *fakeMeta) {
			f.hook = func(name string) {
				if name == "SendFBMessage" {
					f.setErr("SendFBMessage", whatsmeow.ErrMessageTimedOut) // the next one, the file's
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.signIn(t)
			h.f.mu.Lock()
			tc.set(h.f)
			h.f.mu.Unlock()
			id := h.send(t, network.Outgoing{ChatID: tc.chat, Text: canary + " unsure", File: writePhoto(t)})
			if st := h.m.SendState(id); st.State != network.SendUnknown {
				t.Errorf("after its delivery the send is %+v, want unknown", st)
			}
		})
	}

	h := newHarness(t)
	h.signIn(t)
	h.f.setErr("SendMessageTask", context.DeadlineExceeded)
	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " alone"})
	if st := h.m.SendState(id); st.State != network.SendSending {
		t.Errorf("a text alone with an unsure answer is %+v, want sending (its echo settles it)", st)
	}
}

// Review N2: a resumed send whose text went, put in line as the helper
// stops, fails unclear (its text arrived), not refused.
func TestASendStoppedInLineAfterItsTextIsUnclear(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	const id = otidPrefix + "7000000000000094"
	body, _ := json.Marshal(sendRequest{Text: canary + " before", File: writePhoto(t), FileName: "invented.png", Kind: "photo",
		Mimetype: "image/png", OTID: "7000000000000094", FileOTID: "7000000000000095"})
	if err := h.st.PutOutgoing(Name, store.Outgoing{ID: id, ChatID: groupChat, Kind: outMessage, MadeMs: 1, Request: body}); err != nil {
		t.Fatal(err)
	}
	if err := h.m.keepProgress(id, sendProgress{Attachment: 5550000000000009, TextSent: true, TextLineID: "mid.$invented-kept-text"}); err != nil {
		t.Fatal(err)
	}
	h.m.Stop()
	h.m.sends.outbox.Enqueue(id)
	if st := h.m.SendState(id); st.State != network.SendFailed || st.Kind != wire.Unclear {
		t.Errorf("the send is %+v, want failed unclear", st)
	}
}

// Review N3: a send whose "being asked" can't be kept asks nothing and is
// failed (refused, or unclear after its text), never left sending.
func TestASendWhoseAskingCannotBeKeptAsksNothing(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.m.testKeep = func(p sendProgress) error {
		if p.Asking {
			return errors.New("invented store failure") // a plain error, as the database gives
		}
		return nil
	}
	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " not kept"})
	if st := h.m.SendState(id); st.State != network.SendFailed || st.Kind != wire.Refused {
		t.Errorf("the send is %+v, want failed refused", st)
	}
	eid := h.send(t, network.Outgoing{ChatID: aliceChat, Text: canary + " not kept"})
	if st := h.m.SendState(eid); st.State != network.SendFailed || st.Kind != wire.Refused {
		t.Errorf("the encrypted send is %+v, want failed refused", st)
	}
	if n := h.f.count("SendMessageTask") + h.f.count("SendFBMessage"); n != 0 {
		t.Errorf("%d messages were asked", n)
	}
}

// A chat whose only lines after his marker came by an older page: nothing
// is unread, and MarkRead sends no receipt and no task.
func TestMarkReadAsksNothingForAnOlderPagesLines(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	at := h.f.clock.Now().Add(-time.Hour)
	encChat := keyID(encGroup) + groupSuffix
	aliceJID := watypes.NewJID(keyID(aliceFBID), watypes.MessengerServer).String()
	if _, err := h.st.PutMessage(Name, store.Message{ID: "WAINVENTEDH1", ChatID: encChat, SenderID: "his", FromMe: true,
		TimeMs: at.UnixMilli(), Text: canary, Kind: "text", Source: store.SourceLive}); err != nil {
		t.Fatal(err)
	}
	if err := h.st.Batch(Name, func(b *store.Batch) error {
		for i, id := range []string{"WAINVENTEDO1", "WAINVENTEDO2"} {
			if _, err := b.PutMessage(store.Message{ID: id, ChatID: encChat, SenderID: aliceJID,
				TimeMs: at.Add(-time.Duration(i+1) * time.Hour).UnixMilli(), Text: canary, Kind: "text", Source: store.SourceSync}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if c := h.chat(t, encChat); c.Unread != 0 {
		t.Fatalf("unread = %d, want 0", c.Unread)
	}
	h.markRead(t, encChat, "WAINVENTEDO1")
	h.markRead(t, encChat, "")
	if got := h.f.receipts(); len(got) != 0 {
		t.Errorf("MarkRead sent receipts %+v for lines nothing counts as unread", got)
	}
	if n := len(tasksOf[*socket.ThreadMarkReadTask](h.f)); n != 0 {
		t.Errorf("MarkRead sent %d ThreadMarkReadTasks", n)
	}
}
