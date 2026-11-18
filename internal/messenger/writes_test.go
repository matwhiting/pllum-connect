package messenger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
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
	eventually(t, "the sends to go", func() bool { return h.m.busy.Load() == 0 })
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
	if got, want := methodNames((*msgrWriter)(nil)), []string{"DownloadFB", "ExecuteTasks", "MarkRead", "SendFBMessage", "Upload"}; !slices.Equal(got, want) {
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
	data, err := os.ReadFile(first)
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
	if data, _ := os.ReadFile(p); string(data) != "invented restored bytes" {
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
		if data, _ := os.ReadFile(p); string(data) != "invented cdn bytes" {
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

// His own photo, sent to a group, is uploaded once and sent as one
// message with its caption, and opens without a download.
func TestAGroupPhotoIsUploadedThenSent(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	path := filepath.Join(t.TempDir(), "invented.png")
	png := []byte("\x89PNG\r\n\x1a\n" + strings.Repeat("\x00", 64))
	if err := os.WriteFile(path, png, 0o600); err != nil {
		t.Fatal(err)
	}
	id := h.send(t, network.Outgoing{ChatID: groupChat, Text: canary + " caption", File: path})
	sent := tasksOf[*socket.SendMessageTask](h.f)
	if h.f.count("Upload") != 1 || len(sent) != 1 {
		t.Fatalf("%d uploads, %d sends; want one each", h.f.count("Upload"), len(sent))
	}
	if s := sent[0]; s.SendType != table.MEDIA || !slices.Equal(s.AttachmentFBIds, []int64{5550000000000001}) || s.Text != canary+" caption" {
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
	// An encrypted chat takes no file: nothing to upload it with.
	if _, err := h.m.Send(context.Background(), network.Outgoing{ChatID: aliceChat, File: path}); wire.KindOf(err) != wire.Unsupported {
		t.Errorf("a file to an encrypted chat gave %v, want unsupported", err)
	}
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
