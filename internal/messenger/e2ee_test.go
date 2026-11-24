package messenger

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupCommon"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupMessage"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// Task B2: his encrypted chats, read. Every ID and text is invented.

// person is an invented FBID as whatsmeow names it on the Messenger server,
// on one of their devices.
func person(fbid int64) watypes.JID {
	return watypes.JID{User: fmt.Sprint(fbid), Device: 3, Server: watypes.MessengerServer}
}

// oneToOne is an encrypted one-to-one's chat JID; encryptedGroup an
// encrypted group's.
func oneToOne(fbid int64) watypes.JID {
	return watypes.NewJID(fmt.Sprint(fbid), watypes.MessengerServer)
}
func encryptedGroup(id int64) watypes.JID {
	return watypes.NewJID(fmt.Sprint(id), watypes.GroupServer)
}

// content wraps a consumer application's content.
func content(c *waConsumerApplication.ConsumerApplication_Content) *waConsumerApplication.ConsumerApplication {
	return &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: c}}}
}

func textContent(text string) *waConsumerApplication.ConsumerApplication {
	return content(&waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{MessageText: &waCommon.MessageText{Text: proto.String(text)}}})
}

func imageContent(caption string) *waConsumerApplication.ConsumerApplication {
	return content(&waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ImageMessage{ImageMessage: &waConsumerApplication.ConsumerApplication_ImageMessage{
			Image:   &waCommon.SubProtocol{Payload: []byte("image-invented"), Version: proto.Int32(1)},
			Caption: &waCommon.MessageText{Text: proto.String(caption)},
		}}})
}

func reactionContent(target, emoji string) *waConsumerApplication.ConsumerApplication {
	return content(&waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ReactionMessage{ReactionMessage: &waConsumerApplication.ConsumerApplication_ReactionMessage{
			Key: &waCommon.MessageKey{ID: proto.String(target)}, Text: proto.String(emoji)}}})
}

func editContent(target, text string) *waConsumerApplication.ConsumerApplication {
	return content(&waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_EditMessage{EditMessage: &waConsumerApplication.ConsumerApplication_EditMessage{
			Key: &waCommon.MessageKey{ID: proto.String(target)}, Message: &waCommon.MessageText{Text: proto.String(text)}}}})
}

func revokeContent(target string) *waConsumerApplication.ConsumerApplication {
	return &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_ApplicationData{ApplicationData: &waConsumerApplication.ConsumerApplication_ApplicationData{
			ApplicationContent: &waConsumerApplication.ConsumerApplication_ApplicationData_Revoke{Revoke: &waConsumerApplication.ConsumerApplication_RevokeMessage{
				Key: &waCommon.MessageKey{ID: proto.String(target)}}}}}}}
}

// fbMessage is an invented encrypted message as whatsmeow gives it: in
// chat, from sender, quoting quoted when it is set.
func fbMessage(chat, sender watypes.JID, id string, at time.Time, app *waConsumerApplication.ConsumerApplication, quoted string) *events.FBMessage {
	fb := &waMsgApplication.MessageApplication{Metadata: &waMsgApplication.MessageApplication_Metadata{}}
	if quoted != "" {
		fb.Metadata.QuotedMessage = &waMsgApplication.MessageApplication_Metadata_QuotedMessage{StanzaID: proto.String(quoted)}
	}
	return &events.FBMessage{
		Info: watypes.MessageInfo{
			MessageSource: watypes.MessageSource{Chat: chat, Sender: sender, IsFromMe: sender.User == fmt.Sprint(selfFBID),
				IsGroup: chat.Server == watypes.GroupServer},
			ID: id, Timestamp: at,
		},
		Message:       app,
		FBApplication: fb,
	}
}

// deliver hands an event to the encrypted socket's handlers and fails
// unless every handler took it (so whatsmeow would ack it).
func (h *harness) deliver(t *testing.T, evt any) {
	t.Helper()
	if !h.f.e2ee.dispatch(evt) {
		t.Fatalf("the handler did not take a %T", evt)
	}
}

// line reads one line; ok is false when the chat holds none by that ID.
func (h *harness) line(t *testing.T, chat, id string) (store.Message, bool) {
	t.Helper()
	l, ok := h.lines(t, chat)[id]
	return l, ok
}

// An encrypted message lands in its person's chat, <FBID>@msgr (or an
// encrypted group's, <id>@g.us), under whatsmeow's message ID, with its
// text, sender and side, a reply, media kept in raw; reactions, edits,
// revokes and receipts change the lines they name.
func TestAnEncryptedLineLandsInItsPersonsChat(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	at := time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC)
	alice := fmt.Sprint(aliceFBID) + "@msgr"

	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED01", at, textContent(canary+" encrypted hello"), ""))
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(selfFBID), "WAINVENTED02", at.Add(time.Minute), textContent(canary+" mine"), "WAINVENTED01"))
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED03", at.Add(2*time.Minute), imageContent(canary+" caption"), ""))

	l, ok := h.line(t, alice, "WAINVENTED01")
	if !ok {
		t.Fatalf("Alice's encrypted line is not in %s (chats %v)", alice, h.chatIDs(t))
	}
	if l.Text != canary+" encrypted hello" || l.Kind != "text" || l.FromMe || l.SenderID != fmt.Sprint(aliceFBID) ||
		l.Source != store.SourceLive || l.TimeMs != at.UnixMilli() || l.Hidden == nil || *l.Hidden {
		t.Errorf("Alice's line reads %+v", l)
	}
	if l, _ := h.line(t, alice, "WAINVENTED02"); !l.FromMe || l.ReplyTo != "WAINVENTED01" || l.SenderID != fmt.Sprint(selfFBID) {
		t.Errorf("his reply reads %+v", l)
	}
	img, _ := h.line(t, alice, "WAINVENTED03")
	if img.Kind != "photo" || len(img.Media) != 1 || img.Media[0].Kind != "photo" || img.Text != canary+" caption" {
		t.Errorf("the photo reads %+v", img)
	}
	if raw, err := h.st.Raw(Name, alice, "WAINVENTED03"); err != nil || len(raw) == 0 {
		t.Errorf("the photo kept no raw (err %v)", err)
	}
	for _, id := range h.chatIDs(t) {
		if id == fmt.Sprint(aliceFBID) {
			t.Errorf("Alice is also chat %s", id)
		}
	}

	// A reaction, then its removal; an edit; a revoke.
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED04", at.Add(3*time.Minute), reactionContent("WAINVENTED02", "👍"), ""))
	if l, _ := h.line(t, alice, "WAINVENTED02"); len(l.Reactions) != 1 || l.Reactions[0].Emoji != "👍" || l.Reactions[0].SenderID != fmt.Sprint(aliceFBID) {
		t.Errorf("the reaction reads %+v", l.Reactions)
	}
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED05", at.Add(4*time.Minute), reactionContent("WAINVENTED02", ""), ""))
	if l, _ := h.line(t, alice, "WAINVENTED02"); len(l.Reactions) != 0 {
		t.Errorf("the removed reaction is still there: %+v", l.Reactions)
	}
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED06", at.Add(5*time.Minute), editContent("WAINVENTED01", canary+" edited"), ""))
	if l, _ := h.line(t, alice, "WAINVENTED01"); l.Text != canary+" edited" || !l.Edited {
		t.Errorf("the edited line reads %+v", l)
	}
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED07", at.Add(6*time.Minute), revokeContent("WAINVENTED01"), ""))
	if l, _ := h.line(t, alice, "WAINVENTED01"); !l.Deleted {
		t.Errorf("the revoked line is not deleted")
	}
	for _, id := range []string{"WAINVENTED04", "WAINVENTED05", "WAINVENTED06", "WAINVENTED07"} {
		if _, ok := h.line(t, alice, id); ok {
			t.Errorf("news about a line is a line of its own: %s", id)
		}
	}

	// Alice reads his line: seen. He reads hers on another device: read.
	if c := h.chat(t, alice); c.Unread == 0 {
		t.Fatalf("Alice's photo is not unread")
	}
	seen := at.Add(7 * time.Minute)
	h.deliver(t, &events.Receipt{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(aliceFBID)},
		MessageIDs: []string{"WAINVENTED02"}, Timestamp: seen, Type: watypes.ReceiptTypeRead})
	if l, _ := h.line(t, alice, "WAINVENTED02"); l.SeenMs != seen.UnixMilli() {
		t.Errorf("his line's seen time is %d, want %d", l.SeenMs, seen.UnixMilli())
	}
	h.deliver(t, &events.Receipt{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(selfFBID), IsFromMe: true},
		MessageIDs: []string{"WAINVENTED03"}, Timestamp: seen, Type: watypes.ReceiptTypeReadSelf})
	if c := h.chat(t, alice); c.Unread != 0 {
		t.Errorf("after he read it elsewhere, %d unread", c.Unread)
	}

	// An encrypted group's line lands in <id>@g.us.
	group := fmt.Sprint(encGroup) + "@g.us"
	h.deliver(t, fbMessage(encryptedGroup(encGroup), person(bobFBID), "WAINVENTED08", at, textContent(canary+" group"), ""))
	if l, ok := h.line(t, group, "WAINVENTED08"); !ok || l.SenderID != fmt.Sprint(bobFBID) || l.Text != canary+" group" {
		t.Errorf("the encrypted group's line reads %+v (ok %v)", l, ok)
	}
	if c := h.chat(t, group); c.Kind != "group" {
		t.Errorf("the encrypted group is kind %q", c.Kind)
	}
	// A new person's first encrypted line makes their chat, once.
	const erinFBID int64 = 2000055
	h.deliver(t, fbMessage(oneToOne(erinFBID), person(erinFBID), "WAINVENTED09", at, textContent("x"), ""))
	if c := h.chat(t, fmt.Sprint(erinFBID)+"@msgr"); c.Kind != "single" {
		t.Errorf("a new person's chat is kind %q", c.Kind)
	}
	if n := h.m.GateCounts().(GateCounts).LiveEncryptedReceived; n != 9 {
		t.Errorf("live encrypted received %d, want 9", n)
	}
}

// Review Focus 4: one person, one chat. His older thread with Alice, the
// encrypted chat's live lines and its restored history are one chats row,
// <FBID>@msgr, holding the lines of both, read from the newest back.
func TestAnOldThreadAndItsEncryptedThreadAreOneChat(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	old := time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC)
	restored := time.Date(2026, 2, 20, 8, 0, 0, 0, time.UTC)
	live := time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC)
	tbl := inbox()
	page := history(aliceFBID, true, line("mid.$invented-old1", aliceFBID, old, canary+" before encryption"))
	tbl.LSUpsertMessage, tbl.LSInsertNewMessageRange = page.LSUpsertMessage, page.LSInsertNewMessageRange
	h.f.table = tbl
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{before: restored.UnixMilli() - 1,
		msgs: []*securehistory.DecryptedMessage{archived("WAINVENTED20", aliceFBID, restored, textContent(canary+" restored"))}})
	h.signIn(t)
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED21", live, textContent(canary+" live"), ""))
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	eventually(t, "the restored line", func() bool {
		_, ok := h.line(t, alice, "WAINVENTED20")
		return ok
	})

	var aliceChats []string
	for _, id := range h.chatIDs(t) {
		if id == alice || id == fmt.Sprint(aliceFBID) {
			aliceChats = append(aliceChats, id)
		}
	}
	if !slices.Equal(aliceChats, []string{alice}) {
		t.Fatalf("Alice is chats %v, want only %s", aliceChats, alice)
	}
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED21" {
		t.Errorf("her chat's newest line is %q, want the live one", c.LastMessageID)
	}
	// From the newest back: live, restored, then the older thread's.
	var order []string
	cursor := ""
	for range 5 {
		p, err := h.st.Messages(Name, alice, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range p.Items {
			order = append(order, m.ID)
		}
		if !p.HasMore {
			break
		}
		cursor = p.OldestCursor
	}
	if want := []string{"WAINVENTED21", "WAINVENTED20", "mid.$invented-old1"}; !slices.Equal(order, want) {
		t.Errorf("her lines from the newest back are %v, want %v", order, want)
	}
	if l, _ := h.line(t, alice, "WAINVENTED20"); l.Source != store.SourceSync || l.SenderID != fmt.Sprint(aliceFBID) {
		t.Errorf("the restored line reads %+v", l)
	}
}

// A message whose encryption could not be undone is an "undecryptable"
// line under its ID, shown, unread and the chat's newest, and counted; when
// it comes again readable, it is that one line.
func TestUndecryptableIsAShownLineAndCounted(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	at := time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	before := h.chat(t, alice).Unread

	info := watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(aliceFBID)}, ID: "WAINVENTED30", Timestamp: at}
	h.deliver(t, &events.UndecryptableMessage{Info: info})
	l, ok := h.line(t, alice, "WAINVENTED30")
	if !ok || l.Kind != "undecryptable" || (l.Hidden != nil && *l.Hidden) || l.Text != "" {
		t.Fatalf("the undecryptable message reads %+v (ok %v), want a shown undecryptable line", l, ok)
	}
	if c := h.chat(t, alice); c.Unread != before+1 || c.LastMessageID != "WAINVENTED30" {
		t.Errorf("the undecryptable line counts as unread %d (was %d), newest %q: want one more unread and it the newest", c.Unread, before, c.LastMessageID)
	}
	if n := h.m.GateCounts().(GateCounts).LiveUndecryptable; n != 1 {
		t.Errorf("undecryptable count %d, want 1", n)
	}

	// Meta saying the message was sent with no copy for this device: an "unavailable" line, shown the same way
	h.deliver(t, &events.UndecryptableMessage{Info: watypes.MessageInfo{MessageSource: info.MessageSource, ID: "WAINVENTED31", Timestamp: at},
		IsUnavailable: true})
	if l, ok := h.line(t, alice, "WAINVENTED31"); !ok || l.Kind != "unavailable" || (l.Hidden != nil && *l.Hidden) || l.Text != "" {
		t.Errorf("the message with no copy for this device reads %+v (ok %v), want a shown unavailable line", l, ok)
	}

	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED30", at, textContent(canary+" readable now"), ""))
	lines := h.lines(t, alice)
	if l := lines["WAINVENTED30"]; l.Text != canary+" readable now" || l.Kind != "text" || l.Hidden == nil || *l.Hidden {
		t.Errorf("the message that came again reads %+v", l)
	}
	n := 0
	for id := range lines {
		if id == "WAINVENTED30" {
			n++
		}
	}
	// the readable message took its own row over; the one with no copy stays apart
	if n != 1 || len(lines) != 2 {
		t.Errorf("Alice's chat holds %d lines, want the readable one and the unavailable one", len(lines))
	}
}

// A reaction, an edit or an unsend that couldn't be read, sent again and
// read, changes the line it names; the "couldn't read" line under its own
// ID is settled: kept hidden, no longer the chat's newest or unread. An
// "unavailable" one is settled the same way, and settling again changes
// nothing.
func TestAnUnreadableReactionEditOrUnsendSentAgainIsSettled(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	t0 := time.Date(2026, 3, 5, 8, 0, 0, 0, time.UTC)
	before := h.chat(t, alice).Unread

	// a reaction to his line
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(selfFBID), "WAINVENTED96", t0, textContent(canary+" mine"), ""))
	unreadable(t, h, aliceFBID, "WAINVENTED96-react", t0.Add(time.Minute), false)
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED96-react", t0.Add(time.Minute), reactionContent("WAINVENTED96", "❤"), ""))
	if l, _ := h.line(t, alice, "WAINVENTED96"); len(l.Reactions) != 1 || l.Reactions[0].Emoji != "❤" {
		t.Errorf("the reaction on his line reads %+v", l.Reactions)
	}
	settled(t, h, alice, "WAINVENTED96-react", "undecryptable")
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED96" || c.Unread != before {
		t.Errorf("after the reaction, the chat's newest is %q, unread %d: want his line, unread %d", c.LastMessageID, c.Unread, before)
	}

	// an edit of hers
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED97", t0.Add(2*time.Minute), textContent(canary+" first"), ""))
	unreadable(t, h, aliceFBID, "WAINVENTED97-edit", t0.Add(3*time.Minute), false)
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED97-edit", t0.Add(3*time.Minute), editContent("WAINVENTED97", canary+" edited"), ""))
	if l, _ := h.line(t, alice, "WAINVENTED97"); l.Text != canary+" edited" || !l.Edited {
		t.Errorf("the edited line reads %+v", l)
	}
	settled(t, h, alice, "WAINVENTED97-edit", "undecryptable")
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED97" || c.Unread != before+1 {
		t.Errorf("after the edit, the chat's newest is %q, unread %d: want her line, unread %d", c.LastMessageID, c.Unread, before+1)
	}

	// an unsend Meta sent no copy of
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED98", t0.Add(4*time.Minute), textContent(canary+" taken back"), ""))
	unreadable(t, h, aliceFBID, "WAINVENTED98-revoke", t0.Add(5*time.Minute), true)
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED98-revoke", t0.Add(5*time.Minute), revokeContent("WAINVENTED98"), ""))
	if l, _ := h.line(t, alice, "WAINVENTED98"); !l.Deleted {
		t.Errorf("the unsent line reads %+v", l)
	}
	settled(t, h, alice, "WAINVENTED98-revoke", "unavailable")
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED97" || c.Unread != before+1 {
		t.Errorf("after the unsend, the chat's newest is %q, unread %d: want the edited line, unread %d", c.LastMessageID, c.Unread, before+1)
	}

	// the same unsend again settles nothing more
	events := h.quiet()
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED98-revoke", t0.Add(5*time.Minute), revokeContent("WAINVENTED98"), ""))
	if n := h.quiet() - events; n != 0 {
		t.Errorf("the same unsend again sent %d changed events, want 0", n)
	}
	settled(t, h, alice, "WAINVENTED98-revoke", "unavailable")
}

// live needs both connections: the main socket (groups) and the encrypted
// one. With the encrypted one down and groups up, Messenger is offline with
// kind unsupported, and `status` says how the encrypted connection is.
func TestLiveNeedsBothConnections(t *testing.T) {
	h := newHarness(t)
	h.f.e2ee.hold = true
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the encrypted socket to be asked", func() bool {
		h.f.e2ee.mu.Lock()
		defer h.f.e2ee.mu.Unlock()
		return h.f.e2ee.connects == 1
	})
	check := func(what string, state network.State, kind, encrypted string) {
		t.Helper()
		eventually(t, what, func() bool {
			s, k := h.state()
			return s == state && k == kind && h.m.Status().State == state && h.m.Status().Encrypted == encrypted
		})
	}
	check("main up, encrypted connecting", network.Connecting, "", "connecting")

	h.deliver(t, &events.Connected{})
	check("both up", network.Live, "", "live")

	h.deliver(t, &events.Disconnected{})
	check("encrypted down, groups up", network.Offline, "unsupported", "connecting")

	h.deliver(t, &events.Connected{})
	check("encrypted back", network.Live, "", "live")

	h.f.last().dispatch(&messagix.TransientDisconnectEvent{})
	check("main down", network.Offline, "", "live")
	h.f.last().dispatch(&messagix.ReconnectedEvent{})
	check("main back", network.Live, "", "live")

	h.deliver(t, &events.ConnectFailure{Reason: events.ConnectFailureGeneric})
	check("encrypted ended by Meta", network.Offline, "unsupported", "failed")

	// Over the wire, `status` carries it.
	w := serveWith(t, h.m, false)
	r, _ := w.request("status", map[string]any{"network": Name})["result"].(map[string]any)
	if r["state"] != "offline" || r["encrypted"] != "failed" {
		t.Errorf("status answered %v, want offline with encrypted failed", r)
	}
	h.f.e2ee.mu.Lock()
	connects := h.f.e2ee.connects
	h.f.e2ee.mu.Unlock()
	if connects != 1 {
		t.Errorf("the encrypted socket was connected %d times, want once: its reconnects are the library's", connects)
	}
}

// archived is an invented line of his encrypted backup, as Decrypt gives
// it: Meta's backup message around the encrypted transport message.
func archived(id string, sender int64, at time.Time, app *waConsumerApplication.ConsumerApplication, supplementals ...*securehistory.DecryptedProtobuf) *securehistory.DecryptedMessage {
	dm := &securehistory.DecryptedMessage{ID: id, Message: backup(id, sender, at, app), Supplementals: map[string]*securehistory.DecryptedProtobuf{}}
	for i, s := range supplementals {
		dm.Supplementals[fmt.Sprint(i)] = s
	}
	return dm
}

// backup is one backed-up message.
func backup(id string, sender int64, at time.Time, app *waConsumerApplication.ConsumerApplication) *securehistory.DecryptedProtobuf {
	sub := &waMsgApplication.MessageApplication_SubProtocolPayload_ConsumerMessage{}
	if err := sub.Set(app); err != nil {
		panic(err)
	}
	fb := &waMsgApplication.MessageApplication{
		Payload: &waMsgApplication.MessageApplication_Payload{Content: &waMsgApplication.MessageApplication_Payload_SubProtocol{
			SubProtocol: &waMsgApplication.MessageApplication_SubProtocolPayload{SubProtocol: sub}}},
		Metadata: &waMsgApplication.MessageApplication_Metadata{},
	}
	b, err := proto.Marshal(fb)
	if err != nil {
		panic(err)
	}
	return &securehistory.DecryptedProtobuf{
		Message: &waArmadilloBackupMessage.BackupMessage{
			Metadata: &waArmadilloBackupCommon.Metadata{SenderID: proto.String(fmt.Sprint(sender)), MessageID: proto.String(id), TimestampMS: proto.Int64(at.UnixMilli())},
			Payload:  &waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage{EncryptedTransportMessage: b},
		},
		TimestampMS: at.UnixMilli(),
	}
}

// restoredAndSettled signs in (not the gate), restores his history over a
// scripted backup for Alice, and waits for the first read of it to end.
func restoredAndSettled(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.m.gate = false
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{more: true, before: 1}, fakePage{more: true, before: 0})
	h.signIn(t)
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.waitValue(t, "ebackfilled")
	h.settle()
	return h
}

// askEverything reads every chat to its oldest line, with Alice's backup
// made askable again, and reports how many requests that made.
func (h *harness) askEverything(t *testing.T) int {
	t.Helper()
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	_ = h.m.keep("estalled."+alice, "")
	_ = h.m.keep("encDone."+alice, "")
	before := len(h.f.requests())
	for _, chat := range []string{alice, fmt.Sprint(groupOne), fmt.Sprint(bobFBID) + "@msgr"} {
		_, _ = h.m.FetchOlder(context.Background(), chat)
	}
	h.settle()
	return len(h.f.requests()) - before
}

// Review I1 (the reviewer's probe): Meta suspending the encrypted device
// is a suspension of Messenger. It is banned, the pace is blocked, the run
// ends, and nothing is asked: no backup page, no history page, and no
// sign-in with the same cookies, even after a restart.
func TestABannedEncryptedDeviceStopsEveryRequest(t *testing.T) {
	h := restoredAndSettled(t)
	h.deliver(t, &events.TemporaryBan{Code: events.TempBanSentToTooManyPeople})
	eventually(t, "banned", func() bool { s, _ := h.state(); return s == network.Banned })
	if _, blocked := h.m.pace.Blocked(); !blocked {
		t.Fatalf("the pace is not blocked after a ban")
	}
	if n := h.askEverything(t); n != 0 {
		t.Errorf("after a ban, reading every chat made %d requests", n)
	}
	before := len(h.f.requests())
	h.login(t, validSession())
	h.f.clock.Advance(48 * time.Hour)
	if n := len(h.f.requests()) - before; n != 0 {
		t.Errorf("signing in again with the same cookies made %d requests", n)
	}
	if s, _ := h.state(); s != network.Banned {
		t.Errorf("state after the same cookies again is %s, want banned", s)
	}
	h.close()
	again := newHarnessOver(t, h.dir, h.f)
	again.m.gate = false
	before = len(h.f.requests())
	again.login(t, validSession())
	if n := len(h.f.requests()) - before; n != 0 {
		t.Errorf("after a restart, the same cookies made %d requests", n)
	}
}

// Review I1: Meta logging the encrypted device out ends the session
// (reconnectNeeded, nothing asked until he signs in again); any other end
// of the encrypted session leaves groups up but asks no history, encrypted
// or not, until a new sign-in.
func TestAnEndedEncryptedSessionAsksNothing(t *testing.T) {
	t.Run("logged out", func(t *testing.T) {
		h := restoredAndSettled(t)
		h.deliver(t, &events.LoggedOut{Reason: events.ConnectFailureLoggedOut})
		eventually(t, "reconnectNeeded", func() bool { s, _ := h.state(); return s == network.ReconnectNeeded })
		if _, blocked := h.m.pace.Blocked(); !blocked {
			t.Fatalf("the pace is not blocked after the device was logged out")
		}
		if n := h.askEverything(t); n != 0 {
			t.Errorf("after a log-out, reading every chat made %d requests", n)
		}
	})
	t.Run("ended otherwise", func(t *testing.T) {
		h := restoredAndSettled(t)
		h.deliver(t, &events.ConnectFailure{Reason: events.ConnectFailureGeneric})
		eventually(t, "offline", func() bool { s, k := h.state(); return s == network.Offline && k == "unsupported" })
		if n := h.askEverything(t); n != 0 {
			t.Errorf("with the encrypted session ended, reading every chat made %d requests", n)
		}
		if h.m.Status().Encrypted != "failed" {
			t.Errorf("encrypted is %q, want failed", h.m.Status().Encrypted)
		}
	})
}

// Review M2: a message that can never be stored (it panics, or the store
// refuses it) is not left unacked to come back at every connect: it is kept
// as an undecryptable line under its ID when that can be stored, and acked.
// Review M1: only a message that writes something tells Pllum to re-read.
func TestAMessageThatCannotBeStoredIsNotLostForever(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	at := time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	h.m.testPrep = func(evt *events.FBMessage) {
		if evt.Info.ID == "WAINVENTED60" {
			panic("invented")
		}
	}
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED60", at, textContent("x"), ""))
	if l, ok := h.line(t, alice, "WAINVENTED60"); !ok || l.Kind != "undecryptable" || (l.Hidden != nil && *l.Hidden) {
		t.Errorf("the message that panicked reads %+v (ok %v), want an undecryptable line", l, ok)
	}
	// An ID the store refuses: acked, nothing stored.
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "beeper:invented", at, textContent("x"), ""))

	// Nothing written, nothing to re-read: a poll vote, a signal, a line
	// already held sent again.
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED61", at, textContent("y"), ""))
	before := h.quiet()
	vote := content(&waConsumerApplication.ConsumerApplication_Content{Content: &waConsumerApplication.ConsumerApplication_Content_PollUpdateMessage{
		PollUpdateMessage: &waConsumerApplication.ConsumerApplication_PollUpdateMessage{}}})
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED62", at, vote, ""))
	signal := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Signal{Signal: &waConsumerApplication.ConsumerApplication_Signal{}}}}
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED63", at, signal, ""))
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED61", at, textContent("y"), ""))
	if n := h.quiet() - before; n != 0 {
		t.Errorf("messages that wrote nothing sent %d changed events", n)
	}
}

// Review N1 (the reviewer's probe): a line that lands in a row already
// there (an undecryptable notice taken over, an edit's early row, a
// restored line over a notice) is a change Pllum must re-read; only a line
// sent again unchanged is not.
func TestATakenOverRowTellsPllum(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	at := time.Date(2026, 3, 2, 8, 0, 0, 0, time.UTC)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{msgs: []*securehistory.DecryptedMessage{
		archived("WAINVENTED72", aliceFBID, at.Add(-time.Hour), textContent(canary+" restored over a notice"))}})
	h.signIn(t)
	quiet := h.quiet
	info := func(id string) watypes.MessageInfo {
		return watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(aliceFBID)}, ID: id, Timestamp: at}
	}

	h.deliver(t, &events.UndecryptableMessage{Info: info("WAINVENTED70")})
	before := quiet()
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED70", at, textContent(canary+" readable now"), ""))
	if n := quiet() - before; n != 1 {
		t.Errorf("the readable message over its notice sent %d changed events, want 1", n)
	}
	before = h.changedEvents()
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED70", at, textContent(canary+" readable now"), ""))
	if n := quiet() - before; n != 0 {
		t.Errorf("the same line sent again sent %d changed events, want 0", n)
	}

	// An edit that comes before its line: the line, landing in the edit's
	// early row, shows.
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED71-edit", at, editContent("WAINVENTED71", canary+" edited first"), ""))
	before = quiet()
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED71", at, textContent("x"), ""))
	if n := quiet() - before; n != 1 {
		t.Errorf("a line landing in its edit's row sent %d changed events, want 1", n)
	}

	// A restored line over a notice.
	h.deliver(t, &events.UndecryptableMessage{Info: info("WAINVENTED72")})
	before = quiet()
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatal(err)
	}
	h.waitValue(t, "ebackfilled")
	if n := quiet() - before; n < 1 {
		t.Errorf("a restored line over its notice sent no changed event")
	}
	if l, _ := h.line(t, alice, "WAINVENTED72"); l.Hidden == nil || *l.Hidden || l.Text != canary+" restored over a notice" {
		t.Errorf("the restored line reads %+v", l)
	}
}
