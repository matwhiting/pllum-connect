package whatsapp

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

func TestALiveTextIsStoredUnderItsOwnID(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0A1000000000000001", base+1000, "hello from an invented friend"))
	r.send(textMine(aliceJID, "3EB0A1000000000000002", base+2000, "an invented answer"))

	got := r.messages(aliceJID.String())
	if len(got) != 2 {
		t.Fatalf("the chat holds %d lines, want 2", len(got))
	}
	in, out := got[0], got[1]
	if in.ID != "3EB0A1000000000000001" || in.Text != "hello from an invented friend" || in.Kind != "text" ||
		in.FromMe || in.TimeMs != base+1000 || in.SenderID != aliceJID.String() || in.Source != store.SourceLive ||
		in.Hidden == nil || *in.Hidden {
		t.Errorf("the incoming line is %+v", in)
	}
	if out.ID != "3EB0A1000000000000002" || !out.FromMe || out.SenderID != selfJID.String() || out.TimeMs != base+2000 {
		t.Errorf("his own line is %+v", out)
	}
	c := r.chat(aliceJID.String())
	if c.LastMessageID != "3EB0A1000000000000002" || c.Unread != 1 || c.Kind != "single" {
		t.Errorf("the chat is %+v", c)
	}
	if len(r.log.named("changed")) == 0 {
		t.Errorf("no changed event after new lines")
	}
}

func TestAHistoryNotificationIsKeptBeforeTheAckAndProcessedAfterARestart(t *testing.T) {
	r := newRig(t, true)
	hs := &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Progress: proto.Uint32(40),
		Conversations: []*waHistorySync.Conversation{{
			ID:               proto.String(aliceJID.String()),
			LastMsgTimestamp: proto.Uint64(uint64((base + 2000) / 1000)),
			Messages: []*waHistorySync.HistorySyncMsg{
				webMsg(aliceJID, aliceJID, "3EB0H1000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented old line")}),
				webMsg(aliceJID, selfJID, "3EB0H1000000000000002", base+2000, true, &waE2E.Message{Conversation: proto.String("an invented old answer")}),
			},
		}},
	}
	// The first run dies mid-download: the blob never arrives.
	r.onMake = func(fc *fakeClient) {
		fc.block = make(chan struct{})
		fc.history["/v/t62.invented-1"] = hs
	}
	c := r.live()
	if !c.dispatch(historyNotif("3EB0N1000000000000001", "/v/t62.invented-1", base+3000)) {
		t.Fatalf("the notification was not handled, so it would not be acked")
	}
	if _, _, ok, err := r.st.NextPendingHistory("whatsapp"); err != nil || !ok {
		t.Fatalf("the notification is not in pending_history when the handler returns (ok %v, err %v)", ok, err)
	}
	if ids := r.chatIDs(); len(ids) != 0 {
		t.Fatalf("the notification itself made chats %v", ids)
	}

	r.onMake = func(fc *fakeClient) { fc.history["/v/t62.invented-1"] = hs }
	r.restart()
	r.send(&events.Connected{})
	eventually(t, "the leftover notification to be processed", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})
	got := r.messages(aliceJID.String())
	if len(got) != 2 || got[0].Source != store.SourceSync || got[0].Text != "an invented old line" || !got[1].FromMe {
		t.Fatalf("the synced lines are %+v", got)
	}
	if _, _, downloads := r.client().counts(); downloads != 1 {
		t.Fatalf("the restarted run downloaded %d times, want once", downloads)
	}
	eventually(t, "a history event", func() bool { return len(r.log.named("history")) > 0 })
	h := r.log.named("history")
	if last := h[len(h)-1]; last["chats"] != 1 || last["messages"] != 2 || last["done"] != false {
		t.Errorf("the history event is %v", last)
	}

	r.restart()
	r.send(&events.Connected{})
	time.Sleep(50 * time.Millisecond)
	if _, _, downloads := r.client().counts(); downloads != 0 {
		t.Fatalf("a second restart downloaded %d times, want none", downloads)
	}
	if n := len(r.messages(aliceJID.String())); n != 2 {
		t.Fatalf("the chat holds %d lines after another restart, want 2", n)
	}
}

func TestStatusChannelsAndBroadcastsAreIgnored(t *testing.T) {
	r := newRig(t, true)
	r.live()
	for i, chat := range []types.JID{statusJID, listJID, newsJID, botJID, types.NewJID("13135550002", types.DefaultUserServer)} {
		id := "3EB0S100000000000000" + string(rune('1'+i))
		r.send(textFrom(chat, aliceJID, id, base+1000, "an invented broadcast"))
	}
	if ids := r.chatIDs(); len(ids) != 0 {
		t.Fatalf("ignored chats were stored: %v", ids)
	}
}

func TestAReplyKeepsItsQuotedID(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0Q1000000000000001", base+1000, "an invented question"))
	r.send(msg(aliceJID, selfJID, "3EB0Q1000000000000002", base+2000, true, &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String("an invented reply"),
			ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("3EB0Q1000000000000001"), Participant: proto.String(aliceJID.String())},
		},
	}))
	r.send(msg(aliceJID, aliceJID, "3EB0Q1000000000000003", base+3000, false, &waE2E.Message{
		ImageMessage: &waE2E.ImageMessage{
			Caption:     proto.String("an invented captioned reply"),
			ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String("3EB0Q1000000000000002")},
		},
	}))
	if m := r.message(aliceJID.String(), "3EB0Q1000000000000002"); m.ReplyTo != "3EB0Q1000000000000001" || m.Text != "an invented reply" {
		t.Errorf("the text reply is %+v", m)
	}
	if m := r.message(aliceJID.String(), "3EB0Q1000000000000003"); m.ReplyTo != "3EB0Q1000000000000002" || m.Text != "an invented captioned reply" {
		t.Errorf("the photo reply is %+v", m)
	}
}

// editOf is an edit as whatsmeow dispatches it: the protocol message comes
// out of its EditedMessage wrapper under the edit's own ID.
func editOf(chat, sender types.JID, editID, targetID string, ms int64, fromMe bool, body string) *events.Message {
	evt := msg(chat, sender, editID, ms, fromMe, &waE2E.Message{EditedMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Key:           &waCommon.MessageKey{RemoteJID: proto.String(chat.String()), FromMe: proto.Bool(fromMe), ID: proto.String(targetID)},
			Type:          waE2E.ProtocolMessage_MESSAGE_EDIT.Enum(),
			EditedMessage: &waE2E.Message{Conversation: proto.String(body)},
		},
	}}})
	evt.Info.Edit = types.EditAttributeMessageEdit
	return evt
}

func revokeOf(chat, sender types.JID, revokeID, targetID string, ms int64, fromMe bool) *events.Message {
	evt := msg(chat, sender, revokeID, ms, fromMe, &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Key:  &waCommon.MessageKey{RemoteJID: proto.String(chat.String()), FromMe: proto.Bool(fromMe), ID: proto.String(targetID)},
		Type: waE2E.ProtocolMessage_REVOKE.Enum(),
	}})
	evt.Info.Edit = types.EditAttributeSenderRevoke
	return evt
}

func TestAnEditChangesTheTextNotTheID(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0E1000000000000001", base+1000, "an invented tpyo"))
	r.send(editOf(aliceJID, aliceJID, "3EB0E1000000000000099", "3EB0E1000000000000001", base+9000, false, "an invented typo, fixed"))

	got := r.messages(aliceJID.String())
	if len(got) != 1 {
		t.Fatalf("the chat holds %d lines, want the one line", len(got))
	}
	if m := got[0]; m.ID != "3EB0E1000000000000001" || m.Text != "an invented typo, fixed" || !m.Edited || m.TimeMs != base+1000 {
		t.Fatalf("the edited line is %+v", m)
	}
}

func TestARevokeMarksTheLineDeleted(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0R1000000000000001", base+1000, "an invented regret"))
	r.send(revokeOf(aliceJID, aliceJID, "3EB0R1000000000000098", "3EB0R1000000000000001", base+5000, false))
	// A revoke that arrives before its line.
	r.send(revokeOf(aliceJID, aliceJID, "3EB0R1000000000000099", "3EB0R1000000000000002", base+6000, false))
	r.send(textFrom(aliceJID, aliceJID, "3EB0R1000000000000002", base+2000, "an invented second regret"))

	for _, id := range []string{"3EB0R1000000000000001", "3EB0R1000000000000002"} {
		if m := r.message(aliceJID.String(), id); !m.Deleted || m.Text != "" {
			t.Errorf("line %s is %+v, want deleted with no text", id, m)
		}
	}
	if n := len(r.messages(aliceJID.String())); n != 2 {
		t.Errorf("the chat lists %d lines, want 2", n)
	}
	if c := r.chat(aliceJID.String()); c.LastMessageID != "" || c.Unread != 0 {
		t.Errorf("the chat is %+v, want no last line and nothing unread", c)
	}
}

func reactionTo(chat, sender types.JID, id, targetID string, ms int64, fromMe bool, emoji string) *events.Message {
	return msg(chat, sender, id, ms, fromMe, &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{
		Key:               &waCommon.MessageKey{RemoteJID: proto.String(chat.String()), FromMe: proto.Bool(!fromMe), ID: proto.String(targetID)},
		Text:              proto.String(emoji),
		SenderTimestampMS: proto.Int64(ms),
	}})
}

func TestAReactionAndItsRemoval(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textMine(aliceJID, "3EB0X1000000000000001", base+1000, "an invented joke"))
	r.send(reactionTo(aliceJID, aliceJID, "3EB0X1000000000000002", "3EB0X1000000000000001", base+2000, false, "😂"))
	r.send(reactionTo(aliceJID, selfJID, "3EB0X1000000000000003", "3EB0X1000000000000001", base+3000, true, "👍"))

	m := r.message(aliceJID.String(), "3EB0X1000000000000001")
	want := []store.Reaction{{Emoji: "😂", SenderID: aliceJID.String()}, {Emoji: "👍", SenderID: selfJID.String(), FromMe: true}}
	if !reflect.DeepEqual(m.Reactions, want) {
		t.Fatalf("reactions are %+v, want %+v", m.Reactions, want)
	}
	if n := len(r.messages(aliceJID.String())); n != 1 {
		t.Errorf("a reaction became a line: %d lines", n)
	}

	r.send(reactionTo(aliceJID, aliceJID, "3EB0X1000000000000004", "3EB0X1000000000000001", base+4000, false, ""))
	m = r.message(aliceJID.String(), "3EB0X1000000000000001")
	if len(m.Reactions) != 1 || m.Reactions[0].SenderID != selfJID.String() {
		t.Fatalf("after its removal, reactions are %+v", m.Reactions)
	}
}

func TestAPhotoKeepsWhatItNeedsToDownload(t *testing.T) {
	r := newRig(t, true)
	r.live()
	image := &waE2E.ImageMessage{
		Caption:       proto.String("an invented view"),
		Mimetype:      proto.String("image/jpeg"),
		Width:         proto.Uint32(1600),
		Height:        proto.Uint32(1200),
		DirectPath:    proto.String("/v/t62.invented-photo"),
		MediaKey:      []byte("invented-media-key-32-bytes-long"),
		FileEncSHA256: []byte("invented-enc-sha"),
		FileSHA256:    []byte("invented-sha"),
		FileLength:    proto.Uint64(123456),
	}
	r.send(msg(aliceJID, aliceJID, "3EB0P1000000000000001", base+1000, false, &waE2E.Message{ImageMessage: image}))

	m := r.message(aliceJID.String(), "3EB0P1000000000000001")
	if m.Kind != "photo" || m.Text != "an invented view" ||
		!reflect.DeepEqual(m.Media, []store.Media{{Kind: "photo", Width: 1600, Height: 1200, HasFile: true}}) {
		t.Fatalf("the photo line is %+v", m)
	}
	raw, err := r.st.Raw("whatsapp", aliceJID.String(), "3EB0P1000000000000001")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	var kept waE2E.Message
	if err := proto.Unmarshal(raw, &kept); err != nil {
		t.Fatalf("the raw bytes are not a message: %v", err)
	}
	if !proto.Equal(kept.GetImageMessage(), image) {
		t.Fatalf("the kept message lost what a download needs")
	}

	kinds := []struct {
		id    string
		m     *waE2E.Message
		kind  string
		media store.Media
	}{
		{"3EB0P1000000000000002", &waE2E.Message{VideoMessage: &waE2E.VideoMessage{Width: proto.Uint32(640), Height: proto.Uint32(360), DirectPath: proto.String("/v/invented")}},
			"video", store.Media{Kind: "video", Width: 640, Height: 360, HasFile: true}},
		{"3EB0P1000000000000003", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true), DirectPath: proto.String("/v/invented")}},
			"voiceNote", store.Media{Kind: "voiceNote"}},
		{"3EB0P1000000000000004", &waE2E.Message{StickerMessage: &waE2E.StickerMessage{Width: proto.Uint32(512), Height: proto.Uint32(512), DirectPath: proto.String("/v/invented")}},
			"sticker", store.Media{Kind: "sticker", Width: 512, Height: 512, HasFile: true}},
		{"3EB0P1000000000000005", &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{FileName: proto.String("invented.pdf"), DirectPath: proto.String("/v/invented")}},
			"file", store.Media{Kind: "file"}},
		{"3EB0P1000000000000006", &waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(false), DirectPath: proto.String("/v/invented")}},
			"file", store.Media{Kind: "file"}},
	}
	for i, k := range kinds {
		r.send(msg(aliceJID, aliceJID, k.id, base+2000+int64(i)*1000, false, k.m))
		m := r.message(aliceJID.String(), k.id)
		if m.Kind != k.kind || !reflect.DeepEqual(m.Media, []store.Media{k.media}) {
			t.Errorf("%s: the line is kind %q with media %+v, want %q %+v", k.id, m.Kind, m.Media, k.kind, k.media)
		}
		if raw, err := r.st.Raw("whatsapp", aliceJID.String(), k.id); err != nil || len(raw) == 0 {
			t.Errorf("%s: nothing kept to download it (%v)", k.id, err)
		}
	}
}

func TestPollsLocationsCallsAndViewOnceAreLabels(t *testing.T) {
	r := newRig(t, true)
	r.live()
	cases := []struct {
		m    *waE2E.Message
		want string
	}{
		{&waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{Name: proto.String("an invented poll")}}, "[poll]"},
		{&waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{Name: proto.String("an invented poll")}}, "[poll]"},
		{&waE2E.Message{LocationMessage: &waE2E.LocationMessage{DegreesLatitude: proto.Float64(1)}}, "[location]"},
		{&waE2E.Message{LiveLocationMessage: &waE2E.LiveLocationMessage{DegreesLatitude: proto.Float64(1)}}, "[location]"},
		{&waE2E.Message{ContactMessage: &waE2E.ContactMessage{DisplayName: proto.String("An Invented Card")}}, "[contact]"},
		{&waE2E.Message{ContactsArrayMessage: &waE2E.ContactsArrayMessage{DisplayName: proto.String("Invented Cards")}}, "[contact]"},
		{&waE2E.Message{Call: &waE2E.Call{CallKey: []byte("invented")}}, "[call]"},
		{&waE2E.Message{CallLogMesssage: &waE2E.CallLogMessage{IsVideo: proto.Bool(false)}}, "[call]"},
		{&waE2E.Message{ViewOnceMessageV2: &waE2E.FutureProofMessage{Message: &waE2E.Message{
			ImageMessage: &waE2E.ImageMessage{Caption: proto.String("an invented secret"), DirectPath: proto.String("/v/invented")}}}}, "[view once]"},
	}
	for i, c := range cases {
		id := "3EB0L10000000000000" + string(rune('A'+i))
		r.send(msg(aliceJID, aliceJID, id, base+int64(i+1)*1000, false, c.m))
		m := r.message(aliceJID.String(), id)
		if m.Kind != "text" || m.Text != c.want || len(m.Media) != 0 {
			t.Errorf("case %d: the line is kind %q text %q media %v, want the label %q", i, m.Kind, m.Text, m.Media, c.want)
		}
	}

	// A change to disappearing messages is a hidden notice.
	r.send(msg(aliceJID, aliceJID, "3EB0L1000000000000099", base+20000, false, &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type:                waE2E.ProtocolMessage_EPHEMERAL_SETTING.Enum(),
		EphemeralExpiration: proto.Uint32(86400),
	}}))
	m := r.message(aliceJID.String(), "3EB0L1000000000000099")
	if m.Kind != "notice" || m.Hidden == nil || !*m.Hidden {
		t.Errorf("the ephemeral setting is %+v, want a hidden notice", m)
	}
	if c := r.chat(aliceJID.String()); c.LastMessageID == "3EB0L1000000000000099" {
		t.Errorf("a hidden notice became the chat's last line")
	}
}

func TestAPhoneChatIsKeyedByItsNumber(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0K1000000000000001", base+1000, "an invented hello"))
	if c := r.chat(aliceJID.String()); c.ID != "10000000001@s.whatsapp.net" || c.Phone != "+10000000001" || c.Kind != "single" {
		t.Fatalf("the phone chat is %+v", c)
	}

	// A LID whose number the device knows is keyed by the number, with the
	// LID as an alias.
	bobLID := types.NewJID("100000000000002", types.HiddenUserServer)
	if err := r.w.device().LIDs.PutLIDMapping(context.Background(), bobLID, bobJID); err != nil {
		t.Fatal(err)
	}
	r.send(textFrom(bobLID, bobLID, "3EB0K1000000000000002", base+2000, "an invented hello by LID"))
	if c := r.chat(bobLID.String()); c.ID != bobJID.String() || c.Phone != "+10000000002" {
		t.Fatalf("the LID with a known number made chat %+v", c)
	}
	if id, ok := r.st.ChatFor("whatsapp", bobLID.String()); !ok || id != bobJID.String() {
		t.Fatalf("the LID is not an alias of the phone chat (%q, %v)", id, ok)
	}

	// A group is keyed by its own JID; its sender is a member.
	r.send(textFrom(groupJID, aliceJID, "3EB0K1000000000000003", base+3000, "an invented group hello"))
	if c := r.chat(groupJID.String()); c.Kind != "group" || c.Phone != "" {
		t.Fatalf("the group chat is %+v", c)
	}
	if m := r.message(groupJID.String(), "3EB0K1000000000000003"); m.SenderID != aliceJID.String() {
		t.Fatalf("the group line's sender is %q", m.SenderID)
	}

	// The "message yourself" chat is his own number.
	r.send(textMine(selfJID, "3EB0K1000000000000004", base+4000, "an invented note to self"))
	if st := r.w.Status(); st.SelfChatID != selfJID.String() {
		t.Fatalf("SelfChatID = %q, want his own number", st.SelfChatID)
	}
	if c := r.chat(selfJID.String()); c.LastMessageID != "3EB0K1000000000000004" {
		t.Fatalf("the self chat is %+v", c)
	}
	want := []string{groupJID.String(), aliceJID.String(), bobJID.String(), selfJID.String()}
	if got := r.chatIDs(); !reflect.DeepEqual(got, sorted(want)) {
		t.Fatalf("chats are %v, want %v", got, sorted(want))
	}
}

func TestALIDChatKeepsItsIDWhenItsNumberArrives(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceLID, aliceLID, "3EB0J1000000000000001", base+1000, "an invented hello by LID"))
	if ids := r.chatIDs(); !reflect.DeepEqual(ids, []string{aliceLID.String()}) {
		t.Fatalf("chats are %v, want the LID chat", ids)
	}

	// The number arrives later; lines to it land in the LID chat.
	if err := r.w.device().LIDs.PutLIDMapping(context.Background(), aliceLID, aliceJID); err != nil {
		t.Fatal(err)
	}
	r.send(textFrom(aliceJID, aliceJID, "3EB0J1000000000000002", base+2000, "an invented hello by number"))
	r.send(textFrom(aliceLID, aliceLID, "3EB0J1000000000000003", base+3000, "an invented hello by LID again"))
	if ids := r.chatIDs(); !reflect.DeepEqual(ids, []string{aliceLID.String()}) {
		t.Fatalf("chats are %v, want only the LID chat", ids)
	}
	if n := len(r.messages(aliceLID.String())); n != 3 {
		t.Fatalf("the LID chat holds %d lines, want 3", n)
	}
	if id, ok := r.st.ChatFor("whatsapp", aliceJID.String()); !ok || id != aliceLID.String() {
		t.Fatalf("the number is not an alias of the LID chat (%q, %v)", id, ok)
	}

	// So does a number–LID mapping in a history sync, for a chat by number.
	hs := &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		PhoneNumberToLidMappings: []*waHistorySync.PhoneNumberToLIDMapping{{
			PnJID: proto.String(aliceJID.String()), LidJID: proto.String(aliceLID.String()),
		}},
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String(aliceJID.String()),
			Messages: []*waHistorySync.HistorySyncMsg{
				webMsg(aliceJID, aliceJID, "3EB0J1000000000000004", base+500, false, &waE2E.Message{Conversation: proto.String("an invented older line")}),
			},
		}},
	}
	r.client().history["/v/t62.invented-j"] = hs
	r.send(historyNotif("3EB0N1000000000000009", "/v/t62.invented-j", base+4000))
	eventually(t, "the history to land", func() bool { return len(r.messages(aliceLID.String())) == 4 })
	if ids := r.chatIDs(); !reflect.DeepEqual(ids, []string{aliceLID.String()}) {
		t.Fatalf("after the history sync, chats are %v", ids)
	}
}

func TestReadReceiptsMarkHisLinesSeen(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textMine(aliceJID, "3EB0V1000000000000001", base+1000, "an invented question"))
	r.send(msg(aliceJID, selfJID, "3EB0V1000000000000002", base+2000, true,
		&waE2E.Message{AudioMessage: &waE2E.AudioMessage{PTT: proto.Bool(true), DirectPath: proto.String("/v/invented")}}))

	delivered := &events.Receipt{MessageSource: types.MessageSource{Chat: aliceJID, Sender: aliceJID},
		MessageIDs: []string{"3EB0V1000000000000001"}, Timestamp: time.UnixMilli(base + 3000), Type: types.ReceiptTypeDelivered}
	r.send(delivered)
	if m := r.message(aliceJID.String(), "3EB0V1000000000000001"); m.SeenMs != 0 {
		t.Fatalf("a delivery receipt marked the line seen")
	}
	read := *delivered
	read.Type, read.Timestamp = types.ReceiptTypeRead, time.UnixMilli(base+4000)
	r.send(&read)
	played := read
	played.MessageIDs, played.Type, played.Timestamp = []string{"3EB0V1000000000000002"}, types.ReceiptTypePlayed, time.UnixMilli(base+5000)
	r.send(&played)

	if m := r.message(aliceJID.String(), "3EB0V1000000000000001"); m.SeenMs != base+4000 {
		t.Errorf("the read line has seenMs %d, want %d", m.SeenMs, base+4000)
	}
	if m := r.message(aliceJID.String(), "3EB0V1000000000000002"); m.SeenMs != base+5000 {
		t.Errorf("the played voice note has seenMs %d, want %d", m.SeenMs, base+5000)
	}
}

func TestHisReadOnThePhoneClearsUnread(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0U1000000000000001", base+1000, "an invented one"))
	r.send(textFrom(aliceJID, aliceJID, "3EB0U1000000000000002", base+2000, "an invented two"))
	r.send(textFrom(aliceJID, aliceJID, "3EB0U1000000000000003", base+3000, "an invented three"))
	if u := r.chat(aliceJID.String()).Unread; u != 3 {
		t.Fatalf("unread = %d, want 3", u)
	}

	// He reads the first two on his phone.
	r.send(&events.Receipt{
		MessageSource: types.MessageSource{Chat: aliceJID, Sender: types.NewADJID(selfJID.User, 0, 1), IsFromMe: true},
		MessageIDs:    []string{"3EB0U1000000000000001", "3EB0U1000000000000002"},
		Timestamp:     time.UnixMilli(base + 9000),
		Type:          types.ReceiptTypeReadSelf,
	})
	if u := r.chat(aliceJID.String()).Unread; u != 1 {
		t.Fatalf("after his read of two, unread = %d, want 1", u)
	}

	// He marks the chat read on another device.
	r.send(&events.MarkChatAsRead{JID: aliceJID, Timestamp: time.UnixMilli(base + 10000), Action: &waSyncAction.MarkChatAsReadAction{
		Read:         proto.Bool(true),
		MessageRange: &waSyncAction.SyncActionMessageRange{LastMessageTimestamp: proto.Int64((base + 3000) / 1000)},
	}})
	if u := r.chat(aliceJID.String()).Unread; u != 0 {
		t.Fatalf("after marking the chat read, unread = %d, want 0", u)
	}

	// And then unread again.
	r.send(&events.MarkChatAsRead{JID: aliceJID, Timestamp: time.UnixMilli(base + 11000), Action: &waSyncAction.MarkChatAsReadAction{
		Read:         proto.Bool(false),
		MessageRange: &waSyncAction.SyncActionMessageRange{LastMessageTimestamp: proto.Int64((base + 3000) / 1000)},
	}})
	if u := r.chat(aliceJID.String()).Unread; u != 1 {
		t.Fatalf("after marking the chat unread, unread = %d, want 1", u)
	}
}

func TestAMuteFromThePhoneShowsMuted(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0M1000000000000001", base+1000, "an invented chatterbox"))
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(-1)}})
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("a mute forever did not mute the chat")
	}
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(false)}})
	if r.chat(aliceJID.String()).Muted {
		t.Fatalf("an unmute left the chat muted")
	}
	until := r.clock.Now().Add(8 * time.Hour).UnixMilli()
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(until)}})
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("a mute for eight hours did not mute the chat")
	}
	ended := r.clock.Now().Add(-time.Hour).UnixMilli()
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(ended)}})
	if r.chat(aliceJID.String()).Muted {
		t.Fatalf("a mute that has already ended muted the chat")
	}
	// A mute for a chat the store has never seen makes no chat.
	r.send(&events.Mute{JID: bobJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(-1)}})
	if ids := r.chatIDs(); !reflect.DeepEqual(ids, []string{aliceJID.String()}) {
		t.Fatalf("chats are %v", ids)
	}
}

func TestNamesComeFromContactsPushNamesAndGroups(t *testing.T) {
	r := newRig(t, true)
	r.live()
	ctx := context.Background()
	r.send(textFrom(aliceJID, aliceJID, "3EB0T1000000000000001", base+1000, "an invented hello"))
	if c := r.chat(aliceJID.String()); c.Title != "" {
		t.Fatalf("a chat with no name has title %q", c.Title)
	}
	// whatsmeow keeps the name in the device store, then tells of it.
	if _, _, err := r.w.device().Contacts.PutPushName(ctx, aliceJID, "Invented Pushname"); err != nil {
		t.Fatal(err)
	}
	r.send(&events.PushName{JID: aliceJID, NewPushName: "Invented Pushname"})
	if c := r.chat(aliceJID.String()); c.Title != "Invented Pushname" {
		t.Fatalf("after a push name, title = %q", c.Title)
	}
	if err := r.w.device().Contacts.PutContactName(ctx, aliceJID, "Invented", "Invented Contact"); err != nil {
		t.Fatal(err)
	}
	r.send(&events.Contact{JID: aliceJID, Action: &waSyncAction.ContactAction{FullName: proto.String("Invented Contact"), FirstName: proto.String("Invented")}})
	if c := r.chat(aliceJID.String()); c.Title != "Invented Contact" {
		t.Fatalf("after a contact name, title = %q", c.Title)
	}

	r.send(textFrom(groupJID, bobJID, "3EB0T1000000000000002", base+2000, "an invented group hello"))
	r.send(&events.GroupInfo{JID: groupJID, Name: &types.GroupName{Name: "An Invented Group"}})
	if c := r.chat(groupJID.String()); c.Title != "An Invented Group" {
		t.Fatalf("after a group rename, title = %q", c.Title)
	}
}

func TestStatesFollowTheConnection(t *testing.T) {
	cases := []struct {
		evt  any
		want network.State
	}{
		{&events.Connected{}, network.CatchingUp},
		{&events.OfflineSyncCompleted{}, network.Live},
		{&events.Disconnected{}, network.Offline},
		{&events.KeepAliveTimeout{ErrorCount: 1}, network.Offline},
		{&events.LoggedOut{Reason: events.ConnectFailureLoggedOut}, network.ReconnectNeeded},
		{&events.ClientOutdated{}, network.Outdated},
		{&events.TemporaryBan{Code: events.TempBanSentToTooManyPeople, Expire: time.Hour}, network.Banned},
		{&events.StreamReplaced{}, network.Offline},
	}
	for _, c := range cases {
		t.Run(reflect.TypeOf(c.evt).Elem().Name(), func(t *testing.T) {
			r := newRig(t, true)
			r.start()
			if st := r.w.Status().State; st != network.Connecting {
				t.Fatalf("a linked device starts %q, want connecting", st)
			}
			fc := r.client()
			eventually(t, "the first connect", func() bool { n, _, _ := fc.counts(); return n == 1 })
			if _, ok := c.evt.(*events.OfflineSyncCompleted); ok {
				r.send(&events.Connected{})
			}
			r.send(c.evt)
			if st := r.w.Status().State; st != c.want {
				t.Fatalf("after %T the state is %q, want %q", c.evt, st, c.want)
			}
			if last := r.log.lastState(); last == nil || last["state"] != c.want || last["network"] != "whatsapp" {
				t.Fatalf("the last state event is %v, want %q", last, c.want)
			}
		})
	}

	t.Run("a replaced stream waits for a nudge", func(t *testing.T) {
		r := newRig(t, true)
		fc := r.live()
		eventually(t, "the first connect", func() bool { n, _, _ := fc.counts(); return n == 1 })
		fc.mu.Lock()
		fc.connected = false
		fc.mu.Unlock()
		r.send(&events.StreamReplaced{})
		time.Sleep(30 * time.Millisecond)
		if n, _, _ := fc.counts(); n != 1 {
			t.Fatalf("the adapter reconnected by itself after a replaced stream")
		}
		r.w.Nudge()
		eventually(t, "a reconnect after the nudge", func() bool { n, _, _ := fc.counts(); return n == 2 })
	})

	t.Run("an unlinked device starts not connected", func(t *testing.T) {
		r := newRig(t, false)
		r.start()
		if st := r.w.Status(); st.State != network.NotConnected || st.SelfChatID != "" {
			t.Fatalf("an unlinked adapter's status is %+v", st)
		}
	})
}

func TestCatchingUpEndsAfter120Seconds(t *testing.T) {
	r := newRig(t, true)
	r.start()
	r.send(&events.Connected{})
	r.clock.Advance(119 * time.Second)
	if st := r.w.Status().State; st != network.CatchingUp {
		t.Fatalf("after 119 s the state is %q, want catchingUp", st)
	}
	r.clock.Advance(time.Second)
	if st := r.w.Status().State; st != network.Live {
		t.Fatalf("after 120 s the state is %q, want live", st)
	}
	// A reconnect catches up again, and an offline sync ends it at once.
	r.send(&events.Disconnected{})
	r.send(&events.Connected{})
	if st := r.w.Status().State; st != network.CatchingUp {
		t.Fatalf("after reconnecting the state is %q", st)
	}
	r.send(&events.OfflineSyncCompleted{})
	r.clock.Advance(200 * time.Second)
	if st := r.w.Status().State; st != network.Live {
		t.Fatalf("the state is %q, want live", st)
	}
}

func TestAnUnlinkKeepsTheMessagesAndANewLinkContinuesTheSameChats(t *testing.T) {
	carolLID := types.NewJID("100000000000003", types.HiddenUserServer)
	r := newRig(t, true)
	old := r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0W1000000000000001", base+1000, "an invented before"))
	r.send(textFrom(carolLID, carolLID, "3EB0W1000000000000002", base+2000, "an invented LID before"))
	r.send(textFrom(groupJID, aliceJID, "3EB0W1000000000000003", base+3000, "an invented group before"))
	before := r.chatIDs()

	// His phone unlinks Pllum: whatsmeow tells, then deletes the device.
	r.send(&events.LoggedOut{Reason: events.ConnectFailureLoggedOut})
	if err := r.w.device().Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := r.w.Status().State; st != network.ReconnectNeeded {
		t.Fatalf("after an unlink the state is %q", st)
	}
	if got := r.chatIDs(); !reflect.DeepEqual(got, before) {
		t.Fatalf("after an unlink the chats are %v, want %v", got, before)
	}
	if m := r.message(aliceJID.String(), "3EB0W1000000000000001"); m.Text != "an invented before" {
		t.Fatalf("an unlinked chat's line is %+v", m)
	}

	// A new QR scan links a new device for the same account.
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	fc := r.client()
	if fc == old {
		t.Fatalf("the new link reuses the unlinked client")
	}
	if st := r.w.Status().State; st != network.Linking {
		t.Fatalf("while linking the state is %q", st)
	}
	// Events from the unlinked client no longer count.
	old.dispatch(&events.Connected{})
	if st := r.w.Status().State; st != network.Linking {
		t.Fatalf("an old client's event moved the state to %q", st)
	}
	fc.qr <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "invented-qr-1", Timeout: 60 * time.Second}
	linkDevice(t, fc.dev)
	fc.qr <- whatsmeow.QRChannelSuccess
	eventually(t, "connecting after the scan", func() bool { return r.w.Status().State == network.Connecting })
	fc.dispatch(&events.Connected{})
	fc.dispatch(&events.OfflineSyncCompleted{})

	r.send(textFrom(aliceJID, aliceJID, "3EB0W1000000000000004", base+4000, "an invented after"))
	r.send(textFrom(carolLID, carolLID, "3EB0W1000000000000005", base+5000, "an invented LID after"))
	r.send(textFrom(groupJID, bobJID, "3EB0W1000000000000006", base+6000, "an invented group after"))
	if got := r.chatIDs(); !reflect.DeepEqual(got, before) {
		t.Fatalf("after a new link the chats are %v, want the same %v", got, before)
	}
	for _, id := range before {
		if n := len(r.messages(id)); n != 2 {
			t.Errorf("chat %s holds %d lines, want 2", id, n)
		}
	}
	if st := r.w.Status(); st.State != network.Live || st.SelfChatID != selfJID.String() {
		t.Fatalf("after a new link the status is %+v", st)
	}
}

func TestTwoChatsFoundToBeOneKeepTheIDGivenFirst(t *testing.T) {
	for _, c := range []struct {
		name         string
		first, later types.JID
	}{
		{"LID first", aliceLID, aliceJID},
		{"number first", aliceJID, aliceLID},
	} {
		for _, path := range []string{"live", "history"} {
			t.Run(c.name+", "+path, func(t *testing.T) {
				r := newRig(t, true)
				fc := r.live()
				r.send(textFrom(c.first, c.first, "3EB0G1000000000000001", base+5000, "an invented first chat"))
				r.send(textFrom(c.later, c.later, "3EB0G1000000000000002", base+1000, "an invented later chat"))
				if got := r.chatIDs(); len(got) != 2 {
					t.Fatalf("before the mapping the chats are %v, want two", got)
				}
				switch path {
				case "live":
					if err := r.w.device().LIDs.PutLIDMapping(context.Background(), aliceLID, aliceJID); err != nil {
						t.Fatal(err)
					}
					r.send(textFrom(c.later, c.later, "3EB0G1000000000000003", base+3000, "an invented once known"))
				case "history":
					fc.history["/v/map"] = &waHistorySync.HistorySync{
						SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
						PhoneNumberToLidMappings: []*waHistorySync.PhoneNumberToLIDMapping{{
							PnJID: proto.String(aliceJID.String()), LidJID: proto.String(aliceLID.String()),
						}},
						Conversations: []*waHistorySync.Conversation{{ID: proto.String(c.later.String()), Messages: []*waHistorySync.HistorySyncMsg{
							webMsg(c.later, c.later, "3EB0G1000000000000003", base+3000, false, &waE2E.Message{Conversation: proto.String("an invented once known")}),
						}}},
					}
					r.send(historyNotif("3EB0N1000000000000077", "/v/map", base+9000))
					eventually(t, "the history", func() bool {
						_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
						return !ok
					})
				}
				if got := r.chatIDs(); !reflect.DeepEqual(got, []string{c.first.String()}) {
					t.Fatalf("after the mapping the chats are %v, want the first-made %s", got, c.first)
				}
				if n := len(r.messages(c.later.String())); n != 3 {
					t.Fatalf("the merged chat holds %d lines, want 3", n)
				}
			})
		}
	}
}

func TestDevicePropsAskFor730DaysAsPllum(t *testing.T) {
	newRig(t, false)
	p := wastore.DeviceProps
	if p.GetOs() != "Pllum" || p.GetVersion().GetPrimary() != 0 || p.GetVersion().GetSecondary() != 1 || p.GetVersion().GetTertiary() != 0 {
		t.Errorf("the device is %q %v, want Pllum 0.1.0", p.GetOs(), p.GetVersion())
	}
	if p.GetPlatformType() != waCompanionReg.DeviceProps_DESKTOP {
		t.Errorf("the platform is %v, want desktop", p.GetPlatformType())
	}
	if !p.GetRequireFullSync() || p.GetHistorySyncConfig().GetFullSyncDaysLimit() != 730 {
		t.Errorf("history asks for full sync %v over %d days, want true over 730",
			p.GetRequireFullSync(), p.GetHistorySyncConfig().GetFullSyncDaysLimit())
	}

	// The real client acks only after the store has a line, downloads history
	// itself, and keeps itself connected.
	r := newRig(t, false)
	r.start()
	cli := newWhatsmeowClient(r.w.device())
	if !cli.SynchronousAck || !cli.ManualHistorySyncDownload || !cli.EmitAppStateEventsOnFullSync ||
		!cli.AutomaticMessageRerequestFromPhone || !cli.EnableAutoReconnect || !cli.DisableManualHistorySyncReceipt ||
		!cli.EnableDecryptedEventBuffer {
		t.Errorf("the client is set up as %+v", map[string]bool{
			"SynchronousAck": cli.SynchronousAck, "ManualHistorySyncDownload": cli.ManualHistorySyncDownload,
			"EmitAppStateEventsOnFullSync":       cli.EmitAppStateEventsOnFullSync,
			"AutomaticMessageRerequestFromPhone": cli.AutomaticMessageRerequestFromPhone,
			"EnableAutoReconnect":                cli.EnableAutoReconnect,
			"DisableManualHistorySyncReceipt":    cli.DisableManualHistorySyncReceipt,
			"EnableDecryptedEventBuffer":         cli.EnableDecryptedEventBuffer,
		})
	}
}

func TestQRCodesBecomeEventsAndAPasskeyRequestStopsTheLogin(t *testing.T) {
	r := newRig(t, false)
	r.start()
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	fc := r.client()
	if n, _, _ := fc.counts(); n != 1 || fc.qr == nil {
		t.Fatalf("Login connected %d times with a QR channel %v; the channel comes first, then one connect", n, fc.qr != nil)
	}
	now := r.clock.Now().UnixMilli()
	fc.qr <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "invented-qr-1", Timeout: 60 * time.Second}
	fc.qr <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventCode, Code: "invented-qr-2", Timeout: 20 * time.Second}
	eventually(t, "two qr events", func() bool { return len(r.log.named("qr")) == 2 })
	qr := r.log.named("qr")
	if qr[0]["code"] != "invented-qr-1" || qr[0]["expiresMs"] != now+60000 || qr[0]["network"] != "whatsapp" {
		t.Fatalf("the first qr event is %v", qr[0])
	}
	if qr[1]["code"] != "invented-qr-2" || qr[1]["expiresMs"] != now+20000 {
		t.Fatalf("the second qr event is %v", qr[1])
	}

	fc.qr <- whatsmeow.QRChannelItem{Event: whatsmeow.QRChannelEventPasskeyRequest, PasskeyRequest: &events.PairPasskeyRequest{}}
	eventually(t, "the login to stop", func() bool { return r.w.Status().State == network.NotConnected })
	if last := r.log.lastState(); last["kind"] != "unsupported" {
		t.Fatalf("a passkey request ended the login with %v", last)
	}
	if _, d, _ := fc.counts(); d == 0 || fc.qrCtx.Err() == nil {
		t.Fatalf("a passkey request left the login running (disconnects %d)", d)
	}

	finals := []struct {
		item  whatsmeow.QRChannelItem
		state network.State
		kind  any
	}{
		{whatsmeow.QRChannelSuccess, network.Connecting, nil},
		{whatsmeow.QRChannelTimeout, network.NotConnected, "timeout"},
		{whatsmeow.QRChannelClientOutdated, network.Outdated, nil},
		{whatsmeow.QRChannelScannedWithoutMultidevice, network.NotConnected, "refused"},
	}
	for _, f := range finals {
		t.Run(f.item.Event, func(t *testing.T) {
			r := newRig(t, false)
			r.start()
			if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
				t.Fatalf("Login: %v", err)
			}
			fc := r.client()
			fc.qr <- f.item
			eventually(t, "the login's end", func() bool {
				last := r.log.lastState()
				return last != nil && last["state"] == f.state
			})
			if last := r.log.lastState(); last["kind"] != f.kind {
				t.Fatalf("%s ended as %v, want kind %v", f.item.Event, last, f.kind)
			}
		})
	}

	t.Run("cancelLogin", func(t *testing.T) {
		r := newRig(t, false)
		r.start()
		if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
			t.Fatalf("Login: %v", err)
		}
		fc := r.client()
		r.w.CancelLogin()
		if st := r.w.Status().State; st != network.NotConnected {
			t.Fatalf("after cancelLogin the state is %q", st)
		}
		if _, d, _ := fc.counts(); d == 0 || fc.qrCtx.Err() == nil {
			t.Fatalf("cancelLogin left the login running")
		}
	})
}

func TestATimedMuteLapsesAndPllumIsToldToReRead(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0M2000000000000001", base+1000, "an invented chatterbox"))
	until := r.clock.Now().Add(8 * time.Hour).UnixMilli()
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(until)}})
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("a mute for eight hours did not mute the chat")
	}
	r.clock.Advance(8*time.Hour - time.Second)
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("the mute lapsed early")
	}
	before := len(r.log.named("changed"))
	r.clock.Advance(time.Second)
	if r.chat(aliceJID.String()).Muted {
		t.Fatalf("the mute still read muted after eight hours")
	}
	if n := len(r.log.named("changed")); n <= before {
		t.Fatalf("no changed event when the mute lapsed (%d before, %d after)", before, n)
	}
}

func TestAMuteForEverNeverLapsesAndAnUnmuteStopsTheTimer(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0M2000000000000002", base+1000, "an invented chatterbox"))
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(-1)}})
	r.clock.Advance(24 * 400 * time.Hour)
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("a mute for ever lapsed")
	}
	// A timed mute he then undoes leaves nothing to lapse.
	until := r.clock.Now().Add(8 * time.Hour).UnixMilli()
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(until)}})
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(false)}})
	r.clock.Advance(time.Hour) // runs any changed left pending by the events above
	before := len(r.log.named("changed"))
	r.clock.Advance(8 * time.Hour)
	if n := len(r.log.named("changed")); n != before {
		t.Fatalf("a lapse was announced for a mute he had undone (%d before, %d after)", before, n)
	}
}

func TestATimedMuteFromHistoryAndAcrossARestartLapses(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	muteEnd := r.clock.Now().Add(8 * time.Hour).Unix()
	c.history["/v/mute"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Progress: proto.Uint32(10),
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String(aliceJID.String()), UnreadCount: proto.Uint32(0),
			LastMsgTimestamp: proto.Uint64(uint64((base + 3000) / 1000)), MuteEndTime: proto.Uint64(uint64(muteEnd)),
			Messages: []*waHistorySync.HistorySyncMsg{
				webMsg(aliceJID, aliceJID, "3EB0M2000000000000003", base+3000, false, &waE2E.Message{Conversation: proto.String("an invented line")}),
			},
		}},
	}
	r.send(historyNotif("3EB0N2000000000000003", "/v/mute", base+9000))
	eventually(t, "the bootstrap", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})
	if !r.chat(aliceJID.String()).Muted {
		t.Fatalf("a timed mute in history did not mute the chat")
	}
	r.clock.Advance(time.Hour) // let the history's changed events go
	before := len(r.log.named("changed"))
	r.clock.Advance(7*time.Hour + time.Second)
	if r.chat(aliceJID.String()).Muted || len(r.log.named("changed")) <= before {
		t.Fatalf("the history's mute did not lapse with a changed event")
	}

	// A mute still running when the helper restarts is announced when it ends.
	until := r.clock.Now().Add(8 * time.Hour).UnixMilli()
	r.send(&events.Mute{JID: aliceJID, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(until)}})
	r.restart()
	r.client().dispatch(&events.Connected{})
	r.client().dispatch(&events.OfflineSyncCompleted{})
	// The restart's clock starts again at the rig's first moment; the mute's end
	// is read against it, so move it to the mute's own time first.
	r.clock.Advance(time.Duration(until-r.clock.Now().UnixMilli()-int64(time.Hour/time.Millisecond)) * time.Millisecond)
	before = len(r.log.named("changed"))
	r.clock.Advance(time.Hour)
	if r.chat(aliceJID.String()).Muted || len(r.log.named("changed")) <= before {
		t.Fatalf("a mute running across a restart did not lapse with a changed event")
	}
}

func TestHistoryStoresNamesUnreadMuteAndCompletes(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	ctx := context.Background()
	if err := r.w.device().Contacts.PutContactName(ctx, aliceJID, "Invented", "Invented Friend"); err != nil {
		t.Fatal(err)
	}
	muteEnd := r.clock.Now().Add(24 * time.Hour).Unix()
	bootstrap := &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Progress: proto.Uint32(10),
		Conversations: []*waHistorySync.Conversation{
			{
				ID: proto.String(aliceJID.String()), UnreadCount: proto.Uint32(1),
				LastMsgTimestamp: proto.Uint64(uint64((base + 3000) / 1000)), MuteEndTime: proto.Uint64(uint64(muteEnd)),
				Messages: []*waHistorySync.HistorySyncMsg{
					webMsg(aliceJID, aliceJID, "3EB0Y1000000000000003", base+3000, false, &waE2E.Message{Conversation: proto.String("an invented newest")}),
					webMsg(aliceJID, aliceJID, "3EB0Y1000000000000002", base+2000, false, &waE2E.Message{Conversation: proto.String("an invented middle")}),
					webMsg(aliceJID, aliceJID, "3EB0Y1000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented oldest")}),
				},
			},
			{
				ID: proto.String(groupJID.String()), Name: proto.String("An Invented Group"), UnreadCount: proto.Uint32(0),
				Participant: []*waHistorySync.GroupParticipant{{UserJID: proto.String(aliceJID.String())}, {UserJID: proto.String(selfJID.String())}},
				Messages: []*waHistorySync.HistorySyncMsg{
					webMsg(groupJID, bobJID, "3EB0Y1000000000000004", base+4000, false, &waE2E.Message{Conversation: proto.String("an invented group line")}),
				},
			},
		},
	}
	c.history["/v/boot"] = bootstrap
	r.send(historyNotif("3EB0N1000000000000011", "/v/boot", base+9000))
	eventually(t, "the bootstrap", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})

	a := r.chat(aliceJID.String())
	if a.Title != "Invented Friend" || a.Unread != 1 || !a.Muted || a.HistoryFromMs != base+1000 || a.HistoryComplete {
		t.Fatalf("the synced chat is %+v", a)
	}
	g := r.chat(groupJID.String())
	if g.Kind != "group" || g.Title != "An Invented Group" || g.Unread != 0 || len(g.Participants) != 2 {
		t.Fatalf("the synced group is %+v", g)
	}
	selfSeen := false
	for _, p := range g.Participants {
		if p.ID == selfJID.String() && p.IsSelf {
			selfSeen = true
		}
	}
	if !selfSeen {
		t.Errorf("the group's members %+v do not mark him", g.Participants)
	}

	full := &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_FULL.Enum(),
		Progress: proto.Uint32(100),
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String(aliceJID.String()), UnreadCount: proto.Uint32(1),
			LastMsgTimestamp: proto.Uint64(uint64((base + 3000) / 1000)),
			Messages: []*waHistorySync.HistorySyncMsg{
				webMsg(aliceJID, aliceJID, "3EB0Y1000000000000000", base-1000, false, &waE2E.Message{Conversation: proto.String("an invented older still")}),
			},
		}},
	}
	c.history["/v/full"] = full
	r.send(historyNotif("3EB0N1000000000000012", "/v/full", base+9500))
	eventually(t, "the full sync's done", func() bool {
		h := r.log.named("history")
		return len(h) > 0 && h[len(h)-1]["done"] == true
	})
	a = r.chat(aliceJID.String())
	if !a.HistoryComplete || a.HistoryFromMs != base-1000 || a.Unread != 1 {
		t.Fatalf("after a full sync at 100%%, the chat is %+v", a)
	}
	if g := r.chat(groupJID.String()); g.HistoryComplete {
		t.Errorf("a chat the full sync did not hold was marked complete")
	}
	h := r.log.named("history")
	if last := h[len(h)-1]; last["chats"] != 2 || last["messages"] != 5 {
		t.Errorf("the last history event is %v", last)
	}
}

func TestHistoryIsDoneAfter120SecondsWithNoBatch(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	c.history["/v/boot"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Conversations: []*waHistorySync.Conversation{{ID: proto.String(aliceJID.String()), Messages: []*waHistorySync.HistorySyncMsg{
			webMsg(aliceJID, aliceJID, "3EB0Z1000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented line")}),
		}}},
	}
	r.send(historyNotif("3EB0N1000000000000021", "/v/boot", base+9000))
	eventually(t, "a history event", func() bool { return len(r.log.named("history")) == 1 })
	r.clock.Advance(119 * time.Second)
	if n := len(r.log.named("history")); n != 1 {
		t.Fatalf("%d history events after 119 s, want 1", n)
	}
	r.clock.Advance(time.Second)
	h := r.log.named("history")
	if len(h) != 2 || h[1]["done"] != true || h[1]["chats"] != 1 || h[1]["messages"] != 1 {
		t.Fatalf("after 120 s with no batch the history events are %v", h)
	}
}

func TestCopiedHistoryStaysReadWhenWhatsAppsSyncTakesItOver(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	// Pllum copies Beeper's history first: copied lines are never unread.
	if _, err := r.st.Import("whatsapp", store.ImportBatch{
		BeeperChatID: "bc-invented-1", ChatID: aliceJID.String(), Chat: store.ImportChat{Kind: "single"},
		Messages: []store.ImportMessage{
			{BeeperID: "bm-1", NetworkID: "3EB0B1000000000000001", TimeMs: base + 1000, Text: "an invented copied one", Kind: "text", SenderID: aliceJID.String()},
			{BeeperID: "bm-2", NetworkID: "3EB0B1000000000000002", TimeMs: base + 2000, Text: "an invented copied two", Kind: "text", SenderID: aliceJID.String()},
		},
	}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if u := r.chat(aliceJID.String()).Unread; u != 0 {
		t.Fatalf("copied history is unread: %d", u)
	}
	// WhatsApp's own sync then takes the lines over; nothing in it is unread.
	c.history["/v/boot"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String(aliceJID.String()), UnreadCount: proto.Uint32(0),
			LastMsgTimestamp: proto.Uint64(uint64((base + 2000) / 1000)),
			Messages: []*waHistorySync.HistorySyncMsg{
				webMsg(aliceJID, aliceJID, "3EB0B1000000000000002", base+2000, false, &waE2E.Message{Conversation: proto.String("an invented copied two")}),
				webMsg(aliceJID, aliceJID, "3EB0B1000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented copied one")}),
			},
		}},
	}
	r.send(historyNotif("3EB0N1000000000000031", "/v/boot", base+9000))
	eventually(t, "the takeover", func() bool {
		ms := r.messages(aliceJID.String())
		return len(ms) == 2 && ms[0].Source == store.SourceSync && ms[1].Source == store.SourceSync
	})
	if a := r.chat(aliceJID.String()); a.Unread != 0 {
		t.Fatalf("after WhatsApp's sync took copied history over, unread = %d, want 0", a.Unread)
	}
}

func TestAHistoryLineFirstSeenRevokedIsKeptDeleted(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	revoked := webMsg(aliceJID, aliceJID, "3EB0D1000000000000001", base+1000, false, nil)
	revoked.Message.MessageStubType = waWeb.WebMessageInfo_REVOKE.Enum()
	c.history["/v/boot"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Conversations: []*waHistorySync.Conversation{{ID: proto.String(aliceJID.String()), Messages: []*waHistorySync.HistorySyncMsg{
			revoked,
			webMsg(aliceJID, aliceJID, "3EB0D1000000000000002", base+2000, false, &waE2E.Message{Conversation: proto.String("an invented survivor")}),
		}}},
	}
	r.send(historyNotif("3EB0N1000000000000041", "/v/boot", base+9000))
	eventually(t, "the history", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})
	if n := len(r.messages(aliceJID.String())); n != 2 {
		t.Fatalf("the chat lists %d lines, want 2", n)
	}
	if m := r.message(aliceJID.String(), "3EB0D1000000000000001"); !m.Deleted || m.TimeMs != base+1000 || m.FromMe {
		t.Fatalf("the revoked history line is %+v", m)
	}
}

func TestAMessageTheStoreCannotTakeIsNotAcked(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	r.st.Close()
	if c.dispatch(textFrom(aliceJID, aliceJID, "3EB0F1000000000000001", base+1000, "an invented line")) {
		t.Fatalf("a line the store could not take was reported handled, so whatsmeow would ack it")
	}
	if c.dispatch(historyNotif("3EB0F1000000000000002", "/v/lost", base+2000)) {
		t.Fatalf("a history notification the store could not keep was reported handled")
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestAnUnlinkStillShowsAfterARestart(t *testing.T) {
	r := newRig(t, true)
	r.live()
	r.send(textFrom(aliceJID, aliceJID, "3EB0Q2000000000000001", base+1000, "an invented before"))
	r.send(&events.LoggedOut{Reason: events.ConnectFailureLoggedOut})
	if err := r.w.device().Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.restart()
	if st := r.w.Status().State; st != network.ReconnectNeeded {
		t.Fatalf("after an unlink and a restart the state is %q, want reconnectNeeded", st)
	}
	if n := len(r.messages(aliceJID.String())); n != 1 {
		t.Fatalf("the chat holds %d lines after the restart", n)
	}

	// A device never linked starts not connected.
	fresh := newRig(t, false)
	fresh.start()
	if st := fresh.w.Status().State; st != network.NotConnected {
		t.Fatalf("a never-linked adapter starts %q", st)
	}
}

func TestAMessageArrivingAsTheAdapterStopsIsNotAcked(t *testing.T) {
	r := newRig(t, true)
	c := r.live()
	r.w.Stop()
	if c.dispatch(textFrom(aliceJID, aliceJID, "3EB0Q3000000000000001", base+1000, "an invented late line")) {
		t.Fatalf("a line that arrived after Stop was reported handled, so whatsmeow would ack it unstored")
	}
}

// retrying reports whether the history worker has a retry due.
func retrying(r *rig) bool {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	return r.w.retrying
}

func TestAHistoryDownloadThatCannotConnectIsKeptForLater(t *testing.T) {
	r := newRig(t, true)
	fc := r.live()
	fc.history["/v/later"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Conversations: []*waHistorySync.Conversation{{ID: proto.String(aliceJID.String()), Messages: []*waHistorySync.HistorySyncMsg{
			webMsg(aliceJID, aliceJID, "3EB0K2000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented kept line")}),
		}}},
	}
	fc.setDownloadErr(whatsmeow.ErrNotConnected)
	r.send(historyNotif("3EB0N1000000000000081", "/v/later", base+9000))
	tries := historyTries + 3
	for try := 1; try <= tries; try++ {
		eventually(t, "a download try", func() bool { _, _, d := fc.counts(); return d >= try })
		eventually(t, "a retry to be due", func() bool { return retrying(r) })
		r.clock.Advance(historyRetry)
	}
	eventually(t, "the last try", func() bool { _, _, d := fc.counts(); return d > tries })
	eventually(t, "its retry to be due", func() bool { return retrying(r) })
	if _, _, ok, _ := r.st.NextPendingHistory("whatsapp"); !ok {
		t.Fatalf("a notification that could not connect was dropped after %d tries", tries+1)
	}

	// While offline the worker does not try at all.
	r.send(&events.Disconnected{})
	_, _, before := fc.counts()
	r.clock.Advance(historyRetry)
	time.Sleep(30 * time.Millisecond)
	if _, _, after := fc.counts(); after != before {
		t.Fatalf("the worker downloaded %d times while offline", after-before)
	}

	// Connected again, it goes through.
	fc.setDownloadErr(nil)
	r.send(&events.Connected{})
	eventually(t, "the history to land", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})
	if n := len(r.messages(aliceJID.String())); n != 1 {
		t.Fatalf("the chat holds %d lines, want 1", n)
	}
}

func TestABrokenHistoryBlobIsDroppedAfterThreeTries(t *testing.T) {
	r := newRig(t, true)
	fc := r.live()
	fc.setDownloadErr(errCutOff) // not a connection problem: the blob itself is bad
	r.send(historyNotif("3EB0N1000000000000082", "/v/broken", base+9000))
	for try := 1; try < historyTries; try++ {
		eventually(t, "a download try", func() bool { _, _, d := fc.counts(); return d >= try })
		eventually(t, "a retry to be due", func() bool { return retrying(r) })
		r.clock.Advance(historyRetry)
	}
	eventually(t, "the broken notification to be dropped", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})
	if _, _, d := fc.counts(); d != historyTries {
		t.Fatalf("the broken blob was tried %d times, want %d", d, historyTries)
	}
}

func TestTheHistoryReceiptGoesOnlyAfterTheNotificationIsStored(t *testing.T) {
	r := newRig(t, true)
	fc := r.live()
	fc.setDownloadErr(errCutOff)
	storedFirst := true
	fc.onReceipt = func() {
		if _, _, ok, _ := r.st.NextPendingHistory("whatsapp"); !ok {
			storedFirst = false
		}
	}
	r.send(historyNotif("3EB0N1000000000000091", "/v/receipt", base+9000))
	if got := fc.sentReceipts(); !reflect.DeepEqual(got, []string{"hist_sync 3EB0N1000000000000091"}) {
		t.Fatalf("receipts sent: %v, want one history-sync receipt", got)
	}
	if !storedFirst {
		t.Fatalf("the history-sync receipt went before the notification was in the store")
	}

	// A notification the store cannot keep is not acknowledged at all.
	r2 := newRig(t, true)
	fc2 := r2.live()
	r2.st.Close()
	if fc2.dispatch(historyNotif("3EB0N1000000000000092", "/v/receipt-2", base+9500)) {
		t.Fatalf("the unkept notification was reported handled")
	}
	if got := fc2.sentReceipts(); len(got) != 0 {
		t.Fatalf("receipts sent: %v, want none for the unkept notification", got)
	}
}

func TestALoginThatLosesItsConnectionIsOffline(t *testing.T) {
	r := newRig(t, false)
	r.start()
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	fc := r.client()
	// whatsmeow tells of the dropped socket; the QR channel then ends as if timed out.
	fc.dispatch(&events.Disconnected{})
	fc.qr <- whatsmeow.QRChannelTimeout
	eventually(t, "the login's end", func() bool { return r.w.Status().State == network.NotConnected })
	if last := r.log.lastState(); last["kind"] != "offline" {
		t.Fatalf("a login that lost its connection ended as %v, want kind offline", last)
	}
}

// unlinked is a rig whose device WhatsApp has unlinked.
func unlinked(t *testing.T) *rig {
	t.Helper()
	r := newRig(t, true)
	r.live()
	r.send(&events.LoggedOut{Reason: events.ConnectFailureLoggedOut})
	if err := r.w.device().Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestARelinkThatStopsStillNeedsReconnecting(t *testing.T) {
	r := unlinked(t)
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	r.w.CancelLogin()
	if st := r.w.Status().State; st != network.ReconnectNeeded {
		t.Fatalf("a cancelled relink leaves the state %q, want reconnectNeeded", st)
	}

	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	r.client().qr <- whatsmeow.QRChannelTimeout
	eventually(t, "the relink's end", func() bool { return r.w.Status().State != network.Linking })
	last := r.log.lastState()
	if last["state"] != network.ReconnectNeeded || last["kind"] != "timeout" {
		t.Fatalf("a timed-out relink ended as %v, want reconnectNeeded with kind timeout", last)
	}
	r.restart()
	if st := r.w.Status().State; st != network.ReconnectNeeded {
		t.Fatalf("after a restart the state is %q, want reconnectNeeded", st)
	}
}

func TestHistoryIsDoneAfter120SecondsWhenThePhoneSendsNothing(t *testing.T) {
	r := newRig(t, false)
	r.start()
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	fc := r.client()
	linkDevice(t, fc.dev)
	fc.qr <- whatsmeow.QRChannelSuccess
	eventually(t, "connecting after the scan", func() bool { return r.w.Status().State == network.Connecting })
	r.clock.Advance(119 * time.Second)
	if h := r.log.named("history"); len(h) != 0 {
		t.Fatalf("history events before 120 s: %v", h)
	}
	r.clock.Advance(time.Second)
	h := r.log.named("history")
	if len(h) != 1 || h[0]["done"] != true || h[0]["chats"] != 0 || h[0]["messages"] != 0 {
		t.Fatalf("120 s after a link with no history the events are %v, want one done with no counts", h)
	}
}

func TestAMessageWhoseHandlerFailedIsStoredOnceWhenWhatsAppSendsItAgain(t *testing.T) {
	r := newRig(t, true)
	fc := r.live()
	hash := [32]byte{0x51, 0x52, 0x53}
	evt := textFrom(aliceJID, aliceJID, "3EB0B2000000000000001", base+1000, "an invented line that must not be lost")

	// The store fails: the handler says so, nothing is acked, and the
	// decrypted plaintext stays in whatsmeow's buffer on the encrypted
	// device database.
	r.st.Close()
	if acked, err := fc.deliver(hash, evt); err != nil || acked {
		t.Fatalf("a line the store could not take: acked %v, err %v", acked, err)
	}

	// The helper restarts; WhatsApp sends the stanza again, and the buffer
	// replays it (the Signal session has already moved past it).
	r.w.Stop()
	st, err := store.Open(r.dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	r.st = st
	r.restart()
	fc2 := r.client()
	fc2.dispatch(&events.Connected{})
	if acked, err := fc2.deliver(hash, evt); err != nil || !acked {
		t.Fatalf("the redelivered line: acked %v, err %v", acked, err)
	}
	// A third copy is already processed: acked without being handled again.
	if acked, err := fc2.deliver(hash, evt); err != nil || !acked {
		t.Fatalf("a third copy: acked %v, err %v", acked, err)
	}
	got := r.messages(aliceJID.String())
	if len(got) != 1 || got[0].Text != "an invented line that must not be lost" {
		t.Fatalf("the chat holds %+v, want the line once", got)
	}
}

func TestHistoryArrivingInAKeepAliveGapIsFetchedWhenItEnds(t *testing.T) {
	r := newRig(t, true)
	fc := r.live()
	fc.history["/v/gap"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_RECENT.Enum(),
		Conversations: []*waHistorySync.Conversation{{ID: proto.String(aliceJID.String()), Messages: []*waHistorySync.HistorySyncMsg{
			webMsg(aliceJID, aliceJID, "3EB0B3000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented gap line")}),
		}}},
	}
	r.send(&events.KeepAliveTimeout{ErrorCount: 1})
	r.send(historyNotif("3EB0N1000000000000101", "/v/gap", base+9000))
	time.Sleep(30 * time.Millisecond)
	if _, _, d := fc.counts(); d != 0 {
		t.Fatalf("the worker downloaded while offline")
	}
	// whatsmeow sends KeepAliveRestored with no Connected.
	r.send(&events.KeepAliveRestored{})
	eventually(t, "the history to land after the keep-alive gap", func() bool {
		_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
		return !ok
	})
}

func TestALoginSuccessThatNoLongerCountsStartsNoHistoryTimer(t *testing.T) {
	r := newRig(t, false)
	r.start()
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	fc := r.client()
	r.w.CancelLogin()
	r.w.loginSucceeded(fc) // the success that raced the cancel
	r.clock.Advance(3 * historyQuiet)
	if h := r.log.named("history"); len(h) != 0 {
		t.Fatalf("a cancelled login's success sent history events %v", h)
	}
	if st := r.w.Status().State; st != network.NotConnected {
		t.Fatalf("a cancelled login's success moved the state to %q", st)
	}
}

func TestHistoryWaitingToDownloadHoldsTheDoneTimer(t *testing.T) {
	r := newRig(t, false)
	r.start()
	if err := r.w.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	fc := r.client()
	linkDevice(t, fc.dev)
	fc.qr <- whatsmeow.QRChannelSuccess
	eventually(t, "connecting after the scan", func() bool { return r.w.Status().State == network.Connecting })
	fc.history["/v/held"] = &waHistorySync.HistorySync{
		SyncType: waHistorySync.HistorySync_INITIAL_BOOTSTRAP.Enum(),
		Conversations: []*waHistorySync.Conversation{{ID: proto.String(aliceJID.String()), Messages: []*waHistorySync.HistorySyncMsg{
			webMsg(aliceJID, aliceJID, "3EB0B4000000000000001", base+1000, false, &waE2E.Message{Conversation: proto.String("an invented held line")}),
		}}},
	}
	r.clock.Advance(100 * time.Second)
	// A notification arrives before the connection is caught up enough to download.
	r.send(historyNotif("3EB0N1000000000000111", "/v/held", base+9000))
	r.clock.Advance(10 * historyQuiet)
	if h := r.log.named("history"); len(h) != 0 {
		t.Fatalf("the sync was called done while a notification waited: %v", h)
	}
	r.send(&events.Connected{})
	eventually(t, "the batch's history event", func() bool { return len(r.log.named("history")) == 1 })
	if h := r.log.named("history"); h[0]["done"] != false || h[0]["messages"] != 1 {
		t.Fatalf("the batch's history event is %v", h[0])
	}
	r.clock.Advance(historyQuiet)
	h := r.log.named("history")
	if len(h) != 2 || h[1]["done"] != true {
		t.Fatalf("120 s after the batch the history events are %v", h)
	}
}

func TestADroppedRowsFailuresDoNotPassToTheNextRow(t *testing.T) {
	r := newRig(t, true)
	fc := r.live()
	fc.setDownloadErr(errCutOff)
	for n, path := range []string{"/v/broken-1", "/v/broken-2"} {
		r.send(historyNotif("3EB0N100000000000012"+string(rune('1'+n)), path, base+9000+int64(n)*1000))
		for try := 1; try < historyTries; try++ {
			want := n*historyTries + try
			eventually(t, "a download try", func() bool { _, _, d := fc.counts(); return d >= want })
			eventually(t, "a retry to be due", func() bool { return retrying(r) })
			r.clock.Advance(historyRetry)
		}
		eventually(t, "the row to be dropped", func() bool {
			_, _, ok, _ := r.st.NextPendingHistory("whatsapp")
			return !ok
		})
	}
	if _, _, d := fc.counts(); d != 2*historyTries {
		t.Fatalf("two broken rows were tried %d times in all, want %d each", d, historyTries)
	}
}
