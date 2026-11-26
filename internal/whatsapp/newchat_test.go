package whatsapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/matwhiting/pllum-connect/internal/diag"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// New chats by number. Every number, JID and text here is invented.

var (
	carolJID = types.NewJID("10000000003", types.DefaultUserServer) // on WhatsApp, unknown to the device
	daveJID  = types.NewJID("10000000004", types.DefaultUserServer) // not on WhatsApp
)

func phoneTo(jid types.JID) *network.Recipient {
	return &network.Recipient{Phone: "+" + jid.User}
}

// sendTo sends text to a number and fails the test on an error.
func (r *rig) sendTo(to *network.Recipient, text, client string) (id, chat string) {
	r.t.Helper()
	id, chat, err := r.w.SendTo(context.Background(), network.Outgoing{To: to, Text: text, ClientID: client})
	if err != nil {
		r.t.Fatalf("SendTo: %v", err)
	}
	return id, chat
}

func (r *rig) waitState(id, state string) network.SendState {
	r.t.Helper()
	eventually(r.t, "send "+state, func() bool { return r.w.SendState(id).State == state })
	return r.w.SendState(id)
}

// newChatRig is a live, paired adapter whose fake knows carol is on
// WhatsApp and dave is not.
func newChatRig(t *testing.T) (*rig, *fakeClient) {
	t.Helper()
	r := newRig(t, true)
	r.onMake = func(fc *fakeClient) { fc.onWhatsApp = map[string]bool{carolJID.User: true} }
	c := r.live()
	return r, c
}

func TestChatWithFindsTheStoredChatByNumberOrLID(t *testing.T) {
	r, trip := tripRig(t)
	ctx := context.Background()
	// A one-to-one known only by Alice's LID, then the LID → number map.
	r.send(textFrom(aliceLID, aliceLID, "3EB0C0000000000000001", base+1000, "invented hello"))
	if err := r.w.device().LIDs.PutLIDMapping(ctx, aliceLID, aliceJID); err != nil {
		t.Fatal(err)
	}
	// A group Bob is in, and no chat with Bob.
	r.send(textFrom(groupJID, bobJID, "3EB0C0000000000000002", base+2000, "invented group line"))

	trip.arm(true)
	defer trip.arm(false)
	for _, tc := range []struct {
		to   network.Recipient
		want string
	}{
		{network.Recipient{Phone: "+10000000001"}, aliceLID.String()},
		{network.Recipient{UserID: aliceJID.String()}, aliceLID.String()},
		{network.Recipient{UserID: aliceLID.String()}, aliceLID.String()},
		{network.Recipient{Phone: "+10000000002"}, ""},
		{network.Recipient{Phone: "+10000000007"}, ""},
	} {
		chat, ok, err := r.w.ChatWith(ctx, tc.to)
		if err != nil {
			t.Fatalf("ChatWith(%+v): %v", tc.to, err)
		}
		if ok != (tc.want != "") || chat != tc.want {
			t.Errorf("ChatWith(%+v) = %q, %v; want %q", tc.to, chat, ok, tc.want)
		}
	}
	if calls := trip.calls(); len(calls) != 0 {
		t.Errorf("ChatWith called the WhatsApp client: %v", calls)
	}
}

func TestSendToAStoredChatIsAnOrdinarySend(t *testing.T) {
	r, c := newChatRig(t)
	r.send(textFrom(aliceJID, aliceJID, "3EB0C0000000000000011", base+1000, "invented hello"))
	id, chat := r.sendTo(phoneTo(aliceJID), "invented reply", "")
	if chat != aliceJID.String() {
		t.Fatalf("the chat is %q, want Alice's", chat)
	}
	r.waitState(id, network.SendSent)
	if left := r.w.newChats.Left(); left != network.WhatsAppNewChatsPerDay {
		t.Errorf("a send to a stored chat counted a new chat (left %d)", left)
	}
	if n := len(c.checksMade()); n != 0 {
		t.Errorf("a send to a stored chat asked IsOnWhatsApp %d times", n)
	}
}

func TestSendToAKnownContactAsksNoCheck(t *testing.T) {
	r, c := newChatRig(t)
	if err := r.w.device().Contacts.PutContactName(context.Background(), bobJID, "Bob", "Bob Example"); err != nil {
		t.Fatal(err)
	}
	id, chat := r.sendTo(phoneTo(bobJID), "invented hello", "")
	if chat != bobJID.String() {
		t.Fatalf("the chat is %q, want Bob's number", chat)
	}
	r.waitState(id, network.SendSent)
	if n := len(c.checksMade()); n != 0 {
		t.Errorf("a known contact was checked %d times", n)
	}
	if left := r.w.newChats.Left(); left != network.WhatsAppNewChatsPerDay-1 {
		t.Errorf("left %d, want one new chat counted", left)
	}
	sent := c.sentMessages()
	if len(sent) != 1 || sent[0].to != bobJID {
		t.Fatalf("sent %v, want one message to Bob's number", sent)
	}
	found := false
	for _, id := range r.chatIDs() {
		found = found || id == bobJID.String()
	}
	if !found {
		t.Errorf("the new chat is not stored: %v", r.chatIDs())
	}
}

func TestSendToAnUnknownNumberChecksOnce(t *testing.T) {
	t.Run("on WhatsApp", func(t *testing.T) {
		r, c := newChatRig(t)
		id, chat := r.sendTo(phoneTo(carolJID), "invented hello", "")
		if chat != carolJID.String() {
			t.Fatalf("the chat is %q, want Carol's number", chat)
		}
		r.waitState(id, network.SendSent)
		checks := c.checksMade()
		if len(checks) != 1 || len(checks[0]) != 1 || checks[0][0] != "+10000000003" {
			t.Fatalf("IsOnWhatsApp was asked %v, want once with that one number", checks)
		}
		if sent := c.sentMessages(); len(sent) != 1 || sent[0].to != carolJID {
			t.Errorf("sent %v", sent)
		}
		if _, ok, _ := r.st.Chat(Name, carolJID.String()); !ok {
			t.Errorf("the new chat is not stored")
		}
	})
	t.Run("not on WhatsApp", func(t *testing.T) {
		r, c := newChatRig(t)
		id, chat := r.sendTo(phoneTo(daveJID), "invented hello", "")
		if chat != "" {
			t.Errorf("a number not on WhatsApp answered chat %q", chat)
		}
		if st := r.w.SendState(id); st.State != network.SendFailed || st.Kind != wire.NotOnNetwork {
			t.Fatalf("the send is %+v, want failed notOnNetwork", st)
		}
		if n := len(c.checksMade()); n != 1 {
			t.Errorf("IsOnWhatsApp was asked %d times", n)
		}
		c.dispatch(&events.Connected{}) // a reconnect sends nothing
		time.Sleep(20 * time.Millisecond)
		if sent := c.sentMessages(); len(sent) != 0 {
			t.Errorf("a message went to a number not on WhatsApp: %v", sent)
		}
		if _, ok, _ := r.st.Chat(Name, daveJID.String()); ok {
			t.Errorf("a chat was stored for a number not on WhatsApp")
		}
	})
}

func TestARepeatedNewChatIsTheSameSend(t *testing.T) {
	r, c := newChatRig(t)
	id, chat := r.sendTo(phoneTo(carolJID), "invented hello", "client-invented-1")
	id2, chat2 := r.sendTo(phoneTo(carolJID), "invented hello", "client-invented-1")
	if id2 != id || chat2 != chat {
		t.Fatalf("the repeat answered %q %q, want %q %q", id2, chat2, id, chat)
	}
	r.waitState(id, network.SendSent)
	// Again while offline.
	c.dispatch(&events.Disconnected{})
	eventually(t, "offline", func() bool { return r.w.Status().State == network.Offline })
	id3, chat3 := r.sendTo(phoneTo(carolJID), "invented hello", "client-invented-1")
	if id3 != id || chat3 != chat {
		t.Fatalf("the repeat while offline answered %q %q, want %q %q", id3, chat3, id, chat)
	}
	if n := len(c.checksMade()); n != 1 {
		t.Errorf("IsOnWhatsApp was asked %d times, want once", n)
	}
	if left := r.w.newChats.Left(); left != network.WhatsAppNewChatsPerDay-1 {
		t.Errorf("left %d, want one new chat counted", left)
	}
	if sent := c.sentMessages(); len(sent) != 1 {
		t.Errorf("%d messages sent, want 1", len(sent))
	}

	// A number not on WhatsApp, asked again under the same key, is the same
	// failed send: no second check, nothing counted.
	c.dispatch(&events.Connected{})
	eventually(t, "connected", func() bool { return r.w.writeGate() == nil })
	no, _ := r.sendTo(phoneTo(daveJID), "invented hello", "client-invented-2")
	no2, chat4 := r.sendTo(phoneTo(daveJID), "invented hello", "client-invented-2")
	if no2 != no || chat4 != "" {
		t.Fatalf("the repeat answered %q %q, want %q and no chat", no2, chat4, no)
	}
	if n := len(c.checksMade()); n != 2 {
		t.Errorf("IsOnWhatsApp was asked %d times, want twice (once each number)", n)
	}
	if left := r.w.newChats.Left(); left != network.WhatsAppNewChatsPerDay-2 {
		t.Errorf("left %d, want two new chats counted", left)
	}
}

func TestNewChatPastTheCapAsksNothing(t *testing.T) {
	r, trip := tripRig(t)
	for range network.WhatsAppNewChatsPerDay {
		if err := r.w.newChats.Take(); err != nil {
			t.Fatal(err)
		}
	}
	trip.arm(true)
	defer trip.arm(false)
	_, _, err := r.w.SendTo(context.Background(), network.Outgoing{To: phoneTo(carolJID), Text: "invented hello"})
	if wire.KindOf(err) != wire.DailyCap {
		t.Fatalf("past the cap SendTo = %v, want dailyCap", err)
	}
	if calls := trip.calls(); len(calls) != 0 {
		t.Errorf("past the cap the client was called: %v", calls)
	}
}

func TestNewChatDuringAHoldAsksNothing(t *testing.T) {
	r, trip := tripRig(t)
	r.w.newChats.Hold(wire.Refused)
	trip.arm(true)
	defer trip.arm(false)
	_, _, err := r.w.SendTo(context.Background(), network.Outgoing{To: phoneTo(carolJID), Text: "invented hello"})
	if wire.KindOf(err) != wire.NewChatHold {
		t.Fatalf("during a hold SendTo = %v, want newChatHold", err)
	}
	if calls := trip.calls(); len(calls) != 0 {
		t.Errorf("during a hold the client was called: %v", calls)
	}
}

func TestAServerRefusalOfANewChatHoldsNewChats(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		r, c := newChatRig(t)
		c.with(func() { c.sendErr = whatsmeow.ErrServerReturnedError })
		id, _ := r.sendTo(phoneTo(carolJID), "invented hello", "")
		if st := r.waitState(id, network.SendFailed); st.Kind != wire.Refused {
			t.Fatalf("the send is %+v, want failed refused", st)
		}
		if err := r.w.newChats.Take(); wire.KindOf(err) != wire.NewChatHold {
			t.Errorf("after the refusal Take = %v, want newChatHold", err)
		}
		c.with(func() { c.sendErr = nil })
		c.dispatch(&events.Connected{}) // a reconnect sends nothing again
		time.Sleep(20 * time.Millisecond)
		if n := len(c.sentMessages()); n != 1 {
			t.Errorf("%d tries of the refused new chat, want 1", n)
		}
	})
	t.Run("not on WhatsApp", func(t *testing.T) {
		r, c := newChatRig(t)
		c.with(func() { c.sendErr = whatsmeow.ErrRecipientNotOnWhatsApp })
		id, _ := r.sendTo(phoneTo(carolJID), "invented hello", "")
		if st := r.waitState(id, network.SendFailed); st.Kind != wire.NotOnNetwork {
			t.Fatalf("the send is %+v, want failed notOnNetwork", st)
		}
		if err := r.w.newChats.Take(); err != nil {
			t.Errorf("after not on WhatsApp Take = %v, want no hold", err)
		}
	})
}

func TestToAndChatIDTogetherIsABadRequest(t *testing.T) {
	r, _ := newChatRig(t)
	r.send(textFrom(aliceJID, aliceJID, "3EB0C0000000000000021", base+1000, "invented hello"))
	for _, m := range []network.Outgoing{
		{To: phoneTo(aliceJID), ChatID: aliceJID.String(), Text: "x"},
		{To: &network.Recipient{}, Text: "x"},
		{To: &network.Recipient{Phone: "+10000000001", UserID: aliceJID.String()}, Text: "x"},
		{To: &network.Recipient{Phone: "10000000001"}, Text: "x"},
		{To: &network.Recipient{Phone: "+1000-0000001"}, Text: "x"},
	} {
		if _, _, err := r.w.SendTo(context.Background(), m); wire.KindOf(err) != wire.BadRequest {
			t.Errorf("SendTo(%+v) = %v, want badRequest", m.To, err)
		}
	}
}

// sevenDigits is any run of seven digits: a number, or most of one.
var sevenDigits = regexp.MustCompile(`[0-9]{7}`)

func TestNewChatLogsNoNumber(t *testing.T) {
	r, c := newChatRig(t)
	if err := r.w.device().Contacts.PutContactName(context.Background(), bobJID, "Bob", "Bob Example"); err != nil {
		t.Fatal(err)
	}
	id, _ := r.sendTo(phoneTo(bobJID), "invented hello", "client-invented-9")
	r.waitState(id, network.SendSent)
	carol, _ := r.sendTo(phoneTo(carolJID), "invented hello", "")
	r.waitState(carol, network.SendSent)
	dave, _ := r.sendTo(phoneTo(daveJID), "invented hello", "")
	r.waitState(dave, network.SendFailed)
	c.with(func() { c.sendErr = whatsmeow.ErrServerReturnedError })
	_, _, _ = r.w.SendTo(context.Background(), network.Outgoing{To: &network.Recipient{Phone: "+10000000005"}, Text: "x"})
	_, _, _ = r.w.ChatWith(context.Background(), network.Recipient{Phone: "+10000000006"})

	for _, ev := range r.log.All() {
		if ev["event"] == "state" {
			continue // carries his own chat's ID, as it always has
		}
		b, _ := json.Marshal(ev)
		if sevenDigits.Match(b) {
			t.Errorf("an event holds a number: %s", b)
		}
	}
	b, err := os.ReadFile(filepath.Join(r.dir, Name, diag.FileName))
	if err == nil && sevenDigits.Match(b) {
		t.Errorf("the diagnostics file holds a number")
	}
	for _, kind := range []wire.Kind{wire.DailyCap, wire.NewChatHold} {
		if sevenDigits.MatchString((&wire.Error{Kind: kind}).Error()) {
			t.Errorf("%s's text holds a number", kind)
		}
	}
}
