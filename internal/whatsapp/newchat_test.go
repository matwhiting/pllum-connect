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

// newChatAnswers checks the chat IDs a new chat's send and a repeat of it
// under the same key answered: the send answers none (its chat is stored
// only once WhatsApp accepts the message), and the repeat answers none, or
// the chat if WhatsApp accepted the message between the two. Never a chat
// then none, and never another chat.
func newChatAnswers(t *testing.T, want, first, repeat string) {
	t.Helper()
	if first != "" {
		t.Fatalf("the new chat's send answered chat %q, want none until WhatsApp accepts it", first)
	}
	if repeat != "" && repeat != want {
		t.Fatalf("the repeat answered chat %q, want none or %q", repeat, want)
	}
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
	if chat != "" {
		t.Fatalf("a new chat answered chat %q before its message was accepted", chat)
	}
	r.waitState(id, network.SendSent)
	if got, ok, _ := r.w.ChatWith(context.Background(), *phoneTo(bobJID)); !ok || got != bobJID.String() {
		t.Errorf("after the send ChatWith = %q, %v; want Bob's number", got, ok)
	}
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
		if chat != "" {
			t.Fatalf("a new chat answered chat %q before its message was accepted", chat)
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
	if id2 != id {
		t.Fatalf("the repeat answered %q, want %q", id2, id)
	}
	newChatAnswers(t, carolJID.String(), chat, chat2)
	r.waitState(id, network.SendSent)
	// Again while offline.
	c.dispatch(&events.Disconnected{})
	eventually(t, "offline", func() bool { return r.w.Status().State == network.Offline })
	id3, chat3 := r.sendTo(phoneTo(carolJID), "invented hello", "client-invented-1")
	if id3 != id || chat3 != carolJID.String() {
		t.Fatalf("the repeat while offline answered %q %q, want %q and the chat made since", id3, chat3, id)
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
		{To: &network.Recipient{Phone: "+0100000001"}, Text: "x"},
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

func TestTwoSendsUnderOneKeyAtOnceAreOneNewChat(t *testing.T) {
	r, c := newChatRig(t)
	block := make(chan struct{})
	c.with(func() { c.checkBlock = block })
	type answer struct {
		id, chat string
		err      error
	}
	answers := make(chan answer, 2)
	send := func() {
		id, chat, err := r.w.SendTo(context.Background(), network.Outgoing{To: phoneTo(carolJID), Text: "invented hello", ClientID: "client-invented-3"})
		answers <- answer{id, chat, err}
	}
	go send()
	eventually(t, "the first check", func() bool { return len(c.checksMade()) == 1 })
	go send()
	time.Sleep(50 * time.Millisecond) // the second call, if it were not held, would check now
	c.with(func() { c.checkBlock = nil })
	close(block)
	a, b := <-answers, <-answers
	if a.err != nil || b.err != nil {
		t.Fatalf("the two sends answered %v and %v", a.err, b.err)
	}
	if a.id != b.id {
		t.Fatalf("the two sends answered %q and %q, want one send", a.id, b.id)
	}
	// The answers arrive in either order: the first call's is the one with
	// no chat.
	if a.chat != "" {
		a, b = b, a
	}
	newChatAnswers(t, carolJID.String(), a.chat, b.chat)
	r.waitState(a.id, network.SendSent)
	if n := len(c.checksMade()); n != 1 {
		t.Errorf("IsOnWhatsApp was asked %d times, want once", n)
	}
	if left := r.w.newChats.Left(); left != network.WhatsAppNewChatsPerDay-1 {
		t.Errorf("left %d, want one new chat counted", left)
	}
	if sent := c.sentMessages(); len(sent) != 1 {
		t.Errorf("%d messages sent, want 1", len(sent))
	}
}

func TestARefusedLookupHoldsNewChats(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want wire.Kind
	}{
		{"rate-overlimit", whatsmeow.ErrIQRateOverLimit, wire.RateLimited},
		{"another IQ error", &whatsmeow.IQError{Code: 403, Text: "forbidden"}, wire.Refused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c := newChatRig(t)
			c.with(func() { c.checkErr = tc.err })
			_, _, err := r.w.SendTo(context.Background(), network.Outgoing{To: phoneTo(carolJID), Text: "invented hello"})
			if wire.KindOf(err) != tc.want {
				t.Fatalf("SendTo = %v, want %s", err, tc.want)
			}
			c.with(func() { c.checkErr = nil })
			for _, d := range []time.Duration{0, 23 * time.Hour} {
				r.clock.Advance(d)
				_, _, err := r.w.SendTo(context.Background(), network.Outgoing{To: phoneTo(daveJID), Text: "invented hello"})
				if wire.KindOf(err) != wire.NewChatHold {
					t.Fatalf("%v after the refused lookup SendTo = %v, want newChatHold", d, err)
				}
			}
			if n := len(c.checksMade()); n != 1 {
				t.Errorf("IsOnWhatsApp was asked %d times, want once", n)
			}
			if sent := c.sentMessages(); len(sent) != 0 {
				t.Errorf("sent %v", sent)
			}
		})
	}
	t.Run("unclear", func(t *testing.T) {
		r, c := newChatRig(t)
		c.with(func() { c.checkErr = whatsmeow.ErrIQTimedOut })
		_, _, err := r.w.SendTo(context.Background(), network.Outgoing{To: phoneTo(carolJID), Text: "invented hello"})
		if wire.KindOf(err) != wire.Unclear {
			t.Fatalf("SendTo = %v, want unclear", err)
		}
		if err := r.w.newChats.Take(); err != nil {
			t.Errorf("after an unclear lookup Take = %v, want no hold", err)
		}
	})
}

func TestANewChatIsStoredOnlyOnceItsMessageIsAccepted(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		r, c := newChatRig(t)
		block := make(chan struct{})
		c.with(func() { c.sendBlock = block })
		id, _ := r.sendTo(phoneTo(carolJID), "invented hello", "")
		eventually(t, "the send to go", func() bool { return len(c.sentMessages()) == 1 })
		if _, ok, _ := r.st.Chat(Name, carolJID.String()); ok {
			t.Errorf("the chat was stored before WhatsApp accepted its message")
		}
		c.with(func() { c.sendBlock = nil })
		close(block)
		r.waitState(id, network.SendSent)
		chat, ok, _ := r.st.Chat(Name, carolJID.String())
		if !ok {
			t.Fatalf("the chat was not stored once its message was accepted")
		}
		if chat.Kind != "single" || chat.Phone != "+"+carolJID.User {
			t.Errorf("the new chat is %q with phone %q, want a one-to-one with her number", chat.Kind, chat.Phone)
		}
	})
	t.Run("refused", func(t *testing.T) {
		r, c := newChatRig(t)
		c.with(func() { c.sendErr = whatsmeow.ErrServerReturnedError })
		id, _ := r.sendTo(phoneTo(carolJID), "invented hello", "client-invented-4")
		r.waitState(id, network.SendFailed)
		if _, ok, _ := r.st.Chat(Name, carolJID.String()); ok {
			t.Errorf("a refused first message left a chat")
		}
		for _, chat := range r.chatIDs() {
			if chat == carolJID.String() {
				t.Errorf("the chat list holds the refused new chat")
			}
		}
		// The failed send is still the key's answer: nothing asked again.
		id2, chat2 := r.sendTo(phoneTo(carolJID), "invented hello", "client-invented-4")
		if id2 != id || chat2 != "" {
			t.Errorf("the repeat answered %q %q, want %q and no chat", id2, chat2, id)
		}
		if n := len(c.checksMade()); n != 1 {
			t.Errorf("IsOnWhatsApp was asked %d times, want once", n)
		}
	})
}

func TestWhatsAppsFormOfANumberIsFoundNotCheckedAgain(t *testing.T) {
	r, c := newChatRig(t)
	typed := types.NewJID("10000000008", types.DefaultUserServer) // WhatsApp knows it as carol's
	c.with(func() { c.canonical = map[string]string{typed.User: carolJID.User} })
	id, _ := r.sendTo(phoneTo(typed), "invented hello", "")
	r.waitState(id, network.SendSent)
	if sent := c.sentMessages(); len(sent) != 1 || sent[0].to != carolJID {
		t.Fatalf("sent %v, want one message to WhatsApp's form", sent)
	}
	if chat, ok, _ := r.w.ChatWith(context.Background(), *phoneTo(typed)); !ok || chat != carolJID.String() {
		t.Errorf("ChatWith the typed number = %q, %v; want carol's chat", chat, ok)
	}
	id2, chat2 := r.sendTo(phoneTo(typed), "invented again", "")
	if chat2 != carolJID.String() {
		t.Errorf("the second send answered chat %q, want carol's", chat2)
	}
	r.waitState(id2, network.SendSent)
	if n := len(c.checksMade()); n != 1 {
		t.Errorf("IsOnWhatsApp was asked %d times, want once", n)
	}
	if left := r.w.newChats.Left(); left != network.WhatsAppNewChatsPerDay-1 {
		t.Errorf("left %d, want one new chat counted", left)
	}
}

func TestABanOrALogOutHoldsNewChats(t *testing.T) {
	for _, ev := range []any{
		&events.TemporaryBan{Code: events.TempBanSentToTooManyPeople, Expire: time.Hour},
		&events.LoggedOut{Reason: events.ConnectFailureLoggedOut},
	} {
		r, c := newChatRig(t)
		c.dispatch(ev)
		eventually(t, "the session to end", func() bool { return r.w.newChats.Left() == 0 })
		if err := r.w.newChats.Take(); wire.KindOf(err) != wire.NewChatHold {
			t.Errorf("after %T Take = %v, want newChatHold", ev, err)
		}
	}
}
