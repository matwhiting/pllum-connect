package messenger

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// restored signs in, restores his history from a backup with nothing in it
// for anyone, and waits for the first restore's read to end.
func restored(t *testing.T, h *harness) {
	t.Helper()
	h.signIn(t)
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.waitValue(t, "ebackfilled")
	h.waitValue(t, "ecatchup")
	h.settle()
}

// waitLine waits for a chat's line to read as want, or fails.
func waitLine(t *testing.T, h *harness, chat, id, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l, ok := h.line(t, chat, id); ok && l.Text == want && l.Kind == "text" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	l, ok := h.line(t, chat, id)
	t.Fatalf("line %s reads %+v (ok %v), want %q", id, l, ok, want)
}

// A message Pllum couldn't read is filled from his backup two minutes on:
// the newest page of the chat's backup is read, and the message takes its
// line over. Nothing is asked before the two minutes, and the older pages'
// cursor doesn't move.
func TestAnUnreadableMessageIsFilledFromTheBackup(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	restored(t, h)
	at := h.f.clock.Now()
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{more: true, before: at.Add(-time.Hour).UnixMilli(),
		msgs: []*securehistory.DecryptedMessage{archived("WAINVENTED80", aliceFBID, at, textContent(canary+" filled"))}})
	asked := len(h.f.history.asked(fmt.Sprint(aliceFBID)))

	info := watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(aliceFBID)}, ID: "WAINVENTED80", Timestamp: at}
	h.deliver(t, &events.UndecryptableMessage{Info: info})
	h.f.clock.Advance(time.Minute)
	h.settle()
	if n := len(h.f.history.asked(fmt.Sprint(aliceFBID))); n != asked {
		t.Fatalf("the backup was asked %d times within the two minutes, want none", n-asked)
	}
	h.f.clock.Advance(time.Minute)
	waitLine(t, h, alice, "WAINVENTED80", canary+" filled")
	refs := h.f.history.asked(fmt.Sprint(aliceFBID))
	if len(refs) != asked+1 || refs[len(refs)-1] < at.UnixMilli() {
		t.Errorf("the backup was asked from %v, want one newest page (after %d)", refs[asked:], at.UnixMilli())
	}
	if v := h.m.value("ecursor." + alice); v != "" {
		t.Errorf("the older pages' cursor moved to %q", v)
	}
}

// Two unreadable messages in one chat within the two minutes read the
// backup once.
func TestUnreadableMessagesInOneChatFillOnce(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	restored(t, h)
	at := h.f.clock.Now()
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{more: true, before: at.Add(-time.Hour).UnixMilli()})
	asked := len(h.f.history.asked(fmt.Sprint(aliceFBID)))
	for _, id := range []string{"WAINVENTED82", "WAINVENTED83"} {
		info := watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(aliceFBID)}, ID: id, Timestamp: at}
		h.deliver(t, &events.UndecryptableMessage{Info: info, IsUnavailable: true})
	}
	h.f.clock.Advance(2 * time.Minute)
	h.settle()
	time.Sleep(200 * time.Millisecond)
	if n := len(h.f.history.asked(fmt.Sprint(aliceFBID))) - asked; n != 1 {
		t.Errorf("the backup was asked %d times for the chat, want 1", n)
	}
}

// Once a day, the newest page of each encrypted chat active in the last
// three days is read: a message that never came live lands. The first
// restore's read is that day's (it read each chat's newest page), and a
// second run the same day asks nothing.
func TestADailyCatchUpReadsRecentChatsNewestPage(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	restored(t, h)
	gen, cli, _ := h.m.running()
	h.m.mu.Lock()
	fbid := h.m.fbid
	h.m.mu.Unlock()

	before := h.f.history.total()
	h.m.catchUp(context.Background(), gen, cli, fbid)
	if n := h.f.history.total() - before; n != 0 {
		t.Errorf("a catch-up the day of the first restore asked %d pages", n)
	}

	h.f.clock.Advance(25 * time.Hour)
	at := h.f.clock.Now()
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED84", at, textContent("x"), ""))
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{more: true, before: at.Add(-time.Hour).UnixMilli(),
		msgs: []*securehistory.DecryptedMessage{archived("WAINVENTED85", aliceFBID, at.Add(time.Second), textContent(canary+" never came live"))}})
	h.m.catchUp(context.Background(), gen, cli, fbid)
	waitLine(t, h, alice, "WAINVENTED85", canary+" never came live")

	before = h.f.history.total()
	h.m.catchUp(context.Background(), gen, cli, fbid)
	if n := h.f.history.total() - before; n != 0 {
		t.Errorf("a second catch-up the same day asked %d pages", n)
	}
}
