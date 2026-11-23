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

// unreadable delivers a message from sender in Alice's chat that couldn't
// be read (or, unavailable, that Meta sent no copy of for this device).
func unreadable(t *testing.T, h *harness, sender int64, id string, at time.Time, unavailable bool) {
	t.Helper()
	info := watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(sender),
		IsFromMe: sender == selfFBID}, ID: id, Timestamp: at}
	h.deliver(t, &events.UndecryptableMessage{Info: info, IsUnavailable: unavailable})
}

// settled fails unless chat holds id as a hidden line of kind.
func settled(t *testing.T, h *harness, chat, id, kind string) {
	t.Helper()
	l, ok := h.line(t, chat, id)
	if !ok || l.Kind != kind || l.Hidden == nil || !*l.Hidden {
		t.Errorf("the %s line %s reads %+v (ok %v), want it kept, hidden", kind, id, l, ok)
	}
}

// fillingNow reports whether chat's newest page is due or being read.
func (h *harness) fillingNow(chat string) bool {
	h.m.fillmu.Lock()
	defer h.m.fillmu.Unlock()
	return h.m.filling[chat]
}

// storeAlicePage stores a page of Alice's backup as a read of it would.
func storeAlicePage(t *testing.T, h *harness, msgs ...*securehistory.DecryptedMessage) {
	t.Helper()
	if _, ok := h.m.storeArchive(fmt.Sprint(aliceFBID)+"@msgr", fmt.Sprint(aliceFBID), false, selfFBID, msgs); !ok {
		t.Fatalf("the page was not stored")
	}
}

// A reaction Pllum couldn't read never comes back as a line of its own:
// its chat's newest page brings it as a supplemental of the line it reacts
// to. The reaction shows on that line, and the "couldn't read" line under
// the reaction's own ID is settled: kept hidden, no longer the chat's
// newest line or unread.
func TestAnUnreadableReactionIsSettledByTheBackup(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	restored(t, h)
	at := h.f.clock.Now()
	before := h.chat(t, alice).Unread
	react := backup("WAINVENTED90-react", aliceFBID, at, reactionContent("WAINVENTED90", "❤"))
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{more: true, before: at.Add(-time.Hour).UnixMilli(),
		msgs: []*securehistory.DecryptedMessage{archived("WAINVENTED90", selfFBID, at.Add(-time.Minute), textContent(canary+" mine"), react)}})
	asked := len(h.f.history.asked(fmt.Sprint(aliceFBID)))

	unreadable(t, h, aliceFBID, "WAINVENTED90-react", at, false)
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED90-react" || c.Unread != before+1 {
		t.Fatalf("before the backup, the chat's newest is %q, unread %d (was %d)", c.LastMessageID, c.Unread, before)
	}
	h.f.clock.Advance(fillDelay)
	eventually(t, "the chat's newest page to be read", func() bool {
		return len(h.f.history.asked(fmt.Sprint(aliceFBID))) > asked && !h.fillingNow(alice)
	})

	mine, _ := h.line(t, alice, "WAINVENTED90")
	if len(mine.Reactions) != 1 || mine.Reactions[0].Emoji != "❤" || mine.Reactions[0].SenderID != fmt.Sprint(aliceFBID) {
		t.Errorf("the reaction on his line reads %+v", mine.Reactions)
	}
	settled(t, h, alice, "WAINVENTED90-react", "undecryptable")
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED90" || c.Unread != before {
		t.Errorf("after the backup, the chat's newest is %q, unread %d: want his line, unread %d", c.LastMessageID, c.Unread, before)
	}
}

// The same for an edit: the edited line shows its edit, and the line under
// the edit's own ID is settled.
func TestAnUnreadableEditIsSettledByTheBackup(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	t0 := time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)
	unreadable(t, h, aliceFBID, "WAINVENTED91-edit", t0.Add(time.Minute), false)

	edit := backup("WAINVENTED91-edit", aliceFBID, t0.Add(time.Minute), editContent("WAINVENTED91", canary+" edited"))
	storeAlicePage(t, h, archived("WAINVENTED91", aliceFBID, t0, textContent(canary+" first"), edit))

	if l, _ := h.line(t, alice, "WAINVENTED91"); l.Text != canary+" edited" || !l.Edited {
		t.Errorf("the edited line reads %+v", l)
	}
	settled(t, h, alice, "WAINVENTED91-edit", "undecryptable")
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED91" {
		t.Errorf("the chat's newest is %q, want the edited line", c.LastMessageID)
	}
}

// A supplemental passed over for a later one (a reaction its sender
// changed), and a reaction taken back (no emoji), still settle their own
// lines.
func TestSupersededAndRemovedReactionsAreSettled(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	t0 := time.Date(2026, 3, 4, 9, 0, 0, 0, time.UTC)
	unreadable(t, h, aliceFBID, "WAINVENTED94-r1", t0.Add(time.Minute), false)
	unreadable(t, h, selfFBID, "WAINVENTED94-r3", t0.Add(3*time.Minute), true)

	storeAlicePage(t, h, archived("WAINVENTED94", aliceFBID, t0, textContent(canary+" hers"),
		backup("WAINVENTED94-r1", aliceFBID, t0.Add(time.Minute), reactionContent("WAINVENTED94", "❤")),
		backup("WAINVENTED94-r2", aliceFBID, t0.Add(2*time.Minute), reactionContent("WAINVENTED94", "👍")),
		backup("WAINVENTED94-r3", selfFBID, t0.Add(3*time.Minute), reactionContent("WAINVENTED94", ""))))

	if l, _ := h.line(t, alice, "WAINVENTED94"); len(l.Reactions) != 1 || l.Reactions[0].Emoji != "👍" {
		t.Errorf("the reactions on her line read %+v, want her latest alone", l.Reactions)
	}
	settled(t, h, alice, "WAINVENTED94-r1", "undecryptable")
	settled(t, h, alice, "WAINVENTED94-r3", "unavailable")
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED94" {
		t.Errorf("the chat's newest is %q, want her line", c.LastMessageID)
	}
}

// A real line whose ID a supplemental also names is never hidden.
func TestAReadableLineIsNeverSettled(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	alice := fmt.Sprint(aliceFBID) + "@msgr"
	t0 := time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)
	h.deliver(t, fbMessage(oneToOne(aliceFBID), person(aliceFBID), "WAINVENTED93-react", t0.Add(time.Minute), textContent(canary+" real"), ""))

	storeAlicePage(t, h, archived("WAINVENTED93", selfFBID, t0, textContent(canary+" mine"),
		backup("WAINVENTED93-react", aliceFBID, t0.Add(time.Minute), reactionContent("WAINVENTED93", "❤"))))

	l, ok := h.line(t, alice, "WAINVENTED93-react")
	if !ok || l.Kind != "text" || l.Text != canary+" real" || l.Hidden == nil || *l.Hidden {
		t.Errorf("the real line reads %+v (ok %v), want it shown as it was", l, ok)
	}
	if c := h.chat(t, alice); c.LastMessageID != "WAINVENTED93-react" {
		t.Errorf("the chat's newest is %q, want the real line", c.LastMessageID)
	}
}
