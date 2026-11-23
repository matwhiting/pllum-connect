package linkedin

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A chat LinkedIn keeps failing (a 5xx, an answer the helper can't read, a
// timeout) is backed off on its own: the wait before it is asked again
// doubles from passingWait each time, up to 4 hours, and its next success
// starts it over. Other chats keep their pace, and a pause still pauses
// everything.

// chatCalls are the times a conversation's messages were asked.
func chatCalls(r *rig, conv string) []time.Time {
	var out []time.Time
	for _, c := range r.f.callsNow() {
		if (c.name == "GetMessagesBefore" || c.name == "GetMessagesWithPrevCursor") && strings.Fields(c.arg)[0] == conv {
			out = append(out, c.at)
		}
	}
	return out
}

// runFor moves the clock on by d, a minute at a time, letting the
// background work settle after each.
func runFor(r *rig, d time.Duration) {
	r.t.Helper()
	until := r.clock.Now().Add(d)
	for r.clock.Now().Before(until) {
		r.clock.Advance(min(time.Minute, until.Sub(r.clock.Now())))
		r.settled()
	}
}

// settled waits for the background work to settle after the clock moved:
// a timer's wake still in the channel is work not yet begun.
func (r *rig) settled() {
	r.t.Helper()
	r.idle()
	for i := 0; i < 200 && len(r.l.wake) > 0; i++ {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Millisecond)
	r.idle()
}

// gapsOf are the times between consecutive calls.
func gapsOf(at []time.Time) []time.Duration {
	var out []time.Duration
	for i := 1; i < len(at); i++ {
		out = append(out, at[i].Sub(at[i-1]))
	}
	return out
}

// checkGaps fails unless each gap is at least its wait and not over it by
// more than slack (the clock's minute steps and the pace).
func checkGaps(t *testing.T, gaps, want []time.Duration) {
	t.Helper()
	if len(gaps) < len(want) {
		t.Fatalf("the chat was asked %d times after its first failure, want at least %d (gaps %v)", len(gaps), len(want), gaps)
	}
	const slack = time.Minute + 5*time.Second
	for i, w := range want {
		if gaps[i] < w || gaps[i] > w+slack {
			t.Errorf("gap %d was %v, want %v (gaps %v)", i+1, gaps[i], w, gaps[:len(want)])
		}
	}
}

func TestAChatLinkedInKeepsFailingIsAskedLessAndLess(t *testing.T) {
	for name, perr := range map[string]error{"503": statusErr(503), "unreadable": passingErrors()["unreadable"], "timeout": passingErrors()["timeout"]} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
				return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Minute))})
			}
			r.f.convErr = func(_, conv string) error {
				if conv == convURN(1) {
					return perr
				}
				return nil
			}
			r.live()
			runFor(r, 17*time.Hour)
			min := time.Minute
			checkGaps(t, gapsOf(chatCalls(r, convURN(1))),
				[]time.Duration{3 * min, 6 * min, 12 * min, 24 * min, 48 * min, 96 * min, 192 * min, 240 * min, 240 * min})
			if st, kind := r.l.stateNow(); st != network.Live || kind != "" {
				t.Errorf("state = %s/%s, want live: a failing chat is no pause", st, kind)
			}
		})
	}
}

func TestASuccessStartsAChatsBackOffOver(t *testing.T) {
	r := newRig(t)
	alice := person(aliceID, "Alice", "Invented")
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Minute))})
	}
	r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("hist-invented-1", text(1, 7001, alice, start, "invented newest"))
	}
	var mu sync.Mutex
	failing := map[string]bool{"GetMessagesBefore": true, "GetMessagesWithPrevCursor": true}
	r.f.convErr = func(name, conv string) error {
		mu.Lock()
		defer mu.Unlock()
		if failing[name] {
			return statusErr(502)
		}
		return nil
	}
	r.live()
	// Four failures (gaps 3, 6, 12 minutes), then its newest page comes.
	runFor(r, 22*time.Minute)
	if n := len(chatCalls(r, convURN(1))); n != 4 {
		t.Fatalf("the chat was asked %d times in its first 22 minutes, want 4", n)
	}
	mu.Lock()
	failing["GetMessagesBefore"] = false
	mu.Unlock()
	runFor(r, 25*time.Minute)
	if got := r.lines(convURN(1)); len(got) != 1 {
		t.Fatalf("lines = %+v after the chat's fifth ask, want its newest page", got)
	}
	// Its history page fails at once after: the wait is passingWait again,
	// then doubles from there, not from where it was.
	runFor(r, 15*time.Minute)
	calls := chatCalls(r, convURN(1))
	if len(calls) < 8 {
		t.Fatalf("the chat was asked %d times, want 8 by now", len(calls))
	}
	gaps := gapsOf(calls)
	if gaps[4] > 5*time.Second {
		t.Errorf("its history was asked %v after its newest page, want at the pace", gaps[4])
	}
	checkGaps(t, gaps[5:], []time.Duration{passingWait, 2 * passingWait})
}

func TestOtherChatsKeepTheirPaceWhileOneBacksOff(t *testing.T) {
	r := newRig(t)
	alice := person(aliceID, "Alice", "Invented")
	const pages = 150
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(2*time.Minute)), conv(2, start.Add(time.Minute))})
	}
	page := func(i int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		prev := ""
		if i < pages {
			prev = fmt.Sprintf("hist-invented-%d", i+1)
		}
		return messagePage(prev, text(2, 8000+i, alice, start.Add(-time.Duration(i)*time.Hour), fmt.Sprintf("invented %d", i)))
	}
	r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return page(0)
	}
	r.f.prev = func(_, cursor string, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		var i int
		fmt.Sscanf(cursor, "hist-invented-%d", &i)
		return page(i)
	}
	r.f.convErr = func(_, conv string) error {
		if conv == convURN(1) {
			return statusErr(500)
		}
		return nil
	}
	r.live()
	runFor(r, time.Hour)
	if !r.chat(convURN(2)).HistoryComplete {
		t.Fatalf("chat 2's history did not reach its start while chat 1 backed off")
	}
	two := chatCalls(r, convURN(2))
	if len(two) != pages+1 {
		t.Errorf("chat 2 was asked %d times, want %d", len(two), pages+1)
	}
	// Chat 2 is never held by chat 1's growing waits: past the wait any
	// failure makes (passingWait), it goes on at the pace.
	for i, g := range gapsOf(two) {
		if g > passingWait+time.Minute+5*time.Second {
			t.Errorf("chat 2's request %d went %v after the one before, held by chat 1's back-off", i+2, g)
		}
	}
	checkGaps(t, gapsOf(chatCalls(r, convURN(1))), []time.Duration{3 * time.Minute, 6 * time.Minute, 12 * time.Minute})
}

func TestAPauseStillPausesEverythingWhileAChatBacksOff(t *testing.T) {
	r := newRig(t)
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Minute))})
	}
	var mu sync.Mutex
	failWith := statusErr(503)
	r.f.convErr = func(_, conv string) error {
		mu.Lock()
		defer mu.Unlock()
		if conv == convURN(1) {
			return failWith
		}
		return nil
	}
	r.live()
	runFor(r, 10*time.Minute) // asked at 0, 3 and 9 minutes
	if n := len(chatCalls(r, convURN(1))); n != 3 {
		t.Fatalf("the chat was asked %d times in 10 minutes, want 3", n)
	}
	mu.Lock()
	failWith = statusErr(429)
	mu.Unlock()
	runFor(r, 12*time.Minute) // its fourth ask, at 21 minutes, is pushed back
	r.waitState(network.Offline, wire.RateLimited)
	until, blocked := r.l.pause.blocked()
	if !blocked || until.Sub(r.clock.Now()) < 13*time.Minute {
		t.Fatalf("the pause holds until %v (blocked %v), want 15 minutes from the 429", until, blocked)
	}
	n := len(r.f.callsNow())
	runFor(r, until.Sub(r.clock.Now())-time.Second)
	if got := len(r.f.callsNow()); got != n {
		t.Errorf("%d requests during the pause, want none", got-n)
	}
	if st, kind := r.l.stateNow(); st != network.Offline || kind != wire.RateLimited {
		t.Errorf("state = %s/%s during the pause, want offline/rateLimited", st, kind)
	}
}
