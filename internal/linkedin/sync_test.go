package linkedin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// mediaKind is the kind a media fetch of url ends with.
func (l *LinkedIn) mediaKind(ctx context.Context, c liClient, url string) wire.Kind {
	resp, err := l.download(ctx, c, url)
	if resp != nil {
		resp.Body.Close()
	}
	return wire.KindOf(err)
}

// removeCookie deletes a cookie from the jar, as LinkedIn deleting it does.
func removeCookie(jar *linkedingo.StringCookieJar, name string) {
	jar.SetCookies(linkedingo.CookieBaseURL, []*http.Cookie{
		{Name: name, Value: "", Domain: ".linkedin.com", Path: "/", MaxAge: -1},
	})
}

func TestCatchUpFollowsTheSyncTokenThenGoesLive(t *testing.T) {
	r := newRig(t)
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-7")
	r.keep("listed", "1")
	r.f.syncPage = func(token string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Minute))})
	}
	r.f.before = func(c string, before time.Time, count int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		if c != convURN(1) {
			return messagePage("")
		}
		return messagePage("", text(1, 1, person(aliceID, "Alice", "Invented"), start.Add(-time.Minute), "invented hello"))
	}
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()

	if got := r.f.last().token; got != "sync-token-invented-new" {
		t.Errorf("the client holds token %q after the catch-up", got)
	}
	var syncArg string
	for _, c := range r.f.callsNow() {
		if c.name == "GetConversationsBySyncToken" {
			syncArg = c.arg
			break
		}
	}
	if syncArg != "sync-token-invented-7" {
		t.Errorf("the catch-up asked with token %q, want the kept one", syncArg)
	}
	if got := r.value("syncToken"); got != "sync-token-invented-new" {
		t.Errorf("kept token = %q, want the new one", got)
	}
	names := r.f.namesSince(0)
	i := slices.Index(names, "GetConversationsBySyncToken")
	k := slices.Index(names, "RealtimeConnect")
	j := slices.Index(names, "GetMessagesBefore")
	if i < 0 || k < i || j < k {
		t.Errorf("calls = %v, want the sync token, realtime, then the chat's messages behind it", names)
	}
	if slices.Contains(names, "GetConversationsWithNextCursor") || slices.Contains(names, "GetConversationsUpdatedBefore") {
		t.Errorf("a catch-up with a token listed the inbox: %v", names)
	}
	states := r.ev.states()
	want := []string{"connecting", "catchingUp", "live"}
	if !slices.Equal(states[:min(3, len(states))], want) {
		t.Errorf("states = %v, want %v", states, want)
	}
	if got := r.lines(convURN(1)); len(got) != 1 || got[0].Text != "invented hello" {
		t.Errorf("lines = %+v", got)
	}
}

func TestACaughtUpChatPagesBackToAStoredLine(t *testing.T) {
	r := newRig(t)
	alice := person(aliceID, "Alice", "Invented")
	// What an earlier run stored: line 1, an hour ago.
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("", text(1, 1, alice, start.Add(-time.Hour), "invented one"))
	}
	c := r.live()
	// Then 25 lines came: a first page of 20, then the page holding line 1.
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Hour))})
	}
	var newer []linkedingo.Message
	for i := 2; i <= 26; i++ {
		newer = append(newer, text(1, i, alice, start.Add(time.Duration(i)*time.Minute), fmt.Sprintf("invented %d", i)))
	}
	r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("prev-invented-1", newer[5:]...)
	}
	r.f.prev = func(_, cursor string, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		if cursor == "prev-invented-1" {
			return messagePage("prev-invented-2", append([]linkedingo.Message{text(1, 1, alice, start.Add(-time.Hour), "invented one")}, newer[:5]...)...)
		}
		t.Errorf("paged past the stored line, to %q", cursor)
		return messagePage("")
	}
	c.dispatch(convDeleteless())
	r.idle()
	if got := r.lines(convURN(1)); len(got) != 26 {
		t.Fatalf("%d lines, want 26", len(got))
	}
}

func TestAFirstConnectListsByCursorBackAYear(t *testing.T) {
	r := newRig(t)
	// Page p (from 0) holds two chats, last active 10p and 10p+9 days back,
	// so page 36 (369 days) is the first whose oldest is over a year old.
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		p := 0
		if cursor != "" {
			p, _ = strconv.Atoi(strings.TrimPrefix(cursor, "next-invented-"))
		}
		var convs []linkedingo.Conversation
		for i, days := range []int{10 * p, 10*p + 9} {
			convs = append(convs, conv(100+2*p+i, start.Add(-time.Duration(days)*24*time.Hour)))
		}
		return cursorPageOf(fmt.Sprintf("next-invented-%d", p+1), convs...)
	}
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	if n := r.f.count("GetConversationsWithNextCursor"); n != 37 {
		t.Errorf("%d cursor pages, want 37 (until a page's oldest is over 365 days)", n)
	}
	if r.f.count("GetConversationsUpdatedBefore") != 0 {
		t.Errorf("GetConversationsUpdatedBefore was asked")
	}
	if r.value("self") != selfID {
		t.Errorf("his profile ID was not kept")
	}
	if r.value("syncToken") == "" {
		t.Errorf("no sync token was kept after the first connect")
	}
	if r.value("listed") == "" {
		t.Errorf("the first listing was not marked done")
	}
	// A second connect does not list again.
	n := r.f.count("GetConversationsWithNextCursor")
	r.restart()
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	if r.f.count("GetConversationsWithNextCursor") != n {
		t.Errorf("a second connect listed the inbox again")
	}
}

func TestAFirstConnectStopsAtFortyPages(t *testing.T) {
	r := newRig(t)
	page := 0
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		page++
		return cursorPageOf(fmt.Sprintf("next-invented-%d", page), conv(1000+page, start.Add(-time.Duration(page)*time.Minute)))
	}
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	if n := r.f.count("GetConversationsWithNextCursor"); n != 40 {
		t.Errorf("%d cursor pages, want at most 40", n)
	}
}

func TestANewRealtimeSessionPagesBackToTheNewestStored(t *testing.T) {
	r := newRig(t)
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	c := r.live()
	c.dispatch(&linkedingo.ClientConnection{}) // the run's first: a catch-up by token
	r.idle()
	if n := r.f.count("GetConversationsWithNextCursor"); n != 0 {
		t.Fatalf("the first connection listed by cursor (%d pages)", n)
	}

	// The stream reconnects under a new session: what came meanwhile is
	// listed by cursor, back to the newest activity stored (an hour ago).
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		switch cursor {
		case "":
			return cursorPageOf("next-invented-1", conv(2, start.Add(30*time.Minute)), conv(3, start.Add(20*time.Minute)))
		case "next-invented-1":
			return cursorPageOf("next-invented-2", conv(4, start.Add(10*time.Minute)), conv(1, start.Add(-time.Hour)))
		}
		t.Errorf("paged past the newest stored activity, to %q", cursor)
		return cursorPageOf("")
	}
	n := len(r.f.callsNow())
	c.dispatch(&linkedingo.ClientConnection{SessID: [16]byte{1, 2, 3}})
	r.idle()
	if got := r.f.count("GetConversationsWithNextCursor"); got != 2 {
		t.Errorf("%d cursor pages, want 2", got)
	}
	for _, n := range []int{2, 3, 4} {
		r.chat(convURN(n))
	}
	// The same session again is a catch-up by token.
	m := r.f.count("GetConversationsWithNextCursor")
	c.dispatch(&linkedingo.ClientConnection{SessID: [16]byte{1, 2, 3}})
	r.idle()
	if r.f.count("GetConversationsWithNextCursor") != m {
		t.Errorf("the same session listed by cursor again")
	}
	if !slices.Contains(r.f.namesSince(n), "GetConversationsBySyncToken") {
		t.Errorf("no catch-up by token")
	}
}

func TestADeletedConversationBecomesReadOnly(t *testing.T) {
	r := newRig(t)
	alice := person(aliceID, "Alice", "Invented")
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start), conv(2, start)})
	}
	r.f.before = func(c string, _ time.Time, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		if c == convURN(1) {
			return messagePage("", text(1, 1, alice, start, "invented kept"))
		}
		return messagePage("", text(2, 2, alice, start, "invented kept too"))
	}
	c := r.live()
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf(nil, convURN(1))
	}
	c.dispatch(convDeleteless())
	r.idle()
	if ch := r.chat(convURN(1)); !ch.ReadOnly {
		t.Errorf("a conversation LinkedIn deleted is not read-only")
	}
	if got := r.lines(convURN(1)); len(got) != 1 {
		t.Errorf("%d lines after the delete, want 1 (never deleted)", len(got))
	}

	// The realtime delete does the same.
	c.dispatch(&linkedingo.DecoratedEvent{
		Topic: topic(linkedingo.RealtimeEventTopicConversationDelete),
		Payload: linkedingo.DecoratedEventPayload{Data: linkedingo.DecoratedEventData{
			DecoratedConversationDelete: &linkedingo.DecoratedConversation{Result: linkedingo.Conversation{EntityURN: linkedingo.NewURN(convURN(2))}},
		}},
	})
	if ch := r.chat(convURN(2)); !ch.ReadOnly {
		t.Errorf("a realtime delete did not make the chat read-only")
	}
	if got := r.lines(convURN(2)); len(got) != 1 {
		t.Errorf("%d lines after the realtime delete, want 1", len(got))
	}
}

// historyFeed answers a chat's history: a line every 10 days back from
// start, 2 lines a page, ending at the line from end days back (the chat's
// start).
func historyFeed(r *rig, endDays int) {
	alice := person(aliceID, "Alice", "Invented")
	at := func(i int) time.Time { return start.Add(-time.Duration(i*10*24) * time.Hour) }
	pageAt := func(i int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		var msgs []linkedingo.Message
		for j := i + 1; j >= i; j-- {
			if j*10 <= endDays {
				msgs = append(msgs, text(1, 5000+j, alice, at(j), fmt.Sprintf("invented %d", j)))
			}
		}
		prev := ""
		if (i+2)*10 <= endDays {
			prev = fmt.Sprintf("hist-invented-%d", i+2)
		}
		return messagePage(prev, msgs...)
	}
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start)})
	}
	r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return pageAt(0)
	}
	r.f.prev = func(_, cursor string, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		i, _ := strconv.Atoi(strings.TrimPrefix(cursor, "hist-invented-"))
		return pageAt(i)
	}
}

func TestHistoryIsPacedARandomTwoToFourSecondsApart(t *testing.T) {
	r := newRig(t)
	historyFeed(r, 400)
	r.live()
	var last time.Time
	paged := 0
	gaps := map[time.Duration]bool{}
	for _, c := range r.f.callsNow() {
		switch c.name {
		case "GetCurrentUserProfile", "GetConversationsBySyncToken", "GetConversationsWithNextCursor", "GetMessagesBefore", "GetMessagesWithPrevCursor":
		default:
			continue
		}
		if c.name == "GetMessagesWithPrevCursor" {
			paged++
		}
		if !last.IsZero() {
			gap := c.at.Sub(last)
			if gap < 2*time.Second || gap > 4*time.Second {
				t.Errorf("%s went %v after the request before it, want 2 to 4 s", c.name, gap)
			}
			gaps[gap] = true
		}
		last = c.at
	}
	if paged < 10 {
		t.Errorf("%d history pages, want the chat paged back", paged)
	}
	// Meta's pace, not a metronome: the gaps differ.
	if len(gaps) < paged/2 {
		t.Errorf("%d different gaps over %d pages, want a random gap each time", len(gaps), paged)
	}
	if r.f.maxIn > 1 {
		t.Errorf("%d requests at once, want one at a time", r.f.maxIn)
	}
	// 400 days of a chat that starts 400 days back: the start was reached.
	ch := r.chat(convURN(1))
	if !ch.HistoryComplete {
		t.Errorf("the chat's start was reached but it is not historyComplete")
	}
	if ch.HistoryFromMs != start.Add(-400*24*time.Hour).UnixMilli() {
		t.Errorf("historyFromMs = %d, want the oldest line's", ch.HistoryFromMs)
	}
}

func TestHistoryStopsAt730DaysAndResumesAfterARestart(t *testing.T) {
	r := newRig(t)
	historyFeed(r, 2000)
	// The history stops after 6 pages: a pause, as LinkedIn pushing back makes.
	pages := 0
	prev := r.f.prev
	r.f.prev = func(c, cursor string, n int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		pages++
		if pages == 6 {
			r.f.setErr("GetMessagesWithPrevCursor", statusErr(429))
		}
		return prev(c, cursor, n)
	}
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-0")
	r.keep("listed", "1")
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Offline, wire.RateLimited)
	r.idle()
	kept := r.value("cursor." + convURN(1))
	if kept == "" {
		t.Fatalf("no history cursor kept")
	}

	// A restart, after the pause: it goes on from the kept cursor.
	r.f.setErr("GetMessagesWithPrevCursor", nil)
	r.restart()
	r.clock.Advance(16 * time.Minute)
	n := len(r.f.callsNow())
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	var first string
	for _, c := range r.f.callsNow()[n:] {
		if c.name == "GetMessagesWithPrevCursor" {
			first = c.arg
			break
		}
	}
	if !strings.HasSuffix(first, " "+kept) {
		t.Errorf("after the restart history asked %q, want the kept cursor %q", first, kept)
	}
	ch := r.chat(convURN(1))
	if ch.HistoryComplete {
		t.Errorf("a chat older than 730 days is historyComplete")
	}
	limit := start.Add(-730 * 24 * time.Hour).UnixMilli()
	if ch.HistoryFromMs > limit {
		t.Errorf("history reaches back to %v, want past 730 days", time.UnixMilli(ch.HistoryFromMs))
	}
	if ch.HistoryFromMs < start.Add(-750*24*time.Hour).UnixMilli() {
		t.Errorf("history went on to %v, past 730 days and a page", time.UnixMilli(ch.HistoryFromMs))
	}
	// Nothing more is asked of that chat.
	m := len(r.f.callsNow())
	r.restart()
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	for _, c := range r.f.callsNow()[m:] {
		if c.name == "GetMessagesWithPrevCursor" {
			t.Errorf("a chat paged to 730 days was paged again: %s", c.arg)
		}
	}
}

func TestATokenLinkedInWontTakeIsDroppedAndTheRunGoesOn(t *testing.T) {
	r := newRig(t)
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-stale")
	r.keep("listed", "1")
	r.f.failOnce("GetConversationsBySyncToken", statusErr(400))
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	if got := r.value("syncToken"); got != "" {
		t.Errorf("the token LinkedIn refused is still kept: %q", got)
	}
	// The next run starts without it.
	r.restart()
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	if got := r.f.last().token; got != "sync-token-invented-new" {
		t.Errorf("the next run's client holds %q after its catch-up", got)
	}
	var asked []string
	for _, c := range r.f.callsNow() {
		if c.name == "GetConversationsBySyncToken" {
			asked = append(asked, c.arg)
		}
	}
	if len(asked) < 2 || asked[len(asked)-1] != "" {
		t.Errorf("tokens asked with = %q, want the last run's without one", asked)
	}
}

func TestAChatLinkedInWontPageIsPassedOver(t *testing.T) {
	r := newRig(t)
	historyFeed(r, 400)
	r.f.setErr("GetMessagesWithPrevCursor", statusErr(404))
	c := r.live()
	if got := r.f.count("GetMessagesWithPrevCursor"); got != 1 {
		t.Fatalf("%d history requests for a chat LinkedIn won't page, want 1", got)
	}
	c.dispatch(convDeleteless())
	r.idle()
	if got := r.f.count("GetMessagesWithPrevCursor"); got != 1 {
		t.Errorf("the chat was asked again: %d requests", got)
	}
	if st, _ := r.l.stateNow(); st != network.Live {
		t.Errorf("state = %s, want live", st)
	}
}

// convsByDay are n one-to-ones, chat i last active i days back (newest
// first), numbered from base.
func convsByDay(base, n int) []linkedingo.Conversation {
	var out []linkedingo.Conversation
	for i := range n {
		out = append(out, conv(base+i, start.Add(-time.Duration(i)*24*time.Hour)))
	}
	return out
}

// fetchesInOrder checks the newest-page fetches (GetMessagesBefore) came
// after the stream connected, paced, for want in that order.
func fetchesInOrder(t *testing.T, r *rig, from int, want []string) {
	t.Helper()
	calls := r.f.callsNow()[from:]
	connected := -1
	var got []string
	var last time.Time
	for i, c := range calls {
		switch c.name {
		case "RealtimeConnect":
			if connected < 0 {
				connected = i
			}
		case "GetMessagesBefore":
			if connected < 0 && from == 0 {
				t.Fatalf("%s was fetched before the stream connected (live)", c.arg)
			}
			if gap := c.at.Sub(last); !last.IsZero() && (gap < 2*time.Second || gap > 4*time.Second) {
				t.Errorf("fetches %v apart, want 2 to 4 s", gap)
			}
			last = c.at
			got = append(got, c.arg)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("fetched %d chats in order\n%v\nwant\n%v", len(got), got, want)
	}
}

func TestAFirstConnectGoesLiveBeforeFetchingMessages(t *testing.T) {
	r := newRig(t)
	convs := convsByDay(200, 6)
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		switch cursor {
		case "":
			return cursorPageOf("next-invented-1", convs[:3]...)
		case "next-invented-1":
			return cursorPageOf("", convs[3:]...)
		}
		return cursorPageOf("")
	}
	var liveAt time.Time
	r.f.before = func(c string, _ time.Time, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		if st, _ := r.l.stateNow(); st != network.Live {
			t.Errorf("messages of %s fetched in state %s, want live", c, st)
		} else if liveAt.IsZero() {
			liveAt = r.clock.Now()
		}
		return messagePage("")
	}
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	var want []string
	for _, c := range convs {
		want = append(want, urnString(c.EntityURN))
	}
	fetchesInOrder(t, r, 0, want)
	if got := r.f.count("GetConversationsWithNextCursor"); got != 2 {
		t.Errorf("%d listing pages, want 2", got)
	}
}

func TestANewSessionsGapIsFetchedBehindTheListing(t *testing.T) {
	r := newRig(t)
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	c := r.live()
	c.dispatch(&linkedingo.ClientConnection{})
	r.idle()
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		switch cursor {
		case "":
			return cursorPageOf("next-invented-1", conv(2, start.Add(30*time.Minute)), conv(3, start.Add(20*time.Minute)))
		case "next-invented-1":
			return cursorPageOf("next-invented-2", conv(4, start.Add(10*time.Minute)), conv(1, start.Add(-time.Hour)))
		}
		return cursorPageOf("")
	}
	listed := false
	r.f.before = func(c string, _ time.Time, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		if !listed {
			t.Errorf("messages of %s fetched before the listing ended", c)
		}
		return messagePage("")
	}
	page := r.f.cursorPage
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		p := page(cursor)
		listed = listed || cursor == "next-invented-1"
		return p
	}
	n := len(r.f.callsNow())
	c.dispatch(&linkedingo.ClientConnection{SessID: [16]byte{4, 5, 6}})
	r.idle()
	if st, _ := r.l.stateNow(); st != network.Live {
		t.Fatalf("state = %s, want live", st)
	}
	fetchesInOrder(t, r, n, []string{convURN(2), convURN(3), convURN(4)})
}

func TestADeleteOfAConversationNeverStoredMakesNoChat(t *testing.T) {
	r := newRig(t)
	c := r.live()
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf(nil, convURN(77))
	}
	c.dispatch(convDeleteless())
	r.idle()
	if _, ok, _ := r.st.Chat(Name, convURN(77)); ok {
		t.Errorf("a delete of a conversation never stored made a chat")
	}
}

// passingErrors are failures that say nothing about the thing asked for: a
// 5xx, an answer the helper can't read, a timeout.
func passingErrors() map[string]error {
	return map[string]error{
		"500":        statusErr(500),
		"501":        statusErr(501),
		"unreadable": fmt.Errorf("failed to decode response body: %w", errors.New("invented: unexpected end of JSON input")),
		"timeout":    context.DeadlineExceeded,
	}
}

func TestAPassingErrorOnAFreshChatKeepsItsMark(t *testing.T) {
	for name, perr := range passingErrors() {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			alice := person(aliceID, "Alice", "Invented")
			c := r.live()
			// While Pllum was closed, Alice wrote in chat 1.
			r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
				return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Minute))})
			}
			r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
				return messagePage("", text(1, 1, alice, start.Add(time.Minute), "invented while closed"))
			}
			r.f.failOnce("GetMessagesBefore", perr)
			c.dispatch(convDeleteless())
			r.idle()
			if got := r.value("fresh." + convURN(1)); got == "" {
				t.Fatalf("one passing failure cleared the chat's fresh mark")
			}
			if got := r.lines(convURN(1)); len(got) != 0 {
				t.Fatalf("lines = %+v before the retry", got)
			}
			// A later turn asks again, at the pace, and gets it.
			r.clock.Advance(passingWait)
			eventually(t, "the chat fetched again", func() bool { return len(r.lines(convURN(1))) == 1 })
			r.idle()
			if got := r.value("fresh." + convURN(1)); got != "" {
				t.Errorf("the fresh mark is still kept after the fetch")
			}
			if st, _ := r.l.stateNow(); st != network.Live {
				t.Errorf("state = %s, want live", st)
			}
		})
	}
}

func TestAGoneChatSettlesItsFreshMark(t *testing.T) {
	r := newRig(t)
	c := r.live()
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Minute))})
	}
	r.f.setErr("GetMessagesBefore", statusErr(404))
	c.dispatch(convDeleteless())
	r.idle()
	if got := r.value("fresh." + convURN(1)); got != "" {
		t.Errorf("a chat LinkedIn says isn't there kept its fresh mark")
	}
	n := r.f.count("GetMessagesBefore")
	r.clock.Advance(passingWait)
	r.idle()
	if got := r.f.count("GetMessagesBefore"); got != n {
		t.Errorf("a chat LinkedIn says isn't there was asked again (%d, then %d)", n, got)
	}
}

func TestAPassingErrorOnAHistoryPageDoesNotEndIt(t *testing.T) {
	for name, perr := range passingErrors() {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			historyFeed(r, 400)
			r.f.failOnce("GetMessagesWithPrevCursor", perr)
			r.live()
			if got := r.value("histDone." + convURN(1)); got != "" {
				t.Fatalf("one passing failure ended the chat's history")
			}
			if r.chat(convURN(1)).HistoryComplete {
				t.Fatalf("the chat's history is complete before it was paged")
			}
			r.clock.Advance(passingWait)
			eventually(t, "the history paged on to the chat's start", func() bool {
				c, ok, _ := r.st.Chat(Name, convURN(1))
				return ok && c.HistoryComplete
			})
		})
	}
}

func TestMissingDataSettlesAChatsHistory(t *testing.T) {
	r := newRig(t)
	historyFeed(r, 400)
	r.f.setErr("GetMessagesWithPrevCursor", linkedingo.ErrMissingGraphQLData)
	r.live()
	if got := r.value("histDone." + convURN(1)); got == "" {
		t.Errorf("a chat LinkedIn gave no data for is still paged")
	}
}

func TestAPassingErrorKeepsTheSyncToken(t *testing.T) {
	for name, perr := range passingErrors() {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			r.keep("self", selfID)
			r.keep("syncToken", "sync-token-invented-7")
			r.keep("listed", "1")
			r.f.failOnce("GetConversationsBySyncToken", perr)
			if err := r.login(validSession()); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the failed catch-up", func() bool { return r.f.count("GetConversationsBySyncToken") >= 1 })
			r.idle()
			if got := r.value("syncToken"); got != "sync-token-invented-7" {
				t.Fatalf("one passing failure dropped the sync token (kept %q)", got)
			}
			if r.f.count("GetConversationsWithNextCursor") != 0 {
				t.Errorf("a passing failure listed the inbox as if the token were gone")
			}
			// It is asked again with the same token, and catches up.
			r.clock.Advance(passingWait)
			r.l.Nudge()
			eventually(t, "the token asked again", func() bool { return r.f.count("GetConversationsBySyncToken") >= 2 })
			r.waitState(network.Live, "")
			r.idle()
			var asked []string
			for _, c := range r.f.callsNow() {
				if c.name == "GetConversationsBySyncToken" {
					asked = append(asked, c.arg)
				}
			}
			if asked[1] != "sync-token-invented-7" {
				t.Errorf("asked again with %q, want the kept token", asked[1])
			}
			if got := r.value("syncToken"); got != "sync-token-invented-new" {
				t.Errorf("kept token = %q after the catch-up, want the new one", got)
			}
		})
	}
}

func TestADroppedTokensGapIsListedBack(t *testing.T) {
	r := newRig(t)
	// An earlier run stored chat 1, an hour ago.
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	r.live()
	r.restart()
	// LinkedIn no longer takes the token; chats 2 and 3 changed meanwhile.
	r.f.failOnce("GetConversationsBySyncToken", statusErr(400))
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		switch cursor {
		case "":
			return cursorPageOf("next-invented-1", conv(2, start.Add(20*time.Minute)), conv(3, start.Add(10*time.Minute)))
		case "next-invented-1":
			return cursorPageOf("next-invented-2", conv(1, start.Add(-time.Hour)))
		}
		t.Errorf("listed past the newest stored activity, to %q", cursor)
		return cursorPageOf("")
	}
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
	if got := r.value("syncToken"); got != "" {
		t.Errorf("the token LinkedIn refused is still kept: %q", got)
	}
	for _, n := range []int{2, 3} {
		if _, ok, _ := r.st.Chat(Name, convURN(n)); !ok {
			t.Errorf("chat %d, changed while the token was refused, was not listed", n)
		}
	}
}
