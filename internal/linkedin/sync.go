package linkedin

import (
	"context"
	"errors"
	"slices"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Catching up and paging history. Every request here is a background one:
// one at a time, at most one every 2 s (request's pace), none during a pause.
const (
	// pageSize is how many messages a page asks for.
	pageSize = 20
	// firstListing is how far back a first sign-in lists the inbox: until a
	// page's oldest activity is older, or listPages pages.
	firstListing = 365 * 24 * time.Hour
	listPages    = 40
	// catchUpPages is the most earlier pages a caught-up chat is paged back
	// looking for a line the store holds; the history worker has the rest.
	catchUpPages = 10
	// historyReach is how far back the history worker pages each chat.
	historyReach = 730 * 24 * time.Hour
	// passingWait is how long background work a passing failure stopped (a
	// 5xx, an answer the helper can't read, a timeout) waits before it is
	// asked again, unless something wakes it sooner.
	passingWait = 3 * time.Minute
)

// The background work a run does between catch-ups and history.
type job int

const (
	jobNone  job = iota
	jobSync      // a catch-up by the sync token
	jobPages     // the inbox listed by cursor back to the newest activity stored
)

// stayCaughtUp is a signed-in run's work: the catch-up of the chats, then the
// realtime stream (live), then, one request at a time, the catch-ups the
// stream asks for, the messages of the chats found changed, and the history,
// newest chats first, until the run ends.
func (l *LinkedIn) stayCaughtUp(ctx context.Context, gen int, cli liClient) {
	if err := l.catchUp(ctx, gen, cli); err != nil {
		l.failed(gen, network.Offline, wire.KindOf(err))
		return
	}
	l.mu.Lock()
	if gen != l.gen || l.stopped {
		l.mu.Unlock()
		return
	}
	if _, blocked := l.pause.blocked(); blocked {
		l.mu.Unlock()
		return
	}
	// Started under the lock, so a retire that comes after it stops it. It
	// returns at once; the stream runs in the library.
	l.realtime = true
	_ = cli.RealtimeConnect(ctx)
	l.setState(network.Live, "")
	l.mu.Unlock()

	for {
		j, ok := l.takeJob(gen)
		if !ok {
			return
		}
		var err error
		switch j {
		case jobPages:
			err = l.listBack(ctx, gen, cli, false)
		case jobSync:
			err = l.syncByToken(ctx, gen, cli)
		default:
			// What came in the chats a catch-up found changed first, newest
			// chat first; then the history.
			did, ferr := l.freshStep(ctx, gen, cli)
			err = ferr
			if err == nil && !did {
				did, err = l.historyStep(ctx, gen, cli)
			}
			if err == nil && !did {
				if l.askPassedAgain(gen) {
					continue
				}
				if !l.waitForJob(ctx) {
					return
				}
				continue
			}
		}
		if !l.current(gen) {
			return
		}
		if err != nil {
			// A passing failure (no connection, a 5xx, an answer it could not
			// read, a timeout) settles nothing: the work it stopped is asked
			// again in passingWait, or when the stream or a nudge asks sooner,
			// rather than again and again.
			l.later(gen, j)
			if !l.waitForJob(ctx) {
				return
			}
		}
	}
}

// takeJob is the next catch-up the stream asked for, jobNone for none; ok is
// false once run gen has ended.
func (l *LinkedIn) takeJob(gen int) (j job, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || l.stopped {
		return jobNone, false
	}
	switch {
	case l.due.pages:
		l.due.pages = false
		return jobPages, true
	case l.due.sync:
		l.due.sync = false
		return jobSync, true
	}
	return jobNone, true
}

// waitForJob waits, idle, until something asks for work or the run ends.
func (l *LinkedIn) waitForJob(ctx context.Context) bool {
	l.busy.Add(-1)
	defer l.busy.Add(1)
	select {
	case <-l.wake:
		return true
	case <-ctx.Done():
		return false
	}
}

// queue asks the run for a catch-up: by cursor pages (pages) or by the sync
// token. l.mu is held.
func (l *LinkedIn) queue(pages bool) {
	if pages {
		l.due.pages = true
	} else {
		l.due.sync = true
	}
	l.signal()
}

// signal wakes the run's background work.
func (l *LinkedIn) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// catchUp is a run's catch-up before the stream: by the sync token kept in
// the store (syncByToken). The first sign-in has none: the sync token's
// newest chats (and a token), then the inbox listed by cursor back a year.
//
// A passing answer (a 5xx, one the helper can't read) keeps the token: the
// run goes on, and the catch-up is asked again later. No connection or a
// timeout fails the run, which a nudge starts again.
func (l *LinkedIn) catchUp(ctx context.Context, gen int, cli liClient) error {
	if err := l.syncByToken(ctx, gen, cli); err != nil {
		if !l.current(gen) || wire.KindOf(err) != wire.Internal {
			return err
		}
		l.later(gen, jobSync)
	}
	if l.value("listed") == "" {
		return l.listBack(ctx, gen, cli, true)
	}
	return nil
}

// syncByToken is a catch-up by the sync token. A token LinkedIn won't take
// (gone, not a passing failure) is dropped, so the next run starts afresh
// without one, and what changed since it was given is listed by cursor back
// to the newest activity stored: here, and on every later catch-up of this
// run, as its client still holds the token it was made with. Before the
// first listing, that listing covers it.
func (l *LinkedIn) syncByToken(ctx context.Context, gen int, cli liClient) error {
	l.mu.Lock()
	dropped := l.tokenDropped && gen == l.gen
	l.mu.Unlock()
	if dropped {
		return l.listBack(ctx, gen, cli, false)
	}
	err := l.catchUpByToken(ctx, gen, cli)
	if err == nil || !settled(err) || !l.current(gen) {
		return err
	}
	_ = l.keep("syncToken", "")
	l.mu.Lock()
	if gen == l.gen {
		l.tokenDropped = true
	}
	l.mu.Unlock()
	if l.value("listed") == "" {
		return nil
	}
	return l.listBack(ctx, gen, cli, false)
}

// settled reports whether a background request's failure is LinkedIn's
// answer that the thing asked about is not to be had (gone), which settles
// the work for good: a chat's fresh mark, its history, the sync token. Any
// other failure passes, and the work is asked again later.
func settled(err error) bool { return errors.Is(err, errGone) }

// later asks again, in passingWait, for the work a passing failure of run
// gen stopped: the catch-up j, or (jobNone) the chats passed over (pass),
// which the work goes around until then. l.mu is not held.
func (l *LinkedIn) later(gen int, j job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || l.stopped {
		return
	}
	switch j {
	case jobSync:
		l.held.sync = true
	case jobPages:
		l.held.pages = true
	}
	if l.stopLater != nil {
		return
	}
	l.passDue = false
	l.stopLater = l.clock.AfterFunc(passingWait, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if gen != l.gen || l.stopped {
			return
		}
		l.stopLater = nil
		l.passDue = true
		l.due.sync = l.due.sync || l.held.sync
		l.due.pages = l.due.pages || l.held.pages
		l.held.sync, l.held.pages = false, false
		l.signal()
	})
}

// pass sets a chat aside after a passing failure of run gen: the work goes
// around it until later's timer fires, so one chat failing holds up no
// other.
func (l *LinkedIn) pass(gen int, chat string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || l.stopped {
		return
	}
	if l.passed == nil {
		l.passed = map[string]bool{}
	}
	l.passed[chat] = true
}

// passedOver reports whether a chat is set aside (pass).
func (l *LinkedIn) passedOver(chat string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.passed[chat]
}

// askPassedAgain takes the chats set aside back into the work, once later's
// timer has fired and nothing else is left to do; false when there are none
// or it is not yet time.
func (l *LinkedIn) askPassedAgain(gen int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || !l.passDue || len(l.passed) == 0 {
		return false
	}
	l.passed, l.passDue = nil, false
	return true
}

// catchUpByToken asks for the conversations changed since the sync token the
// client holds (the client gives the new token to the handler, which keeps
// it), stores them and makes the deleted ones read-only. What came in each
// changed one is fetched behind it, by the background work (freshStep).
func (l *LinkedIn) catchUpByToken(ctx context.Context, gen int, cli liClient) error {
	var page *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation]
	err := l.request(ctx, gen, true, func(ctx context.Context) (err error) {
		page, err = cli.GetConversationsBySyncToken(ctx)
		return err
	})
	if err != nil {
		return err
	}
	if page == nil {
		return nil
	}
	for _, d := range page.Metadata.DeletedURNs {
		l.readOnly(urnString(d.Conversation.EntityURN))
	}
	l.storeConversations(page.Elements)
	return nil
}

// listBack lists the inbox by cursor, newest activity first: on a first
// sign-in (first) until a page's oldest activity is over a year old, resuming
// where an interrupted listing got to; otherwise back to the newest activity
// the store held before it began. At most listPages pages either way.
func (l *LinkedIn) listBack(ctx context.Context, gen int, cli liClient, first bool) error {
	stopAt := l.clock.Now().Add(-firstListing)
	cursor := ""
	if first {
		cursor = l.value("listCursor")
	} else if newest, ok := l.newestStored(); ok {
		stopAt = newest
	}
	for range listPages {
		var page *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]
		err := l.request(ctx, gen, true, func(ctx context.Context) (err error) {
			page, err = cli.GetConversationsWithNextCursor(ctx, cursor)
			return err
		})
		if settled(err) {
			// LinkedIn lists nothing further: the listing ends here.
			break
		}
		if err != nil {
			return err
		}
		if page == nil {
			break
		}
		var oldest time.Time
		for _, c := range page.Elements {
			if at := c.LastActivityAt.Time; !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
				oldest = at
			}
		}
		if !l.current(gen) {
			// Ended meanwhile (his Disconnect may have forgotten the
			// listing): nothing more is kept for it.
			return errNotConnected
		}
		l.storeConversations(page.Elements)
		next := page.Metadata.NextCursor
		if first {
			_ = l.keep("listCursor", next)
		}
		if len(page.Elements) == 0 || next == "" {
			break
		}
		if !oldest.IsZero() && (first && oldest.Before(stopAt) || !first && !oldest.After(stopAt)) {
			break
		}
		cursor = next
	}
	if first {
		_ = l.keep("listed", "1")
		_ = l.keep("listCursor", "")
	}
	return nil
}

// newestStored is the newest activity of a LinkedIn conversation the store
// holds; ok is false when it holds none.
func (l *LinkedIn) newestStored() (time.Time, bool) {
	cursor := ""
	for range 50 {
		page, err := l.st.Chats(Name, 0, cursor, 200)
		if err != nil {
			return time.Time{}, false
		}
		for _, c := range page.Items {
			if isConversation(c.ID) && c.LastActivityMs > 0 {
				return time.UnixMilli(c.LastActivityMs), true
			}
		}
		if page.Cursor == "" {
			break
		}
		cursor = page.Cursor
	}
	return time.Time{}, false
}

// storeConversations stores conversations and marks each one with activity
// newer than the store had for it (or new) as fresh: what came in it is
// fetched behind the catch-up, by the background work (freshStep), so the
// chats show, and the state goes live, first. The marks are kept in the
// store, so a restart fetches what an earlier run did not get to.
func (l *LinkedIn) storeConversations(convs []linkedingo.Conversation) {
	for _, c := range convs {
		if l.storeConversation(c) {
			_ = l.keep("fresh."+urnString(c.EntityURN), "1")
		}
	}
	if len(convs) > 0 {
		l.changed()
	}
}

// freshStep fetches what came in the newest chat marked fresh (catchUpChat)
// and clears its mark; did is false when none is marked. A chat LinkedIn says
// isn't there (gone) is cleared too. A passing failure keeps the mark, and
// the chat is set aside until later asks again.
func (l *LinkedIn) freshStep(ctx context.Context, gen int, cli liClient) (did bool, err error) {
	chat, ok := l.nextChat(func(c store.Chat) bool {
		return !c.ReadOnly && l.value("fresh."+c.ID) != "" && !l.passedOver(c.ID)
	})
	if !ok {
		return false, nil
	}
	if err := l.catchUpChat(ctx, gen, cli, linkedingo.NewURN(chat)); err != nil && !settled(err) {
		l.pass(gen, chat)
		return true, err
	}
	_ = l.keep("fresh."+chat, "")
	return true, nil
}

// catchUpChat fetches the newest page of a chat, then earlier pages until one
// holds a line the store already had (up to catchUpPages). A chat with no
// lines yet gets its newest page, and its history goes on from there.
func (l *LinkedIn) catchUpChat(ctx context.Context, gen int, cli liClient, conv linkedingo.URN) error {
	chat := urnString(conv)
	_, _, had := l.st.OldestLine(Name, chat, linePrefix)
	var page *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]
	err := l.request(ctx, gen, true, func(ctx context.Context) (err error) {
		page, err = cli.GetMessagesBefore(ctx, conv, l.clock.Now(), pageSize)
		return err
	})
	if err != nil {
		return err
	}
	reached := l.storeMessages(page.Elements, store.SourceSync)
	l.changed()
	if !had {
		if l.value("cursor."+chat) == "" {
			l.historyPaged(chat, "", page)
		}
		return nil
	}
	for range catchUpPages {
		if reached || page.Metadata.PrevCursor == "" {
			return nil
		}
		prev := page.Metadata.PrevCursor
		err := l.request(ctx, gen, true, func(ctx context.Context) (err error) {
			page, err = cli.GetMessagesWithPrevCursor(ctx, conv, prev, pageSize)
			return err
		})
		if err != nil {
			return err
		}
		reached = l.storeMessages(page.Elements, store.SourceSync)
		l.changed()
	}
	return nil
}

// historyStep pages the history of the newest chat that has more to page,
// by one page: back from the cursor kept for it, or from its oldest line (or
// now) when none is kept. did is false when no chat has more.
func (l *LinkedIn) historyStep(ctx context.Context, gen int, cli liClient) (did bool, err error) {
	chat, ok := l.nextHistoryChat()
	if !ok {
		return false, nil
	}
	conv := linkedingo.NewURN(chat)
	cursor := l.value("cursor." + chat)
	var page *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]
	if cursor == "" {
		anchor := l.clock.Now()
		if ms, _, ok := l.st.OldestLine(Name, chat, linePrefix); ok {
			anchor = time.UnixMilli(ms)
		}
		err = l.request(ctx, gen, true, func(ctx context.Context) (err error) {
			page, err = cli.GetMessagesBefore(ctx, conv, anchor, pageSize)
			return err
		})
	} else {
		err = l.request(ctx, gen, true, func(ctx context.Context) (err error) {
			page, err = cli.GetMessagesWithPrevCursor(ctx, conv, cursor, pageSize)
			return err
		})
	}
	if err != nil {
		if settled(err) {
			// LinkedIn says there is nothing to page there: the chat is
			// passed over for good, not asked again and again.
			_ = l.keep("histDone."+chat, "1")
			return true, nil
		}
		// A passing failure settles nothing: the chat is set aside until
		// later asks again.
		l.pass(gen, chat)
		return true, err
	}
	l.storeMessages(page.Elements, store.SourceSync)
	l.changed()
	l.historyPaged(chat, cursor, page)
	return true, nil
}

// historyPaged keeps where a chat's history goes on after page (asked from
// cursor): a page with nothing before it is the chat's start (it is
// historyComplete), and one reaching past 730 days ends its paging.
func (l *LinkedIn) historyPaged(chat, cursor string, page *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]) {
	if page == nil || len(page.Elements) == 0 || page.Metadata.PrevCursor == "" {
		complete := true
		_ = l.st.PutChat(Name, store.ChatInfo{ID: chat, HistoryComplete: &complete})
		_ = l.keep("cursor."+chat, "")
		l.changed()
		return
	}
	prev := page.Metadata.PrevCursor
	_ = l.keep("cursor."+chat, prev)
	oldest := time.Time{}
	for _, m := range page.Elements {
		if at := m.DeliveredAt.Time; !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
			oldest = at
		}
	}
	if prev == cursor || !oldest.IsZero() && oldest.Before(l.clock.Now().Add(-historyReach)) {
		_ = l.keep("histDone."+chat, "1")
	}
}

// nextHistoryChat is the newest LinkedIn conversation whose history has more
// to page: not at its start, not paged to 730 days, not read-only.
func (l *LinkedIn) nextHistoryChat() (string, bool) {
	return l.nextChat(func(c store.Chat) bool {
		return !c.HistoryComplete && !c.ReadOnly && l.value("histDone."+c.ID) == "" && !l.passedOver(c.ID)
	})
}

// nextChat is the newest LinkedIn conversation the store holds for which
// want is true.
func (l *LinkedIn) nextChat(want func(c store.Chat) bool) (string, bool) {
	cursor := ""
	for range 50 {
		page, err := l.st.Chats(Name, 0, cursor, 200)
		if err != nil {
			return "", false
		}
		for _, c := range page.Items {
			if isConversation(c.ID) && want(c) {
				return c.ID, true
			}
		}
		if page.Cursor == "" {
			break
		}
		cursor = page.Cursor
	}
	return "", false
}

// handlersFor is the client's realtime and sync-token handlers for run gen.
// Each one writes what it carries before it returns, and none asks LinkedIn
// anything itself: a catch-up it calls for waits its turn in the run.
func (l *LinkedIn) handlersFor(gen int) linkedingo.Handlers {
	guard := func() {
		_ = recover()
	}
	return linkedingo.Handlers{
		Heartbeat: func(context.Context) {
			defer guard()
			l.heard(gen)
		},
		ClientConnection: func(_ context.Context, c *linkedingo.ClientConnection) {
			defer guard()
			l.clientConnected(gen, c)
		},
		TransientDisconnect: func(_ context.Context, err error) {
			defer guard()
			l.disconnected(gen, err)
		},
		BadCredentials: func(context.Context, error) {
			defer guard()
			l.endSession(gen)
		},
		DecoratedEvent: func(_ context.Context, e *linkedingo.DecoratedEvent) {
			defer guard()
			if l.current(gen) {
				l.onEvent(gen, e)
			}
		},
		ConversationsSyncToken: func(_ context.Context, token string) {
			defer guard()
			if l.current(gen) && token != "" {
				_ = l.keep("syncToken", token)
			}
		},
	}
}

// heard: the stream is alive (a heartbeat); offline is live again.
func (l *LinkedIn) heard(gen int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || l.stopped || !l.realtime {
		return
	}
	if l.state == network.Offline && l.kind == "" {
		l.setState(network.Live, "")
	}
}

// clientConnected: the stream has (re)connected. The run's first connection,
// or one under the same realtime session, catches up by the sync token; one
// under a new session lists the inbox back to the newest activity stored, as
// the token may not cover the gap.
func (l *LinkedIn) clientConnected(gen int, c *linkedingo.ClientConnection) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || l.stopped || !l.realtime {
		return
	}
	if l.state == network.Offline && l.kind == "" {
		l.setState(network.Live, "")
	}
	switch {
	case !l.connected:
		l.connected, l.sessID = true, c.SessID
		l.queue(false)
	case c.SessID != l.sessID:
		l.sessID = c.SessID
		l.queue(true)
	default:
		l.queue(false)
	}
}

// disconnected: the stream dropped, and the library reconnects it. A
// reconnect LinkedIn pushed back on is a pause, like any request's; an ended
// session is the library's to report (BadCredentials).
func (l *LinkedIn) disconnected(gen int, err error) {
	if !l.current(gen) {
		return
	}
	kind, paused := l.pause.check(err)
	if paused {
		_ = l.keep("pause", l.pause.String())
		l.paused(gen)
		return
	}
	if kind == reconnectNeeded {
		l.endSession(gen)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.gen || l.stopped {
		return
	}
	if l.state == network.Live || l.state == network.CatchingUp {
		l.setState(network.Offline, "")
	}
}

// onEvent stores what a realtime event carries. Every topic but these five
// is ignored.
func (l *LinkedIn) onEvent(gen int, e *linkedingo.DecoratedEvent) {
	if e == nil {
		return
	}
	data := e.Payload.Data
	switch e.Topic.NthPrefixPart(2) {
	case linkedingo.RealtimeEventTopicMessages:
		if data.DecoratedMessage != nil {
			m := data.DecoratedMessage.Result
			if skipped(m.Conversation) {
				return
			}
			if c := m.Conversation; !c.EntityURN.IsEmpty() && len(c.ConversationParticipants) > 0 {
				l.storeConversation(c)
			}
			l.storeMessages([]linkedingo.Message{m}, store.SourceLive)
			l.changed()
		}
	case linkedingo.RealtimeEventTopicMessageReactionSummaries:
		if data.DecoratedReactionSummary != nil {
			l.storeReaction(data.DecoratedReactionSummary.Result)
			l.changed()
		}
	case linkedingo.RealtimeEventTopicMessageSeenReceipts:
		if data.DecoratedSeenReceipt != nil {
			l.storeSeen(data.DecoratedSeenReceipt.Result)
			l.changed()
		}
	case linkedingo.RealtimeEventTopicConversations:
		if data.DecoratedConversation == nil {
			// Something changed and LinkedIn didn't say what: a catch-up by
			// the sync token, in turn.
			l.mu.Lock()
			if gen == l.gen && !l.stopped {
				l.queue(false)
			}
			l.mu.Unlock()
			return
		}
		l.storeConversation(data.DecoratedConversation.Result)
		l.changed()
	case linkedingo.RealtimeEventTopicConversationDelete:
		if data.DecoratedConversationDelete != nil {
			l.readOnly(urnString(data.DecoratedConversationDelete.Result.EntityURN))
		}
	}
}

// skipped reports whether a conversation is one Pllum leaves out: LinkedIn's
// spam folder (mautrix-linkedin drops those too).
func skipped(c linkedingo.Conversation) bool { return slices.Contains(c.Categories, "SPAM") }
