package linkedin

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Catching up and paging history. Every request here is a background one:
// one at a time, a random 2 to 4 s apart (paced), none during a pause.
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
	// asideCap is the longest a chat, or a catch-up (by the sync token, or
	// the inbox listed by cursor), LinkedIn keeps failing is set aside:
	// each passing failure of it in a row doubles its wait from
	// passingWait (3, 6, 12 … minutes) up to this.
	asideCap = 4 * time.Hour
	// listedFoldersNow marks (as "listedFolders") that an account's inbox
	// has been listed over listFolders. An account listed before that (its
	// first listing was of the primary inbox alone) lists them once more.
	listedFoldersNow = "2"
)

// listFolders are the LinkedIn folders (the category of the by-category
// query) the listing pages in turn, each by cursor with the same stop rules:
// INBOX (a superset of PRIMARY_INBOX), then ARCHIVE. Spam stays out.
var listFolders = []string{"INBOX", "ARCHIVE"}

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
	if gen != l.runs.Gen || l.stopped {
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
	l.startQueue(gen)
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
				// Nothing to do now: wake when the first chat set aside
				// may be asked again.
				l.wakeForAside(gen)
				if !l.waitForJob(ctx) {
					return
				}
				continue
			}
		}
		if !l.current(gen) {
			return
		}
		if err == nil && j != jobNone {
			l.jobAnswered(j)
		}
		if err != nil {
			// A passing failure (no connection, a 5xx, an answer it could not
			// read, a timeout) settles nothing: the work waits passingWait,
			// or until the stream or a nudge asks sooner, rather than asking
			// again and again. A chat it failed on waits longer each time in
			// a row (pass), and so does a catch-up (later); the rest goes
			// on after passingWait.
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
	if gen != l.runs.Gen || l.stopped {
		return jobNone, false
	}
	// A catch-up backing off (later) waits its turn, however often the
	// stream asks for it.
	if l.due.pages && l.jobWaiting(jobPages) {
		l.due.pages, l.held.pages = false, true
		l.wakeAt(gen, l.jobAside[jobPages].until)
	}
	if l.due.sync && l.jobWaiting(jobSync) {
		l.due.sync, l.held.sync = false, true
		l.wakeAt(gen, l.jobAside[jobSync].until)
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
	select {
	case <-l.wake:
		// busy before woken, so the two never both read idle while a wake
		// is being taken up
		l.busy.Add(1)
		l.woken.Add(-1)
		return true
	case <-ctx.Done():
		l.busy.Add(1)
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
	l.woken.Add(1)
	select {
	case l.wake <- struct{}{}:
	default:
		l.woken.Add(-1) // one is already waiting to be taken up
	}
}

// catchUp is a run's catch-up before the stream: by the sync token kept in
// the store (syncByToken). The first sign-in has none: the sync token's
// newest chats (and a token), then the folders listed by cursor back a year
// (listBack); an account listed before the folders existed lists them once.
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
	} else {
		l.jobAnswered(jobSync)
	}
	if l.listingOwed() {
		return l.listBack(ctx, gen, cli, true)
	}
	return nil
}

// listingOwed reports whether the run still owes the full listing: the first
// sign-in's, or the one-off re-list of an account listed before the folders
// (listedFolders) existed. Either is a listing of every folder back a year,
// chats first (the messages come behind, from the background work), and
// sets both marks.
func (l *LinkedIn) listingOwed() bool {
	return l.value("listed") == "" || l.value("listedFolders") != listedFoldersNow
}

// syncByToken is a catch-up by the sync token. A token LinkedIn won't take
// (gone, not a passing failure) is dropped, so the next run starts afresh
// without one, and what changed since it was given is listed by cursor back
// to the newest activity stored: here, and on every later catch-up of this
// run, as its client still holds the token it was made with. Before the
// first listing, that listing covers it.
func (l *LinkedIn) syncByToken(ctx context.Context, gen int, cli liClient) error {
	l.mu.Lock()
	dropped := l.tokenDropped && gen == l.runs.Gen
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
	if gen == l.runs.Gen {
		l.tokenDropped = true
	}
	l.mu.Unlock()
	if l.listingOwed() {
		// The full listing is still to come in this run, and covers it.
		return nil
	}
	return l.listBack(ctx, gen, cli, false)
}

// settled reports whether a background request's failure is LinkedIn's
// answer that the thing asked about is not to be had (gone), which settles
// the work for good: a chat's fresh mark, its history, the sync token. Any
// other failure passes, and the work is asked again later.
func settled(err error) bool { return errors.Is(err, errGone) }

// later asks again for the work a passing failure of run gen stopped: the
// rest of the work in passingWait (the chat that failed is set aside on its
// own: pass), and the catch-up j (jobSync, jobPages) once its own wait is
// over: passingWait after its first passing failure in a row, then twice as
// long each time, up to asideCap, until LinkedIn next answers it
// (jobAnswered). Held in memory only, across runs: a restart starts afresh.
// A pause is not counted here: it ends the run first. l.mu is not held.
func (l *LinkedIn) later(gen int, j job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.runs.Gen || l.stopped {
		return
	}
	now := l.clock.Now()
	if j == jobSync || j == jobPages {
		if l.jobAside == nil {
			l.jobAside = map[job]backoff{}
		}
		b := l.jobAside[j]
		b.fails++
		b.until = now.Add(asideWait(b.fails))
		l.jobAside[j] = b
		if j == jobSync {
			l.held.sync = true
		} else {
			l.held.pages = true
		}
	}
	l.wakeAt(gen, now.Add(passingWait))
}

// jobWaiting reports whether the catch-up j is backing off now (later).
// l.mu is held.
func (l *LinkedIn) jobWaiting(j job) bool {
	b, ok := l.jobAside[j]
	return ok && l.clock.Now().Before(b.until)
}

// jobAnswered: LinkedIn answered the catch-up j, so its passing failures
// start over.
func (l *LinkedIn) jobAnswered(j job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.jobAside, j)
}

// wakeAt wakes run gen's background work at at, unless a wake is already
// due by then: the catch-ups held by later whose wait is over become due,
// the chats set aside until then may be asked, and a wake is set for the
// first held catch-up still waiting. l.mu is held.
func (l *LinkedIn) wakeAt(gen int, at time.Time) {
	if l.stopLater != nil {
		if !l.laterAt.After(at) {
			return
		}
		l.stopLater()
	}
	l.laterSeq++
	seq := l.laterSeq
	l.laterAt = at
	l.stopLater = l.clock.AfterFunc(max(at.Sub(l.clock.Now()), 0), func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if gen != l.runs.Gen || l.stopped || seq != l.laterSeq {
			return // an older run's, or a wake moved sooner
		}
		l.stopLater = nil
		if l.held.sync && !l.jobWaiting(jobSync) {
			l.due.sync, l.held.sync = true, false
		}
		if l.held.pages && !l.jobWaiting(jobPages) {
			l.due.pages, l.held.pages = true, false
		}
		if l.held.sync {
			l.wakeAt(gen, l.jobAside[jobSync].until)
		}
		if l.held.pages {
			l.wakeAt(gen, l.jobAside[jobPages].until)
		}
		l.signal()
	})
}

// wakeForAside wakes run gen's background work when the first chat set
// aside may be asked again, if any is still waiting.
func (l *LinkedIn) wakeForAside(gen int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.runs.Gen || l.stopped {
		return
	}
	now := l.clock.Now()
	var first time.Time
	for _, a := range l.aside {
		if a.until.After(now) && (first.IsZero() || a.until.Before(first)) {
			first = a.until
		}
	}
	if !first.IsZero() {
		l.wakeAt(gen, first)
	}
}

// backoff is a chat, or a catch-up, LinkedIn has failed on (a passing
// failure) since it last answered: how many times in a row, and when it may
// be asked again.
type backoff struct {
	fails int
	until time.Time
}

// pass sets a chat aside after a passing failure of run gen: the work goes
// around it, so one chat failing holds up no other, for passingWait after
// its first failure in a row, then twice as long each time, up to asideCap,
// so a chat LinkedIn keeps failing is asked less and less (never every few
// minutes all day). Its next success starts it over (answered). This is
// held in memory only, across runs: a restart of the helper starts every
// chat afresh. A pause is not counted here: it ends the run first, and
// pauses every request on its own rule.
func (l *LinkedIn) pass(gen int, chat string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.runs.Gen || l.stopped {
		return
	}
	if l.aside == nil {
		l.aside = map[string]backoff{}
	}
	a := l.aside[chat]
	a.fails++
	a.until = l.clock.Now().Add(asideWait(a.fails))
	l.aside[chat] = a
}

// asideWait is how long a chat is set aside after its nth passing failure
// in a row: passingWait, doubled for each one before it, up to asideCap.
func asideWait(n int) time.Duration {
	d := passingWait
	for i := 1; i < n && d < asideCap; i++ {
		d *= 2
	}
	return min(d, asideCap)
}

// answered: LinkedIn answered for a chat (with the thing, or that it is
// not to be had), so its passing failures start over.
func (l *LinkedIn) answered(chat string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.aside, chat)
}

// passedOver reports whether a chat is set aside (pass) now.
func (l *LinkedIn) passedOver(chat string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.aside[chat]
	return ok && l.clock.Now().Before(a.until)
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

// listBack lists the folders (listFolders) by cursor, newest activity first,
// each in turn: on a full listing (first: a first sign-in, or the one-off
// re-list) until a page's oldest activity is over a year old, resuming where
// an interrupted listing got to; otherwise back to the newest activity the
// store held before it began (taken once, before any folder is stored). At
// most listPages pages a folder either way. A folder LinkedIn refuses (a
// settled answer: a 400, no GraphQL data) is skipped and the others go on; a
// pause or a passing failure stops the listing, which resumes later.
func (l *LinkedIn) listBack(ctx context.Context, gen int, cli liClient, first bool) error {
	stopAt := l.clock.Now().Add(-firstListing)
	folder, cursor := 0, ""
	if first {
		folder, cursor = l.listResume()
	} else if newest, ok := l.newestStored(); ok {
		stopAt = newest
	}
	for ; folder < len(listFolders); folder++ {
		if err := l.listFolder(ctx, gen, cli, listFolders[folder], cursor, first, stopAt); err != nil {
			return err
		}
		cursor = ""
		if first && folder+1 < len(listFolders) {
			_ = l.keep("listCursor", listFolders[folder+1]+":")
		}
	}
	if first {
		_ = l.keep("listed", "1")
		_ = l.keep("listedFolders", listedFoldersNow)
		_ = l.keep("listCursor", "")
	}
	return nil
}

// listResume is where an interrupted full listing goes on: the folder's index
// in listFolders and its cursor ("<folder>:<cursor>" is kept as listCursor).
// A cursor no folder claims (one an earlier version kept for the primary
// inbox) starts afresh.
func (l *LinkedIn) listResume() (folder int, cursor string) {
	name, cursor, ok := strings.Cut(l.value("listCursor"), ":")
	if i := slices.Index(listFolders, name); ok && i >= 0 {
		return i, cursor
	}
	return 0, ""
}

// listFolder lists one folder by cursor from cursor, storing what it finds,
// until its stop rule (see listBack). A refused folder ends quietly; any
// other failure is returned.
func (l *LinkedIn) listFolder(ctx context.Context, gen int, cli liClient, folder, cursor string, first bool, stopAt time.Time) error {
	for range listPages {
		var page *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]
		err := l.request(ctx, gen, true, func(ctx context.Context) (err error) {
			page, err = cli.GetConversationsWithNextCursor(ctx, folder, cursor)
			return err
		})
		if settled(err) {
			// LinkedIn lists nothing further of this folder: it ends here,
			// and the other folders go on.
			return nil
		}
		if err != nil {
			return err
		}
		if page == nil {
			return nil
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
			_ = l.keep("listCursor", folder+":"+next)
		}
		if len(page.Elements) == 0 || next == "" {
			return nil
		}
		if !oldest.IsZero() && (first && oldest.Before(stopAt) || !first && !oldest.After(stopAt)) {
			return nil
		}
		cursor = next
	}
	return nil
}

// newestStored is the newest activity of a LinkedIn conversation the store
// holds; ok is false when it holds none.
func (l *LinkedIn) newestStored() (time.Time, bool) {
	ms, ok, err := l.st.NewestActivity(Name, conversationPrefix)
	if err != nil || !ok {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
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
// the chat is set aside (pass).
func (l *LinkedIn) freshStep(ctx context.Context, gen int, cli liClient) (did bool, err error) {
	chat, ok := l.nextChat(store.ChatFilter{Writable: true}, func(chat string) bool {
		return l.value("fresh."+chat) != "" && !l.passedOver(chat)
	})
	if !ok {
		return false, nil
	}
	if err := l.catchUpChat(ctx, gen, cli, linkedingo.NewURN(chat)); err != nil && !settled(err) {
		l.pass(gen, chat)
		return true, err
	}
	l.answered(chat)
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
	reached, err := l.storeMessages(page.Elements, store.SourceSync)
	if err != nil {
		return err // nothing kept: the mark stays, and the chat is asked again
	}
	l.changed()
	if !had {
		if l.value("cursor."+chat) == "" {
			l.historyPaged(chat, "", page, true)
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
		if reached, err = l.storeMessages(page.Elements, store.SourceSync); err != nil {
			return err
		}
		l.changed()
	}
	return nil
}

// historyStep pages the history of the newest chat that has more to page,
// by one page (pageHistory, stopping at historyReach). did is false when no
// chat has more.
func (l *LinkedIn) historyStep(ctx context.Context, gen int, cli liClient) (did bool, err error) {
	chat, ok := l.nextHistoryChat()
	if !ok {
		return false, nil
	}
	_, err = l.pageHistory(ctx, gen, cli, chat, true)
	return true, err
}

// What one page of a chat's history did.
type pageOutcome int

const (
	pageMoved  pageOutcome = iota // it reached further back
	pageDone                      // the chat's start: historyComplete
	pageReach                     // past historyReach: the worker stops there ("reach")
	pageGone                      // LinkedIn says there is nothing to page there ("gone")
	pageStuck                     // the cursor did not move ("stuck")
	pageFailed                    // a passing failure, or the store lost the page: err says which
)

// The reasons histDone.<chat> keeps for a chat the worker no longer pages.
// An older store's "1" still means done to the worker.
const (
	histReach = "reach" // past 730 days, its cursor kept
	histGone  = "gone"  // LinkedIn's settled answer
	histStuck = "stuck" // the cursor did not move
)

// pageHistory pages one chat's history by one page, back from its kept
// cursor (or its oldest line, or now). reach stops it at historyReach.
func (l *LinkedIn) pageHistory(ctx context.Context, gen int, cli liClient, chat string, reach bool) (out pageOutcome, err error) {
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
			l.answered(chat)
			_ = l.keep("histDone."+chat, histGone)
			return pageGone, nil
		}
		// A passing failure settles nothing: the chat is set aside, for
		// longer each time in a row (pass).
		l.pass(gen, chat)
		return pageFailed, err
	}
	l.answered(chat)
	if _, err := l.storeMessages(page.Elements, store.SourceSync); err != nil {
		// The store lost the page: its cursor is not kept, so the same page
		// is asked again, after the chat's turn aside, as for a passing
		// failure.
		l.pass(gen, chat)
		return pageFailed, err
	}
	l.changed()
	return l.historyPaged(chat, cursor, page, reach), nil
}

// historyPaged keeps where a chat's history goes on after page (asked from
// cursor): a page with nothing before it is the chat's start (it is
// historyComplete); one whose cursor did not move is stuck; with reach, one
// reaching past 730 days ends the worker's paging.
func (l *LinkedIn) historyPaged(chat, cursor string, page *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message], reach bool) pageOutcome {
	if page == nil || len(page.Elements) == 0 || page.Metadata.PrevCursor == "" {
		complete := true
		_ = l.st.PutChat(Name, store.ChatInfo{ID: chat, HistoryComplete: &complete})
		_ = l.keep("cursor."+chat, "")
		l.changed()
		return pageDone
	}
	prev := page.Metadata.PrevCursor
	_ = l.keep("cursor."+chat, prev)
	if prev == cursor {
		_ = l.keep("histDone."+chat, histStuck)
		return pageStuck
	}
	oldest := time.Time{}
	for _, m := range page.Elements {
		if at := m.DeliveredAt.Time; !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
			oldest = at
		}
	}
	if reach && !oldest.IsZero() && oldest.Before(l.clock.Now().Add(-historyReach)) {
		_ = l.keep("histDone."+chat, histReach)
		return pageReach
	}
	return pageMoved
}

// nextHistoryChat is the newest LinkedIn conversation whose history has more
// to page: not at its start, not paged to 730 days, not read-only.
func (l *LinkedIn) nextHistoryChat() (string, bool) {
	return l.nextChat(store.ChatFilter{Writable: true, HistoryOpen: true}, func(chat string) bool {
		return l.value("histDone."+chat) == "" && !l.passedOver(chat)
	})
}

// nextChat is the newest LinkedIn conversation the store holds that f lets
// through and want is true for. The store narrows by what it holds (review
// C2), so only the IDs of conversations it lets through are read, and want
// is asked only of those.
func (l *LinkedIn) nextChat(f store.ChatFilter, want func(chat string) bool) (string, bool) {
	f.Prefix = conversationPrefix
	cursor := ""
	for range 50 {
		ids, next, err := l.st.ChatIDs(Name, f, cursor, 200)
		if err != nil {
			return "", false
		}
		for _, id := range ids {
			if want(id) {
				return id, true
			}
		}
		if next == "" {
			break
		}
		cursor = next
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
	if gen != l.runs.Gen || l.stopped || !l.realtime {
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
	if gen != l.runs.Gen || l.stopped || !l.realtime {
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
	if gen != l.runs.Gen || l.stopped {
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
			_, _ = l.storeMessages([]linkedingo.Message{m}, store.SourceLive) // the stream has no page to hold back
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
			if gen == l.runs.Gen && !l.stopped {
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
