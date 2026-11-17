package instagram

import (
	"context"
	"sync"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// The history the helper fetches by itself, every request through the pace:
//
//   - at the first sign-in, the inbox, page by page (folder INBOX) until a
//     thread older than 365 days, no next page, or 40 pages; then, for each
//     thread active in the last 90 days, up to 5 pages of its messages;
//   - at every later sign-in, and at every inbox reload after a resnapshot,
//     only the threads the inbox's first page shows active since the store
//     last saw them (or that a catch-up cut short left marked behind),
//     newest first, until a line the store holds (up to 5 pages);
//   - when Pllum reads to the oldest line of a chat (FetchOlder), one more
//     page, older than the last.
//
// historyComplete is set when Meta says a thread has no earlier page.

const (
	inboxFolder  = "INBOX"
	mailboxPages = 40
	threadPages  = 5
	pageSize     = 20
	inboxReach   = 365 * 24 * time.Hour
	activeWithin = 90 * 24 * time.Hour
)

// backfill fills in the history after a sign-in; first is the inbox
// LoadIndex returned. It stops when the run does or Meta pushes back, and
// carries on at the next sign-in.
func (g *Instagram) backfill(ctx context.Context, gen int, cli igClient, first *slidetypes.Mailbox) {
	if first == nil {
		return
	}
	now := g.clock.Now()
	done := g.value("backfilled") != ""
	threads := g.storeListing(first.ThreadsByFolder.Edges, done)
	g.changed()

	if !done {
		page := first.ThreadsByFolder.PageInfo
		reached := olderThan(threads, now.Add(-inboxReach).UnixMilli())
		for n := 0; page.HasNextPage && page.EndCursor != "" && !reached && n < mailboxPages; n++ {
			var resp *slidetypes.PaginateMailboxResponse
			cursor := page.EndCursor
			err := g.request(ctx, gen, func(ctx context.Context) (err error) {
				resp, err = cli.PaginateMailbox(ctx, slidetypes.MakePaginateMailboxRequest(g.viewer(), cursor, inboxFolder, nil))
				return err
			})
			if err != nil {
				return
			}
			if resp == nil || resp.Mailbox == nil {
				break
			}
			more := g.storeListing(resp.Mailbox.ThreadsByFolder.Edges, false)
			g.changed()
			threads = append(threads, more...)
			reached = olderThan(more, now.Add(-inboxReach).UnixMilli())
			page = resp.Mailbox.ThreadsByFolder.PageInfo
		}
	}

	active := now.Add(-activeWithin).UnixMilli()
	for _, t := range threads {
		if ctx.Err() != nil || !g.current(gen) {
			return
		}
		var err error
		switch {
		case g.behind(t.chat):
			err = g.catchUp(ctx, gen, cli, t)
		case !done && t.activity >= active:
			_, _, err = g.fetchPages(ctx, gen, cli, t.chat, t.igChatID, threadPages, false)
		}
		if err != nil {
			return
		}
	}
	if !done {
		_ = g.keep("backfilled", "1")
	}
}

// catchUpListing catches up every thread of a listing that is marked
// behind, in order, and stops at the first that fails (it stays marked).
func (g *Instagram) catchUpListing(ctx context.Context, gen int, cli igClient, threads []listed) error {
	for _, t := range threads {
		if ctx.Err() != nil || !g.current(gen) {
			return errNotConnected
		}
		if !g.behind(t.chat) {
			continue
		}
		if err := g.catchUp(ctx, gen, cli, t); err != nil {
			return err
		}
	}
	return nil
}

// catchUp fetches a thread's newest pages until a line the store holds (up
// to threadPages, each through the pace), then forgets that it was behind.
// Until that has worked the mark stays, so a catch-up cut short (a failed
// page, a block, a restart) is done again at the next inbox load, though the
// chat's activity has already moved on.
func (g *Instagram) catchUp(ctx context.Context, gen int, cli igClient, t listed) error {
	if _, _, err := g.fetchPages(ctx, gen, cli, t.chat, t.igChatID, threadPages, true); err != nil {
		return err
	}
	return g.keep("behind."+t.chat, "")
}

// behind reports whether a chat was listed as active since the store last
// saw it and has not been caught up since.
func (g *Instagram) behind(chat string) bool { return g.value("behind."+chat) != "" }

// olderThan reports whether a listing holds a thread last active before ms.
func olderThan(threads []listed, ms int64) bool {
	for _, t := range threads {
		if t.activity > 0 && t.activity < ms {
			return true
		}
	}
	return false
}

// FetchOlder fetches one page of a chat's history older than what the store
// holds (the newest page, for a chat that holds none), and tells Pllum when
// it added lines or found the start.
func (g *Instagram) FetchOlder(ctx context.Context, chatID string) (int, error) {
	if err := g.gate(); err != nil {
		return 0, err
	}
	gen, cli, runCtx := g.running()
	if cli == nil || runCtx == nil {
		return 0, errNotConnected
	}
	igChatID, ok := g.threadOfChat(chatID)
	if !ok {
		return 0, errNotFound // not a thread Meta has listed (a chat only in Beeper's copy)
	}
	// It ends with the run, or with the caller.
	ctx2, cancel := context.WithCancel(runCtx)
	defer cancel()
	defer context.AfterFunc(ctx, cancel)()
	added, complete, err := g.fetchPages(ctx2, gen, cli, chatID, igChatID, 1, false)
	if added > 0 || complete {
		g.changed()
	}
	return added, err
}

// fetchPages fetches up to max pages of a chat's messages and stores them.
// From the newest when fromNewest is set (a catch-up, which stops at a line
// the store holds) or when nothing older has been fetched yet; else on from
// where older history stopped. Only pages that reach further back move that
// place (the cursor) or end it (historyComplete).
func (g *Instagram) fetchPages(ctx context.Context, gen int, cli igClient, chat, igChatID string, max int, fromNewest bool) (added int, complete bool, err error) {
	lock := g.chatLock(chat)
	lock.Lock()
	defer lock.Unlock()

	complete = g.historyComplete(chat)
	if complete && !fromNewest {
		return 0, true, nil // there is nothing older
	}
	cursor := g.value("cursor." + chat)
	held := cursor != "" || complete // older pages have been fetched before
	if fromNewest {
		cursor = ""
	}
	reachesBack := !fromNewest || !held
	complete = false
	for i := 0; i < max; i++ {
		req := &slidetypes.PaginateMessagesRequest{ThreadID: igChatID, FirstN: pageSize, InitialMessagePageCount: pageSize}
		if cursor != "" {
			c := cursor
			req.AfterCursor = &c
		}
		var resp *slidetypes.PaginateMessagesResponse
		err := g.request(ctx, gen, func(ctx context.Context) (err error) {
			resp, err = cli.PaginateMessages(ctx, req)
			return err
		})
		if err != nil {
			return added, complete, err
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.Messages == nil {
			return added, complete, nil
		}
		msgs := resp.ThreadInfo.AsIGDirectThread.Messages
		knew := fromNewest && held && g.holdsAny(chat, msgs.Edges)
		n, err := g.storeMessages(chat, msgs.Edges, store.SourceSync)
		added += n
		if err != nil {
			return added, complete, err
		}
		if n > 0 {
			g.changed()
		}
		if reachesBack {
			complete = g.pageDone(chat, msgs.PageInfo, false)
		}
		if !msgs.PageInfo.HasNextPage || msgs.PageInfo.EndCursor == "" || knew {
			break
		}
		cursor = msgs.PageInfo.EndCursor
	}
	return added, complete, nil
}

// pageDone records how far back a chat's history now reaches: the cursor
// after the page, or, when Meta says there is no earlier page, that the
// history is complete (which it reports). onlyIfNone keeps a cursor already
// held (a newest page, fetched for another reason, reaches no further).
func (g *Instagram) pageDone(chat string, page slidetypes.PageInfo, onlyIfNone bool) bool {
	if onlyIfNone && (g.value("cursor."+chat) != "" || g.historyComplete(chat)) {
		return false
	}
	if page.HasNextPage && page.EndCursor != "" {
		_ = g.keep("cursor."+chat, page.EndCursor)
		return false
	}
	complete := true
	if err := g.st.PutChat(Name, store.ChatInfo{ID: chat, HistoryComplete: &complete}); err != nil {
		return false
	}
	_ = g.keep("cursor."+chat, "")
	return true
}

// historyComplete reports whether the store holds all of a chat's history.
func (g *Instagram) historyComplete(chat string) bool {
	c, ok, err := g.st.Chat(Name, chat)
	return err == nil && ok && c.HistoryComplete
}

// holdsAny reports whether the store holds any of a page's lines.
func (g *Instagram) holdsAny(chat string, edges []slidetypes.Node[*slidetypes.Message]) bool {
	for _, e := range edges {
		if e.Node == nil {
			continue
		}
		id := e.Node.ID
		if id == "" {
			id = e.Node.MessageID
		}
		if _, ok := g.st.LineTime(Name, chat, id); ok {
			return true
		}
	}
	return false
}

// chatLock is the lock that keeps one history fetch per chat at a time.
func (g *Instagram) chatLock(chat string) *sync.Mutex {
	g.lmu.Lock()
	defer g.lmu.Unlock()
	l, ok := g.locks[chat]
	if !ok {
		l = &sync.Mutex{}
		g.locks[chat] = l
	}
	return l
}
