package messenger

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"

	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history the helper fetches by itself, every request through the pace
// (as Instagram's, Task A2):
//
//   - at the first sign-in, every folder a Messenger client shows, page by
//     page with FetchThreadsTask: each parent the messages page names (the
//     inbox is -1; Marketplace and communities have their own) and every
//     folder thread a listing shows, in sync group 1 (the inbox), and under
//     the parents the messages page names in sync group 95 (message
//     requests), as the library's own first fetch does; until Meta says the
//     folder has no more, its oldest thread stops moving, a thread older
//     than 365 days, or 40 pages; 120 pages in all;
//   - then, for each chat active in the last 90 days that has an
//     unencrypted thread and a line of it held, up to 5 pages of its older
//     lines (FetchMessagesTask), newest chat first;
//   - when Pllum reads to the oldest line of a chat (FetchOlder), one more
//     page of its unencrypted thread, older than the last.
//
// The first sign-in's work is one-shot and resumable (review C2): each
// folder's range, each folder listed, the pages listed so far, the listing
// as a whole ("listed"), each chat's backfilled pages ("bf.<chat>") and the
// backfill as a whole ("backfilled") are kept in the store as they happen.
// Meta pushing back, a rate limit or a quit stops it, and the next sign-in
// carries on from there; a folder or thread that fails with an error of its
// own (a page the library cannot parse) is passed over, never retried at
// every launch.
//
// A history page is never asked twice the same way (review C1). A page is
// anchored on the oldest line Meta has given of the thread (its range), else
// the oldest line of it held, as the library's connector does; a thread with
// neither is not asked. A page that never comes, comes without moving, or
// answers with an existing range that does not end the history, holds the
// chat for stallHold, kept in the store, during which FetchOlder asks
// nothing.
//
// With --gate, every sign-in lists every folder afresh (nothing kept, and no
// year's reach, so the counts hold the oldest threads), and no history is
// fetched: the gate only counts (review I4).
//
// messagix's own FetchMoreThreads pages only sync group 1, from whichever
// folder's range Meta sent last, and its first fetch of every folder on
// connect drops Meta's answer; so the helper sends FetchThreadsTask itself,
// one folder at a time, reads each answer's own range, and hands the
// answer back to the library for its sync cursors, as FetchMoreThreads
// does.
//
// A page of history comes back on the main socket, not in the task's
// answer (the library's connector waits for it the same way): the fetch
// waits up to pageWait for the thread's range.

// folderKey is one folder a listing pages: a sync group and a parent
// thread key.
type folderKey struct{ group, parent int64 }

// name is the folder's name in the store's values.
func (f folderKey) name() string { return fmt.Sprintf("%d.%d", f.group, f.parent) }

// folderRange is how far a folder's listing reaches: its oldest thread and
// that thread's last activity, and whether Meta has older ones.
type folderRange struct {
	minKey, minAt int64
	more          bool
}

const (
	folderPages        = 40  // pages of one folder
	listPages          = 120 // pages of all folders together
	maxFolders         = 24  // folders one listing pages
	threadHistoryPages = 5   // pages of one chat's history at the first sign-in
	yearReach          = 365 * 24 * time.Hour
	activeWithin       = 90 * 24 * time.Hour
	topParent          = -1 // the inbox
	// pageWaitDefault is how long a page of history may take to come back.
	pageWaitDefault = 20 * time.Second
	// newestActivity is the reference a folder's first page is asked from.
	newestActivity = 9999999999999
	// stallHold is how long a chat whose page did not move is not asked.
	stallHold = 6 * time.Hour
	// metaLinePrefix starts Meta's own message IDs (an unencrypted
	// thread's lines), as against the encrypted side's.
	metaLinePrefix = "mid."
)

// syncGroups are the sync groups whose folders are listed: 1 holds the
// inbox, 95 message requests.
var syncGroups = []int64{1, 95}

// How a folder's listing ended, as the gate counts it.
const (
	stopComplete = "complete"
	stopYear     = "yearReached"
	stopCapped   = "capped"
	stopFailed   = "failed"
)

// listThreads lists every folder, through the pace, and records how far it
// got. Meta pushing back, a rate limit, no connection or the run ending
// stops it, and ok is false: the next sign-in carries on (the backfill
// waits for a listing that ends). A folder that fails with an error of its
// own is passed over.
func (m *Messenger) listThreads(ctx context.Context, gen int, cli msgrClient) (ok bool) {
	m.gauge.mu.Lock()
	m.gauge.counts.ListPages, m.gauge.counts.ListComplete, m.gauge.counts.ListCapped = 0, false, false
	m.gauge.counts.FolderStops = map[string]int{}
	m.gauge.mu.Unlock()

	keep := !m.gate
	reach := !m.gate
	done := map[folderKey]bool{}
	total := 0
	if keep {
		total, _ = strconv.Atoi(m.value("listPages"))
	}
	complete, capped := true, false
	for {
		queue := m.folders(done, keep)
		if len(queue) == 0 {
			break
		}
		f := queue[0]
		done[f] = true
		stop, err := m.listFolder(ctx, gen, cli, f, reach, keep, &total)
		m.gauge.mu.Lock()
		m.gauge.counts.FolderStops[stop]++
		m.gauge.mu.Unlock()
		if stop == stopFailed {
			if !m.current(gen) || kindOf(err) != wire.Internal {
				m.listed(false, false)
				return false
			}
			complete = false // passed over
		}
		if keep {
			_ = m.keep("folderDone."+f.name(), "1")
			_ = m.keep("listPages", strconv.Itoa(total))
		}
		if stop == stopCapped {
			capped, complete = true, false
		}
		if total >= listPages || len(done) >= maxFolders {
			if len(m.folders(done, keep)) > 0 {
				capped, complete = true, false
			}
			break
		}
	}
	m.listed(complete, capped)
	if keep {
		_ = m.keep("listed", "1")
	}
	return true
}

// folders are the folders to list that are not done (this run, or kept as
// done), in order: sync group, then the inbox before other parents. Sync
// group 95 is listed only under the parents the messages page names.
func (m *Messenger) folders(done map[folderKey]bool, kept bool) []folderKey {
	m.gauge.mu.Lock()
	parents := []int64{topParent}
	config := map[int64]bool{topParent: true}
	for p := range m.gauge.listing.parents {
		if p != topParent {
			parents = append(parents, p)
		}
	}
	for p := range m.gauge.listing.configParents {
		config[p] = true
	}
	m.gauge.mu.Unlock()
	slices.Sort(parents[1:])
	var out []folderKey
	for _, g := range syncGroups {
		for _, p := range parents {
			f := folderKey{g, p}
			if done[f] || g != 1 && !config[p] || kept && m.value("folderDone."+f.name()) != "" {
				continue
			}
			out = append(out, f)
		}
	}
	return out
}

// folderRangeOf is where a folder's listing goes on: the range kept from an
// earlier page (when kept), else the one Meta last sent.
func (m *Messenger) folderRangeOf(f folderKey, kept bool) (folderRange, bool) {
	if kept {
		parts := strings.Split(m.value("folder."+f.name()), ",")
		if len(parts) == 3 {
			k, err1 := strconv.ParseInt(parts[0], 10, 64)
			a, err2 := strconv.ParseInt(parts[1], 10, 64)
			if err1 == nil && err2 == nil {
				return folderRange{minKey: k, minAt: a, more: parts[2] == "1"}, true
			}
		}
	}
	return m.rangeOf(f)
}

// keepFolderRange keeps where a folder's listing has got to.
func (m *Messenger) keepFolderRange(f folderKey, r folderRange) {
	more := "0"
	if r.more {
		more = "1"
	}
	_ = m.keep("folder."+f.name(), strconv.FormatInt(r.minKey, 10)+","+strconv.FormatInt(r.minAt, 10)+","+more)
}

// listFolder pages one folder from where its listing reaches (the top, when
// nothing has been listed of it) and says how it stopped; err is the
// failed request's.
func (m *Messenger) listFolder(ctx context.Context, gen int, cli msgrClient, f folderKey, reach, kept bool, total *int) (string, error) {
	ref := folderRange{minKey: 0, minAt: newestActivity, more: true}
	if r, ok := m.folderRangeOf(f, kept); ok {
		if !r.more {
			return stopComplete, nil
		}
		ref = r
	}
	yearAgo := m.clock.Now().Add(-yearReach).UnixMilli()
	for page := 0; page < folderPages; page++ {
		if *total >= listPages {
			return stopCapped, nil
		}
		task := &socket.FetchThreadsTask{
			ParentThreadKey:            f.parent,
			ReferenceThreadKey:         ref.minKey,
			ReferenceActivityTimestamp: ref.minAt,
			SyncGroup:                  int(f.group),
		}
		if f.group == 1 {
			// As the library's own page of sync group 1 does.
			task.Cursor = cli.GetCursor(1)
		}
		var tbl *table.LSTable
		err := m.request(ctx, gen, func(ctx context.Context) (err error) {
			if tbl, err = cli.ExecuteTasks(ctx, task); err == nil && tbl != nil {
				// The sync cursors a page carries, as FetchMoreThreads hands
				// them on (review M5).
				cli.PostHandlePublishResponse(tbl)
			}
			return err
		})
		*total++
		m.gauge.mu.Lock()
		m.gauge.counts.ListPages++
		m.gauge.mu.Unlock()
		if err != nil || !m.current(gen) {
			return stopFailed, err
		}
		if !m.safeIngest(tbl) {
			// A page that cannot be read fails its folder, as an error of
			// its own does (review N4).
			return stopFailed, errInternal
		}
		next, hasRange := rangeIn(tbl, f)
		oldestKey, oldestAt, rows := oldest(tbl)
		if !hasRange {
			if rows == 0 {
				return stopComplete, nil
			}
			next = folderRange{minKey: oldestKey, minAt: oldestAt, more: true}
		}
		if kept {
			m.keepFolderRange(f, next)
		}
		if reach && oldestAt > 0 && oldestAt < yearAgo {
			return stopYear, nil
		}
		if !next.more || next.minKey == ref.minKey && next.minAt == ref.minAt {
			return stopComplete, nil
		}
		ref = next
	}
	return stopCapped, nil
}

// rangeIn is the range a page gives for folder f.
func rangeIn(tbl *table.LSTable, f folderKey) (folderRange, bool) {
	if tbl == nil {
		return folderRange{}, false
	}
	for _, r := range tbl.LSUpsertSyncGroupThreadsRange {
		if r.SyncGroup == f.group && r.ParentThreadKey == f.parent {
			return folderRange{minKey: r.MinThreadKey, minAt: r.MinLastActivityTimestampMS, more: r.HasMoreBefore}, true
		}
	}
	if f.parent == topParent {
		for _, r := range tbl.LSUpsertInboxThreadsRange {
			if r.SyncGroup == f.group {
				return folderRange{minKey: r.MinThreadKey, minAt: r.MinLastActivityTimestampMs, more: r.HasMoreBefore}, true
			}
		}
	}
	return folderRange{}, false
}

// oldest is a page's least recently active thread, and how many threads it
// lists.
func oldest(tbl *table.LSTable) (key, at int64, rows int) {
	if tbl == nil {
		return 0, 0, 0
	}
	consider := func(k, a int64) {
		rows++
		if a > 0 && (at == 0 || a < at) {
			key, at = k, a
		}
	}
	for _, t := range tbl.LSDeleteThenInsertThread {
		consider(t.ThreadKey, t.LastActivityTimestampMs)
	}
	for _, t := range tbl.LSUpdateOrInsertThread {
		consider(t.ThreadKey, t.LastActivityTimestampMs)
	}
	return key, at, rows
}

// backfill fetches, at the first sign-in, up to threadHistoryPages pages of
// history for each chat active in the last 90 days that has an unencrypted
// thread, newest first, keeping each chat's count as it goes. Meta pushing
// back, a rate limit, no connection or the run ending stops it, and the
// next sign-in gives each chat only its remaining pages; a chat whose page
// fails with an error of its own, or never comes, is passed over.
func (m *Messenger) backfill(ctx context.Context, gen int, cli msgrClient) {
	if m.value("backfilled") != "" {
		return
	}
	chats, ok := m.activeChats()
	if !ok {
		return
	}
	for _, chat := range chats {
		if ctx.Err() != nil || !m.current(gen) {
			return
		}
		key, ok := m.threadOfChat(chat)
		if !ok {
			continue
		}
		n, _ := strconv.Atoi(m.value("bf." + chat))
		for n < threadHistoryPages {
			_, out, err := m.fetchPage(ctx, gen, cli, chat, key)
			if err != nil {
				if k := kindOf(err); !m.current(gen) || k != wire.Internal && k != wire.Timeout {
					return // carried on at the next sign-in
				}
				n = threadHistoryPages // passed over
				break
			}
			if out != pageMoved {
				n = threadHistoryPages
				break
			}
			n++
			_ = m.keep("bf."+chat, strconv.Itoa(n))
		}
		_ = m.keep("bf."+chat, strconv.Itoa(n))
	}
	if m.current(gen) {
		_ = m.keep("backfilled", "1")
	}
}

// activeChats are the chats active in the last 90 days, newest first; ok
// is false when the store could not list them.
func (m *Messenger) activeChats() (chats []string, ok bool) {
	cursor := ""
	since := m.clock.Now().Add(-activeWithin).UnixMilli()
	for {
		page, err := m.st.Chats(Name, since, cursor, 200)
		if err != nil {
			return nil, false
		}
		for _, c := range page.Items {
			chats = append(chats, c.ID)
		}
		if page.Cursor == "" {
			return chats, true
		}
		cursor = page.Cursor
	}
}

// FetchOlder fetches one page of a chat's history older than what the
// store holds, and tells Pllum when it added lines: of its encrypted
// backup first, once his history is restored (enc_history.go), until that has
// nothing older; then of its unencrypted thread (a group's, or the older
// thread of a one-to-one). A chat with neither, or none of its lines held,
// or held after a page that did not move, asks nothing; one the store does
// not hold is notFound.
func (m *Messenger) FetchOlder(ctx context.Context, chatID string) (int, error) {
	if err := m.blocked(); err != nil {
		return 0, err
	}
	gen, cli, runCtx := m.running()
	if cli == nil || runCtx == nil {
		return 0, errNotConnected
	}
	chat, ok := m.st.ChatFor(Name, chatID)
	if !ok {
		return 0, errNotFound
	}
	if m.encryptedEnded() {
		// Meta ended the encrypted session: no history is asked until a
		// new sign-in (review I1).
		return 0, errNotConnected
	}
	// It ends with the run, or with the caller.
	ctx2, cancel := context.WithCancel(runCtx)
	defer cancel()
	defer context.AfterFunc(ctx, cancel)()
	if m.encPaging(chat) {
		m.mu.Lock()
		fbid := m.fbid
		m.mu.Unlock()
		if !m.onDemand() {
			return 0, nil // the day's pages are spent (review M10)
		}
		added, _, asked, err := m.fetchEncPage(ctx2, gen, cli, fbid, chat)
		if !asked {
			m.giveOnDemand()
		}
		return added, err
	}
	key, ok := m.threadOfChat(chat)
	if !ok {
		return 0, nil
	}
	added, _, err := m.fetchPage(ctx2, gen, cli, chat, key)
	if added > 0 {
		m.changed()
	}
	return added, err
}

// What one page of history did.
type pageOutcome int

const (
	pageHeld  pageOutcome = iota // nothing asked: done, held, or nothing to anchor on
	pageMoved                    // a page that reached further back
	pageDone                     // Meta says there is nothing older
	pageStall                    // asked, and it did not move: held now
)

// fetchPage fetches one page of a thread's history older than the chat's
// cursor (else its oldest line of the thread held) and stores it.
func (m *Messenger) fetchPage(ctx context.Context, gen int, cli msgrClient, chat string, key int64) (added int, out pageOutcome, err error) {
	if m.encryptedEnded() {
		return 0, pageHeld, errNotConnected
	}
	lock := m.chatLock(chat)
	lock.Lock()
	defer lock.Unlock()
	if m.value("olderDone."+chat) != "" || m.held(chat) {
		return 0, pageHeld, nil
	}
	ms, id, ok := m.cursorOf(chat)
	if !ok {
		// As the library's connector: anchor on the oldest line held, or
		// ask nothing (review I1).
		if ms, id, ok = m.st.OldestLine(Name, chat, metaLinePrefix); !ok {
			return 0, pageHeld, nil
		}
	}
	task := &socket.FetchMessagesTask{ThreadKey: key, ReferenceTimestampMs: ms, ReferenceMessageId: id, SyncGroup: 1}
	ch := m.await(key)
	defer m.unawait(key, ch)
	var resp *table.LSTable
	err = m.request(ctx, gen, func(ctx context.Context) (err error) {
		task.Cursor = cli.GetCursor(1)
		resp, err = cli.ExecuteTasks(ctx, task)
		return err
	})
	if err != nil {
		// A page that fails to parse or times out in the library would fail
		// the same way at the next read: held, like a page that never came
		// (review N1). No connection, a rate limit or Meta pushing back is
		// not held here: the pace already holds the last two, and the first
		// never reached Meta.
		if k := kindOf(err); ctx.Err() == nil && (k == wire.Internal || k == wire.Timeout) {
			m.stall(chat)
		}
		return 0, pageHeld, err
	}
	if !m.safeIngest(resp) {
		m.stall(chat)
		return 0, pageStall, errInternal
	}
	note, got := m.wait(ctx, ch)
	switch {
	case !got && ctx.Err() != nil:
		return 0, pageHeld, errNotConnected
	case !got:
		m.stall(chat)
		return 0, pageStall, &wire.Error{Kind: wire.Timeout}
	case note.existing:
		// The library's connector ends its fetch on an existing range
		// either way: the start of the history when it gave no lines or
		// these flags, else not, and it does not ask again. One for
		// another reference is not this page's answer (review M2).
		if note.ts == ms && (note.lines == 0 || note.b2 && !note.b3) {
			m.olderDone(chat)
			return note.added, pageDone, nil
		}
		m.stall(chat)
		return note.added, pageStall, nil
	case !note.more:
		m.olderDone(chat)
		return note.added, pageDone, nil
	case note.minMs <= 0 || note.minMs >= ms:
		// It moved only if it reached back past what is held, as the
		// cursor moves (review N2).
		m.stall(chat)
		return note.added, pageStall, nil
	}
	return note.added, pageMoved, nil
}

// safeIngest ingests a page's answer, and reports false when ingesting it
// panicked (a page the helper cannot read): the panic ends there.
func (m *Messenger) safeIngest(tbl *table.LSTable) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	m.ingest(tbl, store.SourceSync)
	return true
}

// stall holds a chat whose page did not move: nothing is asked for it for
// stallHold, across restarts.
func (m *Messenger) stall(chat string) {
	_ = m.keep("stalled."+chat, strconv.FormatInt(m.clock.Now().Add(stallHold).UnixMilli(), 10))
}

// held reports whether a chat is held after a page that did not move.
func (m *Messenger) held(chat string) bool {
	until, err := strconv.ParseInt(m.value("stalled."+chat), 10, 64)
	return err == nil && m.clock.Now().UnixMilli() < until
}

// rangeNote is what a table said of one thread's history: the lines it
// gave (and how many were new), and its range.
type rangeNote struct {
	lines, added int
	minMs        int64
	minID        string
	more         bool
	existing     bool  // an LSUpdateExistingMessageRange, with its time and two flags
	ts           int64 // the existing range's time
	b2, b3       bool
}

// await registers a wait for a thread's next range.
func (m *Messenger) await(key int64) chan rangeNote {
	ch := make(chan rangeNote, 8)
	m.memo.mu.Lock()
	m.memo.waiters[key] = ch
	m.memo.mu.Unlock()
	return ch
}

func (m *Messenger) unawait(key int64, ch chan rangeNote) {
	m.memo.mu.Lock()
	if m.memo.waiters[key] == ch {
		delete(m.memo.waiters, key)
	}
	m.memo.mu.Unlock()
}

// answer hands a thread's range to the FetchOlder waiting on it, if any.
func (m *Messenger) answer(key int64, note rangeNote) {
	m.memo.mu.Lock()
	ch := m.memo.waiters[key]
	m.memo.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- note:
	default:
	}
}

// wait is the range a page brought, or false after pageWait or when ctx
// ends.
func (m *Messenger) wait(ctx context.Context, ch chan rangeNote) (rangeNote, bool) {
	t := time.NewTimer(m.pageWait)
	defer t.Stop()
	select {
	case n := <-ch:
		return n, true
	case <-t.C:
	case <-ctx.Done():
	}
	return rangeNote{}, false
}

// chatLock is the lock that keeps one history fetch per chat at a time.
func (m *Messenger) chatLock(chat string) *sync.Mutex {
	m.memo.mu.Lock()
	defer m.memo.mu.Unlock()
	l, ok := m.memo.locks[chat]
	if !ok {
		l = &sync.Mutex{}
		m.memo.locks[chat] = l
	}
	return l
}

// running is the current run: its number, client and context; cli is nil
// when no client is signed in.
func (m *Messenger) running() (gen int, cli msgrClient, ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runs.Gen, m.cli, m.runs.Ctx()
}

// changed tells Pllum there is something to re-read: at once, or when 500
// ms have passed since the last time.
func (m *Messenger) changed() { m.changes.Tell(m.clock, m.emit) }
