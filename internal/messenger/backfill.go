package messenger

import (
	"cmp"
	"context"
	"slices"
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
//   - at the first sign-in (and at every sign-in with --gate), every folder
//     a Messenger client shows, page by page with FetchThreadsTask: each
//     parent the messages page names (the inbox is -1; Marketplace and
//     communities have their own) and every folder thread a listing shows,
//     in sync group 1 (the inbox) and sync group 95 (message requests),
//     until Meta says the folder has no more, its oldest thread stops
//     moving, a thread older than 365 days (not with --gate, which counts
//     them), or 40 pages; 120 pages in all;
//   - then, for each unencrypted thread active in the last 90 days, up to 5
//     pages of its older lines (FetchMessagesTask), newest thread first;
//   - when Pllum reads to the oldest line of a chat (FetchOlder), one more
//     page of its unencrypted thread, older than the last.
//
// messagix's own FetchMoreThreads pages only sync group 1, from whichever
// folder's range Meta sent last, and its first fetch of every folder on
// connect drops Meta's answer; so the helper sends FetchThreadsTask itself,
// one folder at a time, and reads each answer's own range.
//
// A page of history comes back on the main socket, not in the task's
// answer (the library's connector waits for it the same way): FetchOlder
// waits up to pageWait for the thread's range, and a page that never comes
// is a timeout, with nothing marked.

// folderKey is one folder a listing pages: a sync group and a parent
// thread key.
type folderKey struct{ group, parent int64 }

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
	threadHistoryPages = 5   // pages of one thread's history at the first sign-in
	yearReach          = 365 * 24 * time.Hour
	activeWithin       = 90 * 24 * time.Hour
	topParent          = -1 // the inbox
	// pageWaitDefault is how long a page of history may take to come back.
	pageWaitDefault = 20 * time.Second
	// newestActivity is the reference a folder's first page is asked from.
	newestActivity = 9999999999999
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
// got. A failed request (Meta pushing back, no connection) ends it, and ok
// is false: the first sign-in's backfill then waits for a listing that
// ends.
func (m *Messenger) listThreads(ctx context.Context, gen int, cli msgrClient) (ok bool) {
	m.cmu.Lock()
	m.counts.ListPages, m.counts.ListComplete, m.counts.ListCapped = 0, false, false
	m.counts.FolderStops = map[string]int{}
	m.cmu.Unlock()

	reach := !m.gate
	done := map[folderKey]bool{}
	total, complete, capped := 0, true, false
	for {
		queue := m.folders(done)
		if len(queue) == 0 {
			break
		}
		f := queue[0]
		done[f] = true
		stop := m.listFolder(ctx, gen, cli, f, reach, &total)
		m.cmu.Lock()
		m.counts.FolderStops[stop]++
		m.cmu.Unlock()
		switch stop {
		case stopFailed:
			m.listed(false, false)
			return false
		case stopCapped:
			capped, complete = true, false
		}
		if total >= listPages || len(done) >= maxFolders {
			if len(m.folders(done)) > 0 {
				capped, complete = true, false
			}
			break
		}
	}
	m.listed(complete, capped)
	return true
}

// folders are the folders to list that are not done, in order: sync group,
// then the inbox before other parents.
func (m *Messenger) folders(done map[folderKey]bool) []folderKey {
	m.cmu.Lock()
	parents := []int64{topParent}
	for p := range m.listing.parents {
		if p != topParent {
			parents = append(parents, p)
		}
	}
	m.cmu.Unlock()
	slices.Sort(parents[1:])
	var out []folderKey
	for _, g := range syncGroups {
		for _, p := range parents {
			if f := (folderKey{g, p}); !done[f] {
				out = append(out, f)
			}
		}
	}
	return out
}

// listFolder pages one folder from where its listing reaches (the top, when
// nothing has been listed of it) and says how it stopped.
func (m *Messenger) listFolder(ctx context.Context, gen int, cli msgrClient, f folderKey, reach bool, total *int) string {
	ref := folderRange{minKey: 0, minAt: newestActivity, more: true}
	if r, ok := m.rangeOf(f); ok {
		if !r.more {
			return stopComplete
		}
		ref = r
	}
	yearAgo := m.now().Add(-yearReach).UnixMilli()
	for page := 0; page < folderPages; page++ {
		if *total >= listPages {
			return stopCapped
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
			tbl, err = cli.ExecuteTasks(ctx, task)
			return err
		})
		*total++
		m.cmu.Lock()
		m.counts.ListPages++
		m.cmu.Unlock()
		if err != nil || !m.current(gen) {
			return stopFailed
		}
		m.ingest(tbl, store.SourceSync)
		next, hasRange := rangeIn(tbl, f)
		oldestKey, oldestAt, rows := oldest(tbl)
		if !hasRange {
			if rows == 0 {
				return stopComplete
			}
			next = folderRange{minKey: oldestKey, minAt: oldestAt, more: true}
		}
		if reach && oldestAt > 0 && oldestAt < yearAgo {
			return stopYear
		}
		if !next.more || next.minKey == ref.minKey && next.minAt == ref.minAt {
			return stopComplete
		}
		ref = next
	}
	return stopCapped
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
// history for each unencrypted thread active in the last 90 days, newest
// first. It stops when the run does or Meta pushes back, and carries on at
// the next sign-in; a thread whose page never comes is passed over.
func (m *Messenger) backfill(ctx context.Context, gen int, cli msgrClient) {
	if m.value("backfilled") != "" {
		return
	}
	for _, t := range m.activeThreads(m.now().Add(-activeWithin).UnixMilli()) {
		if ctx.Err() != nil || !m.current(gen) {
			return
		}
		chat, ok := m.chatOfKey(t.key)
		if !ok {
			continue
		}
		if _, _, err := m.fetchHistory(ctx, gen, cli, chat, t.key, threadHistoryPages); err != nil && kindOf(err) != wire.Timeout {
			return
		}
	}
	if m.current(gen) {
		_ = m.keep("backfilled", "1")
	}
}

// activeThread is an unencrypted thread to backfill.
type activeThread struct {
	key, lastMs int64
}

// activeThreads are the unencrypted threads listed that are chats (or
// aliases of one) and were active since ms, newest first.
func (m *Messenger) activeThreads(since int64) []activeThread {
	m.cmu.Lock()
	var out []activeThread
	for t, s := range m.listing.threads {
		if t.encrypted || s.folder == folderSpam || s.lastMs < since {
			continue
		}
		if _, ok := placeThread(t.key, s.tt); !ok {
			continue
		}
		out = append(out, activeThread{key: t.key, lastMs: s.lastMs})
	}
	m.cmu.Unlock()
	slices.SortFunc(out, func(a, b activeThread) int { return cmp.Or(cmp.Compare(b.lastMs, a.lastMs), cmp.Compare(a.key, b.key)) })
	return out
}

// FetchOlder fetches one page of a chat's history older than what the
// store holds, from its unencrypted thread (a group's, or the older thread
// of a one-to-one), and tells Pllum when it added lines. A chat with no
// unencrypted thread asks nothing (its encrypted history is B2's); one the
// store does not hold is notFound.
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
	key, ok := m.threadOfChat(chat)
	if !ok {
		return 0, nil
	}
	// It ends with the run, or with the caller.
	ctx2, cancel := context.WithCancel(runCtx)
	defer cancel()
	defer context.AfterFunc(ctx, cancel)()
	added, _, err := m.fetchHistory(ctx2, gen, cli, chat, key, 1)
	if added > 0 {
		m.changed()
	}
	return added, err
}

// fetchHistory fetches up to max pages of a thread's history older than
// the chat's cursor (from now, when nothing has been fetched) and stores
// them; complete is true once Meta says there is nothing older.
func (m *Messenger) fetchHistory(ctx context.Context, gen int, cli msgrClient, chat string, key int64, max int) (added int, complete bool, err error) {
	lock := m.chatLock(chat)
	lock.Lock()
	defer lock.Unlock()
	for i := 0; i < max; i++ {
		if m.value("olderDone."+chat) != "" {
			return added, true, nil
		}
		ms, id, ok := m.cursorOf(chat)
		if !ok {
			ms, id = m.now().UnixMilli(), ""
		}
		task := &socket.FetchMessagesTask{ThreadKey: key, ReferenceTimestampMs: ms, ReferenceMessageId: id, SyncGroup: 1}
		ch := m.await(key)
		var resp *table.LSTable
		err := m.request(ctx, gen, func(ctx context.Context) (err error) {
			task.Cursor = cli.GetCursor(1)
			resp, err = cli.ExecuteTasks(ctx, task)
			return err
		})
		if err != nil {
			m.unawait(key, ch)
			return added, false, err
		}
		m.ingest(resp, store.SourceSync)
		note, got := m.wait(ctx, ch)
		m.unawait(key, ch)
		if !got {
			if ctx.Err() != nil {
				return added, false, errNotConnected
			}
			return added, false, &wire.Error{Kind: wire.Timeout}
		}
		added += note.added
		if note.existing {
			// The library's connector: an existing range answering with no
			// lines, or with lines and these flags, is the start.
			if note.lines == 0 || note.b2 && !note.b3 {
				m.olderDone(chat)
				return added, true, nil
			}
			continue
		}
		if !note.more {
			return added, true, nil // reached() has completed it
		}
		if note.minMs == ms && note.minID == id {
			return added, false, nil // it did not move: nothing more now
		}
	}
	return added, false, nil
}

// rangeNote is what a table said of one thread's history: the lines it
// gave (and how many were new), and its range.
type rangeNote struct {
	lines, added int
	minMs        int64
	minID        string
	more         bool
	existing     bool // an LSUpdateExistingMessageRange, with its two flags
	b2, b3       bool
}

// await registers a wait for a thread's next range.
func (m *Messenger) await(key int64) chan rangeNote {
	ch := make(chan rangeNote, 8)
	m.wmu.Lock()
	m.waiters[key] = ch
	m.wmu.Unlock()
	return ch
}

func (m *Messenger) unawait(key int64, ch chan rangeNote) {
	m.wmu.Lock()
	if m.waiters[key] == ch {
		delete(m.waiters, key)
	}
	m.wmu.Unlock()
}

// answer hands a thread's range to the FetchOlder waiting on it, if any.
func (m *Messenger) answer(key int64, note rangeNote) {
	m.wmu.Lock()
	ch := m.waiters[key]
	m.wmu.Unlock()
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
	m.lkmu.Lock()
	defer m.lkmu.Unlock()
	l, ok := m.locks[chat]
	if !ok {
		l = &sync.Mutex{}
		m.locks[chat] = l
	}
	return l
}

// running is the current run: its number, client and context; cli is nil
// when no client is signed in.
func (m *Messenger) running() (gen int, cli msgrClient, ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen, m.cli, m.runCtx
}

// changedEvery is the least time between two changed events.
const changedEvery = 500 * time.Millisecond

// changed tells Pllum there is something to re-read: at once, or when 500
// ms have passed since the last time.
func (m *Messenger) changed() {
	m.chmu.Lock()
	defer m.chmu.Unlock()
	if m.chPending {
		return
	}
	now := time.Now()
	if wait := m.chLast.Add(changedEvery).Sub(now); wait > 0 && !m.chLast.IsZero() {
		m.chPending = true
		time.AfterFunc(wait, func() {
			m.chmu.Lock()
			m.chPending = false
			m.chLast = time.Now()
			m.chmu.Unlock()
			m.emit(map[string]any{"event": "changed"})
		})
		return
	}
	m.chLast = now
	m.emit(map[string]any{"event": "changed"})
}
