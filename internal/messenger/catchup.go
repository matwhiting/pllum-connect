package messenger

import (
	"context"
	"strconv"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Catching up from his restored backup on what the encrypted socket didn't
// bring: a message Pllum couldn't read (or got no copy of) comes back from
// the newest page of its chat's backup, which his phone keeps, and takes
// its undecryptable or unavailable line over by its ID.
//
//   - fillDelay after such a line from someone, its chat's newest page is
//     read, once for every such line in that wait (a re-send may have
//     landed first);
//   - refillDelay after it, the newest page is read again (his phone may
//     not have backed the message up by fillDelay), once for every such
//     line in that wait, counted apart from fillDelay's, and only while the
//     chat still shows a line from someone that couldn't be read (one taken
//     over or settled asks nothing);
//   - once a day, after the first restore's read, the newest page of each
//     encrypted chat active in the last catchUpWithin, at most catchUpChats
//     of them, newest first ("ecatchup" holds when it last ran);
//   - each newest page counts in the day's on-demand pages (onDemandPages),
//     goes through the pace, and leaves the older pages' cursor alone; a
//     held chat, a paused backup or an ended session asks nothing.
const (
	fillDelay     = 2 * time.Minute
	refillDelay   = time.Hour
	catchUpWithin = 3 * 24 * time.Hour
	catchUpChats  = 30
)

// fillLater reads chat's newest page of the backup fillDelay from now,
// unless that is already due, and again refillDelay from now, unless that
// is already due, if the chat still shows a line that couldn't be read.
func (m *Messenger) fillLater(chat string) {
	m.fillAfter(chat, fillDelay, &m.filling, false)
	m.fillAfter(chat, refillDelay, &m.refilling, true)
}

// fillAfter reads chat's newest page of the backup d from now, unless due
// (filling or refilling, under m.fillmu) already marks one for it; with
// unreadOnly, only if the chat then still shows a line that couldn't be
// read.
func (m *Messenger) fillAfter(chat string, d time.Duration, due *map[string]bool, unreadOnly bool) {
	m.fillmu.Lock()
	if *due == nil {
		*due = map[string]bool{}
	}
	if (*due)[chat] {
		m.fillmu.Unlock()
		return
	}
	(*due)[chat] = true
	m.fillmu.Unlock()
	done := func() {
		m.fillmu.Lock()
		delete(*due, chat)
		m.fillmu.Unlock()
	}
	m.afterFunc(d, func() {
		m.mu.Lock()
		gen, cli, runCtx, fbid := m.gen, m.cli, m.runCtx, m.fbid
		if m.stopped || cli == nil || runCtx == nil {
			m.mu.Unlock()
			done()
			return
		}
		m.work.Add(1)
		m.mu.Unlock()
		go func() {
			defer m.work.Done()
			defer done()
			defer func() { _ = recover() }()
			if unreadOnly && !m.st.HoldsUnreadable(Name, chat) {
				return // taken over or settled since: no page wasted
			}
			_, _, _ = m.fetchEncNewest(runCtx, gen, cli, fbid, chat)
		}()
	})
}

// catchUp reads the newest page of each encrypted chat active lately, once
// a day.
func (m *Messenger) catchUp(ctx context.Context, gen int, cli msgrClient, fbid int64) {
	if m.gate || m.HistoryState() != historyRestored {
		return
	}
	now := m.now()
	if last, err := strconv.ParseInt(m.value("ecatchup"), 10, 64); err == nil && now.UnixMilli()-last < (24*time.Hour).Milliseconds() && now.UnixMilli() >= last {
		return
	}
	page, err := m.st.Chats(Name, now.Add(-catchUpWithin).UnixMilli(), "", catchUpChats)
	if err != nil {
		return
	}
	for _, c := range page.Items {
		if ctx.Err() != nil || !m.current(gen) || m.encPaused() || m.encryptedEnded() || m.HistoryState() != historyRestored {
			return // asked again at the next run
		}
		if !m.hasEncryptedSide(c.ID) {
			continue
		}
		if _, _, err := m.fetchEncNewest(ctx, gen, cli, fbid, c.ID); err != nil {
			if k := kindOf(err); !m.current(gen) || ctx.Err() != nil || k != wire.Internal && k != wire.Timeout {
				return
			}
		}
	}
	_ = m.keep("ecatchup", strconv.FormatInt(now.UnixMilli(), 10))
}

// fetchEncNewest reads the newest page of a chat's backup and stores its
// lines, a line already held under its ID being that row. The older pages'
// cursor doesn't move. asked reports whether the request reached Meta.
func (m *Messenger) fetchEncNewest(ctx context.Context, gen int, cli msgrClient, fbid int64, chat string) (added int, asked bool, err error) {
	if m.encryptedEnded() {
		return 0, false, errNotConnected
	}
	lock := m.chatLock(chat)
	lock.Lock()
	defer lock.Unlock()
	if m.encHeld(chat) || m.encPaused() {
		return 0, false, nil
	}
	device := m.historyDevice()
	if device == nil || device.ID == "" || fbid == 0 || m.HistoryState() != historyRestored {
		return 0, false, nil
	}
	if !m.onDemand() {
		return 0, false, nil // the day's pages are spent
	}
	thread, group := encThread(chat)
	ref := m.now().Add(time.Minute).UnixMilli()
	h := cli.History()
	var page *securehistory.MessageRange
	var answer error
	err = m.request(ctx, gen, func(ctx context.Context) (err error) {
		asked = true
		page, err = h.FetchMessages(ctx, device, thread, true, ref, encPageSize)
		answer = err
		return err
	})
	if !asked {
		m.giveOnDemand()
	}
	if err != nil {
		if !asked {
			return 0, false, err
		}
		switch backupAnswer(answer) {
		case answerNoThread:
			m.encAnswered()
			return 0, true, nil
		case answerGone:
			m.encAnswered()
			m.dropHistory()
			return 0, true, errNotFound
		}
		if k := kindOf(err); ctx.Err() == nil && (k == wire.Internal || k == wire.Timeout) {
			m.encStall(chat)
			m.encFailed()
		}
		return 0, true, err
	}
	if page == nil || page.Range == nil {
		m.encStall(chat)
		m.encFailed()
		return 0, true, errInternal
	}
	device = m.saveEpochs(device, page)
	msgs := h.Decrypt(ctx, page, device, thread, fbid, group)
	added, ok := m.storeArchive(chat, thread, group, fbid, msgs)
	if !ok {
		m.encStall(chat)
		m.encFailed()
		return added, true, errInternal
	}
	m.encAnswered()
	return added, true, nil
}
