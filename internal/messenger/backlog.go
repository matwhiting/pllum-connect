package messenger

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue (network.Backlog): in the background, one page at a
// time, it pages each chat whose history is open back to its start, the
// newest chat first: its encrypted backup while that has more (never
// spending FetchOlder's day's pages), then its unencrypted thread, through
// olderStep and so through request (the pace, the kept block, Traffic). It
// runs only with WithHistoryQueue, and never with the gate.

// WithHistoryQueue makes the network's history queue; without it there is
// none. Call it before Start.
func (m *Messenger) WithHistoryQueue() *Messenger {
	m.bq = &network.Backlog{
		Name:    Name,
		Dir:     m.dir,
		Clock:   networkClock{m},
		Values:  m.values,
		Traffic: &m.traffic,
		Live:    func() bool { return m.Status().State == network.Live },
		Ready:   m.queueReady,
		Blocked: m.pace.Blocked,
		Kind:    kindOf,
		Next:    m.backlogNext,
		Left:    m.openChats,
	}
	m.bqGen = -1
	return m
}

// ScrolledBack is Pllum reading older lines (network.ScrollBack): the queue
// steps aside for a while.
func (m *Messenger) ScrolledBack() {
	if m.bq != nil {
		m.bq.Yield()
	}
}

var _ network.ScrollBack = (*Messenger)(nil)

// queueReady: the first sign-in's own history is done (the listing, the
// threads' backfill, and the backup's first read once his history is
// restored). Never with the gate.
func (m *Messenger) queueReady() bool {
	if m.gate || m.value("listed") == "" || m.value("backfilled") == "" {
		return false
	}
	return m.HistoryState() != historyRestored || m.value("ebackfilled") != ""
}

// startQueue starts the queue's Run for run gen, once, under the run's
// context; never with the gate. m.mu is held.
func (m *Messenger) startQueue(gen int) {
	ctx := m.runs.Ctx()
	if m.bq == nil || m.gate || m.bqGen == gen || ctx == nil {
		return
	}
	m.bqGen = gen
	m.work.Add(1)
	m.bqRuns.Add(1)
	go func() {
		defer m.work.Done()
		defer m.bqRuns.Add(-1)
		m.bq.Run(ctx)
	}()
}

// queuePushedBack tells the queue that Meta pushed back with kind, for live
// work as for its own (a send, a restore, either socket): it holds a day.
func (m *Messenger) queuePushedBack(kind wire.Kind) {
	if m.bq == nil {
		return
	}
	switch kind {
	case wire.RateLimited, wire.Checkpoint, meta.ReconnectNeeded, meta.Banned, wire.RegistrationUnclear, wire.Unsupported:
		m.bq.PushedBack(kind)
	}
}

// queueBackupPages is the most pages of his backup the history queue reads
// in a local day (ruling 8), within the queue's own BacklogDayCap: once
// they are spent it pages only threads that day. Kept in hq.ebp as
// <YYYY-MM-DD>\n<pages>.
const queueBackupPages = 300

// backupDay is the local day the queue's backup pages are counted in, and
// how many it has had.
func (m *Messenger) backupDay() (day string, n int) {
	day = m.clock.Now().Local().Format(time.DateOnly)
	d, c, ok := strings.Cut(m.value("hq.ebp"), "\n")
	if !ok || d != day {
		return day, 0
	}
	n, err := strconv.Atoi(c)
	if err != nil || n < 0 {
		return day, queueBackupPages // unreadable: none left today
	}
	return day, n
}

// takeBackupPage takes one of the day's backup pages for the queue: false
// when they are spent, or the count can't be kept (then nothing is asked).
// giveBackupPage puts back one not asked.
func (m *Messenger) takeBackupPage() bool {
	day, n := m.backupDay()
	if n >= queueBackupPages {
		return false
	}
	return m.keep("hq.ebp", day+"\n"+strconv.Itoa(n+1)) == nil
}

func (m *Messenger) giveBackupPage() {
	if day, n := m.backupDay(); n > 0 {
		_ = m.keep("hq.ebp", day+"\n"+strconv.Itoa(n-1))
	}
}

// backlogNext pages the newest open chat the queue has not passed over and
// can ask now by one page, under ctx (the queue's, so its request is
// counted as the queue's). A step that asked nothing (the chat finished or
// held by a scroll-back while it waited for the chat) is passed over; any
// other step reports what it did.
func (m *Messenger) backlogNext(ctx context.Context, passed func(string) bool) network.Step {
	gen, cli, runCtx := m.running()
	if cli == nil || runCtx == nil || m.encryptedEnded() {
		return network.Step{Err: errNotConnected}
	}
	chat, err := m.nextQueueChat(passed)
	if err != nil {
		return network.Step{Err: err}
	}
	if chat == "" {
		return network.Step{Outcome: network.StepNothing}
	}
	backup := m.encPaging(chat)
	if backup && !m.takeBackupPage() {
		return network.Step{Outcome: network.StepPassed, Chat: chat}
	}
	_, out, asked, err := m.olderStep(ctx, gen, cli, chat, false)
	if backup && !asked {
		m.giveBackupPage()
	}
	switch {
	case err != nil:
		return network.Step{Outcome: network.StepPassed, Chat: chat, Err: err}
	case out == pageMoved:
		return network.Step{Outcome: network.StepMoved, Chat: chat}
	case out == pageDone:
		m.changed()
		return network.Step{Outcome: network.StepStart, Chat: chat}
	}
	// pageStall (asked, did not move: held now) or pageHeld (nothing asked:
	// finished, held or with nothing to anchor on by the time it had the
	// chat).
	return network.Step{Outcome: network.StepPassed, Chat: chat}
}

// nextQueueChat is the newest chat whose history is open, not passed over,
// and with a side the queue can ask now; "" when there is none.
func (m *Messenger) nextQueueChat(passed func(string) bool) (string, error) {
	cursor := ""
	for {
		ids, next, err := m.st.ChatIDs(Name, store.ChatFilter{HistoryOpen: true}, cursor, 200)
		if err != nil {
			return "", err
		}
		for _, id := range ids {
			if !passed(id) && m.askable(id) {
				return id, nil
			}
		}
		if next == "" {
			return "", nil
		}
		cursor = next
	}
}

// askable reports whether olderStep would ask Meta for chat now: its
// backup while that has more (not held, the whole backup not paused), else
// its unencrypted thread (not done, not held, with a line to anchor on).
func (m *Messenger) askable(chat string) bool {
	if m.encPaging(chat) {
		_, n := m.backupDay()
		return !m.encHeld(chat) && !m.encPaused() && n < queueBackupPages
	}
	if _, ok := m.threadOfChat(chat); !ok || m.value("olderDone."+chat) != "" || m.held(chat) {
		return false
	}
	if _, _, ok := m.cursorOf(chat); ok {
		return true
	}
	_, _, ok := m.st.OldestLine(Name, chat, metaLinePrefix)
	return ok
}

// openChats is how many chats' history is open, for the queue's day line.
func (m *Messenger) openChats() (int, bool) {
	n, cursor := 0, ""
	for {
		ids, next, err := m.st.ChatIDs(Name, store.ChatFilter{HistoryOpen: true}, cursor, 200)
		if err != nil {
			return 0, false
		}
		n += len(ids)
		if next == "" {
			return n, true
		}
		cursor = next
	}
}

// networkClock is the network's clock as it is when used, so a test's
// clock, set after New, is the one the queue runs on.
type networkClock struct{ m *Messenger }

func (c networkClock) Now() time.Time { return c.m.clock.Now() }

func (c networkClock) AfterFunc(d time.Duration, f func()) func() bool {
	return c.m.clock.AfterFunc(d, f)
}

func (c networkClock) Sleep(ctx context.Context, d time.Duration) error {
	return c.m.clock.Sleep(ctx, d)
}
