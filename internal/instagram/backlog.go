package instagram

import (
	"context"
	"time"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue (network.Backlog): in the background, one page at a
// time, it pages each chat whose history is open back to its start, the
// newest chat first, through fetchPages and so through request (the pace,
// the kept block, Traffic). It runs only with WithHistoryQueue.

// WithHistoryQueue makes the network's history queue; without it there is
// none. Call it before Start.
func (g *Instagram) WithHistoryQueue() *Instagram {
	g.bq = &network.Backlog{
		Name:    Name,
		Dir:     g.dir,
		Clock:   networkClock{g},
		Values:  g.values,
		Traffic: &g.traffic,
		Live:    func() bool { return g.Status().State == network.Live },
		Ready:   func() bool { return g.value("backfilled") != "" },
		Blocked: g.pace.Blocked,
		Kind:    kindOf,
		Next:    g.backlogNext,
		Left:    g.openChats,
	}
	g.bqGen = -1
	return g
}

// ScrolledBack is Pllum reading older lines (network.ScrollBack): the queue
// steps aside for a while.
func (g *Instagram) ScrolledBack() {
	if g.bq != nil {
		g.bq.Yield()
	}
}

var _ network.ScrollBack = (*Instagram)(nil)

// startQueue starts the queue's Run for run gen, once, under the run's
// context; g.mu is held.
func (g *Instagram) startQueue(gen int) {
	ctx := g.runs.Ctx()
	if g.bq == nil || g.bqGen == gen || ctx == nil {
		return
	}
	g.bqGen = gen
	g.work.Add(1)
	g.bqRuns.Add(1)
	go func() {
		defer g.work.Done()
		defer g.bqRuns.Add(-1)
		g.bq.Run(ctx)
	}()
}

// queuePushedBack tells the queue that Meta pushed back with kind, for
// live work as for its own: it holds a day.
func (g *Instagram) queuePushedBack(kind wire.Kind) {
	if g.bq == nil {
		return
	}
	switch kind {
	case wire.RateLimited, wire.Checkpoint, meta.ReconnectNeeded, meta.Banned:
		g.bq.PushedBack(kind)
	}
}

// backlogNext pages the newest open chat the queue has not passed over by
// one page, older than what the store holds, under ctx (the queue's, so
// its one request is counted as the queue's).
func (g *Instagram) backlogNext(ctx context.Context, passed func(string) bool) network.Step {
	gen, cli, runCtx := g.running()
	if cli == nil || runCtx == nil {
		return network.Step{Err: errNotConnected}
	}
	chat, igChatID, err := g.nextOpenChat(passed)
	if err != nil {
		return network.Step{Err: err}
	}
	if chat == "" {
		return network.Step{Outcome: network.StepNothing}
	}
	before, asked := g.value("cursor."+chat), g.traffic.BacklogAsked()
	_, complete, err := g.fetchPages(ctx, gen, cli, chat, igChatID, 1, false)
	switch {
	case err != nil:
		return network.Step{Outcome: network.StepPassed, Chat: chat, Err: err}
	case g.traffic.BacklogAsked() == asked:
		// Nothing was asked: a scroll-back finished the chat while this
		// step waited for it (fetchPages found it complete). Not a page,
		// and no fault.
		return network.Step{Outcome: network.StepPassed, Chat: chat}
	case complete:
		g.changed()
		return network.Step{Outcome: network.StepStart, Chat: chat}
	case g.value("cursor."+chat) != before:
		return network.Step{Outcome: network.StepMoved, Chat: chat}
	}
	return network.Step{Outcome: network.StepPassed, Chat: chat}
}

// nextOpenChat is the newest chat whose history is open, not passed over,
// and a thread Meta has listed (a chat only in Beeper's copy has none); ""
// when there is none.
func (g *Instagram) nextOpenChat(passed func(string) bool) (chat, igChatID string, err error) {
	cursor := ""
	for {
		ids, next, err := g.st.ChatIDs(Name, store.ChatFilter{HistoryOpen: true}, cursor, 200)
		if err != nil {
			return "", "", err
		}
		for _, id := range ids {
			if passed(id) || g.historyComplete(id) {
				continue
			}
			if igChatID, ok := g.threadOfChat(id); ok {
				return id, igChatID, nil
			}
		}
		if next == "" {
			return "", "", nil
		}
		cursor = next
	}
}

// openChats is how many chats' history is open, for the queue's day line.
func (g *Instagram) openChats() (int, bool) {
	n, cursor := 0, ""
	for {
		ids, next, err := g.st.ChatIDs(Name, store.ChatFilter{HistoryOpen: true}, cursor, 200)
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
type networkClock struct{ g *Instagram }

func (c networkClock) Now() time.Time { return c.g.clock.Now() }

func (c networkClock) AfterFunc(d time.Duration, f func()) func() bool {
	return c.g.clock.AfterFunc(d, f)
}

func (c networkClock) Sleep(ctx context.Context, d time.Duration) error {
	return c.g.clock.Sleep(ctx, d)
}
