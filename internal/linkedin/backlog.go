package linkedin

import (
	"context"
	"io"
	"time"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The history queue (network.Backlog): in the background, one page at a
// time, it pages each conversation the worker stopped at 730 days (histDone
// "reach", or an older version's "1" with a cursor kept) on to its start,
// newest first, through pageHistory without the reach, and so through
// request (paced, refused during a pause, Traffic). It runs only with
// Options.HistoryQueue, once the worker has nothing within reach.

// newBacklog is the network's history queue.
func (l *LinkedIn) newBacklog() *network.Backlog {
	return &network.Backlog{
		Name:    Name,
		Dir:     l.dir,
		Clock:   networkClock{l},
		Values:  l.values,
		Traffic: &l.traffic,
		Live:    func() bool { return l.Status().State == network.Live },
		Ready:   l.queueReady,
		Blocked: func() (wire.Kind, bool) {
			_, blocked := l.pause.blocked()
			return wire.RateLimited, blocked
		},
		Kind: kindOf,
		Next: l.backlogNext,
		Left: l.openChats,
	}
}

// ScrolledBack is Pllum reading older lines (network.ScrollBack): the queue
// steps aside for a while.
func (l *LinkedIn) ScrolledBack() {
	if l.bq != nil {
		l.bq.Yield()
	}
}

var _ network.ScrollBack = (*LinkedIn)(nil)

// queueReady: the listing is done, and the worker has nothing left to do
// within reach: no chat whose history it pages (set aside or not), and no
// chat marked fresh.
func (l *LinkedIn) queueReady() bool {
	if l.listingOwed() {
		return false
	}
	if _, ok := l.nextChat(store.ChatFilter{Writable: true, HistoryOpen: true}, func(chat string) bool {
		return l.value("histDone."+chat) == ""
	}); ok {
		return false
	}
	_, fresh := l.nextChat(store.ChatFilter{Writable: true}, func(chat string) bool {
		return l.value("fresh."+chat) != ""
	})
	return !fresh
}

// startQueue starts the queue's Run for run gen, once, under the run's
// context. l.mu is held.
func (l *LinkedIn) startQueue(gen int) {
	ctx := l.runs.Ctx()
	if l.bq == nil || l.bqGen == gen || ctx == nil {
		return
	}
	l.bqGen = gen
	l.work.Add(1)
	l.bqRuns.Add(1)
	go func() {
		defer l.work.Done()
		defer l.bqRuns.Add(-1)
		l.bq.Run(ctx)
	}()
}

// queuePushedBack tells the queue LinkedIn pushed back (a pause status, on
// any request, live work's included; a media CDN's 429 or 999) or ended the
// session: it holds a day.
func (l *LinkedIn) queuePushedBack(kind wire.Kind) {
	if l.bq != nil {
		l.bq.PushedBack(kind)
	}
}

// backlogNext pages the newest conversation the worker stopped at its reach
// that the queue has not passed over by one page, under ctx (the queue's,
// so its request is counted as the queue's). A conversation LinkedIn won't
// page, or whose cursor did not move, is marked so and never asked again.
func (l *LinkedIn) backlogNext(ctx context.Context, passed func(string) bool) network.Step {
	l.mu.Lock()
	gen, cli, runCtx := l.runs.Gen, l.cli, l.runs.Ctx()
	l.mu.Unlock()
	if cli == nil || runCtx == nil {
		return network.Step{Err: errNotConnected}
	}
	chat, ok := l.nextChat(store.ChatFilter{Writable: true, HistoryOpen: true}, func(chat string) bool {
		if passed(chat) || l.passedOver(chat) {
			return false
		}
		switch l.value("histDone." + chat) {
		case histReach:
			return true
		case "1":
			return l.value("cursor."+chat) != ""
		}
		return false
	})
	if !ok {
		return network.Step{Outcome: network.StepNothing}
	}
	out, err := l.pageHistory(ctx, gen, cli, chat, false)
	switch {
	case err != nil:
		return network.Step{Outcome: network.StepPassed, Chat: chat, Err: err}
	case out == pageDone:
		return network.Step{Outcome: network.StepStart, Chat: chat}
	case out == pageMoved:
		return network.Step{Outcome: network.StepMoved, Chat: chat}
	}
	// gone or stuck: marked, never asked again.
	return network.Step{Outcome: network.StepPassed, Chat: chat}
}

// openChats is how many conversations' history is open, for the queue's
// day line.
func (l *LinkedIn) openChats() (int, bool) {
	n, cursor := 0, ""
	for {
		ids, next, err := l.st.ChatIDs(Name, store.ChatFilter{Writable: true, HistoryOpen: true, Prefix: conversationPrefix}, cursor, 200)
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

// endOnClose is a media response's body, which ends its Traffic count when
// closed: the fetch is in flight until its body is read.
type endOnClose struct {
	io.ReadCloser
	end func()
}

func (b endOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.end()
	return err
}

// networkClock is the network's clock as it is when used.
type networkClock struct{ l *LinkedIn }

func (c networkClock) Now() time.Time { return c.l.clock.Now() }

func (c networkClock) AfterFunc(d time.Duration, f func()) func() bool {
	return c.l.clock.AfterFunc(d, f)
}

func (c networkClock) Sleep(ctx context.Context, d time.Duration) error {
	return c.l.clock.Sleep(ctx, d)
}
