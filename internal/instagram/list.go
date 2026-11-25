package instagram

import (
	"context"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/network"
)

// The history queue's listing of chats older than the first sign-in's year
// (network.Backlog.List): one more page of the inbox at a time, through
// PaginateMailbox as the first sign-in's listing pages it, from where that
// listing stopped ("olderCursor"; from the top for a store listed before
// it was kept), until the inbox has nothing older ("olderListed").

// keepOlderListing keeps where the first listing stopped: the next page's
// cursor, or that there is nothing older.
func (g *Instagram) keepOlderListing(page slidetypes.PageInfo) {
	if page.HasNextPage && page.EndCursor != "" {
		_ = g.keep("olderCursor", page.EndCursor)
		return
	}
	_ = g.keep("olderListed", "1")
}

// olderListed reports whether the inbox has been listed to its end.
func (g *Instagram) olderListed() bool { return g.value("olderListed") != "" }

// listOlder lists one more page of the inbox past the chats listed, under
// ctx (the queue's): its threads are stored as the first sign-in stores
// them (no unread, at their own last activity). The listing ends when the
// inbox has no next page, the cursor does not move, a page is empty, or a
// page past the year brings no chat the store did not hold.
func (g *Instagram) listOlder(ctx context.Context) network.Step {
	if g.olderListed() {
		return network.Step{Outcome: network.StepPassed}
	}
	gen, cli, runCtx := g.running()
	if cli == nil || runCtx == nil {
		return network.Step{Err: errNotConnected}
	}
	cursor := g.value("olderCursor")
	var resp *slidetypes.PaginateMailboxResponse
	err := g.request(ctx, gen, func(ctx context.Context) (err error) {
		resp, err = cli.PaginateMailbox(ctx, slidetypes.MakePaginateMailboxRequest(g.viewer(), cursor, inboxFolder, nil))
		return err
	})
	if err != nil {
		return network.Step{Outcome: network.StepPassed, Err: err}
	}
	if resp == nil || resp.Mailbox == nil {
		_ = g.keep("olderListed", "1")
		return network.Step{Outcome: network.StepMoved}
	}
	edges := resp.Mailbox.ThreadsByFolder.Edges
	fresh := false
	for _, e := range edges {
		if t := e.Node.AsIGDirectThread; t != nil && t.ThreadKey != 0 {
			if _, ok, err := g.st.Chat(Name, chatIDOf(t.ThreadKey)); err == nil && !ok {
				fresh = true
			}
		}
	}
	threads := g.storeListing(edges, false)
	g.changed()
	page := resp.Mailbox.ThreadsByFolder.PageInfo
	pastYear := olderThan(threads, g.clock.Now().Add(-inboxReach).UnixMilli())
	if len(threads) == 0 || page.EndCursor == cursor || pastYear && !fresh {
		_ = g.keep("olderListed", "1")
		return network.Step{Outcome: network.StepMoved}
	}
	g.keepOlderListing(page)
	return network.Step{Outcome: network.StepMoved}
}
