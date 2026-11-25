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
// it was kept, the cursor is taken from the first page the next sign-in
// that loads the inbox gets; until then nothing is asked: never an empty
// cursor, which no Instagram client sends), until the inbox has nothing
// older ("olderListed").

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
// ctx (the queue's). Only the threads the store does not hold (or holds
// with no thread ID, as a chat only in Beeper's copy) are stored, as the
// first sign-in stores them (no unread, at their own last activity): a
// chat the store holds is left as it is, its activity and its mark that
// newer lines are still to fetch with it. The listing ends when the inbox
// has no next page, the cursor does not move, a page is empty, or a page
// past the year brings no chat the store did not hold.
func (g *Instagram) listOlder(ctx context.Context) network.Step {
	cursor := g.value("olderCursor")
	if g.olderListed() || cursor == "" {
		return network.Step{Outcome: network.StepPassed}
	}
	gen, cli, runCtx := g.running()
	if cli == nil || runCtx == nil {
		return network.Step{Err: errNotConnected}
	}
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
	var store []slidetypes.Node[slidetypes.WrappedThreadInfo]
	fresh, pastYear := false, false
	yearAgo := g.clock.Now().Add(-inboxReach).UnixMilli()
	for _, e := range edges {
		t := e.Node.AsIGDirectThread
		if t == nil || t.ThreadKey == 0 {
			continue
		}
		if at := t.LastActivityTimestampMS.UnixMilli(); at > 0 && at < yearAgo {
			pastYear = true
		}
		chat := chatIDOf(t.ThreadKey)
		_, held, err := g.st.Chat(Name, chat)
		if err != nil {
			continue
		}
		_, placed := g.threadOfChat(chat)
		if !held {
			fresh = true
		}
		if !held || !placed {
			store = append(store, e)
		}
	}
	if len(store) > 0 {
		g.storeListing(store, false)
		g.changed()
	}
	page := resp.Mailbox.ThreadsByFolder.PageInfo
	if len(edges) == 0 || page.EndCursor == cursor || pastYear && !fresh {
		_ = g.keep("olderListed", "1")
		return network.Step{Outcome: network.StepMoved}
	}
	g.keepOlderListing(page)
	return network.Step{Outcome: network.StepMoved}
}
