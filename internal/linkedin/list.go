package linkedin

import (
	"context"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
)

// The history queue's listing of conversations older than the first
// sign-in's year (network.Backlog.List): one more page of a folder at a
// time, by the cursor the first listing pages it by, from where that
// listing stopped ("olderCursor.<folder>"; from the top for a store listed
// before it was kept), until each folder is listed to its end
// ("listedOlder.<folder>", then "olderListed"). What a page brings is
// stored as the first listing stores it: a new conversation is marked
// fresh, so the worker fetches its newest page and history.

// olderListed reports whether every folder has been listed to its end.
func (l *LinkedIn) olderListed() bool { return l.value("olderListed") != "" }

// listOlder lists one more page of the first folder not listed to its end,
// under ctx (the queue's). A folder's listing ends when LinkedIn says so
// (a settled answer), a page is empty, its cursor ends or does not move, or
// a page past the year brings no conversation the store did not hold.
func (l *LinkedIn) listOlder(ctx context.Context) network.Step {
	if l.olderListed() {
		return network.Step{Outcome: network.StepPassed}
	}
	l.mu.Lock()
	gen, cli, runCtx := l.runs.Gen, l.cli, l.runs.Ctx()
	l.mu.Unlock()
	if cli == nil || runCtx == nil {
		return network.Step{Err: errNotConnected}
	}
	for _, folder := range listFolders {
		done := "listedOlder." + folder
		if l.value(done) != "" {
			continue
		}
		cursor := l.value("olderCursor." + folder)
		var page *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]
		err := l.request(ctx, gen, true, func(ctx context.Context) (err error) {
			page, err = cli.GetConversationsWithNextCursor(ctx, folder, cursor)
			return err
		})
		if settled(err) {
			_ = l.keep(done, "1")
			return network.Step{Outcome: network.StepPassed}
		}
		if err != nil {
			return network.Step{Outcome: network.StepPassed, Err: err}
		}
		if page == nil || !l.current(gen) {
			_ = l.keep(done, "1")
			return network.Step{Outcome: network.StepMoved}
		}
		var oldest time.Time
		fresh := false
		for _, c := range page.Elements {
			if at := c.LastActivityAt.Time; !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
				oldest = at
			}
			if _, ok, err := l.st.Chat(Name, urnString(c.EntityURN)); err == nil && !ok {
				fresh = true
			}
		}
		l.storeConversations(page.Elements)
		if fresh {
			l.signal() // the worker fetches what came in the new ones
		}
		next := page.Metadata.NextCursor
		pastYear := !oldest.IsZero() && oldest.Before(l.clock.Now().Add(-firstListing))
		if len(page.Elements) == 0 || next == "" || next == cursor || pastYear && !fresh {
			_ = l.keep(done, "1")
			_ = l.keep("olderCursor."+folder, "")
		} else {
			_ = l.keep("olderCursor."+folder, next)
		}
		return network.Step{Outcome: network.StepMoved}
	}
	_ = l.keep("olderListed", "1")
	return network.Step{Outcome: network.StepPassed}
}
