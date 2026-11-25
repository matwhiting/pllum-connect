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
// ("listedOlder.<folder>", then "olderListed"). Only a conversation a page
// brings that the store did not hold is stored, and it is the queue's alone
// (ruling 9): stored without the fresh mark and marked histDone "queue", so
// the worker (at its 2 to 4 s pace) never asks for it, and only the queue
// pages it. A conversation the store holds is left as it is.

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
		if wait, stop := l.queueServerError(err); wait {
			return network.Step{Outcome: network.StepPassed}
		} else if stop {
			return network.Step{Outcome: network.StepPassed, Err: err}
		}
		if settled(err) {
			_ = l.keep(done, "1")
			return network.Step{Outcome: network.StepPassed}
		}
		if err != nil {
			return network.Step{Outcome: network.StepPassed, Err: err}
		}
		if !l.current(gen) {
			// The run ended while the page was on its way: nothing of it
			// is taken, and a later run lists on from the same place.
			return network.Step{Outcome: network.StepPassed, Err: errNotConnected}
		}
		if page == nil {
			_ = l.keep(done, "1")
			return network.Step{Outcome: network.StepMoved}
		}
		var oldest time.Time
		fresh := false
		for _, c := range page.Elements {
			if at := c.LastActivityAt.Time; !at.IsZero() && (oldest.IsZero() || at.Before(oldest)) {
				oldest = at
			}
			id := urnString(c.EntityURN)
			_, had, err := l.st.Chat(Name, id)
			if err != nil {
				continue
			}
			if had {
				// Held: left as it is (its activity is what the next
				// catch-up compares the listing with).
				continue
			}
			// The queue's: marked before it is stored, so the worker never
			// sees it unmarked.
			_ = l.keep("histDone."+id, histQueue)
			if l.storeConversation(c) {
				fresh = true
			}
		}
		if len(page.Elements) > 0 {
			l.changed()
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
