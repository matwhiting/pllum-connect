package messenger

import (
	"context"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// The history queue's listing of chats older than the first sign-in's year
// (network.Backlog.List): one more page of a folder at a time, through
// listPage as the first sign-in's listing pages it, from the range that
// listing kept for the folder, until each folder Meta has more of is
// listed to its end ("listedOlder.<folder>", then "olderListed").

// olderListed reports whether every folder has been listed to its end.
func (m *Messenger) olderListed() bool { return m.value("olderListed") != "" }

// listOlder lists one more page of the first folder with more past what is
// listed, under ctx (the queue's): its threads are stored as the first
// sign-in stores them. A folder's listing ends when Meta has no more, its
// range stops moving, or a page past the year brings no chat the store did
// not hold.
func (m *Messenger) listOlder(ctx context.Context) network.Step {
	if m.olderListed() || m.gate {
		return network.Step{Outcome: network.StepPassed}
	}
	gen, cli, runCtx := m.running()
	if cli == nil || runCtx == nil {
		return network.Step{Err: errNotConnected}
	}
	for _, f := range m.folders(map[folderKey]bool{}, false) {
		name := "listedOlder." + f.name()
		if m.value(name) != "" {
			continue
		}
		ref, ok := m.folderRangeOf(f, true)
		if !ok || !ref.more {
			_ = m.keep(name, "1")
			continue
		}
		before := m.chatCount()
		total := 0
		next, oldestAt, stop, err := m.listPage(ctx, gen, cli, f, ref, true, &total)
		switch {
		case err != nil:
			return network.Step{Outcome: network.StepPassed, Err: err}
		case stop == stopFailed:
			return network.Step{Err: errNotConnected}
		case stop != "":
			_ = m.keep(name, "1")
			return network.Step{Outcome: network.StepMoved}
		}
		m.changed()
		pastYear := oldestAt > 0 && oldestAt < m.clock.Now().Add(-yearReach).UnixMilli()
		if !next.more || next.minKey == ref.minKey && next.minAt == ref.minAt || pastYear && m.chatCount() == before {
			_ = m.keep(name, "1")
		}
		return network.Step{Outcome: network.StepMoved}
	}
	_ = m.keep("olderListed", "1")
	return network.Step{Outcome: network.StepPassed}
}

// chatCount is how many chats the store holds (-1 when it can't say).
func (m *Messenger) chatCount() int {
	n, cursor := 0, ""
	for {
		ids, next, err := m.st.ChatIDs(Name, store.ChatFilter{}, cursor, 200)
		if err != nil {
			return -1
		}
		n += len(ids)
		if next == "" {
			return n
		}
		cursor = next
	}
}
