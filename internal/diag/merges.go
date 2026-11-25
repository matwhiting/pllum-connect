package diag

import (
	"context"
	"fmt"
	"time"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// MergeLine is the day line's text: the local date, how many of Beeper's
// copied lines the network merged that day, and how many receipts, edits
// and revokes it matched to the copy naming their line, and nothing else.
const MergeLine = "beeper copies merged on %s: %d, matched news: %d"

// mergeEvery is how often Merges looks whether the day has turned.
const mergeEvery = time.Minute

// Merges writes, once a day, one line per network to its diagnostics file:
// how many of Beeper's copied lines it merged that (local) day and how much
// news it matched to them, zero included (store.CloseMergeDays), so a run
// of zero days shows when the matching has stopped firing.
type Merges struct {
	st       *store.Store
	dir      string
	networks []string
	lastDay  string // the day every network was last closed for
}

// NewMerges is the day line for each of networks, from st, into the
// diagnostics files under dir.
func NewMerges(st *store.Store, dir string, networks []string) *Merges {
	return &Merges{st: st, dir: dir, networks: networks}
}

// Check closes the days before now's for each network and writes a line
// for each. Once every network is closed for a day it reads nothing more
// until the day turns; a network that failed is tried again next time.
func (m *Merges) Check(now time.Time) {
	today := now.Local().Format("2006-01-02")
	if today == m.lastDay {
		return
	}
	ok := true
	for _, network := range m.networks {
		days, err := m.st.CloseMergeDays(network, today)
		if err != nil {
			ok = false
			continue
		}
		f := For(m.dir, network)
		for _, d := range days {
			f.Line(now, fmt.Sprintf(MergeLine, d.Day, d.Merged, d.News))
		}
	}
	if ok {
		m.lastDay = today
	}
}

// Run checks now and then every minute, on now's time, until ctx ends.
func (m *Merges) Run(ctx context.Context, now func() time.Time) {
	m.Check(now())
	t := time.NewTicker(mergeEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Check(now())
		}
	}
}
