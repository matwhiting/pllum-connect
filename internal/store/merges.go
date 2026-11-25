package store

import (
	"database/sql"
	"errors"
	"strings"
	"time"
)

// Beeper merges. Copied history stays, and the matching that merges a line
// the network delivers again into its copy stays until it stops firing. To
// see when that is, every merge is counted, per network, in beeper_merges:
// a direct line taking over a copied line (takeOver), and a copied line
// folded into a direct one (foldLine: absorbBeeperCopy, a twin fold, the
// stored-twins upgrade, a chat merge). The count rides in the merge's own
// transaction, so it is exact across restarts. Once a day the diagnostics
// files get one counts-only line per network (diag.Merges), zero included.

// dayLayout is a day as CloseMergeDays takes and gives it.
const dayLayout = "2006-01-02"

// maxZeroDays is the most days with no merge CloseMergeDays gives at once
// (a store left closed for longer gives that many, ending yesterday).
const maxZeroDays = 400

// countMerge counts one merge of a copied line for network.
func countMerge(tx *sql.Tx, network string) error {
	_, err := tx.Exec(`INSERT INTO beeper_merges (network, merged) VALUES (?, 1)
		ON CONFLICT (network) DO UPDATE SET merged = merged + 1`, network)
	return err
}

// countMergeOf counts a fold of line fromID into line toID when it merges a
// copied line into a direct one: fromID is a beeper: ID and toID is not.
func countMergeOf(tx *sql.Tx, network, fromID, toID string) error {
	if !strings.HasPrefix(fromID, beeperPrefix) || strings.HasPrefix(toID, beeperPrefix) {
		return nil
	}
	return countMerge(tx, network)
}

// MergeDay is one day's count of copied lines merged in a network.
type MergeDay struct {
	Day    string // the local date, YYYY-MM-DD
	Merged int
}

// CloseMergeDays closes the days before today (a local date, YYYY-MM-DD)
// of network's merge count and gives each, oldest first: the open day with
// what was merged since it opened, then each later day before today with
// 0 (the helper was not running, so nothing merged). The count starts again
// for today. The first call for a network gives nothing and opens today, so
// what was merged before it counts for today; a call on the open day, or
// with a today before it (the clock moved back), gives nothing.
func (s *Store) CloseMergeDays(network, today string) ([]MergeDay, error) {
	if network == "" {
		return nil, errBadRequest
	}
	now, err := time.Parse(dayLayout, today)
	if err != nil {
		return nil, errBadRequest
	}
	var out []MergeDay
	err = s.write(func(tx *sql.Tx) error {
		out = nil
		var merged int
		var day string
		err := tx.QueryRow(`SELECT merged, day FROM beeper_merges WHERE network = ?`, network).Scan(&merged, &day)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.Exec(`INSERT INTO beeper_merges (network, merged, day) VALUES (?, 0, ?)`, network, today)
			return err
		}
		if err != nil {
			return err
		}
		open, perr := time.Parse(dayLayout, day)
		if perr != nil {
			// Never opened (merges counted before the first call): today.
			_, err = tx.Exec(`UPDATE beeper_merges SET day = ? WHERE network = ?`, today, network)
			return err
		}
		if !open.Before(now) {
			return nil
		}
		out = append(out, MergeDay{Day: day, Merged: merged})
		first := open.AddDate(0, 0, 1)
		if limit := now.AddDate(0, 0, -maxZeroDays); first.Before(limit) {
			first = limit
		}
		for d := first; d.Before(now); d = d.AddDate(0, 0, 1) {
			out = append(out, MergeDay{Day: d.Format(dayLayout)})
		}
		_, err = tx.Exec(`UPDATE beeper_merges SET merged = 0, day = ? WHERE network = ?`, today, network)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
