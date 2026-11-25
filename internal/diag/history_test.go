package diag

import (
	"fmt"
	"regexp"
	"testing"
)

// historyShape is every history queue line, its time included: the day,
// counts, yes/no and a kind word, nothing else.
var historyShape = regexp.MustCompile(`^history queue on \d{4}-\d{2}-\d{2}: pages \d+, chats finished \d+, chats left (\d+|unknown), minutes running \d+, paused (no|yes \w+), stopped (no|yes \w+)$`)

func TestHistoryLineHoldsOnlyCounts(t *testing.T) {
	for _, line := range []string{
		fmt.Sprintf(HistoryLine, "2026-03-01", 4, 1, "7", 12, "no", "no"),
		fmt.Sprintf(HistoryLine, "2026-03-02", 0, 0, "unknown", 0, "yes rateLimited", "yes internal"),
	} {
		if !historyShape.MatchString(line) {
			t.Errorf("%q is not a counts-only history line", line)
		}
	}
}
