package diag

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// harness is a store, and the day line for two networks over it.
type harness struct {
	st     *store.Store
	dir    string
	m      *Merges
	merges int // how many merge has made, so each line is new
}

// testKey is an invented store key.
var testKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(i + 7)
	}
	return k
}()

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{dir: t.TempDir()}
	h.open(t)
	return h
}

// open opens the store (again) and makes a new day line over it, as a
// helper starting does.
func (h *harness) open(t *testing.T) {
	t.Helper()
	st, err := store.Open(filepath.Join(h.dir, "store"), testKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	h.st, h.m = st, NewMerges(st, h.dir, []string{"whatsapp", "linkedin"})
}

// merge seeds n copied lines in network and has the network deliver each
// again, under the ID the copy named: n merges.
func (h *harness) merge(t *testing.T, network string, n int) {
	t.Helper()
	for range n {
		i := h.merges
		h.merges++
		chat := fmt.Sprintf("chat-%s-%d", network, i)
		id := fmt.Sprintf("line-%s-%d", network, i)
		ms := int64(1767225600000 + i*60_000)
		if _, err := h.st.SeedCopied(network, chat, "bc-"+chat,
			[]store.CopiedLine{{BeeperID: "bm-" + id, NetworkID: id, TimeMs: ms, Text: "invented words"}}); err != nil {
			t.Fatal(err)
		}
		added, err := h.st.PutMessage(network, store.Message{ID: id, ChatID: chat, SenderID: "someone",
			TimeMs: ms, Text: "invented words", Kind: "text", Source: store.SourceSync})
		if err != nil || added {
			t.Fatalf("the line was added beside its copy (%v)", err)
		}
	}
}

// news seeds n copied lines in network and has the network send an edit
// of each under the ID the copy named: n matched news, no merge.
func (h *harness) news(t *testing.T, network string, n int) {
	t.Helper()
	for range n {
		i := h.merges
		h.merges++
		chat := fmt.Sprintf("chat-%s-%d", network, i)
		id := fmt.Sprintf("line-%s-%d", network, i)
		if _, err := h.st.SeedCopied(network, chat, "bc-"+chat, []store.CopiedLine{{BeeperID: "bm-" + id, NetworkID: id,
			TimeMs: int64(1767225600000 + i*60_000), Text: "invented words"}}); err != nil {
			t.Fatal(err)
		}
		added, err := h.st.PutMessage(network, store.Message{ID: id, ChatID: chat, Text: "invented edit", Edited: true})
		if err != nil || added {
			t.Fatalf("the edit was added beside its copy (%v)", err)
		}
	}
}

func (h *harness) lines(t *testing.T, network string) []string {
	t.Helper()
	var out []string
	for _, l := range readLines(t, filepath.Join(h.dir, network, FileName)) {
		_, rest, _ := strings.Cut(l, " ")
		out = append(out, rest)
	}
	return out
}

// The day line: each merge and each matched news counts, the line is
// written once a day with both counts, a zero day included, and it holds
// nothing but the day and the counts.
func TestTheDayLineCountsEachMergeOnceADay(t *testing.T) {
	h := newHarness(t)
	h.m.Check(at(1, 9)) // the first start: opens 1 March, writes nothing
	if got := h.lines(t, "whatsapp"); got != nil {
		t.Fatalf("the first start wrote %q", got)
	}
	h.merge(t, "whatsapp", 3)
	h.news(t, "whatsapp", 2)
	h.merge(t, "linkedin", 1)
	h.m.Check(at(1, 23)) // the same day: nothing yet
	if got := h.lines(t, "whatsapp"); got != nil {
		t.Fatalf("a line was written before the day ended: %q", got)
	}

	h.m.Check(at(2, 0))
	h.m.Check(at(2, 0).Add(time.Minute)) // once a day, however often it looks
	h.m.Check(at(2, 12))
	if got := h.lines(t, "whatsapp"); len(got) != 1 || got[0] != "beeper copies merged on 2026-03-01: 3, matched news: 2" {
		t.Fatalf("WhatsApp's file reads %q", got)
	}
	if got := h.lines(t, "linkedin"); len(got) != 1 || got[0] != "beeper copies merged on 2026-03-01: 1, matched news: 0" {
		t.Fatalf("LinkedIn's file reads %q", got)
	}

	// A day with no merge still gets its line, as does each day the helper
	// was not running.
	h.m.Check(at(3, 8))
	h.m.Check(at(6, 8))
	want := []string{
		"beeper copies merged on 2026-03-01: 3, matched news: 2",
		"beeper copies merged on 2026-03-02: 0, matched news: 0",
		"beeper copies merged on 2026-03-03: 0, matched news: 0",
		"beeper copies merged on 2026-03-04: 0, matched news: 0",
		"beeper copies merged on 2026-03-05: 0, matched news: 0",
	}
	if got := h.lines(t, "whatsapp"); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("WhatsApp's file reads %q, want %q", got, want)
	}

	// Nothing identifying: every line is the time, the words, the day and two numbers.
	shape := regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ beeper copies merged on \d{4}-\d\d-\d\d: \d+, matched news: \d+$`)
	for _, network := range []string{"whatsapp", "linkedin"} {
		path := filepath.Join(h.dir, network, FileName)
		b, _ := os.ReadFile(path)
		for _, bad := range []string{"line-", "chat-", "bm-", "bc-", "invented", "someone"} {
			if strings.Contains(string(b), bad) {
				t.Errorf("%s's file holds %q", network, bad)
			}
		}
		for _, l := range readLines(t, path) {
			if !shape.MatchString(l) {
				t.Errorf("%s's file has the line %q", network, l)
			}
		}
		if info, _ := os.Stat(path); info.Mode().Perm() != 0o600 {
			t.Errorf("%s's file is %v, want 0600", network, info.Mode().Perm())
		}
	}
}

// A restart keeps the count: the day line covers every merge of the day,
// whichever run made it.
func TestTheCountOutlivesARestart(t *testing.T) {
	h := newHarness(t)
	h.m.Check(at(1, 9))
	h.merge(t, "whatsapp", 2)
	h.st.Close()
	h.open(t) // the helper started again
	h.m.Check(at(1, 15))
	h.merge(t, "whatsapp", 1)
	h.m.Check(at(2, 9))
	if got := h.lines(t, "whatsapp"); len(got) != 1 || got[0] != "beeper copies merged on 2026-03-01: 3, matched news: 0" {
		t.Fatalf("WhatsApp's file reads %q", got)
	}
}

// Run looks at once and stops with its context.
func TestRunChecksAtOnceAndStops(t *testing.T) {
	h := newHarness(t)
	h.m.Check(at(1, 9))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.m.Run(ctx, func() time.Time { return at(2, 9) })
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for len(h.lines(t, "linkedin")) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if got := h.lines(t, "linkedin"); len(got) != 1 || got[0] != "beeper copies merged on 2026-03-01: 0, matched news: 0" {
		t.Fatalf("LinkedIn's file reads %q", got)
	}
}
