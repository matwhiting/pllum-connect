package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/diag"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// Once hello has made the networks, each one's day line of Beeper merges is
// written to its diagnostics file: here a store whose count was opened on
// an invented day before the server's clock, so the line is due at once.
func TestHelloStartsTheDayLineOfMerges(t *testing.T) {
	now := time.Date(2026, time.March, 2, 9, 0, 0, 0, time.Local)
	dataDir := make(chan string, 1)
	open := func(dir string, key [32]byte) (*store.Store, error) {
		st, err := store.Open(filepath.Join(dir, "store"), key)
		if err != nil {
			return nil, err
		}
		if _, err := st.CloseMergeDays("whatsapp", "2026-03-01"); err != nil {
			return nil, err
		}
		dataDir <- dir
		return st, nil
	}
	h := startNets(t, func(emit network.Emit) []network.Network {
		return []network.Network{&fakeNetwork{name: "whatsapp", emit: emit.For("whatsapp")}}
	}, open, func() time.Time { return now })
	if r := result(t, h.hello()); r == nil {
		t.Fatalf("hello failed")
	}
	path := filepath.Join(<-dataDir, "whatsapp", diag.FileName)
	deadline := time.Now().Add(5 * time.Second)
	var b []byte
	for time.Now().Before(deadline) {
		if b, _ = os.ReadFile(path); len(b) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.HasSuffix(strings.TrimSpace(string(b)), " beeper copies merged on 2026-03-01: 0, matched news: 0") {
		t.Fatalf("the diagnostics file reads %q", b)
	}
}
