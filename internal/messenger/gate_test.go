package messenger

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The gate (Task B0): counts only, a PIN that is passed once and kept
// nowhere, and a hidden method that exists only when the helper is started
// with --gate.

// canaryPIN is an invented six-digit PIN.
const canaryPIN = "730519"

// stateWords are the only strings the gate's counts may hold: historyState's
// fixed values.
var stateWords = map[string]bool{"": true, "noBackup": true, "needsPIN": true, "restored": true, "skipped": true}

func TestGateCountsAreCountsOnly(t *testing.T) {
	h := newHarness(t)
	h.f.history.sample = 3
	h.signIn(t)
	if got := h.m.HistoryState(); got != "needsPIN" {
		t.Fatalf("history state after sign-in = %q, want needsPIN", got)
	}
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.f.e2ee.dispatch(&events.FBMessage{})
	h.f.e2ee.dispatch(&events.FBMessage{})
	h.f.e2ee.dispatch(&events.UndecryptableMessage{})

	eventually(t, "the restored sample", func() bool { return h.m.GateCounts().(GateCounts).RestoredMessages == 3 })
	b, err := json.Marshal(h.m.GateCounts())
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	invented := map[float64]bool{}
	for _, id := range []int64{selfFBID, aliceFBID, bobFBID, carolFBID, groupOne, groupTwo, encGroup, daveFBID} {
		invented[float64(id)] = true
	}
	for k, v := range got {
		switch v := v.(type) {
		case bool:
		case float64:
			if invented[v] {
				t.Errorf("%s holds an ID, not a count", k)
			}
		case string:
			if k != "historyState" || !stateWords[v] {
				t.Errorf("%s holds a string that is not a fixed state", k)
			}
		default:
			t.Errorf("%s is %T, not a number or a boolean", k, v)
		}
	}
	if bytes.Contains(b, []byte(canary)) || bytes.Contains(b, []byte(canaryPIN)) {
		t.Fatalf("the counts hold invented text or the PIN")
	}

	want := map[string]any{
		"groups":                float64(2),
		"encryptedThreads":      float64(4), // three one-to-ones and a group
		"encryptedOneToOnes":    float64(3),
		"encryptedPointers":     float64(4), // Alice, Bob, Carol and Dave's older threads, each once
		"registered":            true,
		"e2eeConnected":         true,
		"historyState":          "restored",
		"restoredMessages":      float64(3),
		"liveEncryptedReceived": float64(2),
		"liveUndecryptable":     float64(1),
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %v, want %v", k, got[k], w)
		}
	}
	if ms, ok := got["registerMs"].(float64); !ok || ms < 0 {
		t.Errorf("registerMs = %v, want a duration in ms", got["registerMs"])
	}
	h.f.history.mu.Lock()
	fetches := h.f.history.fetches
	h.f.history.mu.Unlock()
	if fetches != 1 {
		t.Errorf("the sample asked for %d pages of history, want 1", fetches)
	}
}

func TestThePINIsNeverStored(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	pin := []byte(canaryPIN)
	if err := h.m.RestoreHistory(context.Background(), pin); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.f.history.mu.Lock()
	got, passed := h.f.history.gotPIN, h.f.history.pinSlice
	h.f.history.mu.Unlock()
	if got != canaryPIN {
		t.Fatalf("Meta was asked with a PIN other than the one typed")
	}
	if !bytes.Equal(pin, make([]byte, len(pin))) || !bytes.Equal(passed, make([]byte, len(passed))) {
		t.Fatalf("the PIN's bytes were not zeroed after use")
	}
	if h.m.HistoryState() != "restored" {
		t.Fatalf("history state = %q, want restored", h.m.HistoryState())
	}
	// The history device is kept, and so is the store; the PIN is in neither.
	if v, ok, _ := h.st.NetworkValue(Name, "history"); !ok || v == "" {
		t.Fatalf("the history device was not kept in the store's meta")
	}
	h.m.Stop()
	h.st.Close()

	for _, db := range []string{filepath.Join(h.dir, "messages.db"), filepath.Join(h.dir, "messenger", "device.db")} {
		if holds, where := dbHolds(t, db, canaryPIN); holds {
			t.Fatalf("%s holds the PIN in %s", filepath.Base(db), where)
		}
	}
	err := filepath.Walk(h.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(b, []byte(canaryPIN)) {
			return fmt.Errorf("%s holds the PIN", filepath.Base(path))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// dbHolds reports whether any value in any ordinary table of the encrypted
// database at path holds s, and where.
func dbHolds(t *testing.T, path, s string) (bool, string) {
	t.Helper()
	db, err := store.OpenDevice(path, testKey)
	if err != nil {
		t.Fatalf("opening %s: %v", filepath.Base(path), err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND sql NOT LIKE 'CREATE VIRTUAL%'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	rows.Close()
	if len(tables) == 0 {
		t.Fatalf("%s has no tables", filepath.Base(path))
	}
	for _, table := range tables {
		rows, err := db.Query(`SELECT * FROM "` + table + `"`)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for _, v := range vals {
				var b []byte
				switch v := v.(type) {
				case []byte:
					b = v
				case string:
					b = []byte(v)
				}
				if bytes.Contains(b, []byte(s)) {
					rows.Close()
					return true, table
				}
			}
		}
		rows.Close()
	}
	return false, ""
}

func TestAWrongPINCarriesAttemptsRemaining(t *testing.T) {
	two, zero := 2, 0
	for _, c := range []struct {
		name     string
		err      error
		kind     wire.Kind
		attempts *int
	}{
		{"two left", &securehistory.RecoveryError{Code: "invented", AttemptsRemaining: &two}, wire.PinWrong, &two},
		{"count unknown", &securehistory.RecoveryError{Code: "invented"}, wire.PinWrong, nil},
		{"none left", &securehistory.RecoveryError{Code: "invented", AttemptsRemaining: &zero}, wire.PinLimit, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.signIn(t)
			h.f.history.recoverErr = c.err
			pin := []byte(canaryPIN)
			err := h.m.RestoreHistory(context.Background(), pin)
			var we *wire.Error
			if !errors.As(err, &we) || we.Kind != c.kind {
				t.Fatalf("RestoreHistory error = %v, want kind %s", err, c.kind)
			}
			switch {
			case c.attempts == nil && we.AttemptsRemaining != nil:
				t.Fatalf("attemptsRemaining = %d, want none", *we.AttemptsRemaining)
			case c.attempts != nil && (we.AttemptsRemaining == nil || *we.AttemptsRemaining != *c.attempts):
				t.Fatalf("attemptsRemaining = %v, want %d", we.AttemptsRemaining, *c.attempts)
			}
			if !bytes.Equal(pin, make([]byte, len(pin))) {
				t.Fatalf("a wrong PIN's bytes were not zeroed")
			}
			if strings.Contains(err.Error(), canaryPIN) || strings.Contains(err.Error(), "invented") {
				t.Fatalf("the error carries more than its kind")
			}
			h.f.history.mu.Lock()
			calls, added := h.f.history.recoverCalls, h.f.history.added
			h.f.history.mu.Unlock()
			if calls != 1 || added != 0 {
				t.Fatalf("Meta was asked %d times and a device added %d times, want once and never", calls, added)
			}
			if h.m.HistoryState() != "needsPIN" {
				t.Fatalf("history state = %q, want needsPIN", h.m.HistoryState())
			}
		})
	}

	t.Run("not six digits asks Meta nothing", func(t *testing.T) {
		h := newHarness(t)
		h.signIn(t)
		pin := []byte("73051")
		err := h.m.RestoreHistory(context.Background(), pin)
		var we *wire.Error
		if !errors.As(err, &we) || we.Kind != wire.BadRequest {
			t.Fatalf("RestoreHistory error = %v, want badRequest", err)
		}
		h.f.history.mu.Lock()
		calls := h.f.history.recoverCalls
		h.f.history.mu.Unlock()
		if calls != 0 || !bytes.Equal(pin, make([]byte, len(pin))) {
			t.Fatalf("a malformed PIN reached Meta (%d calls) or was not zeroed", calls)
		}
	})

	t.Run("over the wire", func(t *testing.T) {
		h := newHarness(t)
		h.signIn(t)
		h.f.history.recoverErr = &securehistory.RecoveryError{Code: "invented", AttemptsRemaining: &two}
		w := serveWith(t, h.m, false)
		a := w.request("restoreHistory", map[string]any{"network": Name, "pin": canaryPIN})
		e, _ := a["error"].(map[string]any)
		if e["kind"] != "pinWrong" || e["attemptsRemaining"] != float64(2) {
			t.Fatalf("restoreHistory answered %v, want pinWrong with attemptsRemaining 2", a)
		}
	})
}

func TestTheGateMethodExistsOnlyWithTheFlag(t *testing.T) {
	for _, gate := range []bool{false, true} {
		t.Run(fmt.Sprintf("gate %v", gate), func(t *testing.T) {
			h := newHarness(t)
			w := serveWith(t, h.m, gate)
			a := w.request("gateCounts", map[string]any{"network": Name})
			if !gate {
				e, _ := a["error"].(map[string]any)
				if e["kind"] != "unknownMethod" {
					t.Fatalf("without --gate, gateCounts answered %v, want unknownMethod", a)
				}
				return
			}
			r, ok := a["result"].(map[string]any)
			if !ok {
				t.Fatalf("with --gate, gateCounts answered %v, want counts", a)
			}
			if r["registered"] != false || r["groups"] != float64(0) {
				t.Fatalf("before sign-in the counts are %v, want none", r)
			}
		})
	}
}

// wire is one server over a network, spoken to over pipes.
type wireConn struct {
	t     *testing.T
	in    io.Writer
	lines chan map[string]any
	n     int
}

// serveWith runs the server over n (already made), with the gate flag as
// given, and says hello.
func serveWith(t *testing.T, n network.Network, gate bool) *wireConn {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "server")
	deps := server.Deps{
		Dir:      dir,
		Open:     func(key [32]byte) (*store.Store, error) { return store.Open(dir, key) },
		Networks: func(*store.Store, string, [32]byte, network.Emit) []network.Network { return []network.Network{n} },
		Gate:     gate,
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = server.Serve(ctx, inR, outW, deps); outW.Close() }()
	t.Cleanup(func() { cancel(); inW.Close() })
	w := &wireConn{t: t, in: inW, lines: make(chan map[string]any, 64)}
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var v map[string]any
			if json.Unmarshal(sc.Bytes(), &v) == nil {
				w.lines <- v
			}
		}
		close(w.lines)
	}()
	w.request("hello", map[string]any{"protocol": 1, "storeKey": hex.EncodeToString(testKey[:]), "app": "0.0.1-test"})
	return w
}

func (w *wireConn) request(method string, params map[string]any) map[string]any {
	w.t.Helper()
	w.n++
	id := fmt.Sprintf("r%d", w.n)
	b, _ := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
	if _, err := w.in.Write(append(b, '\n')); err != nil {
		w.t.Fatalf("writing %s: %v", method, err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case v, ok := <-w.lines:
			if !ok {
				w.t.Fatalf("the output ended before %s was answered", method)
			}
			if v["id"] == id {
				return v
			}
		case <-deadline:
			w.t.Fatalf("no answer to %s", method)
		}
	}
}
