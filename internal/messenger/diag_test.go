package messenger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/matwhiting/pllum-connect/internal/diag"
)

// A diagnostic line keeps the words and loses every number: IDs, JIDs,
// device numbers and times all go.
func TestADiagnosticLineHasNoNumbers(t *testing.T) {
	got := sanitize("Error decrypting message 3EB0A1B2C3 from 2000011:7@msgr: failed to decrypt prekey message: invalid prekey ID 421")
	if strings.ContainsAny(got, "0123456789") {
		t.Errorf("a number survived: %q", got)
	}
	if !strings.Contains(got, "failed to decrypt prekey message: invalid prekey ID") {
		t.Errorf("the words were lost: %q", got)
	}
}

// The library's warnings about decrypting are written to the diagnostics
// file, without numbers; anything else it says is not.
func TestOnlyDecryptWarningsAreWritten(t *testing.T) {
	h := newHarness(t)
	log := h.m.diagLogger()
	log.Warnf("Error decrypting message %s from %s: %v", "ABC123", "2000011:7@msgr", "bad MAC")
	log.Warnf("Unavailable message %s from %s (type: %q)", "ABC124", "2000011:7@msgr", "hosted")
	log.Infof("Connected to %s", "somewhere 42")
	log.Debugf("Sending %s", "a message 43")
	b, err := os.ReadFile(filepath.Join(h.dir, Name, "diagnostics.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "bad MAC") || !strings.Contains(lines[1], "Unavailable message") {
		t.Fatalf("the diagnostics file reads %q", b)
	}
	for _, l := range lines {
		if strings.ContainsAny(strings.SplitN(l, " ", 2)[1], "0123456789") {
			t.Errorf("a number survived after the time: %q", l)
		}
	}
	if info, _ := os.Stat(filepath.Join(h.dir, Name, "diagnostics.txt")); info.Mode().Perm() != 0o600 {
		t.Errorf("the diagnostics file is %v, want 0600", info.Mode().Perm())
	}
}

// The file is kept small: past its limit it starts again.
func TestTheDiagnosticsFileStaysSmall(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 2000; i++ {
		h.m.diag("Error decrypting message: invented reason that is long enough to fill the file quickly")
	}
	info, err := os.Stat(filepath.Join(h.dir, Name, "diagnostics.txt"))
	if err != nil || info.Size() > diag.Limit {
		t.Errorf("the diagnostics file is %d bytes, want at most %d", info.Size(), diag.Limit)
	}
}

// passedOver is the diagnostics file's lines about backup lines the
// library could not decrypt and left out.
func passedOver(t *testing.T, h *harness) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.dir, Name, "diagnostics.txt"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if _, rest, ok := strings.Cut(l, " "); ok && strings.HasPrefix(rest, "backup page:") {
			out = append(out, rest)
		}
	}
	return out
}

// A page of his backup with a line the library could not decrypt says so
// in the diagnostics file, once, in counts only; a page read whole says
// nothing.
func TestABackupPageWithAnUnreadableLineIsWritten(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	t0 := time.Date(2026, 2, 10, 8, 0, 0, 0, time.UTC)
	h.f.history.script(fmt.Sprint(aliceFBID),
		fakePage{before: t0.Add(time.Hour).UnixMilli(), more: true, unreadable: 1, msgs: []*securehistory.DecryptedMessage{
			archived("WAINVENTED61", aliceFBID, t0.Add(2*time.Hour), textContent(canary+" one")),
			archived("WAINVENTED62", aliceFBID, t0.Add(3*time.Hour), textContent(canary+" two")),
		}},
		fakePage{more: false, msgs: []*securehistory.DecryptedMessage{
			archived("WAINVENTED60", aliceFBID, t0, textContent(canary+" whole")),
		}},
	)
	h.signIn(t)
	if err := h.m.RestoreHistory(context.Background(), []byte(canaryPIN)); err != nil {
		t.Fatalf("RestoreHistory: %v", err)
	}
	h.waitValue(t, "ebackfilled")
	h.settle()
	if n := len(h.lines(t, fmt.Sprint(aliceFBID)+"@msgr")); n != 3 {
		t.Fatalf("Alice's chat holds %d lines, want the 3 read", n)
	}
	if got, want := passedOver(t, h), []string{"backup page: 1 of 3 lines could not be read"}; !slices.Equal(got, want) {
		t.Errorf("the diagnostics file says %q, want %q", got, want)
	}
}

// The catch-up read of a chat's newest page says so too.
func TestACatchUpPageWithAnUnreadableLineIsWritten(t *testing.T) {
	h := newHarness(t)
	h.m.gate = false
	restored(t, h)
	at := h.f.clock.Now()
	h.f.history.script(fmt.Sprint(aliceFBID), fakePage{more: true, before: at.Add(-time.Hour).UnixMilli(), unreadable: 2,
		msgs: []*securehistory.DecryptedMessage{archived("WAINVENTED70", aliceFBID, at, textContent(canary+" filled"))}})
	info := watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: oneToOne(aliceFBID), Sender: person(aliceFBID)}, ID: "WAINVENTED70", Timestamp: at}
	h.deliver(t, &events.UndecryptableMessage{Info: info})
	h.f.clock.Advance(2 * time.Minute)
	waitLine(t, h, fmt.Sprint(aliceFBID)+"@msgr", "WAINVENTED70", canary+" filled")
	h.settle()
	if got, want := passedOver(t, h), []string{"backup page: 2 of 3 lines could not be read"}; !slices.Equal(got, want) {
		t.Errorf("the diagnostics file says %q, want %q", got, want)
	}
}
