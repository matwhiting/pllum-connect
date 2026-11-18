package messenger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if err != nil || info.Size() > diagLimit {
		t.Errorf("the diagnostics file is %d bytes, want at most %d", info.Size(), diagLimit)
	}
}
