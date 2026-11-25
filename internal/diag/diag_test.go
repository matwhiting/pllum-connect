package diag

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Every ID, chat and text here is invented.

// at is an invented local moment on day d of March 2026.
func at(d, hour int) time.Time { return time.Date(2026, time.March, d, hour, 0, 0, 0, time.Local) }

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func TestAFileIsPrivateAndStaysSmall(t *testing.T) {
	dir := t.TempDir()
	f := For(dir, "whatsapp")
	if For(dir, "whatsapp") != f {
		t.Fatalf("two writers of one path got two Files")
	}
	f.Line(at(1, 9), "a line")
	info, err := os.Stat(filepath.Join(dir, "whatsapp", FileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("the file is %v, want 0600", info.Mode().Perm())
	}
	if d, _ := os.Stat(filepath.Join(dir, "whatsapp")); d.Mode().Perm() != 0o700 {
		t.Errorf("the folder is %v, want 0700", d.Mode().Perm())
	}
	if got := readLines(t, f.Path()); len(got) != 1 || got[0] != at(1, 9).UTC().Format(time.RFC3339)+" a line" {
		t.Fatalf("the file reads %q", got)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 250; j++ {
				For(dir, "whatsapp").Line(at(1, 9), "an invented line long enough to fill the file before long")
			}
		}()
	}
	wg.Wait()
	info, err = os.Stat(f.Path())
	if err != nil || info.Size() > Limit {
		t.Fatalf("the file is %d bytes, want at most %d", info.Size(), Limit)
	}
	for _, l := range readLines(t, f.Path()) {
		if !strings.HasSuffix(l, " an invented line long enough to fill the file before long") {
			t.Fatalf("a line was torn: %q", l)
		}
	}
}
