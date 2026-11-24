package mediacache

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matwhiting/pllum-connect/internal/sealed"
)

// The key is invented.
var testKey = [32]byte{7, 7, 7}

func TestAFileIsWrittenSealedUnderItsName(t *testing.T) {
	c := New(t.TempDir(), testKey)
	path := c.Path("instagram", "chat-invented", "line-invented")
	if filepath.Dir(path) != c.Dir() || path == c.Path("messenger", "chat-invented", "line-invented") {
		t.Fatalf("path %s: want one per network, chat and line, in the folder", path)
	}
	if err := c.Write(path, []byte("invented photo")); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if !sealed.IsSealed(raw) || bytes.Contains(raw, []byte("invented photo")) {
		t.Fatalf("the file is not sealed")
	}
	if got, err := sealed.ReadFile(path, sealed.Key(testKey)); err != nil || string(got) != "invented photo" {
		t.Fatalf("opened %q, %v", got, err)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v, want 0600", fi.Mode().Perm())
	}
	if !c.Kept(path) || c.Kept(path+"-none") {
		t.Errorf("Kept is wrong")
	}
}

// The folder is trimmed oldest first, the file just written kept, once the
// count passes the cap: and only then, so a write under it scans nothing.
func TestTheFolderIsTrimmedOldestFirstOnceOverTheCap(t *testing.T) {
	c := New(t.TempDir(), testKey)
	c.SetCap(200) // a sealed file is 37 bytes over its contents
	if err := os.MkdirAll(c.Dir(), 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	for i, name := range []string{"aaa-oldest", "bbb-older"} {
		p := filepath.Join(c.Dir(), name)
		_ = os.WriteFile(p, bytes.Repeat([]byte{'x'}, 40), 0o600)
		at := old.Add(time.Duration(i) * time.Minute)
		_ = os.Chtimes(p, at, at)
	}
	first := c.Path("whatsapp", "c", "1")
	if err := c.Write(first, bytes.Repeat([]byte{'y'}, 30)); err != nil { // 40+40+67 = 147
		t.Fatal(err)
	}
	if n := names(t, c); len(n) != 3 {
		t.Fatalf("left %v, want all three under the cap", n)
	}
	second := c.Path("whatsapp", "c", "2")
	if err := c.Write(second, bytes.Repeat([]byte{'z'}, 30)); err != nil { // 214 > 200
		t.Fatal(err)
	}
	left := names(t, c)
	if len(left) != 3 || left["aaa-oldest"] || !left["bbb-older"] || !left[filepath.Base(second)] {
		t.Fatalf("left %v, want the oldest gone", left)
	}
	// Writing the same file again counts its size once.
	for range 5 {
		if err := c.Write(second, bytes.Repeat([]byte{'z'}, 30)); err != nil {
			t.Fatal(err)
		}
	}
	if len(names(t, c)) != 3 {
		t.Fatalf("rewriting one file trimmed others: %v", names(t, c))
	}
}

func TestALockIsOneAtATimeAndForgottenWhenFree(t *testing.T) {
	c := New(t.TempDir(), testKey)
	path := c.Path("linkedin", "c", "1")
	var mu sync.Mutex
	inside, most := 0, 0
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := c.Lock(path)
			mu.Lock()
			inside++
			most = max(most, inside)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
			unlock()
		}()
	}
	wg.Wait()
	if most != 1 {
		t.Errorf("%d held the lock at once, want 1", most)
	}
	c.mu.Lock()
	n := len(c.locks)
	c.mu.Unlock()
	if n != 0 {
		t.Errorf("%d locks still held in memory, want none", n)
	}
}

func TestForgetDeletesEveryFile(t *testing.T) {
	c := New(t.TempDir(), testKey)
	c.Keep("whatsapp", "c", "1", []byte("invented"))
	c.Forget()
	if _, err := os.Stat(c.Dir()); !os.IsNotExist(err) {
		t.Fatalf("the folder is still there")
	}
	c.Keep("whatsapp", "c", "2", []byte("invented again"))
	if len(names(t, c)) != 1 {
		t.Errorf("a write after Forget left %v", names(t, c))
	}
}

func names(t *testing.T, c *Cache) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(c.Dir())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, e := range entries {
		out[e.Name()] = true
	}
	return out
}
