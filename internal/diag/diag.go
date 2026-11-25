// Package diag is each network's diagnostics file: <data>/<network>/
// diagnostics.txt, 0600 in a 0700 folder, on his Mac only, started again
// once it passes Limit. Each line is the time it was written (UTC, RFC 3339)
// and what was written. What goes in is the writer's to keep clean: counts,
// yes/no and words only, never an ID, a name or a word of a message.
package diag

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Limit is the most a diagnostics file grows to: one that has passed
// Limit-1KiB is started again before the next line.
const Limit = 64 << 10

// FileName is the diagnostics file's name in a network's folder.
const FileName = "diagnostics.txt"

// File is one network's diagnostics file. Every writer of one path shares
// one File (For), so two never write it at once.
type File struct {
	path string
	mu   sync.Mutex
}

var (
	filesMu sync.Mutex
	files   = map[string]*File{}
)

// For is network's diagnostics file in the data folder dir.
func For(dir, network string) *File {
	path := filepath.Join(dir, network, FileName)
	filesMu.Lock()
	defer filesMu.Unlock()
	f, ok := files[path]
	if !ok {
		f = &File{path: path}
		files[path] = f
	}
	return f
}

// Path is where the file is.
func (f *File) Path() string { return f.path }

// Line appends line, as it is, under the time now. A failure is dropped:
// diagnostics never stop the helper.
func (f *File) Line(now time.Time, line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if info, err := os.Stat(f.path); err == nil && info.Size() > Limit-1024 {
		_ = os.Remove(f.path)
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o700); err != nil {
		return
	}
	out, err := os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer out.Close()
	_, _ = fmt.Fprintf(out, "%s %s\n", now.UTC().Format(time.RFC3339), line)
}
