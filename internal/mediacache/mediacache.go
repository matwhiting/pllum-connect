// Package mediacache is the folder of media files every network shares:
// <data folder>/media, each file named by a SHA-256 of network|chat|message,
// sealed under the files key (package sealed), 0600 in a 0700 folder, and
// the folder kept under Cap bytes, the least recently used files going
// first. A network keeps only its own fetch; where the file goes, the lock
// that fetches it once at a time, the write and the trim are here.
//
// One Cache serves the whole folder: main makes one and gives it to every
// network, so the folder's size is counted once, not by four networks
// trimming the same folder against each other. The networks' caps were all
// Cap, so one shared cap changes nothing.
package mediacache

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/matwhiting/pllum-connect/internal/sealed"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Cap is the most the folder holds, in bytes.
const Cap = 500 << 20

var errInternal = &wire.Error{Kind: wire.Internal}

// Cache is the media folder. Its methods are safe from any goroutine.
type Cache struct {
	dir string
	key [32]byte // the files key

	mu    sync.Mutex
	locks map[string]*fileLock // one fetch per file at a time; dropped once no one holds or waits

	// tmu guards the folder's size as counted: known once the folder has
	// been scanned (at the first write), then moved by each write and trim.
	// A write trims only when the count passes the cap; the trim scans the
	// folder and counts it again from what it finds.
	tmu   sync.Mutex
	cap   int64
	known bool
	total int64
}

type fileLock struct {
	mu   sync.Mutex
	refs int
}

// New is the media folder of the data folder dataDir, sealed under the
// files key of storeKey.
func New(dataDir string, storeKey [32]byte) *Cache {
	return &Cache{
		dir:   filepath.Join(dataDir, "media"),
		key:   sealed.Key(storeKey),
		locks: map[string]*fileLock{},
		cap:   Cap,
	}
}

// SetCap changes the most the folder holds (tests only).
func (c *Cache) SetCap(n int64) {
	c.tmu.Lock()
	defer c.tmu.Unlock()
	c.cap = n
}

// Dir is the folder.
func (c *Cache) Dir() string { return c.dir }

// Path is where a network's line's file is kept.
func (c *Cache) Path(network, chat, msgID string) string {
	sum := sha256.Sum256([]byte(network + "|" + chat + "|" + msgID))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:]))
}

// Lock takes the lock for one file, so it is fetched once at a time; unlock
// gives it back, and the lock is forgotten once no one holds or waits for
// it.
func (c *Cache) Lock(path string) (unlock func()) {
	c.mu.Lock()
	l, ok := c.locks[path]
	if !ok {
		l = &fileLock{}
		c.locks[path] = l
	}
	l.refs++
	c.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		c.mu.Lock()
		l.refs--
		if l.refs == 0 && c.locks[path] == l {
			delete(c.locks, path)
		}
		c.mu.Unlock()
	}
}

// Kept reports whether a file is kept, marking it just used.
func (c *Cache) Kept(path string) bool {
	if fi, err := os.Stat(path); err != nil || !fi.Mode().IsRegular() {
		return false
	}
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	return true
}

// Write writes a file sealed under the files key, whole (never half), 0600,
// then trims the folder if it holds more than the cap. The caller holds the
// file's lock. Its error is internal.
func (c *Cache) Write(path string, data []byte) error {
	var before int64
	if fi, err := os.Stat(path); err == nil && fi.Mode().IsRegular() {
		before = fi.Size()
	}
	if err := sealed.WriteFile(c.dir, path, c.key, data); err != nil {
		return errInternal
	}
	var after int64
	if fi, err := os.Stat(path); err == nil {
		after = fi.Size()
	}
	c.tmu.Lock()
	defer c.tmu.Unlock()
	if !c.known {
		c.total, c.known = c.scanSize(), true
	} else {
		c.total += after - before
	}
	if c.total > c.cap {
		c.trim(path)
	}
	return nil
}

// Keep writes a network's file he sent, so it opens without a download; a
// failure is left unsaid (the file is fetched when asked for).
func (c *Cache) Keep(network, chat, msgID string, data []byte) {
	path := c.Path(network, chat, msgID)
	unlock := c.Lock(path)
	defer unlock()
	_ = c.Write(path, data)
}

// Forget deletes every kept file of every network (the folder is shared, as it
// always was): on his Disconnect his photos do not stay behind. The size is
// counted afresh at the next write, so a write racing the delete can't leave
// the count wrong.
func (c *Cache) Forget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tmu.Lock()
	defer c.tmu.Unlock()
	_ = os.RemoveAll(c.dir)
	c.total, c.known = 0, false
}

type file struct {
	path string
	size int64
	at   time.Time
}

// files are the folder's files (no dotfiles: a write in progress), with
// their total size.
func (c *Cache) files() ([]file, int64) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, 0
	}
	var files []file
	var total int64
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || !e.Type().IsRegular() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, file{filepath.Join(c.dir, e.Name()), fi.Size(), fi.ModTime()})
		total += fi.Size()
	}
	return files, total
}

func (c *Cache) scanSize() int64 {
	_, total := c.files()
	return total
}

// trim removes the least recently used files until the folder holds at
// most the cap; keep (the file just written) stays. c.tmu is held.
func (c *Cache) trim(keep string) {
	files, total := c.files()
	defer func() { c.total = total }()
	if total <= c.cap {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if !files[i].at.Equal(files[j].at) {
			return files[i].at.Before(files[j].at)
		}
		return files[i].path < files[j].path
	})
	for _, f := range files {
		if total <= c.cap {
			return
		}
		if f.path == keep {
			continue
		}
		if os.Remove(f.path) == nil {
			total -= f.size
		}
	}
}
