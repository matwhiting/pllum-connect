package network

import (
	"sync"
	"sync/atomic"
)

// Work is a network's goroutines: a sync.WaitGroup that also says how many
// are running, so a test can wait for the work to end instead of for time
// to pass. The zero value is ready to use.
type Work struct {
	wg sync.WaitGroup
	n  atomic.Int64
}

// Add counts n more goroutines.
func (w *Work) Add(n int) {
	w.n.Add(int64(n))
	w.wg.Add(n)
}

// Done ends one.
func (w *Work) Done() {
	w.n.Add(-1)
	w.wg.Done()
}

// Wait waits for every one to end.
func (w *Work) Wait() { w.wg.Wait() }

// Running is how many have not ended.
func (w *Work) Running() int64 { return w.n.Load() }
