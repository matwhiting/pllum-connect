package network

import (
	"sync"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// Values is a network's own values in the store (its meta table, under the
// network's name), read once and then held in memory. Its methods are safe
// from any goroutine. NewValues makes one.
type Values struct {
	st      *store.Store
	network string

	mu sync.Mutex
	m  map[string]string
}

// NewValues is network's values in st.
func NewValues(st *store.Store, network string) *Values {
	return &Values{st: st, network: network, m: map[string]string{}}
}

// Get is the value called name, or "" (also when it can't be read, which
// is not held, so the next Get reads it again).
func (v *Values) Get(name string) string {
	s, _ := v.Lookup(name)
	return s
}

// Lookup is Get with the store's error: a value that can't be read is
// "" and the error.
func (v *Values) Lookup(name string) (string, error) {
	v.mu.Lock()
	s, ok := v.m[name]
	v.mu.Unlock()
	if ok {
		return s, nil
	}
	s, _, err := v.st.NetworkValue(v.network, name)
	if err != nil {
		return "", err
	}
	v.mu.Lock()
	v.m[name] = s
	v.mu.Unlock()
	return s, nil
}

// Keep stores the value called name ("" removes it), writing only when it
// changes.
func (v *Values) Keep(name, s string) error {
	if v.Get(name) == s {
		return nil
	}
	return v.Put(name, s)
}

// Put stores the value called name ("" removes it), whatever the memory of
// it says.
func (v *Values) Put(name, s string) error {
	if err := v.st.PutNetworkValue(v.network, name, s); err != nil {
		return err
	}
	v.mu.Lock()
	v.m[name] = s
	v.mu.Unlock()
	return nil
}

// Held reports whether the value called name is held in memory (it has
// been read or written), for tests.
func (v *Values) Held(name string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.m[name]
	return ok
}

// Drop forgets every value held in memory, so each is read from the store
// again (tests only: the store is the network's own to write).
func (v *Values) Drop(names ...string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(names) == 0 {
		v.m = map[string]string{}
		return
	}
	for _, name := range names {
		delete(v.m, name)
	}
}
