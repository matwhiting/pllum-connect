package messenger

import (
	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// maxDrops is how many main-socket failures in a row, without it
// connecting, the library's own loop is let run (Instagram's rule).
const maxDrops = 5

// passing reports whether a failure of this kind may be retried: no
// connection, or a timeout.
func passing(kind wire.Kind) bool { return kind == wire.Offline || kind == wire.Timeout }

// again starts a new run with the session held, unless one is in force or
// the pace forbids a request now; ok reports whether it started. m.mu is
// held.
func (m *Messenger) again() (ok bool) {
	if !m.started || m.stopped || m.session == nil || m.cli != nil {
		return false
	}
	if _, blocked := m.pace.Blocked(); blocked {
		return false
	}
	m.retire()
	m.startRun()
	return true
}

// calm forgets every retry the network had planned (a new sign-in, a
// sign-out, Meta stopping the session). m.mu is held.
func (m *Messenger) calm() {
	if m.stopRetry != nil {
		m.stopRetry()
		m.stopRetry = nil
	}
	m.drops, m.redials = 0, 0
	m.redialDue, m.redialing, m.retry = false, false, false
}

// disconnected records a main-socket failure of run gen. messagix redials
// by itself (2 s, doubling to 5 minutes) for ever, and each dial is a
// handshake with his cookies that the pace never sees; a checkpoint or a
// dead session can look like any other failure there. So past maxDrops in
// a row the run is stopped (both sockets), the state is offline, and one
// redial is due through the pace, which a checkpoint found then stops as
// any request's would. Instagram's disconnected, for Messenger.
func (m *Messenger) disconnected(gen int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.runs.Gen || m.stopped {
		return
	}
	m.drops++
	if m.drops <= maxDrops {
		return
	}
	m.drops = 0
	m.retire() // messagix's Disconnect stops its loop (in its own goroutine)
	m.setState(network.Offline, "")
	m.scheduleRedial()
}

// connected records the main socket of run gen connecting: its failures
// in a row, and the redials', start over.
func (m *Messenger) connected(gen int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen == m.runs.Gen {
		m.drops, m.redials, m.redialing = 0, 0, false
	}
}

// scheduleRedial: one redial is due meta.Backoff(redials) on (15 minutes,
// doubling to 4 hours while redials fail; a connect starts it over).
// m.mu is held.
func (m *Messenger) scheduleRedial() {
	if m.stopRetry != nil {
		m.stopRetry()
	}
	m.redialDue = true
	gen := m.runs.Gen
	m.stopRetry = m.clock.AfterFunc(meta.Backoff(m.redials), func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if gen == m.runs.Gen {
			m.redial()
		}
	})
}

// redial runs the redial due, once: a new run with the session held (its
// messages page loaded through the pace, then the socket connected). While
// the pace forbids a request it waits for the next, twice as long; a run
// that fails for any other reason than Meta stopping the session does too
// (failed). m.mu is held.
func (m *Messenger) redial() {
	if !m.redialDue || m.stopped || m.session == nil {
		return
	}
	m.redialDue = false
	if m.stopRetry != nil {
		m.stopRetry()
		m.stopRetry = nil
	}
	m.redials++
	if kind, blocked := m.pace.Blocked(); blocked {
		if kind == wire.RateLimited {
			m.setState(network.Offline, wire.RateLimited)
		}
		m.scheduleRedial()
		return
	}
	if m.again() {
		m.redialing = true
	}
}
