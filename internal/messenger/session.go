package messenger

import (
	"context"
	"maps"
	"slices"

	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// request makes one request to Meta through the pace (meta.Session): none
// while a block is in force, then a 2 to 4 s gap after the last. A success
// may carry rotated cookies to Pllum; Meta pushing back blocks the pace
// and, for a checkpoint, an ended session or a suspension, ends the run. A
// caller that expects an answer Meta gives as an error (no backup, a wrong
// PIN) catches it inside fn and returns nil: Meta answered.
//
// The history queue's request (ctx from network.WithBacklog) goes only if,
// once its turn in the pace has come, no other request is in flight or
// ended within network.BacklogQuiet, and the queue may still ask (not
// held, stopped or yielding to a scroll-back): a send or a scroll-back
// that began while it waited goes alone. Otherwise it is not made
// (notConnected), and the queue asks again after its gap.
func (m *Messenger) request(ctx context.Context, gen int, fn func(ctx context.Context) error) error {
	live := func() bool { return m.current(gen) }
	if network.IsBacklog(ctx) {
		live = func() bool {
			return m.current(gen) && m.traffic.Quiet(m.clock.Now(), network.BacklogQuiet) && (m.bq == nil || m.bq.MayAsk())
		}
	}
	return m.sess.Request(ctx, live, func(ctx context.Context) error {
		end := m.traffic.Begin(ctx, m.clock.Now())
		defer func() { end(m.clock.Now()) }()
		return fn(ctx)
	}, func() { m.checkSession(gen) }, func(kind wire.Kind) { m.pushedBack(gen, kind) })
}

// blocked is the error for a request now, when the pace forbids one.
func (m *Messenger) blocked() error { return m.sess.Gate() }

// kindOf is the kind of a client error, as Instagram's (meta.KindOf), and
// also: messagix's "please log in" is an ended session, and a connection
// that failed at the network (the encrypted socket's dial, say) is no
// connection, or a timeout.
func kindOf(err error) wire.Kind {
	return meta.KindOf(err, meta.KindRules{Ended: []error{types.ErrPleaseLogIn}, Net: true})
}

// errorKind is the wire kind to return for kind (meta.ErrorKind).
func errorKind(kind wire.Kind) wire.Kind { return meta.ErrorKind(kind) }

// stateFor is the state Meta pushing back with kind leaves: ok is false for
// a kind that does not end the run.
func stateFor(kind wire.Kind) (s network.State, k wire.Kind, ok bool) {
	switch kind {
	case wire.Checkpoint:
		return network.ReconnectNeeded, wire.Checkpoint, true
	case meta.ReconnectNeeded:
		return network.ReconnectNeeded, "", true
	case meta.Banned:
		return network.Banned, "", true
	case wire.RegistrationUnclear:
		return network.ReconnectNeeded, wire.RegistrationUnclear, true
	}
	return "", "", false
}

// paceKind is what a block stops the pace with: a registration whose
// answer was lost stops it as an ended session does.
func paceKind(kind wire.Kind) wire.Kind {
	if kind == wire.RegistrationUnclear {
		return meta.ReconnectNeeded
	}
	return kind
}

// pushedBack: Meta asked him to confirm it is him, ended the session or
// suspended the account (the pace is already blocked). The run ends, its
// encrypted socket too, and nothing is asked again until a new login.
func (m *Messenger) pushedBack(gen int, kind wire.Kind) {
	// The history queue holds a day after any pushback, whichever run's
	// and whatever request's it was.
	m.queuePushedBack(kind)
	s, k, ok := stateFor(kind)
	if !ok {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.runs.Gen || m.stopped {
		return
	}
	m.retire()
	m.calm() // nothing redials or retries until a new login
	m.setState(s, k)
	// Kept, so neither a restart nor any version of these cookies sent again
	// asks Meta.
	hashes := m.hashes
	if len(hashes) == 0 {
		hashes = []string{sessionHash(m.session)}
	}
	m.sess.KeepBlock(kind, hashes)
}

// terminal reports whether kind stops the session until a new login.
func terminal(kind wire.Kind) bool {
	_, _, ok := stateFor(kind)
	return ok
}

// sessionHash is a SHA-256 of a session's required cookies
// (meta.SessionHash): what tells the session Meta stopped from a new
// sign-in, with no cookie value kept.
func sessionHash(s network.Session) string { return meta.SessionHash(meta.FacebookPage, s) }

// keptBlock is the kind that stopped a session in this or an earlier run,
// and the hashes of every version of that session; ok is false when none
// is kept.
func (m *Messenger) keptBlock() (wire.Kind, []string, bool) { return m.sess.KeptBlock() }

// holdBlock reports a kept block's state again and blocks the pace on it
// (never Clear: only a new session lifts it). m.mu is held.
func (m *Messenger) holdBlock(kind wire.Kind) {
	st, k, _ := stateFor(kind)
	m.pace.Fail(paceKind(kind))
	m.setState(st, k)
}

// checkSession sends the session event when the client's cookies have
// changed (Meta rotates some as it answers): Pllum keeps the copy it signs
// in again from.
func (m *Messenger) checkSession(gen int) {
	m.mu.Lock()
	cli := m.cli
	ok := gen == m.runs.Gen && cli != nil
	m.mu.Unlock()
	if !ok {
		return
	}
	s, changed := m.diff.Changed(cli.GetCookies(), m.clock.Now())
	if !changed {
		return
	}
	// The rotated set is the session now: a block names it as well as the
	// versions before.
	m.mu.Lock()
	if gen == m.runs.Gen {
		m.session = network.Session(maps.Clone(s))
		if hash := sessionHash(m.session); !slices.Contains(m.hashes, hash) {
			m.hashes = append(m.hashes, hash)
		}
	}
	m.mu.Unlock()
	m.emit(map[string]any{"event": "session", "session": s})
}

// setState moves the state and tells Pllum; m.mu is held. The state event
// goes out under the lock, so state events keep their order.
func (m *Messenger) setState(s network.State, kind wire.Kind) {
	if s == m.state && kind == m.kind {
		return
	}
	m.state, m.kind = s, kind
	ev := map[string]any{"event": "state", "state": s}
	if kind != "" {
		ev["kind"] = string(kind)
	}
	m.emit(ev)
}
