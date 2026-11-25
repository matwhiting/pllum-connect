// Package instagram is Pllum Connect's Instagram network, built on
// mautrix-meta's instameow client. It signs in with the cookies of a web
// session Pllum took from its own sign-in window (`login` with method
// "session", sent from the Keychain on every start), reads the inbox and the
// recent history into the store, and stays caught up on the live socket.
// Reads are the server's, from the store.
//
// Meta locks accounts that behave like a script, so every request goes
// through one meta.Pace: one at a time, 2 to 4 s apart, none for a while
// after a rateLimited, and none at all after a checkpoint, an ended session
// or a suspension until he signs in again (a new `login`). The socket's own
// reconnect timing is the library's, until it fails six times in a row
// without subscribing: then the helper stops it and tries once more itself,
// through the pace (redial).
//
// Nothing here logs: the client gets zerolog.Nop(), and every error returned
// carries only a fixed kind. The cookies are held in memory only; the store
// keeps the client's state (its dumped configs, the sequence ID and its
// time, his FBID) under "instagram.*" in its meta table.
package instagram

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/mediacache"
	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Name is the network's name in the protocol and in the store.
const Name = "instagram"

const (
	// catchUpLimit is how long after the socket subscribes the state stays
	// catchingUp when no sequence ID update comes.
	catchUpLimit = 60 * time.Second
	// maxDrops is how many socket failures in a row, without it subscribing
	// in between, the library may dial again after; the next stops it.
	maxDrops = 5
)

var (
	errBadRequest   = &wire.Error{Kind: wire.BadRequest}
	errInternal     = &wire.Error{Kind: wire.Internal}
	errNotConnected = &wire.Error{Kind: wire.NotConnected}
	errNotFound     = &wire.Error{Kind: wire.NotFound}
	errUnsupported  = &wire.Error{Kind: wire.Unsupported}
	// errStop is what the event handler returns to stop the library
	// reconnecting a socket Meta has refused.
	errStop = errors.New("stop")
)

// igClient is what the adapter uses of instameow's client: signing in,
// staying connected and reading. *instameow.Client satisfies it; tests use a
// fake. FetchThreadID is a read the sends need (Task A3).
type igClient interface {
	LoadIndex(ctx context.Context) (*types.PolarisViewer, *slidetypes.Mailbox, error)
	LoadState(state json.RawMessage) error
	DumpState() (json.RawMessage, error)
	Connect(ctx context.Context)
	Disconnect()
	PaginateMailbox(ctx context.Context, req *slidetypes.PaginateMailboxRequest) (*slidetypes.PaginateMailboxResponse, error)
	GetThread(ctx context.Context, req *slidetypes.GetThreadInfoRequest) (*slidetypes.ThreadInfoResponse, error)
	PaginateMessages(ctx context.Context, req *slidetypes.PaginateMessagesRequest) (*slidetypes.PaginateMessagesResponse, error)
	FetchThreadID(ctx context.Context, threadFBID int64) (*instameow.ThreadIGIDs, error)
	GetOwnFBID() int64
	GetCookies() *cookies.Cookies
}

var _ igClient = (*instameow.Client)(nil)

// Instagram is the instagram network. Its methods are safe from any
// goroutine.
type Instagram struct {
	st   *store.Store
	dir  string
	key  [32]byte
	emit network.Emit

	newClient     func(p instameow.ClientParams) igClient
	newWriter     func(c igClient) igWriter     // the client's writes (writes.go)
	newDownloader func(c igClient) igDownloader // the client's media downloads (media.go)
	clock         network.Clock
	pace          *meta.Pace
	sess          *meta.Session // the pace, and the block and rate limit kept in the store
	diff          *meta.SessionDiff

	mu          sync.Mutex
	started     bool
	stopped     bool
	ctx         context.Context
	cancel      context.CancelFunc
	session     network.Session // the cookies held now (the login's, as Meta rotated them); never written anywhere
	hashes      []string        // sessionHash of every version of the session held since the login
	retry       bool            // the run ended for a passing reason: a nudge may start another
	stopRetry   func() bool     // the timer for the next try: the rate limit's end, or a redial
	drops       int             // socket failures in a row since it last subscribed
	redials     int             // redials since the socket last subscribed: the next waits meta.Backoff(redials)
	redialDue   bool            // the socket is stopped and a redial is waiting: a nudge runs it now
	runs        network.Run     // the run in force (its number and context)
	cli         igClient
	state       network.State
	kind        wire.Kind
	stopCatchUp func() bool
	work        sync.WaitGroup
	busy        atomic.Int32 // sign-ins, inbox reloads and sends still running
	changes     network.Changed
	traffic     network.Traffic // the requests that reach Meta, for the history queue

	values *network.Values // the network's own store values, as read or written

	lmu   sync.Mutex
	locks map[string]*sync.Mutex // one history fetch per chat at a time

	outbox network.Outbox // the sends in line, sent one at a time (writes.go)
	rmu    sync.Mutex     // one resume of a failed send at a time (writes.go)

	media *mediacache.Cache // the media folder every network shares (media.go)
}

var (
	_ network.Network     = (*Instagram)(nil)
	_ network.SignInPager = (*Instagram)(nil)
	_ network.OlderPages  = (*Instagram)(nil)
)

// New makes the instagram network over st; emit must be
// emit.For("instagram"). Nothing is asked of Meta until a `login`.
func New(st *store.Store, dir string, key [32]byte, emit network.Emit) *Instagram {
	g := &Instagram{
		st:     st,
		dir:    dir,
		key:    key,
		emit:   emit,
		clock:  network.RealClock{},
		diff:   meta.NewSessionDiff(meta.InstagramPage),
		state:  network.NotConnected,
		values: network.NewValues(st, Name),
		locks:  map[string]*sync.Mutex{},

		newWriter:     writerFor,
		newDownloader: downloaderFor,
		media:         mediacache.New(dir, key),
	}
	g.newClient = func(p instameow.ClientParams) igClient { return instameow.NewClient(p) }
	// The pace reads the adapter's clock when it runs, so a test's clock,
	// set after New, is the one it uses.
	g.pace = &meta.Pace{
		Now:   func() time.Time { return g.clock.Now() },
		Sleep: func(ctx context.Context, d time.Duration) error { return g.clock.Sleep(ctx, d) },
	}
	g.sess = &meta.Session{Page: meta.InstagramPage, Pace: g.pace, Values: g.values, KindOf: kindOf, Terminal: terminal}
	g.outbox = network.Outbox{Begin: g.beginSends, End: g.work.Done, Deliver: func(_ context.Context, id string) { g.deliver(id) },
		Stopped: func(id string) { g.fail(id, wire.Refused) }, Busy: func(d int) { g.busy.Add(int32(d)) }}
	return g
}

// WithMedia makes the network keep its files in media, the folder every
// network shares; main gives each network the same one. Call it before
// Start.
func (g *Instagram) WithMedia(media *mediacache.Cache) *Instagram {
	g.media = media
	return g
}

// Name is "instagram".
func (g *Instagram) Name() string { return Name }

// SignIn is Instagram's sign-in page.
func (g *Instagram) SignIn() meta.SignInPage { return meta.InstagramPage }

// Start readies the network. It asks nothing of Meta: the cookies live in
// Pllum's Keychain, and Pllum sends them as a `login` on every start. A send
// an earlier run left sending becomes unknown, never to be sent again.
func (g *Instagram) Start(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started || g.stopped {
		return nil
	}
	g.ctx, g.cancel = context.WithCancel(context.WithoutCancel(ctx))
	g.started = true
	g.forgetCutOff()
	// A rate limit from an earlier run runs to its end, and the next one
	// doubles from where it got to.
	if until, step, ok := g.sess.KeptRateLimit(); ok {
		g.pace.RestoreRateLimit(until, step)
	}
	// A session Meta stopped in an earlier run stays stopped: its state is
	// reported again and nothing is asked until a new sign-in.
	if kind, _, ok := g.keptBlock(); ok {
		g.holdBlock(kind)
	}
	return nil
}

// Status is the network's state. Instagram has no "message yourself" chat
// that Pllum shows, so SelfChatID is empty.
func (g *Instagram) Status() network.Status {
	g.mu.Lock()
	defer g.mu.Unlock()
	return network.Status{State: g.state}
}

// Login signs in with the cookies of a web session. A session missing a
// required cookie, or holding one empty, is badRequest before anything is
// asked of Meta. Otherwise it answers at once, and the outcome comes as
// state events: connecting, then catchingUp and live, or a failure.
func (g *Instagram) Login(ctx context.Context, p network.LoginParams) error {
	if p.Method != network.LoginSession {
		return errUnsupported
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.started || g.stopped {
		return errInternal
	}
	session, ok := named(p.Session)
	if !ok {
		g.setState(network.NotConnected, wire.BadRequest)
		return errBadRequest
	}
	g.retire()
	hash := sessionHash(session)
	if kind, stopped, ok := g.keptBlock(); ok {
		if slices.Contains(stopped, hash) {
			// The session Meta stopped (any version of it Pllum was given),
			// sent again from the Keychain at start: refused here, with no
			// request.
			g.holdBlock(kind)
			return nil
		}
		// A new session: he has signed in again in the window.
		_ = g.keep("block", "")
	}
	// He has signed in again in the window: whatever Meta asked of him
	// before (a checkpoint, a new session) is done. Only here.
	g.pace.Clear()
	g.session = session
	g.hashes = []string{hash}
	g.diff.Seed(session)
	g.setState(network.Connecting, "")
	g.startRun()
	return nil
}

// named is the session's named cookies (the page's required and optional
// ones; anything else is dropped), and whether every required one is there
// with a value.
func named(s network.Session) (network.Session, bool) {
	out := network.Session{}
	for _, name := range meta.InstagramPage.Names() {
		if v := s[name]; v != "" {
			out[name] = v
		}
	}
	for _, name := range meta.InstagramPage.Required {
		if out[name] == "" {
			return nil, false
		}
	}
	return out, true
}

// CancelLogin stops a sign-in still connecting.
func (g *Instagram) CancelLogin() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != network.Connecting {
		return
	}
	g.retire()
	g.session = nil
	g.setState(network.NotConnected, "")
}

// Logout forgets the session here: it disconnects, drops the cookies and
// the client's saved state. Nothing is sent to Meta (signing out of Meta is
// the website's), and the chats stay in the store.
func (g *Instagram) Logout(ctx context.Context) error {
	g.mu.Lock()
	g.retire()
	g.session = nil
	g.setState(network.NotConnected, "")
	g.mu.Unlock()
	for _, name := range []string{"state", "account", "seq", "seqTS"} {
		if err := g.keep(name, ""); err != nil {
			return err
		}
	}
	return nil
}

// Nudge tries again, with the cookies it holds, after a sign-in or an inbox
// reload failed for a passing reason: no connection or a timeout. A rate
// limit is retried when its block ends (not before), a dropped socket is
// the library's to reconnect, anything else waits for a new login.
//
// A socket the helper stopped after repeated failures waits for its redial;
// a nudge (the Mac woke) runs that one redial now instead.
func (g *Instagram) Nudge() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.redialDue {
		g.redial(g.runs.Gen)
		return
	}
	if g.retry {
		g.again()
	}
}

// passing reports whether a failure of this kind may be retried: no
// connection, or a timeout.
func passing(kind wire.Kind) bool { return kind == wire.Offline || kind == wire.Timeout }

// again starts a new run with the held cookies, unless the pace forbids a
// request now. g.mu is held.
func (g *Instagram) again() {
	if !g.started || g.stopped || g.session == nil || g.cli != nil {
		return
	}
	if _, blocked := g.pace.Blocked(); blocked {
		return
	}
	g.retire()
	g.setState(network.Connecting, "")
	g.startRun()
}

// failed ends run gen after a failure that is not Meta stopping the session
// (pushedBack has set those): the state is s with the kind, a passing kind
// may be nudged, and a rate limit tries again by itself when its block
// ends. g.mu is held.
func (g *Instagram) failed(gen int, s network.State, kind wire.Kind) {
	if gen != g.runs.Gen || g.stopped {
		return
	}
	g.retire()
	shown := kind
	if s == network.Offline && kind == wire.Offline {
		shown = ""
	}
	g.setState(s, shown)
	g.retry = passing(kind)
	if kind != wire.RateLimited {
		return
	}
	until, _ := g.pace.RateLimit()
	gen = g.runs.Gen
	g.stopRetry = g.clock.AfterFunc(until.Sub(g.clock.Now()), func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if gen == g.runs.Gen {
			g.again()
		}
	})
}

// Stop disconnects and waits briefly for the network's work to end. The
// store is the server's to close.
func (g *Instagram) Stop() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	g.stopped = true
	if !g.started {
		g.mu.Unlock()
		return
	}
	g.retire()
	g.cancel()
	g.mu.Unlock()

	done := make(chan struct{})
	go func() {
		g.work.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// A request is stuck; it ends with the process.
	}
}

// retire ends the current run, if any: its context, its client's socket,
// and (by moving gen on) any event or result still to come from it. g.mu is
// held.
func (g *Instagram) retire() {
	g.runs.Retire()
	g.retry = false
	g.drops, g.redials, g.redialDue = 0, 0, false
	if g.stopRetry != nil {
		g.stopRetry()
		g.stopRetry = nil
	}
	if g.stopCatchUp != nil {
		g.stopCatchUp()
		g.stopCatchUp = nil
	}
	if old := g.cli; old != nil {
		g.cli = nil
		// Disconnect waits for the socket to stop, which may be inside an
		// event handler on this very path.
		g.work.Add(1)
		go func() {
			defer g.work.Done()
			old.Disconnect()
		}()
	}
}

// startRun signs in with g.session in the background, as run gen. g.mu is
// held.
func (g *Instagram) startRun() {
	ctx, gen := g.runs.Begin(g.ctx)
	session := g.session
	g.work.Add(1)
	g.busy.Add(1)
	go func() {
		defer g.work.Done()
		defer g.busy.Add(-1)
		g.run(ctx, gen, session)
	}()
}

// current reports whether gen is still the run in force.
func (g *Instagram) current(gen int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return gen == g.runs.Gen && !g.stopped
}

// running is the current run: its number, client and context; cli is nil
// when no client is signed in.
func (g *Instagram) running() (gen int, cli igClient, ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.runs.Gen, g.cli, g.runs.Ctx()
}

// run makes a client for the session and connects it. With the client's
// saved state for this session it resumes, as igconnector does: LoadState
// keeps the saved seq, and the socket replays what came while Pllum was
// closed. Otherwise (none saved, too old, the first backfill unfinished, or
// a resnapshot cleared the seq) it loads the inbox (LoadIndex), stores it,
// connects and fills in the history. Nothing is made while the pace
// forbids a request.
func (g *Instagram) run(ctx context.Context, gen int, session network.Session) {
	if err := g.gate(); err != nil {
		g.signInFailed(gen, kindOf(err))
		return
	}
	ck := &cookies.Cookies{Platform: types.Instagram}
	values := make(map[cookies.MetaCookieName]string, len(session))
	for name, v := range session {
		values[cookies.MetaCookieName(name)] = v
	}
	ck.UpdateValues(values)
	seq, seqTS := g.savedSeq()
	cli := g.newClient(instameow.ClientParams{
		Cookies:       ck,
		Log:           zerolog.Nop(),
		EventHandler:  g.handlerFor(gen),
		DisableTyping: true,
		SeqID:         seq,
		SeqIDTS:       seqTS,
	})
	g.mu.Lock()
	if gen != g.runs.Gen || g.stopped {
		g.mu.Unlock()
		return
	}
	g.cli = cli
	g.mu.Unlock()

	if state, ok := g.savedState(session); ok && seq != 0 && g.value("backfilled") != "" {
		if cli.LoadState(state) == nil {
			g.setViewer(cli.GetOwnFBID(), "")
			g.work.Add(1)
			go func() {
				defer g.work.Done()
				cli.Connect(ctx)
			}()
			return
		}
	}
	var viewer *types.PolarisViewer
	var mailbox *slidetypes.Mailbox
	err := g.request(ctx, gen, func(ctx context.Context) (err error) {
		viewer, mailbox, err = cli.LoadIndex(ctx)
		return err
	})
	if err != nil {
		g.signInFailed(gen, wire.KindOf(err))
		return
	}
	viewerIG := ""
	if viewer != nil {
		viewerIG = viewer.ID
	}
	g.setViewer(cli.GetOwnFBID(), viewerIG)
	if !g.current(gen) {
		return
	}
	if mailbox != nil {
		_ = g.keep("seq", strconv.FormatInt(mailbox.UQSeqID, 10))
		_ = g.keep("seqTS", strconv.FormatInt(g.clock.Now().UnixMilli(), 10))
	}
	g.work.Add(1)
	go func() {
		defer g.work.Done()
		cli.Connect(ctx)
	}()
	g.backfill(ctx, gen, cli, mailbox)
}

// signInFailed: the run's LoadIndex failed with kind. A checkpoint, an
// ended session or a suspension has already set its state (pushedBack);
// anything else leaves him not connected, with the kind (see failed).
func (g *Instagram) signInFailed(gen int, kind wire.Kind) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failed(gen, network.NotConnected, kind)
}

// request makes one request to Meta through the pace (meta.Session): none
// while a block is in force, then a 2 to 4 s gap after the last. A success
// may carry rotated cookies to Pllum; Meta pushing back blocks the pace
// and, for a checkpoint, an ended session or a suspension, ends the run.
func (g *Instagram) request(ctx context.Context, gen int, fn func(ctx context.Context) error) error {
	return g.sess.Request(ctx, func() bool { return g.current(gen) }, func(ctx context.Context) error {
		end := g.traffic.Begin(ctx, g.clock.Now())
		defer func() { end(g.clock.Now()) }()
		return fn(ctx)
	}, func() { g.checkSession(gen) }, func(kind wire.Kind) { g.pushedBack(gen, kind) })
}

// gate is the error for a request now, when the pace forbids one.
func (g *Instagram) gate() error { return g.sess.Gate() }

// kindOf is the kind of a client error (meta.KindOf).
func kindOf(err error) wire.Kind { return meta.KindOf(err, meta.KindRules{}) }

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
	}
	return "", "", false
}

// pushedBack: Meta asked him to confirm it is him, ended the session or
// suspended the account (the pace is already blocked). The run ends, its
// socket too, and nothing is asked again until a new login.
func (g *Instagram) pushedBack(gen int, kind wire.Kind) {
	s, k, ok := stateFor(kind)
	if !ok {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.runs.Gen || g.stopped {
		return
	}
	g.retire()
	g.setState(s, k)
	// Kept, so neither a restart nor any version of these cookies sent again
	// asks Meta.
	hashes := g.hashes
	if len(hashes) == 0 {
		hashes = []string{sessionHash(g.session)}
	}
	g.sess.KeepBlock(kind, hashes)
}

// terminal reports whether kind stops the session until a new login.
func terminal(kind wire.Kind) bool {
	_, _, ok := stateFor(kind)
	return ok
}

// sessionHash is a SHA-256 of a session's required cookies
// (meta.SessionHash): what tells the session Meta stopped from a new
// sign-in, with no cookie value kept.
func sessionHash(s network.Session) string { return meta.SessionHash(meta.InstagramPage, s) }

// keptBlock is the kind that stopped a session in this or an earlier run,
// and the hashes of every version of that session; ok is false when none
// is kept.
func (g *Instagram) keptBlock() (wire.Kind, []string, bool) { return g.sess.KeptBlock() }

// holdBlock reports a kept block's state again and blocks the pace on it
// (never Clear: only a new session lifts it). g.mu is held.
func (g *Instagram) holdBlock(kind wire.Kind) {
	st, k, _ := stateFor(kind)
	g.pace.Fail(kind)
	g.setState(st, k)
}

// checkSession sends the session event when the client's cookies have
// changed (Meta rotates some as it answers): Pllum keeps the copy it signs
// in again from.
func (g *Instagram) checkSession(gen int) {
	g.mu.Lock()
	cli := g.cli
	ok := gen == g.runs.Gen && cli != nil
	g.mu.Unlock()
	if !ok {
		return
	}
	s, changed := g.diff.Changed(cli.GetCookies(), g.clock.Now())
	if !changed {
		return
	}
	// The rotated set is the session now: Pllum keeps it, a nudge signs in
	// with it, and a block names it as well as the versions before.
	g.mu.Lock()
	if gen == g.runs.Gen {
		g.session = network.Session(maps.Clone(s))
		hash := sessionHash(g.session)
		if !slices.Contains(g.hashes, hash) {
			g.hashes = append(g.hashes, hash)
		}
		if g.value("state") != "" {
			_ = g.keep("account", hash)
		}
	}
	g.mu.Unlock()
	g.emit(map[string]any{"event": "session", "session": s})
}

// setState moves the state and tells Pllum; g.mu is held. The state event
// goes out under the lock, so state events keep their order.
func (g *Instagram) setState(s network.State, kind wire.Kind) {
	if s == g.state && kind == g.kind {
		return
	}
	g.state, g.kind = s, kind
	ev := map[string]any{"event": "state", "state": s}
	if kind != "" {
		ev["kind"] = string(kind)
	}
	g.emit(ev)
}

// handlerFor is the client's event handler for run gen. It never asks the
// library to reconnect over something the store could not keep: a
// reconnect is a request to Meta, and one per failed event could repeat
// without a pause. Only a socket Meta has refused stops it.
func (g *Instagram) handlerFor(gen int) instameow.EventHandler {
	return func(ctx context.Context, evt slidetypes.ClientEvent) (err error) {
		defer func() {
			if recover() != nil {
				err = nil
			}
		}()
		if !g.current(gen) {
			return nil
		}
		switch e := evt.(type) {
		case *slidetypes.Connected:
			g.connected(gen)
		case *slidetypes.Disconnected:
			return g.disconnected(gen, e.Error)
		case *slidetypes.AuthError:
			kind := kindOf(e.Error)
			g.pace.Fail(kind)
			g.pushedBack(gen, kind)
		case *slidetypes.SeqIDUpdate:
			g.seqUpdated(gen, e)
		case *slidetypes.ReconnectionStateUpdate:
			g.saveState(e.State)
		case *slidetypes.ResnapshotRequired:
			// The saved seq is too far behind to resume from: the next
			// start loads the inbox.
			_ = g.keep("seq", "")
			_ = g.keep("seqTS", "")
			g.resnapshot(gen)
		case *slidetypes.Delta:
			g.onDelta(ctx, gen, e)
		}
		return nil
	}
}

// connected: the socket has subscribed, and what came while it was away
// arrives now. It is caught up at the first sequence ID update, or 60 s on.
func (g *Instagram) connected(gen int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.runs.Gen || g.stopped {
		return
	}
	if g.stopCatchUp != nil {
		g.stopCatchUp()
	}
	g.drops, g.redials = 0, 0
	g.setState(network.CatchingUp, "")
	g.stopCatchUp = g.clock.AfterFunc(catchUpLimit, func() { g.caughtUp(gen) })
}

func (g *Instagram) caughtUp(gen int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.runs.Gen || g.stopped {
		return
	}
	if g.stopCatchUp != nil {
		g.stopCatchUp()
		g.stopCatchUp = nil
	}
	if g.state == network.CatchingUp {
		g.setState(network.Live, "")
	}
}

// disconnected: the socket dropped; the library reconnects it, on its own
// timing (2 s, doubling to 60 s, while it fails to subscribe). A socket Meta
// refused as unauthorized is an ended session: the library is told to stop.
// So is a socket that has failed more than maxDrops times in a row: each
// dial is a handshake with his cookies that the pace never sees, and a
// checkpoint or a dead session can look like any other failure at the
// socket. It goes offline and waits for one redial, through the pace, which
// a checkpoint found then stops as any request's would. (igconnector stops
// after more than 5 failures in a row too.)
func (g *Instagram) disconnected(gen int, err error) error {
	if dgw.IsUnauthorized(err) || errors.Is(err, instameow.ErrUserIDMissing) {
		g.pace.Fail(meta.ReconnectNeeded)
		g.pushedBack(gen, meta.ReconnectNeeded)
		return errStop
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.runs.Gen || g.stopped {
		return nil
	}
	if g.stopCatchUp != nil {
		g.stopCatchUp()
		g.stopCatchUp = nil
	}
	g.drops++
	if g.drops > maxDrops {
		g.drops = 0
		g.setState(network.Offline, "")
		g.scheduleRedial(gen)
		return errStop
	}
	switch g.state {
	case network.Connecting, network.CatchingUp, network.Live:
		g.setState(network.Offline, "")
	}
	return nil
}

// scheduleRedial: the socket of run gen is stopped; one redial is due
// meta.Backoff(redials) on (15 minutes, doubling to 4 hours while redials
// fail; a subscribe starts it over). g.mu is held.
func (g *Instagram) scheduleRedial(gen int) {
	if g.stopRetry != nil {
		g.stopRetry()
	}
	g.redialDue = true
	g.stopRetry = g.clock.AfterFunc(meta.Backoff(g.redials), func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.redial(gen)
	})
}

// redial runs the redial due for run gen, once: the inbox is loaded through
// the pace (refused, with no request, while a block is in force), what came
// meanwhile is caught up, and the socket connects anew (reload). A
// checkpoint, ended session or suspension it meets ends the run as any
// request's does; any other failure waits for the next redial, twice as
// long. g.mu is held.
func (g *Instagram) redial(gen int) {
	if gen != g.runs.Gen || g.stopped || !g.redialDue || g.cli == nil || g.runs.Ctx() == nil {
		return
	}
	g.redialDue = false
	if g.stopRetry != nil {
		g.stopRetry()
		g.stopRetry = nil
	}
	g.redials++
	cli, ctx := g.cli, g.runs.Ctx()
	g.work.Add(1)
	g.busy.Add(1)
	go func() {
		defer g.work.Done()
		defer g.busy.Add(-1)
		err := g.reload(ctx, gen, cli)
		if err == nil {
			return
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if gen != g.runs.Gen || g.stopped {
			return // Meta stopped the session: pushedBack has ended the run
		}
		kind := kindOf(err)
		if kind == wire.Offline {
			kind = ""
		}
		g.setState(network.Offline, kind)
		g.scheduleRedial(gen)
	}()
}

// seqUpdated keeps where the socket got to, so a restart can resume there,
// and ends catching up.
func (g *Instagram) seqUpdated(gen int, e *slidetypes.SeqIDUpdate) {
	_ = g.keep("seq", strconv.FormatInt(e.SeqID, 10))
	_ = g.keep("seqTS", strconv.FormatInt(e.Timestamp.UnixMilli(), 10))
	g.caughtUp(gen)
}

// savedSeq is the sequence ID and its time the last run got to.
func (g *Instagram) savedSeq() (int64, time.Time) {
	seq, _ := strconv.ParseInt(g.value("seq"), 10, 64)
	ms, _ := strconv.ParseInt(g.value("seqTS"), 10, 64)
	if seq == 0 || ms == 0 {
		return 0, time.Time{}
	}
	return seq, time.UnixMilli(ms)
}

// saveState keeps the client's dumped state, for the session held now (by
// its hash: the state's tokens belong to that session).
func (g *Instagram) saveState(state json.RawMessage) {
	g.mu.Lock()
	session := g.session
	g.mu.Unlock()
	if len(state) == 0 || session == nil {
		return
	}
	_ = g.keep("state", string(state))
	_ = g.keep("account", sessionHash(session))
}

// savedState is the dumped state kept for this session; another session's
// (another account, or a new sign-in) is never loaded.
func (g *Instagram) savedState(session network.Session) (json.RawMessage, bool) {
	state := g.value("state")
	if state == "" || g.value("account") != sessionHash(session) {
		return nil, false
	}
	return json.RawMessage(state), true
}

// resnapshot: Meta says the socket is too far behind to resume. The library
// has stopped it; the inbox is loaded again and caught up from (reload),
// and it connects anew. If the inbox reload fails, the run ends offline
// (with rateLimited while a block is in force), for a nudge or the block's
// end to retry.
func (g *Instagram) resnapshot(gen int) {
	cur, cli, ctx := g.running()
	if cur != gen || cli == nil || ctx == nil {
		return
	}
	g.work.Add(1)
	g.busy.Add(1)
	go func() {
		defer g.work.Done()
		defer g.busy.Add(-1)
		if err := g.reload(ctx, gen, cli); err != nil {
			g.mu.Lock()
			g.failed(gen, network.Offline, kindOf(err))
			g.mu.Unlock()
		}
	}()
}

// reload loads the inbox again on a client whose socket has stopped, and
// connects it anew. Every request goes through the pace: the LoadIndex,
// then, for each thread of its first page active since the store last saw
// it, the newest pages until a line the store holds (up to threadPages), as
// at a sign-in. That catch-up runs before the socket connects, so no live
// line can stop it short of what came while the socket was away, and a
// thread it does not finish stays marked behind for the next inbox load.
// The error is the LoadIndex's; a catch-up cut short is not one.
func (g *Instagram) reload(ctx context.Context, gen int, cli igClient) error {
	var mailbox *slidetypes.Mailbox
	err := g.request(ctx, gen, func(ctx context.Context) (err error) {
		_, mailbox, err = cli.LoadIndex(ctx)
		return err
	})
	if err != nil {
		return err
	}
	if !g.current(gen) {
		return nil
	}
	if mailbox != nil {
		threads := g.storeListing(mailbox.ThreadsByFolder.Edges, true)
		g.changed()
		_ = g.catchUpListing(ctx, gen, cli, threads)
	}
	if !g.current(gen) {
		return nil
	}
	g.work.Add(1)
	go func() {
		defer g.work.Done()
		cli.Connect(ctx)
	}()
	return nil
}

// changed tells Pllum there is something to re-read: at once, or when 500 ms
// have passed since the last time.
func (g *Instagram) changed() { g.changes.Tell(g.clock, g.emit) }
