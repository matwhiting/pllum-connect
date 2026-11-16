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
// reconnect timing is the library's.
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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

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
	// changedEvery is the least time between two changed events.
	changedEvery = 500 * time.Millisecond
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

// clock is the time the adapter's timers and its pace run on; tests move a
// fake one.
type clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) (stop func() bool)
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Instagram is the instagram network. Its methods are safe from any
// goroutine.
type Instagram struct {
	st   *store.Store
	dir  string
	key  [32]byte
	emit network.Emit

	newClient func(p instameow.ClientParams) igClient
	clock     clock
	pace      *meta.Pace
	diff      *meta.SessionDiff

	mu          sync.Mutex
	started     bool
	stopped     bool
	ctx         context.Context
	cancel      context.CancelFunc
	session     network.Session // the cookies of the last login; never written anywhere
	gen         int             // counts runs: events and results of an older one are dropped
	cli         igClient
	runCtx      context.Context // the current run's: ended when it is replaced
	runCancel   context.CancelFunc
	state       network.State
	kind        wire.Kind
	stopCatchUp func() bool
	work        sync.WaitGroup
	busy        atomic.Int32 // sign-ins and inbox reloads still running

	vmu    sync.Mutex
	values map[string]string // the network's own store values, as read or written

	lmu   sync.Mutex
	locks map[string]*sync.Mutex // one history fetch per chat at a time

	cmu      sync.Mutex
	cLast    time.Time
	cPending bool
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
		clock:  realClock{},
		diff:   meta.NewSessionDiff(meta.InstagramPage),
		state:  network.NotConnected,
		values: map[string]string{},
		locks:  map[string]*sync.Mutex{},
	}
	g.newClient = func(p instameow.ClientParams) igClient { return instameow.NewClient(p) }
	// The pace reads the adapter's clock when it runs, so a test's clock,
	// set after New, is the one it uses.
	g.pace = &meta.Pace{
		Now:   func() time.Time { return g.clock.Now() },
		Sleep: func(ctx context.Context, d time.Duration) error { return g.clock.Sleep(ctx, d) },
	}
	return g
}

// Name is "instagram".
func (g *Instagram) Name() string { return Name }

// SignIn is Instagram's sign-in page.
func (g *Instagram) SignIn() meta.SignInPage { return meta.InstagramPage }

// Start readies the network. It asks nothing of Meta: the cookies live in
// Pllum's Keychain, and Pllum sends them as a `login` on every start.
func (g *Instagram) Start(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started || g.stopped {
		return nil
	}
	g.ctx, g.cancel = context.WithCancel(context.WithoutCancel(ctx))
	g.started = true
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
	// He has signed in again in the window: whatever Meta asked of him
	// before (a checkpoint, a new session) is done. Only here.
	g.pace.Clear()
	g.session = session
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

// Nudge tries the sign-in again, with the cookies it was given, after it
// failed for a passing reason (no connection, a rate limit that has run
// out). A dropped socket is the library's to reconnect, and a checkpoint,
// an ended session or a suspension waits for a new login.
func (g *Instagram) Nudge() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.started || g.stopped || g.session == nil || g.cli != nil || g.state != network.NotConnected {
		return
	}
	switch g.kind {
	case wire.Offline, wire.Timeout, wire.RateLimited, wire.Internal:
	default:
		return
	}
	if _, blocked := g.pace.Blocked(); blocked {
		return
	}
	g.retire()
	g.setState(network.Connecting, "")
	g.startRun()
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

// The writes are Task A3's. Until then Instagram is read-only here.

func (g *Instagram) Send(ctx context.Context, m network.Outgoing) (string, error) {
	return "", errUnsupported
}

func (g *Instagram) SendState(messageID string) network.SendState {
	return network.SendState{State: network.SendUnknown}
}

func (g *Instagram) SendFor(clientID string) (string, error) { return "", nil }

func (g *Instagram) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	return errUnsupported
}

func (g *Instagram) MarkRead(ctx context.Context, chatID, through string) error {
	return errUnsupported
}

func (g *Instagram) MarkUnread(ctx context.Context, chatID string) error { return errUnsupported }

func (g *Instagram) SetMuted(ctx context.Context, chatID string, muted bool) error {
	return errUnsupported
}

func (g *Instagram) Media(ctx context.Context, chatID, messageID string) (string, error) {
	return "", errUnsupported
}

// retire ends the current run, if any: its context, its client's socket,
// and (by moving gen on) any event or result still to come from it. g.mu is
// held.
func (g *Instagram) retire() {
	g.gen++
	if g.runCancel != nil {
		g.runCancel()
		g.runCancel = nil
	}
	g.runCtx = nil
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
	ctx, cancel := context.WithCancel(g.ctx)
	g.runCtx, g.runCancel = ctx, cancel
	gen, session := g.gen, g.session
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
	return gen == g.gen && !g.stopped
}

// running is the current run: its number, client and context; cli is nil
// when no client is signed in.
func (g *Instagram) running() (gen int, cli igClient, ctx context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gen, g.cli, g.runCtx
}

// run makes a client for the session, loads the inbox (LoadIndex), stores
// it, connects the socket and fills in the history.
func (g *Instagram) run(ctx context.Context, gen int, session network.Session) {
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
	if gen != g.gen || g.stopped {
		g.mu.Unlock()
		return
	}
	g.cli = cli
	g.mu.Unlock()

	// The configs the client loaded last time, for this same account, spare
	// it a page load when they are fresh. Too old, and the client says so
	// and loads them again.
	if state, ok := g.savedState(ck); ok {
		_ = cli.LoadState(state)
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
// anything else leaves him not connected, with the kind, and a nudge may
// try again.
func (g *Instagram) signInFailed(gen int, kind wire.Kind) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.gen || g.stopped {
		return
	}
	g.retire()
	g.setState(network.NotConnected, kind)
}

// request makes one request to Meta through the pace: none while a block is
// in force, then a 2 to 4 s gap after the last. A success may carry rotated
// cookies to Pllum; Meta pushing back blocks the pace and, for a
// checkpoint, an ended session or a suspension, ends the run.
func (g *Instagram) request(ctx context.Context, gen int, fn func(ctx context.Context) error) error {
	if err := g.gate(); err != nil {
		return err
	}
	if err := g.pace.Wait(ctx); err != nil {
		return &wire.Error{Kind: wire.Timeout}
	}
	if err := g.gate(); err != nil {
		return err
	}
	if !g.current(gen) {
		return errNotConnected
	}
	err := fn(ctx)
	if err == nil {
		g.pace.Succeed()
		g.checkSession(gen)
		return nil
	}
	kind := kindOf(err)
	g.pace.Fail(kind)
	g.pushedBack(gen, kind)
	return &wire.Error{Kind: errorKind(kind)}
}

// gate is the error for a request now, when the pace forbids one.
func (g *Instagram) gate() error {
	if kind, blocked := g.pace.Blocked(); blocked {
		return &wire.Error{Kind: errorKind(kind)}
	}
	return nil
}

// kindOf is the kind of a client error. A checkpoint, challenge or consent
// page is checkpoint; a session Meta has ended is reconnectNeeded; a
// suspended account is banned (these two are Pace's, not wire kinds).
func kindOf(err error) wire.Kind {
	switch {
	case errors.Is(err, httpclient.ErrChallengeRequired),
		errors.Is(err, httpclient.ErrCheckpointRequired),
		errors.Is(err, httpclient.ErrConsentRequired):
		return wire.Checkpoint
	case errors.Is(err, httpclient.ErrTokenInvalidated):
		return meta.ReconnectNeeded
	case errors.Is(err, httpclient.ErrAccountSuspended):
		return meta.Banned
	case errors.Is(err, httpclient.ErrRateLimited):
		return wire.RateLimited
	case errors.Is(err, context.DeadlineExceeded):
		return wire.Timeout
	case errors.Is(err, httpclient.ErrRequestFailed):
		return wire.Offline
	}
	var e *wire.Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return wire.Internal
}

// errorKind is the wire kind to return for kind: reconnectNeeded and banned
// are states, so a request refused for them is notConnected.
func errorKind(kind wire.Kind) wire.Kind {
	switch kind {
	case meta.ReconnectNeeded, meta.Banned:
		return wire.NotConnected
	}
	return kind
}

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
	if gen != g.gen || g.stopped {
		return
	}
	g.retire()
	g.setState(s, k)
}

// checkSession sends the session event when the client's cookies have
// changed (Meta rotates some as it answers): Pllum keeps the copy it signs
// in again from.
func (g *Instagram) checkSession(gen int) {
	g.mu.Lock()
	cli := g.cli
	ok := gen == g.gen && cli != nil
	g.mu.Unlock()
	if !ok {
		return
	}
	if s, changed := g.diff.Changed(cli.GetCookies(), g.clock.Now()); changed {
		g.emit(map[string]any{"event": "session", "session": s})
	}
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
	if gen != g.gen || g.stopped {
		return
	}
	if g.stopCatchUp != nil {
		g.stopCatchUp()
	}
	g.setState(network.CatchingUp, "")
	g.stopCatchUp = g.clock.AfterFunc(catchUpLimit, func() { g.caughtUp(gen) })
}

func (g *Instagram) caughtUp(gen int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.gen || g.stopped {
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
// timing. A socket Meta refused as unauthorized is an ended session: the
// library is told to stop.
func (g *Instagram) disconnected(gen int, err error) error {
	if dgw.IsUnauthorized(err) || errors.Is(err, instameow.ErrUserIDMissing) {
		g.pace.Fail(meta.ReconnectNeeded)
		g.pushedBack(gen, meta.ReconnectNeeded)
		return errStop
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if gen != g.gen || g.stopped {
		return nil
	}
	if g.stopCatchUp != nil {
		g.stopCatchUp()
		g.stopCatchUp = nil
	}
	switch g.state {
	case network.Connecting, network.CatchingUp, network.Live:
		g.setState(network.Offline, "")
	}
	return nil
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

// saveState keeps the client's dumped state, for the account signed in now.
func (g *Instagram) saveState(state json.RawMessage) {
	g.mu.Lock()
	account := g.session[string(cookies.IGCookieDSUserID)]
	g.mu.Unlock()
	if len(state) == 0 || account == "" {
		return
	}
	_ = g.keep("state", string(state))
	_ = g.keep("account", account)
}

// savedState is the dumped state kept for the account these cookies sign in
// to; another account's is never loaded.
func (g *Instagram) savedState(ck *cookies.Cookies) (json.RawMessage, bool) {
	state := g.value("state")
	if state == "" || g.value("account") != ck.Get(cookies.IGCookieDSUserID) {
		return nil, false
	}
	return json.RawMessage(state), true
}

// resnapshot: Meta says the socket is too far behind to resume. The library
// has stopped it; the inbox is loaded again (through the pace) and it
// connects anew.
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
		var mailbox *slidetypes.Mailbox
		err := g.request(ctx, gen, func(ctx context.Context) (err error) {
			_, mailbox, err = cli.LoadIndex(ctx)
			return err
		})
		if err != nil || !g.current(gen) {
			return
		}
		if mailbox != nil {
			g.storeListing(mailbox.ThreadsByFolder.Edges)
			g.changed()
		}
		g.work.Add(1)
		go func() {
			defer g.work.Done()
			cli.Connect(ctx)
		}()
	}()
}

// changed tells Pllum there is something to re-read: at once, or when 500 ms
// have passed since the last time.
func (g *Instagram) changed() {
	g.cmu.Lock()
	defer g.cmu.Unlock()
	if g.cPending {
		return
	}
	now := g.clock.Now()
	if wait := g.cLast.Add(changedEvery).Sub(now); wait > 0 && !g.cLast.IsZero() {
		g.cPending = true
		g.clock.AfterFunc(wait, func() {
			g.cmu.Lock()
			g.cPending = false
			g.cLast = g.clock.Now()
			g.cmu.Unlock()
			g.emit(map[string]any{"event": "changed"})
		})
		return
	}
	g.cLast = now
	g.emit(map[string]any{"event": "changed"})
}
