// Package messenger is Pllum Connect's Messenger network, built on
// mautrix-meta's messagix client (his facebook.com web session) and
// whatsmeow's Messenger mode (his end-to-end encrypted chats).
//
// This is its first slice, the gate (Task B0 of step 3's plan): it signs in
// with the cookies of a facebook.com session Pllum took from its own sign-in
// window (`login` with method "session"), loads the messages page once and
// connects the main (lightspeed) socket; on its first connect, as
// mautrix-meta's connector does, it registers Pllum as a Messenger device
// (once, ever) and connects the encrypted socket (e2ee.go), looks for his
// history backup, which his PIN restores (history.go), and pages the older
// threads of his inbox, counting what it sees (gate.go). It writes no chat or
// line into the store yet, and it sends nothing: every write method is
// `unsupported`.
//
// Meta locks accounts that behave like a script, so every request goes
// through one meta.Pace: one at a time, 2 to 4 s apart, none for a while
// after a rateLimited, and none at all after a checkpoint, an ended session
// or a suspension until he signs in again (a new `login`). Nothing here
// retries a request on its own; the encrypted socket's reconnect timing is
// the library's.
//
// Nothing here logs: the client gets zerolog.Nop(), whatsmeow gets
// waLog.Noop, and every error returned carries only a fixed kind. The
// cookies and the PIN are held in memory only; the store keeps the
// secure-history device under "messenger.history" in its meta table, and the
// whatsmeow device lives in <dir>/messenger/device.db.
package messenger

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Name is the network's name in the protocol and in the store.
const Name = "messenger"

var (
	errBadRequest   = &wire.Error{Kind: wire.BadRequest}
	errInternal     = &wire.Error{Kind: wire.Internal}
	errNotConnected = &wire.Error{Kind: wire.NotConnected}
	errUnsupported  = &wire.Error{Kind: wire.Unsupported}
	errUnclear      = &wire.Error{Kind: wire.Unclear}
)

// msgrClient is what the adapter uses of messagix's client: the messages
// page, the main (lightspeed) socket and the older pages of threads over it,
// the cookies (for the session event), and the encrypted side's
// registration. *messagix.Client satisfies it through realClient; tests use
// a fake.
type msgrClient interface {
	LoadMessagesPage(ctx context.Context) (types.UserInfo, *table.LSTable, error)
	SetEventHandler(h messagix.EventHandler)
	Connect(ctx context.Context) error
	Disconnect()
	FetchMoreThreads(ctx context.Context, syncGroup int64) (*socket.KeyStoreData, *table.LSTable, error)
	GetCookies() *cookies.Cookies
	SetDevice(dev *wastore.Device)
	RegisterE2EE(ctx context.Context, fbid int64) error
	// PrepareE2EE is the whatsmeow client for the encrypted chats, made
	// silent and acking only once its handlers have run.
	PrepareE2EE() (e2eeClient, error)
	// History is the secure-history client (the PIN-protected backup).
	History() historyClient
}

// realClient is messagix's client, with the two pieces the adapter wraps.
type realClient struct{ *messagix.Client }

var _ msgrClient = realClient{}

// newRealClient is the real client for a session's cookies. Its logger is
// zerolog.Nop() and login responses are never logged, redacted or not.
func newRealClient(ck *cookies.Cookies) msgrClient {
	return realClient{messagix.NewClient(ck, zerolog.Nop(), &messagix.Config{LogRedactedLoginResponses: false})}
}

func (c realClient) History() historyClient {
	return realHistory{c.Client.GetSecureHistoryClient()}
}

// Messenger is the messenger network. Its methods are safe from any
// goroutine.
type Messenger struct {
	st   *store.Store
	dir  string
	key  [32]byte
	emit network.Emit

	newClient func(ck *cookies.Cookies) msgrClient
	pace      *meta.Pace
	diff      *meta.SessionDiff
	now       func() time.Time
	gate      bool // started with --gate: the gate's one sample page of history is fetched

	mu        sync.Mutex
	started   bool
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	session   network.Session // the cookies held now; never written anywhere
	gen       int             // counts runs: events and results of an older one are dropped
	cli       msgrClient
	runCtx    context.Context
	runCancel context.CancelFunc
	fbid      int64
	e2ee      e2eeClient
	e2eeBegun bool // this run has started setting up the encrypted side
	state     network.State
	kind      wire.Kind
	work      sync.WaitGroup

	// the whatsmeow device store (e2ee.go)
	dmu       sync.Mutex
	db        *sql.DB
	container *sqlstore.Container
	dev       *wastore.Device // his registered, saved device
	// pending is a device made for pendingFor whose registration or save
	// failed in this helper: the next sign-in registers its keys again.
	pending    *wastore.Device
	pendingFor int64

	// the secure history (history.go)
	hmu     sync.Mutex // one restore at a time
	backup  *securehistory.Backup
	history string // historyState: "", noBackup, needsPIN, restored, skipped

	// what the gate counts (gate.go)
	cmu                   sync.Mutex
	counts                GateCounts
	listing               listing
	newestOneToOne        int64 // the thread key of the newest encrypted one-to-one, 0 for none
	newestAt              int64 // its last activity, in ms
	liveEncryptedReceived atomic.Int64
	liveUndecryptable     atomic.Int64
}

var (
	_ network.Network          = (*Messenger)(nil)
	_ network.SignInPager      = (*Messenger)(nil)
	_ network.EncryptedHistory = (*Messenger)(nil)
	_ network.Gate             = (*Messenger)(nil)
)

// New makes the messenger network over st; emit must be emit.For("messenger").
// Nothing is asked of Meta, and nothing is written, until a `login`.
func New(st *store.Store, dir string, key [32]byte, emit network.Emit) *Messenger {
	return &Messenger{
		st:        st,
		dir:       dir,
		key:       key,
		emit:      emit,
		newClient: newRealClient,
		pace:      &meta.Pace{},
		diff:      meta.NewSessionDiff(meta.FacebookPage),
		now:       time.Now,
		state:     network.NotConnected,
	}
}

// WithGate turns on what only the gate needs: after his history is restored,
// one page of the newest encrypted one-to-one is fetched, decrypted and
// counted. main calls it when the helper is started with --gate.
func (m *Messenger) WithGate() *Messenger {
	m.gate = true
	return m
}

// Name is "messenger".
func (m *Messenger) Name() string { return Name }

// SignIn is Facebook's sign-in page.
func (m *Messenger) SignIn() meta.SignInPage { return meta.FacebookPage }

// Start readies the network. It asks nothing of Meta and opens nothing: the
// cookies live in Pllum's Keychain and come as a `login`.
func (m *Messenger) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started || m.stopped {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(context.WithoutCancel(ctx))
	m.started = true
	return nil
}

// Status is the network's state. Messenger has no "message yourself" chat
// that Pllum shows, so SelfChatID is empty.
func (m *Messenger) Status() network.Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return network.Status{State: m.state}
}

// Login signs in with the cookies of a facebook.com session. A session
// missing a required cookie, or holding one empty, is badRequest before
// anything is asked of Meta. Otherwise it answers at once, and the outcome
// comes as state events: connecting, then live once the encrypted socket is
// up, or a failure.
func (m *Messenger) Login(ctx context.Context, p network.LoginParams) error {
	if p.Method != network.LoginSession {
		return errUnsupported
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started || m.stopped {
		return errInternal
	}
	session, ok := named(p.Session)
	if !ok {
		m.setState(network.NotConnected, wire.BadRequest)
		return errBadRequest
	}
	m.retire()
	// He has signed in again in the window: whatever Meta asked of him
	// before is done. Only here.
	m.pace.Clear()
	m.session = session
	m.diff.Seed(session)
	m.setState(network.Connecting, "")
	m.startRun()
	return nil
}

// named is the session's named cookies (the page's; anything else is
// dropped), and whether every required one is there with a value.
func named(s network.Session) (network.Session, bool) {
	out := network.Session{}
	for _, name := range meta.FacebookPage.Names() {
		if v := s[name]; v != "" {
			out[name] = v
		}
	}
	for _, name := range meta.FacebookPage.Required {
		if out[name] == "" {
			return nil, false
		}
	}
	return out, true
}

// CancelLogin stops a sign-in still connecting.
func (m *Messenger) CancelLogin() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != network.Connecting {
		return
	}
	m.retire()
	m.session = nil
	m.setState(network.NotConnected, "")
}

// Logout forgets the session here: it disconnects and drops the cookies.
// Nothing is sent to Meta (signing out is the website's), and the device
// Pllum registered stays, so the next sign-in is the same device.
func (m *Messenger) Logout(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retire()
	m.session = nil
	m.setState(network.NotConnected, "")
	return nil
}

// Nudge does nothing: the gate never retries on its own, and the encrypted
// socket reconnects on the library's timing.
func (m *Messenger) Nudge() {}

// The writes come with Task B3; until then each is unsupported, and nothing
// is ever sent.

func (m *Messenger) Send(ctx context.Context, o network.Outgoing) (string, error) {
	return "", errUnsupported
}

func (m *Messenger) SendState(messageID string) network.SendState {
	return network.SendState{State: network.SendUnknown}
}

func (m *Messenger) SendFor(clientID string) (string, error) { return "", nil }

func (m *Messenger) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	return errUnsupported
}

func (m *Messenger) MarkRead(ctx context.Context, chatID, through string) error {
	return errUnsupported
}

func (m *Messenger) MarkUnread(ctx context.Context, chatID string) error { return errUnsupported }

func (m *Messenger) SetMuted(ctx context.Context, chatID string, muted bool) error {
	return errUnsupported
}

func (m *Messenger) Media(ctx context.Context, chatID, messageID string) (string, error) {
	return "", errUnsupported
}

// Stop disconnects, waits briefly for the network's work to end, and closes
// the device database. The store is the server's to close.
func (m *Messenger) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	if !m.started {
		m.mu.Unlock()
		return
	}
	m.retire()
	m.cancel()
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.work.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// A request is stuck; it ends with the process.
	}
	m.dmu.Lock()
	if m.db != nil {
		m.db.Close()
		m.db, m.container, m.dev, m.pending = nil, nil, nil, nil
	}
	m.dmu.Unlock()
}

// retire ends the current run, if any: its context, its encrypted socket,
// and (by moving gen on) any event or result still to come from it. m.mu is
// held.
func (m *Messenger) retire() {
	m.gen++
	if m.runCancel != nil {
		m.runCancel()
		m.runCancel = nil
	}
	m.runCtx = nil
	m.e2eeBegun = false
	if old := m.cli; old != nil {
		m.cli = nil
		// messagix's Disconnect waits for its connection loop, which may be
		// inside our event handler on this very path.
		m.work.Add(1)
		go func() {
			defer m.work.Done()
			old.Disconnect()
		}()
	}
	if old := m.e2ee; old != nil {
		m.e2ee = nil
		// Disconnect waits for the socket to stop, which may be inside an
		// event handler on this very path.
		m.work.Add(1)
		go func() {
			defer m.work.Done()
			old.Disconnect()
		}()
	}
	m.cmu.Lock()
	m.counts.E2EEConnected = false
	m.counts.MainConnected = false
	m.cmu.Unlock()
}

// startRun signs in with m.session in the background, as run gen. m.mu is
// held.
func (m *Messenger) startRun() {
	ctx, cancel := context.WithCancel(m.ctx)
	m.runCtx, m.runCancel = ctx, cancel
	gen, session := m.gen, m.session
	m.work.Add(1)
	go func() {
		defer m.work.Done()
		m.run(ctx, gen, session)
	}()
}

// current reports whether gen is still the run in force.
func (m *Messenger) current(gen int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return gen == m.gen && !m.stopped
}

// run makes a client for the session, loads the messages page (which signs
// it in and lists the first page of his inbox), and connects the main
// socket. The rest happens when that socket first says it is connected
// (mainHandler), in mautrix-meta's connector's order.
func (m *Messenger) run(ctx context.Context, gen int, session network.Session) {
	ck := &cookies.Cookies{Platform: types.Facebook}
	values := make(map[cookies.MetaCookieName]string, len(session))
	for name, v := range session {
		values[cookies.MetaCookieName(name)] = v
	}
	ck.UpdateValues(values)
	cli := m.newClient(ck)
	m.mu.Lock()
	if gen != m.gen || m.stopped {
		m.mu.Unlock()
		return
	}
	m.cli = cli
	m.mu.Unlock()

	var user types.UserInfo
	var tbl *table.LSTable
	err := m.request(ctx, gen, func(ctx context.Context) (err error) {
		user, tbl, err = cli.LoadMessagesPage(ctx)
		return err
	})
	if err != nil {
		m.failed(gen, wire.KindOf(err))
		return
	}
	if user == nil || user.GetFBID() == 0 {
		m.failed(gen, wire.Internal)
		return
	}
	fbid := user.GetFBID()
	m.mu.Lock()
	if gen != m.gen || m.stopped {
		m.mu.Unlock()
		return
	}
	m.fbid = fbid
	m.mu.Unlock()
	m.addTable(tbl)

	cli.SetEventHandler(m.mainHandler(gen, cli, fbid))
	if err := m.request(ctx, gen, func(ctx context.Context) error { return cli.Connect(ctx) }); err != nil {
		m.failed(gen, wire.KindOf(err))
	}
}

// mainHandler is the main socket's event handler for run gen. The first
// connect (or reconnect, if no connect came) sets up the encrypted side,
// once per run, as the connector's handleMetaEvent does; the socket's own
// reconnects are messagix's. A socket Meta refuses as unauthorized is an
// ended session. Tables only add to the gate's counts.
func (m *Messenger) mainHandler(gen int, cli msgrClient, fbid int64) messagix.EventHandler {
	return func(_ context.Context, evt any) {
		defer func() { _ = recover() }()
		if !m.current(gen) {
			return
		}
		switch e := evt.(type) {
		case *messagix.ConnectedEvent, *messagix.ReconnectedEvent:
			m.socketUp(gen, false, true)
			m.beginE2EE(gen, cli, fbid)
		case *messagix.TransientDisconnectEvent:
			m.socketUp(gen, false, false)
		case *messagix.PermanentErrorEvent:
			m.pace.Fail(meta.ReconnectNeeded)
			m.pushedBack(gen, meta.ReconnectNeeded)
		case *table.LSTable:
			m.addTable(e)
		}
	}
}

// beginE2EE sets up the encrypted side for run gen, once: the device and
// the encrypted socket, then the history check, then the older pages of his
// inbox. All of it through the pace, in one goroutine.
func (m *Messenger) beginE2EE(gen int, cli msgrClient, fbid int64) {
	m.mu.Lock()
	ctx := m.runCtx
	if gen != m.gen || m.stopped || m.e2eeBegun || ctx == nil {
		m.mu.Unlock()
		return
	}
	m.e2eeBegun = true
	m.work.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.work.Done()
		if err := m.connectE2EE(ctx, gen, cli, fbid); err != nil {
			m.failed(gen, wire.KindOf(err))
			return
		}
		m.checkHistory(ctx, gen, cli, fbid)
		m.pageThreads(ctx, gen, cli)
	}()
}

// socketUp records the main socket (encrypted false) or the encrypted socket
// going up or down for run gen: live while both are up, offline while the
// libraries reconnect either.
func (m *Messenger) socketUp(gen int, encrypted, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.gen || m.stopped {
		return
	}
	m.cmu.Lock()
	if encrypted {
		m.counts.E2EEConnected = up
	} else {
		m.counts.MainConnected = up
	}
	both := m.counts.MainConnected && m.counts.E2EEConnected
	m.cmu.Unlock()
	switch {
	case both:
		m.setState(network.Live, "")
	case m.state == network.Live:
		m.setState(network.Offline, "")
	}
}

// pageThreads fetches the older pages of his inbox through the pace, as the
// connector's thread backfill does, until the listing ends (no page, no more
// before, or the oldest thread key stops moving) or maxThreadPages, and adds
// each to the gate's counts.
func (m *Messenger) pageThreads(ctx context.Context, gen int, cli msgrClient) {
	var prev int64
	for page := 0; page < maxThreadPages; page++ {
		var keys *socket.KeyStoreData
		var tbl *table.LSTable
		err := m.request(ctx, gen, func(ctx context.Context) (err error) {
			keys, tbl, err = cli.FetchMoreThreads(ctx, 1)
			return err
		})
		if err != nil || !m.current(gen) {
			return
		}
		if tbl == nil {
			m.listed(page, true, false)
			return
		}
		m.addTable(tbl)
		if keys == nil || !keys.HasMoreBefore || keys.MinThreadKey == prev {
			m.listed(page+1, true, false)
			return
		}
		prev = keys.MinThreadKey
		m.listed(page+1, false, false)
	}
	m.listed(maxThreadPages, false, true)
}

// failed ends run gen after a failure that is not Meta stopping the session
// (pushedBack has set those): he is left not connected, with the kind, until
// a new login.
func (m *Messenger) failed(gen int, kind wire.Kind) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.gen || m.stopped {
		return
	}
	m.retire()
	m.setState(network.NotConnected, kind)
}

// request makes one request to Meta through the pace: none while a block is
// in force, then a 2 to 4 s gap after the last. A success may carry rotated
// cookies to Pllum; Meta pushing back blocks the pace and, for a checkpoint,
// an ended session or a suspension, ends the run. A caller that expects an
// answer Meta gives as an error (no backup, a wrong PIN) catches it inside
// fn and returns nil: Meta answered.
func (m *Messenger) request(ctx context.Context, gen int, fn func(ctx context.Context) error) error {
	if err := m.blocked(); err != nil {
		return err
	}
	if err := m.pace.Wait(ctx); err != nil {
		return &wire.Error{Kind: wire.Timeout}
	}
	if err := m.blocked(); err != nil {
		return err
	}
	if !m.current(gen) {
		return errNotConnected
	}
	err := fn(ctx)
	if err == nil {
		m.pace.Succeed()
		m.checkSession(gen)
		return nil
	}
	kind := kindOf(err)
	m.pace.Fail(kind)
	m.pushedBack(gen, kind)
	return &wire.Error{Kind: errorKind(kind)}
}

// blocked is the error for a request now, when the pace forbids one.
func (m *Messenger) blocked() error {
	if kind, blocked := m.pace.Blocked(); blocked {
		return &wire.Error{Kind: errorKind(kind)}
	}
	return nil
}

// kindOf is the kind of a client error, as Instagram's: a checkpoint,
// challenge or consent page is checkpoint; a session Meta has ended is
// reconnectNeeded; a suspended account is banned (these two are Pace's, not
// wire kinds).
func kindOf(err error) wire.Kind {
	switch {
	case errors.Is(err, httpclient.ErrChallengeRequired),
		errors.Is(err, httpclient.ErrCheckpointRequired),
		errors.Is(err, httpclient.ErrConsentRequired):
		return wire.Checkpoint
	case errors.Is(err, httpclient.ErrTokenInvalidated),
		errors.Is(err, types.ErrPleaseLogIn),
		errors.Is(err, httpclient.ErrTooManyRedirects),
		isMetaError(err, 1357053):
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

// isMetaError reports whether err is Meta's error response with code.
func isMetaError(err error, code int) bool {
	var e *types.ErrorResponse
	return errors.As(err, &e) && e != nil && e.ErrorCode == code
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
// encrypted socket too, and nothing is asked again until a new login.
func (m *Messenger) pushedBack(gen int, kind wire.Kind) {
	s, k, ok := stateFor(kind)
	if !ok {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.gen || m.stopped {
		return
	}
	m.retire()
	m.setState(s, k)
}

// checkSession sends the session event when the client's cookies have
// changed (Meta rotates some as it answers): Pllum keeps the copy it signs
// in again from.
func (m *Messenger) checkSession(gen int) {
	m.mu.Lock()
	cli := m.cli
	ok := gen == m.gen && cli != nil
	m.mu.Unlock()
	if !ok {
		return
	}
	s, changed := m.diff.Changed(cli.GetCookies(), m.now())
	if !changed {
		return
	}
	m.mu.Lock()
	if gen == m.gen {
		m.session = network.Session(maps.Clone(s))
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
