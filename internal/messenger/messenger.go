// Package messenger is Pllum Connect's Messenger network, built on
// mautrix-meta's messagix client (his facebook.com web session) and
// whatsmeow's Messenger mode (his end-to-end encrypted chats).
//
// It signs in with the cookies of a facebook.com session Pllum took from its
// own sign-in window (`login` with method "session"), loads the messages
// page and connects the main (lightspeed) socket; on its first connect, as
// mautrix-meta's connector does, it registers Pllum as a Messenger device
// (once, ever) and connects the encrypted socket (e2ee.go), looks for his
// history backup, which his PIN restores (history.go), lists every folder
// of his inbox and fetches the recent history of the unencrypted threads
// (backfill.go). Everything the messages page, the pages and the main
// socket say is written into the store (ingest.go): groups as chats, his
// older one-to-one threads as aliases of their encrypted chats (ids.go),
// their lines, reactions, read receipts, mutes and people; and so is
// everything the encrypted socket says of the encrypted chats (their lines,
// receipts and undecryptable messages) and, once his PIN has restored it,
// his backup of them (history.go). The gate (gate.go) counts what it sees.
// It sends, reacts, marks read and mutes (writes.go: the group's main socket
// tasks and whatsmeow's encrypted sends, nothing else, each sent at most
// once), and fetches photos and stickers once each (media.go).
//
// Meta locks accounts that behave like a script, so every request goes
// through one meta.Pace: one at a time, 2 to 4 s apart, none for a while
// after a rateLimited, and none at all after a checkpoint, an ended session
// or a suspension until he signs in again (a new `login`). Both blocks are
// kept in the store, so neither a restart nor the same cookies sent again
// at Pllum's start asks Meta anything. Nothing here retries a request on
// its own; the sockets' reconnect timing is the library's.
//
// Nothing here logs: the client gets zerolog.Nop(), whatsmeow gets
// waLog.Noop, and every error returned carries only a fixed kind. The
// cookies and the PIN are held in memory only; the store keeps the
// secure-history device under "messenger.history" in its meta table (with
// the values ids.go lists), and the whatsmeow device lives in
// <dir>/messenger/device.db.
package messenger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
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
	"go.mau.fi/whatsmeow/types/events"

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
	errNotFound     = &wire.Error{Kind: wire.NotFound}
	errUnsupported  = &wire.Error{Kind: wire.Unsupported}
	errUnclear      = &wire.Error{Kind: wire.Unclear}
	// errRegistrationUnclear is a registration asked of Meta whose answer was
	// lost (or whose device could not be saved): a device may exist.
	errRegistrationUnclear = &wire.Error{Kind: wire.RegistrationUnclear}
)

// msgrClient is what the adapter uses of messagix's client: the messages
// page, the main (lightspeed) socket, the reads sent over it (ExecuteTasks
// with FetchThreadsTask and FetchMessagesTask only, which realClient
// enforces, and the sync group cursor those carry), the cookies (for the
// session event), and the encrypted side's registration. Its writes go
// through msgrWriter (writes.go). *messagix.Client satisfies it through
// realClient; tests use a fake.
type msgrClient interface {
	LoadMessagesPage(ctx context.Context) (types.UserInfo, *table.LSTable, error)
	SetEventHandler(h messagix.EventHandler)
	Connect(ctx context.Context) error
	Disconnect()
	ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error)
	GetCursor(db int64) string
	PostHandlePublishResponse(tbl *table.LSTable)
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

// ExecuteTasks hands Meta one of the two reads (a page of a folder's
// threads, a page of a thread's history); any other task, a write above
// all, is refused here and never reaches the client. The writes have their
// own door, msgrWriter.
func (c realClient) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if len(tasks) != 1 {
		return nil, errNotAllowed
	}
	switch tasks[0].(type) {
	case *socket.FetchThreadsTask, *socket.FetchMessagesTask:
		return c.Client.ExecuteTasks(ctx, tasks...)
	}
	return nil, errNotAllowed
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

	newClient     func(ck *cookies.Cookies) msgrClient
	newWriter     func(cli msgrClient, e e2eeClient) msgrWriter // nil when the clients can't write
	newDownloader func(cli msgrClient) msgrDownloader
	pace          *meta.Pace
	diff          *meta.SessionDiff
	now           func() time.Time
	afterFunc     func(d time.Duration, f func()) (stop func() bool) // the network's timers
	gate          bool                                               // started with --gate: every sign-in lists every folder, and one page of restored history is sampled
	pageWait      time.Duration                                      // how long a page of history may take to come back on the socket
	writeWait     time.Duration                                      // how long one write's request may wait for its turn and its answer

	mu        sync.Mutex
	started   bool
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	session   network.Session // the cookies held now; never written anywhere
	hashes    []string        // sessionHash of every version of the session held since the login
	gen       int             // counts runs: events and results of an older one are dropped
	cli       msgrClient
	runCtx    context.Context
	runCancel context.CancelFunc
	fbid      int64
	e2ee      e2eeClient
	e2eeBegun bool // this run has started setting up the encrypted side
	// e2eeEverUp: this run's encrypted socket has been up; e2eeFailed: Meta
	// ended its encrypted session, or its registration failed.
	e2eeEverUp bool
	e2eeFailed bool
	state      network.State
	kind       wire.Kind

	// The main socket's own redials (as Instagram's): drops counts its
	// failures in a row since it last connected; past maxDrops the run is
	// stopped and one redial (a new run with the session held) is due
	// meta.Backoff(redials) on, redialDue until it runs; stopRetry stops
	// its timer. redialing: the run in force is a redial. retry: the run
	// ended for a passing reason (offline, timeout), so a nudge may start
	// another.
	drops     int
	redials   int
	redialDue bool
	redialing bool
	retry     bool
	stopRetry func() bool
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
	hmu      sync.Mutex // one restore at a time
	backup   *securehistory.Backup
	history  string      // historyState: "", noBackup, needsPIN, restored, skipped
	epmu     sync.Mutex  // one change to the history device's keys at a time
	ebActive atomic.Bool // the first read of his backup is running
	efmu     sync.Mutex  // the run of failed backup requests
	odmu     sync.Mutex  // the day's pages of the backup asked on demand

	// what the gate counts (gate.go)
	cmu                   sync.Mutex
	counts                GateCounts
	listing               listing
	newestOneToOne        int64 // the thread key of the newest encrypted one-to-one, 0 for none
	newestAt              int64 // its last activity, in ms
	liveEncryptedReceived atomic.Int64
	liveUndecryptable     atomic.Int64

	vmu    sync.Mutex
	values map[string]string // the network's own store values, as read or written

	nmu   sync.Mutex
	names map[int64]string // people's names from contact rows, this run only

	lmu         sync.Mutex
	recent      map[string]string // a line seen this run → its chat, for edits
	recentOrder []string

	wmu     sync.Mutex
	waiters map[int64]chan rangeNote // a thread key → the FetchOlder waiting for its page

	lkmu  sync.Mutex
	locks map[string]*sync.Mutex // one history fetch per chat at a time

	chmu      sync.Mutex
	chPending bool
	chLast    time.Time

	// the sends waiting their turn (writes.go)
	smu      sync.Mutex
	queue    []string
	draining bool
	busy     atomic.Int64 // sends in line or going

	// the media files (media.go)
	mmu      sync.Mutex
	mlocks   map[string]*sync.Mutex
	mediaCap int64

	// testPrep, set only by tests, sees each encrypted message before it is
	// worked out.
	testPrep func(evt *events.FBMessage)
	// testKeep, set only by tests, fails a send's progress as the store
	// would.
	testKeep func(p sendProgress) error
}

var (
	_ network.Network          = (*Messenger)(nil)
	_ network.SignInPager      = (*Messenger)(nil)
	_ network.EncryptedHistory = (*Messenger)(nil)
	_ network.Gate             = (*Messenger)(nil)
	_ network.OlderPages       = (*Messenger)(nil)
)

// New makes the messenger network over st; emit must be emit.For("messenger").
// Nothing is asked of Meta, and nothing is written, until a `login`.
func New(st *store.Store, dir string, key [32]byte, emit network.Emit) *Messenger {
	return &Messenger{
		st:            st,
		dir:           dir,
		key:           key,
		emit:          emit,
		newClient:     newRealClient,
		newWriter:     writerFor,
		newDownloader: downloaderFor,
		pace:          &meta.Pace{},
		diff:          meta.NewSessionDiff(meta.FacebookPage),
		now:           time.Now,
		afterFunc:     func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop },
		pageWait:      pageWaitDefault,
		writeWait:     writeWait,
		state:         network.NotConnected,
		values:        map[string]string{},
		names:         map[int64]string{},
		recent:        map[string]string{},
		waiters:       map[int64]chan rangeNote{},
		locks:         map[string]*sync.Mutex{},
		mlocks:        map[string]*sync.Mutex{},
		mediaCap:      mediaCap,
	}
}

// WithGate turns on what only the gate needs: every sign-in lists every
// folder (with no year's reach, so the counts hold the oldest threads too),
// and after his history is restored, one page of the newest encrypted
// one-to-one is fetched, decrypted and counted. main calls it when the
// helper is started with --gate.
func (m *Messenger) WithGate() *Messenger {
	m.gate = true
	return m
}

// Name is "messenger".
func (m *Messenger) Name() string { return Name }

// SignIn is Facebook's sign-in page.
func (m *Messenger) SignIn() meta.SignInPage { return meta.FacebookPage }

// Start readies the network. It asks nothing of Meta and opens nothing: the
// cookies live in Pllum's Keychain and come as a `login`. A rate limit from
// an earlier run runs on to its end, and a session Meta stopped in an
// earlier run stays stopped.
func (m *Messenger) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started || m.stopped {
		return nil
	}
	m.ctx, m.cancel = context.WithCancel(context.WithoutCancel(ctx))
	m.started = true
	if until, step, ok := m.keptRateLimit(); ok {
		m.pace.RestoreRateLimit(until, step)
	}
	if kind, _, ok := m.keptBlock(); ok {
		m.holdBlock(kind)
	}
	// A write that may have reached Meta before an earlier run stopped is
	// never sent again.
	m.forgetCutOff()
	return nil
}

// Status is the network's state, and how its encrypted connection is.
// Messenger has no "message yourself" chat that Pllum shows, so SelfChatID
// is empty.
func (m *Messenger) Status() network.Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return network.Status{State: m.state, Encrypted: m.encryptedState()}
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
	m.calm()
	hash := sessionHash(session)
	if kind, stopped, ok := m.keptBlock(); ok {
		if slices.Contains(stopped, hash) {
			// The session Meta stopped (any version of it Pllum was given),
			// sent again from the Keychain at start: refused here, with no
			// request.
			m.holdBlock(kind)
			return nil
		}
		// A new session: he has signed in again in the window.
		_ = m.keep("block", "")
	}
	// He has signed in again in the window: whatever Meta asked of him
	// before is done. Only here.
	m.pace.Clear()
	m.session = session
	m.hashes = []string{hash}
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
	m.calm()
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
	m.calm()
	m.session = nil
	m.setState(network.NotConnected, "")
	return nil
}

// Nudge runs a waiting redial now, or, after a run that ended for a
// passing reason (no connection, a timeout; never a block), starts one new
// run with the session held: once per nudge, through the pace. Pllum
// nudges when the Mac wakes.
func (m *Messenger) Nudge() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.redialDue {
		m.redial()
		return
	}
	if m.retry {
		m.retry = false
		m.again()
	}
}

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
	if gen != m.gen || m.stopped {
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
	if gen == m.gen {
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
	gen := m.gen
	m.stopRetry = m.afterFunc(meta.Backoff(m.redials), func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if gen == m.gen {
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
	m.calm()
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
	m.e2eeEverUp, m.e2eeFailed = false, false
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
	m.ingest(tbl, store.SourceSync)

	cli.SetEventHandler(m.mainHandler(gen, cli, fbid))
	if err := m.request(ctx, gen, func(ctx context.Context) error { return cli.Connect(ctx) }); err != nil {
		m.failed(gen, wire.KindOf(err))
	}
}

// mainHandler is the main socket's event handler for run gen. The first
// connect (or reconnect, if no connect came) sets up the encrypted side,
// once per run, as the connector's handleMetaEvent does; the socket's own
// reconnects are messagix's. A socket Meta refuses as unauthorized is an
// ended session. Its tables are written into the store.
func (m *Messenger) mainHandler(gen int, cli msgrClient, fbid int64) messagix.EventHandler {
	return func(_ context.Context, evt any) {
		defer func() { _ = recover() }()
		if !m.current(gen) {
			return
		}
		switch e := evt.(type) {
		case *messagix.ConnectedEvent, *messagix.ReconnectedEvent:
			m.connected(gen)
			m.socketUp(gen, false, true)
			m.beginE2EE(gen, cli, fbid)
		case *messagix.TransientDisconnectEvent:
			m.socketUp(gen, false, false)
			m.disconnected(gen)
		case *messagix.PermanentErrorEvent:
			m.pace.Fail(meta.ReconnectNeeded)
			m.pushedBack(gen, meta.ReconnectNeeded)
		case *table.LSTable:
			m.ingest(e, store.SourceLive)
		}
	}
}

// beginE2EE sets up the encrypted side for run gen, once: the device and
// the encrypted socket, then the history check, then (until the first
// sign-in's listing has ended, or every sign-in with --gate) the listing of
// every folder, then (not with --gate) the first sign-in's history. All of
// it through the pace, in one goroutine.
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
		// A panic in a page's ingest must not end the helper or reach
		// standard error, as in the main handler.
		defer func() { _ = recover() }()
		if err := m.connectE2EE(ctx, gen, cli, fbid); err != nil {
			if wire.KindOf(err) == wire.RegistrationUnclear {
				// A device may exist: kept as the session's block, so
				// nothing registers again (with new keys, after a restart)
				// until he signs in again.
				m.mu.Lock()
				if gen == m.gen {
					m.e2eeFailed = true
				}
				m.mu.Unlock()
				m.pace.Fail(meta.ReconnectNeeded)
				m.pushedBack(gen, wire.RegistrationUnclear)
				return
			}
			if k := wire.KindOf(err); k == wire.Unclear || k == wire.Internal {
				m.mu.Lock()
				if gen == m.gen {
					m.e2eeFailed = true
				}
				m.mu.Unlock()
			}
			m.failed(gen, wire.KindOf(err))
			return
		}
		m.checkHistory(ctx, gen, cli, fbid)
		if m.gate || m.value("listed") == "" {
			if !m.listThreads(ctx, gen, cli) {
				return
			}
		}
		if !m.gate {
			// The gate only counts: it fetches no history (review I4).
			m.backfill(ctx, gen, cli)
			// His restored backup, once (history.go); a restore made later
			// in this run starts it itself.
			m.encryptedBackfill(ctx, gen, cli, fbid)
		}
	}()
}

// socketUp records the main socket (encrypted false) or the encrypted socket
// going up or down for run gen. Messenger is live only while both are up.
// With the main socket up (groups read) and the encrypted one down after it
// had been up, or ended, it is offline with kind unsupported: the encrypted
// chats can't be read. With the main socket down it is offline while the
// library reconnects it. Until both have first come up it stays connecting.
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
	main, enc := m.counts.MainConnected, m.counts.E2EEConnected
	m.cmu.Unlock()
	if encrypted && up {
		m.e2eeEverUp, m.e2eeFailed = true, false
	}
	switch {
	case main && enc:
		m.setState(network.Live, "")
	case main && (m.e2eeEverUp || m.e2eeFailed):
		m.setState(network.Offline, wire.Unsupported)
	case !main && (m.state == network.Live || m.state == network.Offline):
		m.setState(network.Offline, "")
	}
}

// failed ends run gen after a failure that is not Meta stopping the session
// (pushedBack has set those). A redial's run is offline, with the kind, and
// the next redial is due twice as long on. Any other run is not connected,
// with the kind: a passing kind (offline, timeout) waits for a nudge, which
// starts a new run with the session held; any other waits for a new login.
func (m *Messenger) failed(gen int, kind wire.Kind) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if gen != m.gen || m.stopped {
		return
	}
	m.retire()
	if m.redialing {
		m.redialing = false
		shown := kind
		if kind == wire.Offline {
			shown = ""
		}
		m.setState(network.Offline, shown)
		m.scheduleRedial()
		return
	}
	m.setState(network.NotConnected, kind)
	m.retry = passing(kind)
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
		m.keepRateLimit()
		m.checkSession(gen)
		return nil
	}
	kind := kindOf(err)
	m.pace.Fail(kind)
	m.keepRateLimit()
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
	// A connection that failed at the network (the encrypted socket's dial,
	// say): no connection, or a timeout.
	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return wire.Timeout
		}
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
	m.calm() // nothing redials or retries until a new login
	m.setState(s, k)
	// Kept, so neither a restart nor any version of these cookies sent again
	// asks Meta.
	hashes := m.hashes
	if len(hashes) == 0 {
		hashes = []string{sessionHash(m.session)}
	}
	_ = m.keep("block", string(kind)+"\n"+strings.Join(hashes, ","))
}

// sessionHash is a SHA-256 of a session's required cookies: what tells the
// session Meta stopped from a new sign-in, with no cookie value kept.
func sessionHash(s network.Session) string {
	h := sha256.New()
	for _, name := range meta.FacebookPage.Required {
		h.Write([]byte(name + "=" + s[name] + "\x00"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// keptBlock is the kind that stopped a session in this or an earlier run,
// and the hashes of every version of that session; ok is false when none
// is kept.
func (m *Messenger) keptBlock() (kind wire.Kind, hashes []string, ok bool) {
	k, list, ok := strings.Cut(m.value("block"), "\n")
	if !ok || list == "" {
		return "", nil, false
	}
	if _, _, terminal := stateFor(wire.Kind(k)); !terminal {
		return "", nil, false
	}
	return wire.Kind(k), strings.Split(list, ","), true
}

// holdBlock reports a kept block's state again and blocks the pace on it
// (never Clear: only a new session lifts it). m.mu is held.
func (m *Messenger) holdBlock(kind wire.Kind) {
	st, k, _ := stateFor(kind)
	m.pace.Fail(paceKind(kind))
	m.setState(st, k)
}

// keepRateLimit keeps the pace's rate limit (its end and its count) so a
// restart carries it on; nothing when there is none.
func (m *Messenger) keepRateLimit() {
	until, step := m.pace.RateLimit()
	v := ""
	if step > 0 || !until.IsZero() {
		v = strconv.FormatInt(until.UnixMilli(), 10) + "\n" + strconv.Itoa(step)
	}
	_ = m.keep("rate", v)
}

// keptRateLimit is the rate limit an earlier run kept.
func (m *Messenger) keptRateLimit() (until time.Time, step int, ok bool) {
	ms, n, ok := strings.Cut(m.value("rate"), "\n")
	if !ok {
		return time.Time{}, 0, false
	}
	untilMs, err1 := strconv.ParseInt(ms, 10, 64)
	step, err2 := strconv.Atoi(n)
	if err1 != nil || err2 != nil {
		return time.Time{}, 0, false
	}
	if untilMs > 0 {
		until = time.UnixMilli(untilMs)
	}
	return until, step, true
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
	// The rotated set is the session now: a block names it as well as the
	// versions before.
	m.mu.Lock()
	if gen == m.gen {
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
