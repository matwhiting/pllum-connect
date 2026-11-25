// Package messenger is Pllum Connect's Messenger network, built on
// mautrix-meta's messagix client (his facebook.com web session) and
// whatsmeow's Messenger mode (his end-to-end encrypted chats).
//
// It signs in with the cookies of a facebook.com session Pllum took from its
// own sign-in window (`login` with method "session"), loads the messages
// page and connects the main (lightspeed) socket; on its first connect, as
// mautrix-meta's connector does, it registers Pllum as a Messenger device
// (once, ever; replaced once, ever,
// if the self-check finds its identity missing from his account's ICDC
// list) and connects the encrypted socket (e2ee.go), looks for his
// history backup, which his PIN restores (history.go), lists every folder
// of his inbox and fetches the recent history of the unencrypted threads
// (backfill.go). Everything the messages page, the pages and the main
// socket say is written into the store (ingest.go; the encrypted side's in
// enc_ingest.go): groups as chats, his
// older one-to-one threads as aliases of their encrypted chats (ids.go),
// their lines, reactions, read receipts, mutes and people; and so is
// everything the encrypted socket says of the encrypted chats (their lines,
// receipts and undecryptable messages) and, once his PIN has restored it,
// his backup of them (enc_history.go). The gate (gate.go) counts what it
// sees. It sends (send.go, its files uploaded by upload.go), reacts, marks
// read and mutes (marks.go): the group's main socket tasks and whatsmeow's
// encrypted sends, nothing else (writes.go), each sent at most once; and it
// fetches photos and stickers once each (media.go). The run, its redials
// (redial.go) and the session's requests and blocks (session.go) are
// below; the state each lock guards is in state.go.
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
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/matwhiting/pllum-connect/internal/mediacache"
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
	// FetchICDC is the account's ICDC list as Meta gives it: every device
	// identity in it, and its seq (icdc.go).
	FetchICDC(ctx context.Context, fbid int64, deviceUUID uuid.UUID) (identities [][32]byte, seq int, err error)
	// SessionDeviceID is a Messenger Lite session's Facebook device ID,
	// uuid.Nil for a web session (icdc.go).
	SessionDeviceID() uuid.UUID
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
	clock         network.Clock // the network's timers and its pace; tests move a fake one
	pace          *meta.Pace
	sess          *meta.Session // the pace, and the block and rate limit kept in the store
	diff          *meta.SessionDiff
	gate          bool          // started with --gate: every sign-in lists every folder, and one page of restored history is sampled
	pageWait      time.Duration // how long a page of history may take to come back on the socket
	writeWait     time.Duration // how long one write's request may wait for its turn and its answer

	mu        sync.Mutex
	started   bool
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	session   network.Session // the cookies held now; never written anywhere
	hashes    []string        // sessionHash of every version of the session held since the login
	runs      network.Run     // the run in force: its number (events and results of an older one are dropped) and context
	cli       msgrClient
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
	work      network.Work
	// side counts the goroutines outside work still running (the
	// self-check, which Stop does not wait for); tests settle on it.
	side atomic.Int32

	devStore deviceStore // the whatsmeow device store (e2ee.go)
	restore  restoreState
	book     backupBook
	checked  atomic.Bool // this run's self-check has run (diag.go)
	ebActive atomic.Bool // the first read of his backup is running (enc_history.go)
	fills    fills
	gauge    gauge
	memo     runMemo
	values   *network.Values // the network's own store values, as read or written
	changes  network.Changed // the changed events (backfill.go)
	traffic  network.Traffic // the requests that reach Meta, for the history queue
	sends    sends

	// the media folder every network shares (media.go)
	media *mediacache.Cache

	// The history queue (backlog.go), with WithHistoryQueue only: nil
	// without. bqGen is the run its Run was started for; bqRuns counts the
	// Runs still running (tests settle on it).
	bq     *network.Backlog
	bqGen  int
	bqRuns atomic.Int32

	// testPrep, set only by tests, sees each encrypted message before it is
	// worked out.
	testPrep func(evt *events.FBMessage)
	// testReregister, set only by tests, runs as a re-registration begins,
	// once its marker is kept.
	testReregister func()
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
	m := &Messenger{
		st:            st,
		dir:           dir,
		key:           key,
		emit:          emit,
		newClient:     newRealClient,
		newWriter:     writerFor,
		newDownloader: downloaderFor,
		clock:         network.RealClock{},
		diff:          meta.NewSessionDiff(meta.FacebookPage),
		pageWait:      pageWaitDefault,
		writeWait:     writeWait,
		state:         network.NotConnected,
		values:        network.NewValues(st, Name),
		memo: runMemo{
			names:   map[int64]string{},
			recent:  map[string]string{},
			waiters: map[int64]chan rangeNote{},
			locks:   map[string]*sync.Mutex{},
		},
		media: mediacache.New(dir, key),
	}
	// The pace reads the network's clock when it runs, so a test's clock,
	// set after New, is the one it uses.
	m.pace = &meta.Pace{
		Now:   func() time.Time { return m.clock.Now() },
		Sleep: func(ctx context.Context, d time.Duration) error { return m.clock.Sleep(ctx, d) },
	}
	m.sess = &meta.Session{Page: meta.FacebookPage, Pace: m.pace, Values: m.values, KindOf: kindOf, Terminal: terminal}
	m.sends.outbox = network.Outbox{Begin: m.beginSends, End: m.work.Done, Deliver: func(_ context.Context, id string) { m.deliver(id) },
		Stopped: m.failStopped, Busy: func(d int) { m.sends.busy.Add(int64(d)) }}
	return m
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

// WithMedia makes the network keep its files in media, the folder every
// network shares; main gives each network the same one. Call it before
// Start.
func (m *Messenger) WithMedia(media *mediacache.Cache) *Messenger {
	m.media = media
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
	if until, step, ok := m.sess.KeptRateLimit(); ok {
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
	m.devStore.mu.Lock()
	if m.devStore.db != nil {
		m.devStore.db.Close()
		m.devStore.db, m.devStore.container, m.devStore.dev, m.devStore.pending, m.devStore.replacing = nil, nil, nil, nil, nil
	}
	m.devStore.mu.Unlock()
}

// retire ends the current run, if any: its context, its encrypted socket,
// and (by moving gen on) any event or result still to come from it. m.mu is
// held.
func (m *Messenger) retire() {
	m.runs.Retire()
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
	m.gauge.mu.Lock()
	m.gauge.counts.E2EEConnected = false
	m.gauge.counts.MainConnected = false
	m.gauge.mu.Unlock()
}

// startRun signs in with m.session in the background, as run gen. m.mu is
// held.
func (m *Messenger) startRun() {
	ctx, gen := m.runs.Begin(m.ctx)
	m.e2eeEverUp, m.e2eeFailed = false, false
	session := m.session
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
	return gen == m.runs.Gen && !m.stopped
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
	if gen != m.runs.Gen || m.stopped {
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
	if gen != m.runs.Gen || m.stopped {
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
	ctx := m.runs.Ctx()
	if gen != m.runs.Gen || m.stopped || m.e2eeBegun || ctx == nil {
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
			m.e2eeSetupFailed(gen, err) // e2ee.go
			return
		}
		// the self-check's ICDC list, and the one re-registration it may
		// call for (diag.go), before anything reads with the device
		if !m.icdcCheck(ctx, gen) {
			return
		}
		m.checkHistory(ctx, gen, cli, fbid)
		// a twin "<key>@msgr" of a chat its key already belongs to joins it (ids.go)
		m.foldTwins()
		if m.gate || m.value("listed") == "" {
			if !m.listThreads(ctx, gen, cli) {
				return
			}
		}
		if !m.gate {
			// The gate only counts: it fetches no history (review I4).
			m.backfill(ctx, gen, cli)
			// His restored backup, once (enc_history.go); a restore made later
			// in this run starts it itself.
			m.encryptedBackfill(ctx, gen, cli, fbid)
			// what the encrypted socket didn't bring, once a day (catchup.go)
			m.catchUp(ctx, gen, cli, fbid)
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
	if gen != m.runs.Gen || m.stopped {
		return
	}
	m.gauge.mu.Lock()
	if encrypted {
		m.gauge.counts.E2EEConnected = up
	} else {
		m.gauge.counts.MainConnected = up
	}
	main, enc := m.gauge.counts.MainConnected, m.gauge.counts.E2EEConnected
	m.gauge.mu.Unlock()
	if encrypted && up {
		m.e2eeEverUp, m.e2eeFailed = true, false
	}
	switch {
	case main && enc:
		m.setState(network.Live, "")
		m.startQueue(gen)
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
	if gen != m.runs.Gen || m.stopped {
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
