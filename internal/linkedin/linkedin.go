// Package linkedin is Pllum Connect's LinkedIn network, built on
// mautrix-linkedin's linkedingo client (a patched copy in
// third_party/mautrix-linkedin; see its PLLUM-CHANGES.md). It signs in with
// the cookies of a linkedin.com session Pllum took from its own sign-in
// window (`login` with method "session", sent from the Keychain on every
// start), catches up through LinkedIn's sync token, stays live on LinkedIn's
// realtime stream, and pages history slowly in the background. Reads are the
// server's, from the store. The writes (a send, a reaction, a mark, a mute)
// are writes.go's, and photo fetches media.go's.
//
// LinkedIn restricts accounts that behave like a script, so the network makes
// only the calls liClient lists, its background requests go one at a time, a
// random 2 to 4 s apart (Meta's pace), and LinkedIn pushing back (a 403, 429 or 999) pauses
// every request, the realtime stream included (pause.go). A session LinkedIn
// ends (a 401, a 302, li_at deleted) is reconnectNeeded, and nothing asks
// with it again: only a new sign-in in the window does.
//
// Nothing here logs: the library logs only through zerolog, which every
// context it is given carries as a no-op (and quiet.Init switches off), and
// every error returned carries only a fixed kind. The cookies are held in
// memory only.
package linkedin

import (
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/mediacache"
	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

var (
	errBadRequest   = &wire.Error{Kind: wire.BadRequest}
	errInternal     = &wire.Error{Kind: wire.Internal}
	errNotConnected = &wire.Error{Kind: wire.NotConnected}
	errUnsupported  = &wire.Error{Kind: wire.Unsupported}
	errRateLimited  = &wire.Error{Kind: wire.RateLimited}
	// errGone is a background request's failure that is LinkedIn's answer
	// that the thing asked about is not to be had (gone): only it settles
	// background work for good. Its kind is internal.
	errGone = &wire.Error{Kind: wire.Internal}
)

// liClient is every LinkedIn call the helper may make, and nothing else:
// the reads, the realtime stream, and the writes (a send, its upload, a
// reaction, a mark and his sign-out). *linkedingo.Client satisfies it; tests
// use a fake. GetConversationsUpdatedBefore is allowed but never used: LinkedIn
// ignores its lastUpdatedBefore (mautrix/linkedin#56), so conversations are
// listed, a folder at a time, by GetConversationsWithNextCursor, Pllum's patch.
type liClient interface {
	GetCurrentUserProfile(ctx context.Context) (*linkedingo.UserProfile, error)
	GetConversationsBySyncToken(ctx context.Context) (*linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation], error)
	GetConversationsUpdatedBefore(ctx context.Context, updatedBefore time.Time) (*linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation], error)
	GetConversationsWithNextCursor(ctx context.Context, category, cursor string) (*linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation], error)
	GetMessagesBefore(ctx context.Context, conversationURN linkedingo.URN, before time.Time, count int) (*linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message], error)
	GetMessagesWithPrevCursor(ctx context.Context, conversationURN linkedingo.URN, prevCursor string, count int) (*linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message], error)
	DownloadHTTP(ctx context.Context, url string) (*http.Response, error)
	RealtimeConnect(ctx context.Context) error
	RealtimeDisconnect()
	IsLoggedIn() bool

	SendMessage(ctx context.Context, conversationURN linkedingo.URN, body linkedingo.SendMessageBody, renderContent []linkedingo.SendRenderContent, transactionID string) (*linkedingo.MessageSentResponse, error)
	UploadMedia(ctx context.Context, mediaUploadType linkedingo.MediaUploadType, filename, contentType string, size int, r io.Reader) (linkedingo.URN, error)
	SendReaction(ctx context.Context, messageURN linkedingo.URN, emoji string) error
	RemoveReaction(ctx context.Context, messageURN linkedingo.URN, emoji string) error
	MarkConversationRead(ctx context.Context, convURNs ...linkedingo.URN) (*linkedingo.MarkThreadReadResponse, error)
	MarkConversationUnread(ctx context.Context, convURNs ...linkedingo.URN) (*linkedingo.MarkThreadReadResponse, error)
	Logout(ctx context.Context) error
}

var _ liClient = (*linkedingo.Client)(nil)

// Clock is the time the network's timers and its pace run on; tests move a
// fake one.
type Clock = network.Clock

// Options are what tests change; the zero value is the real thing.
type Options struct {
	// NewClient makes a client signed in as self (an empty URN before his
	// profile is known) with the cookies in jar, presenting userAgent alone
	// as its browser, resuming LinkedIn's conversations sync from syncToken.
	NewClient func(self linkedingo.URN, jar *linkedingo.StringCookieJar, userAgent, syncToken string, h linkedingo.Handlers) liClient
	Clock     Clock
	// Rand gives the background pace's randomness, in [0, 1).
	Rand func() float64
	// Media is the media folder every network shares; main gives each
	// network the same one. nil makes one of its own.
	Media *mediacache.Cache
}

// realClient is linkedingo's client, with the page's User-Agent in place of
// the library's browser headers. Its page instance and tracking header are
// the library's defaults (the window injects no script, so it can't capture
// the page's own).
func realClient(self linkedingo.URN, jar *linkedingo.StringCookieJar, ua, syncToken string, h linkedingo.Handlers) liClient {
	c := linkedingo.NewClient(silent(context.Background()), self, jar, "", "", syncToken, h)
	c.SetBrowserHeaders(browserHeaders(ua))
	return c
}

// silent is ctx carrying a logger that writes nothing, for the library.
func silent(ctx context.Context) context.Context { return zerolog.Nop().WithContext(ctx) }

// LinkedIn is the linkedin network. Its methods are safe from any goroutine.
type LinkedIn struct {
	st   *store.Store
	dir  string
	key  [32]byte
	emit network.Emit

	newClient func(self linkedingo.URN, jar *linkedingo.StringCookieJar, userAgent, syncToken string, h linkedingo.Handlers) liClient
	clock     Clock
	rand      func() float64
	pause     *pause
	diff      *meta.SessionDiff

	mu         sync.Mutex
	started    bool
	stopped    bool
	ctx        context.Context
	cancel     context.CancelFunc
	state      network.State
	kind       wire.Kind
	runs       network.Run     // the run in force: its number (events and results of an older one are dropped) and context
	session    network.Session // the cookies held now; never written anywhere
	hashes     []string        // sessionHash of every version of the session held since the login
	cli        liClient        // the run's signed-in client, nil before the profile check
	jar        *linkedingo.StringCookieJar
	realtime   bool      // the run's client has its realtime stream started
	connected  bool      // a ClientConnection has come in this run
	sessID     uuid.UUID // the realtime session of the last ClientConnection
	retry      bool      // the run ended for a passing reason: a nudge may start another
	stopResume func() bool
	due        struct{ sync, pages bool } // catch-ups the realtime stream asked for

	// Background work a passing failure stopped (sync.go's later): the
	// catch-ups to ask again when the timer fires (at laterAt; laterSeq
	// tells a timer replaced by a sooner one). All belong to the run in
	// force, and retire forgets them.
	stopLater    func() bool
	laterAt      time.Time
	laterSeq     int
	held         struct{ sync, pages bool }
	tokenDropped bool // the run's client holds a sync token LinkedIn won't take
	// aside are the chats LinkedIn has failed on (sync.go's pass), each
	// backed off on its own. Kept across runs, in memory only: a restart
	// starts every chat afresh.
	aside map[string]backoff
	// jobAside are the catch-ups (jobSync, jobPages) LinkedIn has failed
	// on (sync.go's later), backed off the same way, kept the same way.
	jobAside map[job]backoff

	// A sign-in being checked: its profile is read before anything else
	// changes, so another account's is refused with the run in force, the
	// kept block and the store as they were.
	probing     bool
	probeGen    int
	probeCancel context.CancelFunc
	prevState   network.State // the state before the sign-in began, for a refusal to go back to
	prevKind    wire.Kind

	wake  chan struct{}
	woken atomic.Int32 // wakes in wake not yet taken up (tests settle on it)
	work  sync.WaitGroup
	busy  atomic.Int32 // runs at work and stream stops still running

	values *network.Values

	pmu    sync.Mutex
	nextBg time.Time // the earliest the next background request may go

	changes network.Changed
	traffic network.Traffic // the requests that reach LinkedIn, for the history queue

	// The sends in line (writes.go): one goroutine sends them, in order.
	outbox network.Outbox

	// The media folder every network shares (media.go).
	media *mediacache.Cache
}

var (
	_ network.Network     = (*LinkedIn)(nil)
	_ network.SignInPager = (*LinkedIn)(nil)
)

// New makes the linkedin network over st; emit must be emit.For("linkedin").
// Nothing is asked of LinkedIn until a `login`.
func New(st *store.Store, dir string, key [32]byte, emit network.Emit, opts Options) *LinkedIn {
	l := &LinkedIn{
		st:        st,
		dir:       dir,
		key:       key,
		emit:      emit,
		newClient: opts.NewClient,
		clock:     opts.Clock,
		rand:      opts.Rand,
		diff:      meta.NewSessionDiff(SignInPage()),
		state:     network.NotConnected,
		values:    network.NewValues(st, Name),
		wake:      make(chan struct{}, 1),
		media:     opts.Media,
	}
	if l.newClient == nil {
		l.newClient = realClient
	}
	if l.clock == nil {
		l.clock = network.RealClock{}
	}
	if l.rand == nil {
		l.rand = rand.Float64
	}
	if l.media == nil {
		l.media = mediacache.New(dir, key)
	}
	l.outbox = network.Outbox{Begin: l.beginSends, End: l.work.Done, Deliver: l.deliver,
		Stopped: func(id string) { l.fail(id, wire.NotConnected) }, Busy: func(d int) { l.busy.Add(int32(d)) }}
	l.pause = &pause{now: func() time.Time { return l.clock.Now() }}
	return l
}

// Name is "linkedin".
func (l *LinkedIn) Name() string { return Name }

// SignIn is LinkedIn's sign-in page.
func (l *LinkedIn) SignIn() meta.SignInPage { return SignInPage() }

// Start readies the network. It asks nothing of LinkedIn: the cookies live in
// Pllum's Keychain, and Pllum sends them as a `login` on every start. A pause
// from an earlier run runs to its end, and a session LinkedIn ended stays
// ended. A send an earlier run left sending is unknown, and never sent again.
func (l *LinkedIn) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started || l.stopped {
		return nil
	}
	l.ctx, l.cancel = context.WithCancel(silent(context.WithoutCancel(ctx)))
	l.started = true
	l.pause.restore(l.value("pause"))
	l.forgetCutOff()
	if len(l.keptBlock()) > 0 {
		l.setState(network.ReconnectNeeded, "")
	}
	return nil
}

// Status is the network's state. LinkedIn has no "message yourself" chat.
func (l *LinkedIn) Status() network.Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	return network.Status{State: l.state}
}

// Login signs in with the cookies of a linkedin.com session. A session
// without li_at or JSESSIONID is badRequest before anything is asked. The
// session LinkedIn ended (any version of it), sent again, is reconnectNeeded
// with nothing asked. Otherwise it answers at once, and the outcome comes as
// state events: connecting, catchingUp and live; notConnected with refused
// for another account than his; or a failure. Nothing else changes (the run
// in force, a kept ended session) until the profile read shows the session
// is his (signIn).
func (l *LinkedIn) Login(ctx context.Context, p network.LoginParams) error {
	if p.Method != network.LoginSession {
		return errUnsupported
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.started || l.stopped {
		return errInternal
	}
	// Every outcome is said, even one equal to the state in force: Pllum
	// clears the row as it sends a session, and waits for the answer.
	session, ok := named(p.Session)
	if !ok {
		l.sayState(network.NotConnected, wire.BadRequest)
		return errBadRequest
	}
	hash := sessionHash(session)
	if slices.Contains(l.keptBlock(), hash) {
		l.endProbe()
		l.retire()
		l.session = nil
		l.sayState(network.ReconnectNeeded, "")
		return nil
	}
	if _, blocked := l.pause.blocked(); blocked {
		// Nothing may be asked; the sign-in is checked when the pause ends.
		l.endProbe()
		l.retire()
		l.session = session
		l.hashes = []string{hash}
		l.sayState(network.Offline, wire.RateLimited)
		l.scheduleResume()
		return nil
	}
	l.startProbe(session, hash)
	return nil
}

// CancelLogin stops a sign-in still connecting.
func (l *LinkedIn) CancelLogin() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != network.Connecting {
		return
	}
	l.endProbe()
	l.retire()
	l.session = nil
	l.setState(network.NotConnected, "")
}

// Logout is his Disconnect: it signs the session out of LinkedIn (once,
// unless a pause or an ended session forbids a request), stops the stream,
// forgets the cookies and forgets the account (forgetAccount), so the next
// sign-in may be any account's. The chats stay in the store, readable; a
// pause stays, as it is LinkedIn's.
func (l *LinkedIn) Logout(ctx context.Context) error {
	l.mu.Lock()
	cli := l.cli
	l.endProbe()
	l.retire()
	l.session = nil
	l.hashes = nil
	l.forgetAccount()
	l.setState(network.NotConnected, "")
	l.mu.Unlock()
	if cli == nil {
		return nil
	}
	if _, blocked := l.pause.blocked(); blocked {
		return nil
	}
	cctx, cancel := context.WithTimeout(silent(ctx), 20*time.Second)
	defer cancel()
	_ = cli.Logout(cctx)
	return nil
}

// Nudge tries again, with the cookies it holds, after a sign-in failed for a
// passing reason (no connection, a timeout), and wakes background work that
// stopped on one. A pause is never ended by it: its own end is.
func (l *LinkedIn) Nudge() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.retry {
		l.again()
		return
	}
	l.signal()
}

// Stop disconnects and waits briefly for the network's work to end. The
// store is the server's to close.
func (l *LinkedIn) Stop() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	if !l.started {
		l.mu.Unlock()
		return
	}
	l.endProbe()
	l.retire()
	l.cancel()
	l.mu.Unlock()

	done := make(chan struct{})
	go func() {
		l.work.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// A request is stuck; it ends with the process.
	}
}

// passing reports whether a failure of this kind may be retried by a nudge.
func passing(kind wire.Kind) bool { return kind == wire.Offline || kind == wire.Timeout }

// retire ends the current run, if any: its context, its stream, any timer,
// and (by moving gen on) any event or result still to come from it. l.mu is
// held.
func (l *LinkedIn) retire() {
	l.runs.Retire()
	l.retry = false
	if l.stopResume != nil {
		l.stopResume()
		l.stopResume = nil
	}
	l.due.sync, l.due.pages = false, false
	if l.stopLater != nil {
		l.stopLater()
		l.stopLater = nil
	}
	l.held.sync, l.held.pages = false, false
	l.tokenDropped = false
	l.connected, l.sessID = false, uuid.Nil
	if old := l.cli; old != nil && l.realtime {
		// RealtimeDisconnect waits for the stream to stop, which may be
		// inside one of its handlers on this very path.
		l.work.Add(1)
		l.busy.Add(1)
		go func() {
			defer l.work.Done()
			defer l.busy.Add(-1)
			old.RealtimeDisconnect()
		}()
	}
	l.cli, l.jar, l.realtime = nil, nil, false
}

// startRun starts the next run in the background, signed in as urn with the
// cookies in jar (signIn has checked they are his). l.mu is held.
func (l *LinkedIn) startRun(urn linkedingo.URN, jar *linkedingo.StringCookieJar) {
	ctx, gen := l.runs.Begin(l.ctx)
	l.jar = jar
	l.work.Add(1)
	l.busy.Add(1)
	go func() {
		defer l.work.Done()
		defer l.busy.Add(-1)
		l.run(ctx, gen, urn, jar)
	}()
}

// again signs in again with the held cookies, unless a run or a sign-in is
// in force, there are none, or a pause forbids a request. l.mu is held.
func (l *LinkedIn) again() {
	if !l.started || l.stopped || l.session == nil || l.runs.Ctx() != nil || l.probing {
		return
	}
	if _, blocked := l.pause.blocked(); blocked {
		return
	}
	l.retire()
	l.startProbe(l.session, sessionHash(l.session))
}

// startProbe begins checking a sign-in: its profile is read in the
// background (signIn). The state is connecting meanwhile. l.mu is held.
func (l *LinkedIn) startProbe(session network.Session, hash string) {
	if l.probeCancel != nil {
		l.probeCancel()
	}
	if !l.probing {
		l.prevState, l.prevKind = l.state, l.kind
	}
	l.probing = true
	l.probeGen++
	pg := l.probeGen
	ctx, cancel := context.WithCancel(l.ctx)
	l.probeCancel = cancel
	l.sayState(network.Connecting, "")
	l.work.Add(1)
	l.busy.Add(1)
	go func() {
		defer l.work.Done()
		defer l.busy.Add(-1)
		l.signIn(ctx, pg, session, hash)
	}()
}

// endProbe drops the sign-in being checked, if any. l.mu is held.
func (l *LinkedIn) endProbe() {
	l.probing = false
	if l.probeCancel != nil {
		l.probeCancel()
		l.probeCancel = nil
	}
}

// probeCurrent reports whether pg is still the sign-in being checked. l.mu is
// held.
func (l *LinkedIn) probeCurrent(pg int) bool {
	return l.probing && pg == l.probeGen && !l.stopped
}

// backToBefore puts the state back as it was before a sign-in that changed
// nothing, when a run is still in force. The sign-in's own outcome (s with
// kind) is said first, so the run's earlier state never reads as the new
// session taken (Pllum keeps a session only once connecting is followed by
// catchingUp). l.mu is held.
func (l *LinkedIn) backToBefore(s network.State, kind wire.Kind) {
	if l.runs.Ctx() == nil {
		return
	}
	l.setState(s, kind)
	l.setState(l.prevState, l.prevKind)
}

// signIn checks a sign-in: the profile read with a client signed in as no
// one. Another account than the one whose chats the store holds is refused,
// and nothing changes: the run in force goes on, a kept ended session stays
// kept. Only his own profile ends the run in force, clears the kept block
// and starts his run (adopt).
func (l *LinkedIn) signIn(ctx context.Context, pg int, session network.Session, hash string) {
	jar := jarOf(session)
	probe := l.newClient(linkedingo.NewURN(""), jar, userAgent, "", noHandlers())
	var profile *linkedingo.UserProfile
	err := l.probeRequest(ctx, pg, session, hash, func(ctx context.Context) (err error) {
		profile, err = probe.GetCurrentUserProfile(ctx)
		return err
	})
	if err != nil {
		l.probeFailed(pg, session, hash, wire.KindOf(err))
		return
	}
	urn := profile.MiniProfile.DashEntityURN
	if urn.IsEmpty() {
		urn = profile.MiniProfile.EntityURN
	}
	id := profileID(urn)
	if id == "" {
		l.probeFailed(pg, session, hash, wire.Internal)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.probeCurrent(pg) {
		return
	}
	l.endProbe()
	self := l.self()
	if self != "" && self != id {
		l.setState(network.NotConnected, wire.Refused)
		l.backToBefore(network.NotConnected, wire.Refused)
		return
	}
	if err := l.keep("self", id); err != nil {
		l.setState(network.NotConnected, wire.Internal)
		l.backToBefore(network.NotConnected, wire.Internal)
		return
	}
	if self == "" {
		// The first sign-in since the account was forgotten (or ever): the
		// chats of any account before it stay readable, but read-only.
		l.retireOtherAccounts(id)
	}
	// His: the run in force ends, a kept ended session is cleared (he has
	// signed in again), and his run starts.
	l.retire()
	_ = l.keep("block", "")
	l.session = session
	l.hashes = []string{hash}
	l.diff.Seed(session)
	l.startRun(urn.WithPrefix("urn", "li", "fsd_profile"), jar)
}

// noHandlers are handlers that do nothing, for the client that only reads
// the profile.
func noHandlers() linkedingo.Handlers {
	return linkedingo.Handlers{
		Heartbeat:              func(context.Context) {},
		ClientConnection:       func(context.Context, *linkedingo.ClientConnection) {},
		TransientDisconnect:    func(context.Context, error) {},
		BadCredentials:         func(context.Context, error) {},
		DecoratedEvent:         func(context.Context, *linkedingo.DecoratedEvent) {},
		ConversationsSyncToken: func(context.Context, string) {},
	}
}

// probeRequest is request for the profile read of sign-in pg: none during a
// pause or once the sign-in is dropped, in the background's turn. LinkedIn
// pushing back pauses everything, the run in force included; an ended
// session ends this sign-in.
func (l *LinkedIn) probeRequest(ctx context.Context, pg int, session network.Session, hash string, fn func(ctx context.Context) error) error {
	gate := func() error {
		l.mu.Lock()
		ok := l.probeCurrent(pg)
		l.mu.Unlock()
		if !ok {
			return errNotConnected
		}
		if _, blocked := l.pause.blocked(); blocked {
			return errRateLimited
		}
		return nil
	}
	if err := gate(); err != nil {
		return err
	}
	if err := l.paced(ctx); err != nil {
		return errNotConnected
	}
	if err := gate(); err != nil {
		return err
	}
	err := fn(ctx)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return errNotConnected
	}
	kind, paused := l.pause.check(err)
	if paused {
		_ = l.keep("pause", l.pause.String())
		return errRateLimited
	}
	if kind == reconnectNeeded {
		l.mu.Lock()
		defer l.mu.Unlock()
		if !l.probeCurrent(pg) {
			return errNotConnected
		}
		// This session LinkedIn won't take is kept as ended too, beside any
		// kept before; the run in force, if any, goes on.
		l.endProbe()
		_ = l.keep("block", strings.Join(append(l.keptBlock(), hash), ","))
		if l.runs.Ctx() != nil {
			l.backToBefore(network.ReconnectNeeded, "")
			return errNotConnected
		}
		l.session = nil
		l.setState(network.ReconnectNeeded, "")
		return errNotConnected
	}
	return &wire.Error{Kind: kind}
}

// probeFailed: sign-in pg's profile read failed with kind. A pause (begun by
// it, or in force) stops everything, the run in force included, and the
// sign-in is checked again when it ends. Any other failure leaves a run in
// force as it was; with none, the state is notConnected with the kind, and a
// passing one may be nudged.
func (l *LinkedIn) probeFailed(pg int, session network.Session, hash string, kind wire.Kind) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.probeCurrent(pg) {
		return
	}
	l.endProbe()
	if kind == wire.RateLimited {
		l.retire()
		l.session = session
		l.hashes = []string{hash}
		l.setState(network.Offline, wire.RateLimited)
		l.resumeAfterPause()
		return
	}
	if l.runs.Ctx() != nil {
		l.backToBefore(network.NotConnected, kind)
		return
	}
	l.session = session
	l.hashes = []string{hash}
	l.setState(network.NotConnected, kind)
	l.retry = passing(kind)
}

// resumeAfterPause starts a new run when the pause ends, or now if it has
// already ended. l.mu is held.
func (l *LinkedIn) resumeAfterPause() {
	if _, blocked := l.pause.blocked(); blocked {
		l.scheduleResume()
		return
	}
	l.again()
}

// scheduleResume starts a new run when the pause in force ends. l.mu is
// held.
func (l *LinkedIn) scheduleResume() {
	until, blocked := l.pause.blocked()
	if !blocked {
		return
	}
	if l.stopResume != nil {
		l.stopResume()
	}
	gen := l.runs.Gen
	l.stopResume = l.clock.AfterFunc(until.Sub(l.clock.Now()), func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if gen == l.runs.Gen {
			l.stopResume = nil
			l.again()
		}
	})
}

// current reports whether gen is still the run in force.
func (l *LinkedIn) current(gen int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return gen == l.runs.Gen && !l.stopped
}

// run is his signed-in run gen: his client, the catch-up, the realtime
// stream and the background work (sync.go).
func (l *LinkedIn) run(ctx context.Context, gen int, urn linkedingo.URN, jar *linkedingo.StringCookieJar) {
	cli := l.newClient(urn, jar, userAgent, l.value("syncToken"), l.handlersFor(gen))
	l.mu.Lock()
	if gen != l.runs.Gen || l.stopped {
		l.mu.Unlock()
		return
	}
	l.cli = cli
	l.setState(network.CatchingUp, "")
	l.mu.Unlock()

	l.stayCaughtUp(ctx, gen, cli)
}

// failed ends run gen after a failure that has not already ended it (a pause
// or an ended session have): the state is s with the kind, and a passing one
// may be nudged. A request refused for a pause waits for its end.
func (l *LinkedIn) failed(gen int, s network.State, kind wire.Kind) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.runs.Gen || l.stopped {
		return
	}
	l.retire()
	if kind == wire.RateLimited {
		l.setState(network.Offline, wire.RateLimited)
		l.resumeAfterPause()
		return
	}
	shown := kind
	if s == network.Offline && kind == wire.Offline {
		shown = ""
	}
	l.setState(s, shown)
	l.retry = passing(kind)
}

// request makes one request of run gen: none while a pause is in force or
// once the run has ended; a background one waits its turn (paced). A success may carry changed cookies to Pllum; LinkedIn pushing back
// pauses everything, and an ended session ends the run.
func (l *LinkedIn) request(ctx context.Context, gen int, background bool, fn func(ctx context.Context) error) error {
	if err := l.gate(gen); err != nil {
		return err
	}
	if background {
		if err := l.paced(ctx); err != nil {
			return errNotConnected
		}
		if err := l.gate(gen); err != nil {
			return err
		}
	}
	err := func() error {
		end := l.traffic.Begin(ctx, l.clock.Now())
		defer func() { end(l.clock.Now()) }()
		return fn(ctx)
	}()
	if err == nil {
		l.afterSuccess(gen)
		return nil
	}
	if ctx.Err() != nil {
		return errNotConnected
	}
	kind, paused := l.pause.check(err)
	if paused {
		_ = l.keep("pause", l.pause.String())
		l.paused(gen)
		return errRateLimited
	}
	if kind == reconnectNeeded {
		l.endSession(gen)
		return errNotConnected
	}
	if gone(err) {
		return errGone
	}
	return &wire.Error{Kind: kind}
}

// gate is the error for a request of run gen now, when one may not go.
func (l *LinkedIn) gate(gen int) error {
	if !l.current(gen) {
		return errNotConnected
	}
	if _, blocked := l.pause.blocked(); blocked {
		return errRateLimited
	}
	return nil
}

// paced waits until a background request may go, in the order they ask: a
// random 2 to 4 s after the one before (Meta's pace, meta.MinGap to
// meta.MaxGap), so they never tick like a script; at once when the last was
// longer ago than that.
func (l *LinkedIn) paced(ctx context.Context) error {
	l.pmu.Lock()
	now := l.clock.Now()
	at := now
	if l.nextBg.After(at) {
		at = l.nextBg
	}
	l.nextBg = at.Add(meta.MinGap + time.Duration(l.rand()*float64(meta.MaxGap-meta.MinGap)))
	l.pmu.Unlock()
	return l.clock.Sleep(ctx, at.Sub(now))
}

// download fetches a media file through the client: never during a pause,
// and a CDN refusing it never pauses LinkedIn (mediaGone, or rateLimited for
// a 429 or 999).
func (l *LinkedIn) download(ctx context.Context, cli liClient, url string) (*http.Response, error) {
	resp, _, err := l.downloadRaw(ctx, cli, url)
	return resp, err
}

// downloadRaw is download, with the client's own error too (raw), nil when
// nothing was asked.
func (l *LinkedIn) downloadRaw(ctx context.Context, cli liClient, url string) (resp *http.Response, raw, err error) {
	if _, blocked := l.pause.blocked(); blocked {
		return nil, nil, errRateLimited
	}
	resp, raw = cli.DownloadHTTP(ctx, url)
	if raw == nil {
		return resp, nil, nil
	}
	kind := l.pause.checkMedia(raw)
	if kind == reconnectNeeded {
		l.mu.Lock()
		gen := l.runs.Gen
		l.mu.Unlock()
		l.endSession(gen)
		kind = wire.NotConnected
	}
	return nil, raw, &wire.Error{Kind: kind}
}

// paused: LinkedIn pushed back, and the pause has begun. The run ends, its
// stream too, and a new one starts when the pause ends.
func (l *LinkedIn) paused(gen int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.runs.Gen || l.stopped {
		return
	}
	l.retire()
	l.setState(network.Offline, wire.RateLimited)
	l.resumeAfterPause()
}

// endSession: LinkedIn ended the session (bad credentials, a 401, a 302, li_at
// deleted). The run ends, its stream too, and nothing asks with these
// cookies again, after a restart either, until a new sign-in.
func (l *LinkedIn) endSession(gen int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if gen != l.runs.Gen || l.stopped || l.session == nil {
		return
	}
	hashes := l.hashes
	if len(hashes) == 0 {
		hashes = []string{sessionHash(l.session)}
	}
	l.retire()
	l.session = nil
	l.setState(network.ReconnectNeeded, "")
	_ = l.keep("block", strings.Join(hashes, ","))
}

// keptBlock is the hashes of every version of the session LinkedIn ended, in
// this or an earlier run; none when no session has been ended since the last
// new sign-in.
func (l *LinkedIn) keptBlock() []string {
	v := l.value("block")
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// afterSuccess runs after each request LinkedIn answered: a session whose
// li_at LinkedIn deleted has ended, and changed cookies go to Pllum (the
// session event, at most once a minute), which keeps the copy it signs in
// again from.
func (l *LinkedIn) afterSuccess(gen int) {
	l.mu.Lock()
	jar := l.jar
	ok := gen == l.runs.Gen && !l.stopped
	l.mu.Unlock()
	if !ok || jar == nil {
		return
	}
	if jarValue(jar, "li_at") == "" {
		l.endSession(gen)
		return
	}
	s, changed := l.diff.ChangedBy(func(name string) string { return jarValue(jar, name) }, l.clock.Now())
	if !changed {
		return
	}
	l.mu.Lock()
	if gen == l.runs.Gen {
		l.session = network.Session(s)
		if h := sessionHash(l.session); !slices.Contains(l.hashes, h) {
			l.hashes = append(l.hashes, h)
		}
	}
	l.mu.Unlock()
	l.emit(map[string]any{"event": "session", "session": s})
}

// setState moves the state and tells Pllum; l.mu is held. The state event
// goes out under the lock, so state events keep their order.
func (l *LinkedIn) setState(s network.State, kind wire.Kind) {
	if s == l.state && kind == l.kind {
		return
	}
	l.sayState(s, kind)
}

// sayState is setState that tells Pllum even when the state is unchanged:
// a login's outcome, which Pllum waits for after clearing the row. l.mu is
// held.
func (l *LinkedIn) sayState(s network.State, kind wire.Kind) {
	l.state, l.kind = s, kind
	ev := map[string]any{"event": "state", "state": s}
	if kind != "" {
		ev["kind"] = string(kind)
	}
	l.emit(ev)
}

// stateNow is the state and its kind.
func (l *LinkedIn) stateNow() (network.State, wire.Kind) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state, l.kind
}

// idleNow reports whether nothing is at work: no run doing anything but
// waiting for the stream to ask, and no catch-up asked for.
func (l *LinkedIn) idleNow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.busy.Load() == 0 && !l.due.sync && !l.due.pages && !l.probing
}

// changed tells Pllum there is something to re-read: at once, or when 500 ms
// have passed since the last time.
func (l *LinkedIn) changed() { l.changes.Tell(l.clock, l.emit) }
