// Package whatsapp is the helper's WhatsApp network, built on whatsmeow. It
// links by QR code, writes every message, receipt and chat change into the
// store before WhatsApp is told it arrived, downloads the link-time history,
// and keeps the connection's state for Pllum. Reads are the server's, from
// the store; writes (sending, reacting, marking) come in a later step.
//
// Nothing here logs: whatsmeow gets waLog.Noop for its client and its store,
// and every error returned carries only a fixed kind.
package whatsapp

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Name is the network's name in the protocol and in the store.
const Name = "whatsapp"

const (
	// catchUpLimit is how long after connecting the state stays catchingUp
	// when WhatsApp never says the offline backlog is through.
	catchUpLimit = 120 * time.Second
	// historyQuiet is how long after the last history batch the sync counts
	// as done when no batch reached 100%.
	historyQuiet = 120 * time.Second
	// changedEvery is the least time between two changed events.
	changedEvery = 500 * time.Millisecond
	// historyRetry is how long a history download that failed waits before
	// it is tried again, when nothing else (a reconnect) prompts it first.
	historyRetry = time.Minute
	// historyTries is how many failed downloads of one notification, in one
	// run, give it up.
	historyTries = 3
)

var (
	errInternal    = &wire.Error{Kind: wire.Internal}
	errUnsupported = &wire.Error{Kind: wire.Unsupported}
)

// waClient is what the adapter uses of whatsmeow's client to sign in, stay
// connected and read history. *whatsmeow.Client satisfies it; tests use a
// fake.
type waClient interface {
	AddEventHandlerWithSuccessStatus(handler whatsmeow.EventHandlerWithSuccessStatus) uint32
	GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error)
	Connect() error
	Disconnect()
	IsConnected() bool
	DownloadHistorySync(ctx context.Context, notif *waE2E.HistorySyncNotification, synchronousStorage bool) (*waHistorySync.HistorySync, error)
	ParseWebMessage(chatJID types.JID, webMsg *waWeb.WebMessageInfo) (*events.Message, error)
	SendProtocolMessageReceipt(ctx context.Context, id types.MessageID, msgType types.ReceiptType) error
}

// clock is the time the adapter's timers run on; tests move a fake one.
type clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

// WhatsApp is the whatsapp network. Its methods are safe from any goroutine.
type WhatsApp struct {
	st   *store.Store
	dir  string
	key  [32]byte
	emit network.Emit

	newClient func(dev *wastore.Device) waClient
	clock     clock
	kick      chan struct{} // wakes the history worker

	mu          sync.Mutex
	started     bool
	stopped     bool
	ctx         context.Context
	cancel      context.CancelFunc
	db          *sql.DB
	container   *sqlstore.Container
	dev         *wastore.Device
	cli         waClient
	state       network.State
	kind        wire.Kind
	selfID      string             // his own number's JID: the "message yourself" chat
	loginStop   context.CancelFunc // set while a QR login runs
	loginLost   bool               // the login's connection dropped
	stopCatchUp func() bool
	replaced    bool // another client took the session: wait for a nudge
	retrying    bool // a history retry is due
	tries       map[int64]int
	worker      sync.WaitGroup

	hmu       sync.Mutex
	hChats    map[string]bool // chats the history sync has written this run
	hMessages int
	hFull     map[string]bool // chats a FULL sync has held this run
	hDone     bool
	hStop     func() bool

	cmu      sync.Mutex
	cLast    time.Time
	cPending bool
}

var _ network.Network = (*WhatsApp)(nil)

// New makes the whatsapp network over st. Its device database is
// <dir>/whatsapp/device.db, encrypted under key; emit must be
// emit.For("whatsapp"). Nothing connects until Start.
func New(st *store.Store, dir string, key [32]byte, emit network.Emit) *WhatsApp {
	setDeviceProps()
	return &WhatsApp{
		st:        st,
		dir:       dir,
		key:       key,
		emit:      emit,
		newClient: func(dev *wastore.Device) waClient { return newWhatsmeowClient(dev) },
		clock:     realClock{},
		kick:      make(chan struct{}, 1),
		state:     network.NotConnected,
		tries:     map[int64]int{},
		hChats:    map[string]bool{},
		hFull:     map[string]bool{},
	}
}

var propsOnce sync.Once

// setDeviceProps is what the phone is told when it links: Pllum 0.1.0 on a
// desktop, asking for a full history of 730 days (the spec's 24 months).
func setDeviceProps() {
	propsOnce.Do(func() {
		wastore.SetOSInfo("Pllum", [3]uint32{0, 1, 0})
		wastore.DeviceProps.PlatformType = waCompanionReg.DeviceProps_DESKTOP.Enum()
		wastore.DeviceProps.RequireFullSync = proto.Bool(true)
		if wastore.DeviceProps.HistorySyncConfig == nil {
			wastore.DeviceProps.HistorySyncConfig = &waCompanionReg.DeviceProps_HistorySyncConfig{}
		}
		wastore.DeviceProps.HistorySyncConfig.FullSyncDaysLimit = proto.Uint32(730)
	})
}

// newWhatsmeowClient is the real client: it acks a message only once every
// handler has returned (so it is in the store first), leaves history
// downloads to the adapter, emits app state on a full sync, asks his phone
// for a message it could not decrypt, and reconnects by itself. The
// history-sync receipt is the adapter's, sent once the notification is in
// the store.
func newWhatsmeowClient(dev *wastore.Device) *whatsmeow.Client {
	cli := whatsmeow.NewClient(dev, waLog.Noop)
	cli.SynchronousAck = true
	// Decrypted plaintext is kept in the (encrypted) device database until
	// every handler has succeeded, and replayed when WhatsApp sends the
	// stanza again. Without it a handler's failure loses the message: the
	// Signal session has moved past it, so the redelivered copy fails to
	// decrypt and is acked unhandled.
	cli.EnableDecryptedEventBuffer = true
	cli.ManualHistorySyncDownload = true
	cli.DisableManualHistorySyncReceipt = true // sent once the notification is stored
	cli.EmitAppStateEventsOnFullSync = true
	cli.AutomaticMessageRerequestFromPhone = true
	cli.EnableAutoReconnect = true
	cli.InitialAutoReconnect = true
	return cli
}

// Name is "whatsapp".
func (w *WhatsApp) Name() string { return Name }

// Start opens the device database and, when a device is linked, connects in
// the background. It also resumes any history notification left from an
// earlier run.
func (w *WhatsApp) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.stopped {
		return nil
	}
	db, err := store.OpenDevice(filepath.Join(w.dir, "whatsapp", "device.db"), w.key)
	if err != nil {
		return err
	}
	c := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := c.Upgrade(ctx); err != nil {
		db.Close()
		return errInternal
	}
	dev, err := c.GetFirstDevice(ctx)
	if err != nil {
		db.Close()
		return errInternal
	}
	w.ctx, w.cancel = context.WithCancel(context.WithoutCancel(ctx))
	w.db, w.container, w.dev = db, c, dev
	w.started = true
	if dev.ID != nil {
		w.selfID = dev.ID.ToNonAD().String()
		cli := w.makeClient(dev)
		w.setState(network.Connecting, "")
		go w.connect(cli)
	} else {
		// After WhatsApp unlinked the device in an earlier run (whatsmeow
		// deleted it), the chats are here and a QR scan continues them.
		w.setState(w.signedOut(), "")
	}
	w.worker.Add(1)
	go w.historyLoop(w.ctx)
	w.kickHistory()
	return nil
}

// makeClient makes the client for dev and makes it the current one; w.mu is
// held. Events from any other client are ignored.
func (w *WhatsApp) makeClient(dev *wastore.Device) waClient {
	cli := w.newClient(dev)
	cli.AddEventHandlerWithSuccessStatus(func(evt any) bool { return w.handle(cli, evt) })
	w.cli = cli
	return cli
}

// connect connects cli; a failure that whatsmeow will not retry by itself
// leaves the network offline until a nudge.
func (w *WhatsApp) connect(cli waClient) {
	err := cli.Connect()
	if err == nil || errors.Is(err, whatsmeow.ErrAlreadyConnected) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cli == cli && (w.state == network.Connecting || w.state == network.Offline) {
		w.setState(network.Offline, "")
	}
}

// Status is the network's state and his "message yourself" chat.
func (w *WhatsApp) Status() network.Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return network.Status{State: w.state, SelfChatID: w.selfID}
}

// Logout belongs to the writes, which come in a later step.
func (w *WhatsApp) Logout(ctx context.Context) error { return errUnsupported }

// Nudge reconnects now when a linked device is not connected: after the Mac
// wakes, or after another client took the session.
func (w *WhatsApp) Nudge() {
	w.mu.Lock()
	cli := w.cli
	linked := w.dev != nil && w.dev.ID != nil && !w.dev.Deleted
	ok := cli != nil && linked && !w.stopped &&
		(w.state == network.Offline || w.state == network.Banned || w.state == network.Connecting)
	if ok {
		w.replaced = false
	}
	w.mu.Unlock()
	if ok && !cli.IsConnected() {
		go w.connect(cli)
	}
}

// Send and the other writes come in a later step.
func (w *WhatsApp) Send(ctx context.Context, m network.Outgoing) (string, error) {
	return "", errUnsupported
}

// SendState knows no sends yet.
func (w *WhatsApp) SendState(messageID string) network.SendState {
	return network.SendState{State: network.SendUnknown}
}

// React comes in a later step.
func (w *WhatsApp) React(ctx context.Context, chatID, messageID, emoji, transactionID string) error {
	return errUnsupported
}

// MarkRead comes in a later step.
func (w *WhatsApp) MarkRead(ctx context.Context, chatID, through string) error {
	return errUnsupported
}

// MarkUnread comes in a later step.
func (w *WhatsApp) MarkUnread(ctx context.Context, chatID string) error { return errUnsupported }

// SetMuted comes in a later step.
func (w *WhatsApp) SetMuted(ctx context.Context, chatID string, muted bool) error {
	return errUnsupported
}

// Media comes in a later step.
func (w *WhatsApp) Media(ctx context.Context, chatID, messageID string) (string, error) {
	return "", errUnsupported
}

// Stop disconnects, waits for the history worker, and closes the device
// database. The store is the server's to close.
func (w *WhatsApp) Stop() {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.stopped = true
	if !w.started {
		w.mu.Unlock()
		return
	}
	w.cancel()
	cli := w.cli
	if w.stopCatchUp != nil {
		w.stopCatchUp()
	}
	w.mu.Unlock()

	w.hmu.Lock()
	if w.hStop != nil {
		w.hStop()
	}
	w.hmu.Unlock()

	if cli != nil {
		cli.Disconnect()
	}
	done := make(chan struct{})
	go func() {
		w.worker.Wait()
		close(done)
	}()
	select {
	case <-done:
		w.db.Close()
	case <-time.After(2 * time.Second):
		// The worker is stuck in a download; the database stays open for it
		// and closes with the process.
	}
}

// linkedMarker is an empty file that says a device was linked here once, so
// an unlink still shows as Reconnect needed after a restart, when whatsmeow
// has deleted the device. It holds nothing.
func (w *WhatsApp) linkedMarker() string { return filepath.Join(w.dir, "whatsapp", "linked") }

func (w *WhatsApp) wasLinked() bool {
	_, err := os.Stat(w.linkedMarker())
	return err == nil
}

// markLinked writes the marker once a linked device connects.
func (w *WhatsApp) markLinked() {
	if w.wasLinked() {
		return
	}
	if f, err := os.OpenFile(w.linkedMarker(), os.O_WRONLY|os.O_CREATE, 0o600); err == nil {
		f.Close()
	}
}

// signedOut is the state with no linked device: reconnectNeeded when a
// device was linked here before (WhatsApp unlinked it), else notConnected.
func (w *WhatsApp) signedOut() network.State {
	if w.wasLinked() {
		return network.ReconnectNeeded
	}
	return network.NotConnected
}

// device is the current whatsmeow device.
func (w *WhatsApp) device() *wastore.Device {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dev
}

// client is the current client, or nil when none is made.
func (w *WhatsApp) client() waClient {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cli
}

// self is his own number's JID string.
func (w *WhatsApp) self() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.selfID
}

// bg is the context the adapter's own work runs under.
func (w *WhatsApp) bg() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx == nil {
		return context.Background()
	}
	return w.ctx
}

// setState moves the state and tells Pllum; w.mu is held. The state event
// goes out under the lock, so state events keep their order.
func (w *WhatsApp) setState(s network.State, kind wire.Kind) {
	if s == w.state && kind == w.kind {
		return
	}
	w.state, w.kind = s, kind
	ev := map[string]any{"event": "state", "state": s}
	if kind != "" {
		ev["kind"] = string(kind)
	}
	w.emit(ev)
}

// handle is whatsmeow's event handler for cli. It reports false when an event
// could not be written to the store, so whatsmeow does not ack it and
// WhatsApp sends it again. A panic is a failure too.
func (w *WhatsApp) handle(cli waClient, evt any) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	w.mu.Lock()
	current := w.cli == cli && !w.stopped
	w.mu.Unlock()
	if !current {
		// A message for an old client, or after Stop, is not stored, so it
		// must not be acked: WhatsApp sends it again.
		switch evt.(type) {
		case *events.Message, *events.Receipt:
			return false
		}
		return true
	}
	switch e := evt.(type) {
	case *events.Connected:
		w.connected()
		w.markLinked()
	case *events.OfflineSyncCompleted:
		w.caughtUp()
	case *events.Disconnected, *events.KeepAliveTimeout:
		w.lost(false)
	case *events.KeepAliveRestored:
		w.restored()
	case *events.StreamReplaced:
		w.lost(true)
	case *events.LoggedOut:
		w.ended(network.ReconnectNeeded)
	case *events.ClientOutdated:
		w.ended(network.Outdated)
	case *events.TemporaryBan:
		w.ended(network.Banned)
	case *events.Message:
		return w.done(w.onMessage(e, store.SourceLive))
	case *events.Receipt:
		return w.done(w.onReceipt(e))
	case *events.Mute:
		return w.done(w.onMute(e))
	case *events.MarkChatAsRead:
		return w.done(w.onMarkChatAsRead(e))
	case *events.PushName:
		return w.done(w.refreshTitles(e.JID, e.JIDAlt))
	case *events.BusinessName:
		return w.done(w.refreshTitles(e.JID))
	case *events.Contact:
		return w.done(w.onContact(e))
	case *events.GroupInfo:
		return w.done(w.onGroupInfo(e))
	}
	return true
}

// done turns a write's error into the handler's answer, telling Pllum there
// is something to re-read when it worked.
func (w *WhatsApp) done(err error) bool {
	if err != nil {
		return false
	}
	w.changed()
	return true
}

// connected: the backlog WhatsApp kept while the device was away arrives
// now. It is caught up when WhatsApp says so, or 120 s on.
func (w *WhatsApp) connected() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dev != nil && w.dev.ID != nil {
		w.selfID = w.dev.ID.ToNonAD().String()
	}
	w.replaced = false
	if w.stopCatchUp != nil {
		w.stopCatchUp()
	}
	w.setState(network.CatchingUp, "")
	w.stopCatchUp = w.clock.AfterFunc(catchUpLimit, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.state == network.CatchingUp && !w.stopped {
			w.setState(network.Live, "")
		}
	})
	w.kickHistory()
}

func (w *WhatsApp) caughtUp() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopCatchUp != nil {
		w.stopCatchUp()
		w.stopCatchUp = nil
	}
	if w.state == network.CatchingUp {
		w.setState(network.Live, "")
	}
}

// lost: the connection dropped. whatsmeow reconnects by itself, except
// after another client took the session (replaced), which waits for a nudge.
func (w *WhatsApp) lost(replaced bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopCatchUp != nil {
		w.stopCatchUp()
		w.stopCatchUp = nil
	}
	switch w.state {
	case network.Linking:
		w.loginLost = true // the QR channel reports it as a timeout
	case network.Connecting, network.CatchingUp, network.Live, network.Offline:
		w.setState(network.Offline, "")
		if replaced {
			w.replaced = true
		}
	}
}

func (w *WhatsApp) restored() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state == network.Offline && !w.replaced {
		w.setState(network.Live, "")
		// No Connected follows a restored keep-alive: wake the history
		// worker for anything that arrived while it waited.
		w.kickHistory()
	}
}

// ended: WhatsApp unlinked the device, refused this version or banned the
// account for a while. The chats stay in the store, readable.
func (w *WhatsApp) ended(s network.State) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopCatchUp != nil {
		w.stopCatchUp()
		w.stopCatchUp = nil
	}
	w.setState(s, "")
}

// changed tells Pllum there is something to re-read: at once, or when 500 ms
// have passed since the last time.
func (w *WhatsApp) changed() {
	w.cmu.Lock()
	defer w.cmu.Unlock()
	if w.cPending {
		return
	}
	now := w.clock.Now()
	if wait := w.cLast.Add(changedEvery).Sub(now); wait > 0 {
		w.cPending = true
		w.clock.AfterFunc(wait, func() {
			w.cmu.Lock()
			w.cPending = false
			w.cLast = w.clock.Now()
			w.cmu.Unlock()
			w.emit(map[string]any{"event": "changed"})
		})
		return
	}
	w.cLast = now
	w.emit(map[string]any{"event": "changed"})
}
