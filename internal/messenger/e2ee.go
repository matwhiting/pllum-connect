package messenger

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The encrypted side: Messenger's end-to-end encrypted chats run over
// whatsmeow in Messenger mode, as a device Pllum registers once with Meta
// (ICDC) and keeps in <dir>/messenger/device.db, encrypted under the store
// key as WhatsApp's is.
//
// Registering (messagix's RegisterE2EE) fetches the account's ICDC list
// (the signed list of every device identity his account has, which senders
// check a device against), adds this device's identity, signs the list with
// it and posts it to Meta with the device's keys and its Facebook device
// ID. The Facebook device ID is the session's (sessionDeviceID), never one
// RegisterE2EE would make up. Once, ever, the self-check (diag.go) finding
// this device's identity missing from that list replaces the device with a
// new one (reregister).

// The network's own store values for the device (ids.go lists the rest):
// reregisteredKey is the day the one re-registration was begun, and
// deviceUUIDKey a Facebook device ID made here once, for a session that
// names none.
const (
	reregisteredKey = "ereregistered"
	deviceUUIDKey   = "fbdevice"
)

// e2eeClient is what the adapter uses of the whatsmeow client: its events,
// and the socket. waClient wraps *whatsmeow.Client; tests use a fake.
type e2eeClient interface {
	AddEventHandlerWithSuccessStatus(h func(evt any) bool)
	Connect() error
	Disconnect()
}

// waClient is whatsmeow's client, its event handler taking a plain func
// that reports whether the event was kept.
type waClient struct{ *whatsmeow.Client }

func (c waClient) AddEventHandlerWithSuccessStatus(h func(evt any) bool) {
	c.Client.AddEventHandlerWithSuccessStatus(h)
}

// PrepareE2EE is messagix's whatsmeow client for the device SetDevice gave
// it, made silent (waLog.Noop; its receive and send loggers are made from
// the client's zerolog.Nop() already) and acking a message only once every
// handler has returned, with the plaintext buffered (in the encrypted device
// database) until then, as step 2's WhatsApp does. The library turns on its
// own reconnects.
func (c realClient) PrepareE2EE() (e2eeClient, error) {
	cli, err := c.Client.PrepareE2EEClient()
	if err != nil {
		return nil, err
	}
	cli.Log = waLog.Noop
	cli.SynchronousAck = true
	cli.EnableDecryptedEventBuffer = true
	return waClient{cli}, nil
}

// device is his Messenger device for this account, registered with Meta
// and saved: the one saved in an earlier run or sign-in, or else one
// registered now (once, timed), with the session's Facebook device ID
// (sessionDeviceID). While a re-registration is pending (m.replacing, set
// by reregister) the saved device is passed over: the pending device is
// registered, and once it is, the replaced device's row is deleted before
// the new one is saved (a device Meta gives the same number would
// otherwise keep the old keys in the row). It holds dmu throughout, so two
// runs never each make one. A registration that fails (Meta's answer lost,
// or the save after it) keeps its unsaved device, keys and Facebook device
// ID, for the next sign-in in this helper to register again: Meta finds the
// same keys and keeps one device, where new keys would add a second. Its
// error is
// registrationUnclear: it may have happened, and the network keeps it as
// the session's block (messenger.go), so neither a restart nor the same
// session sent again registers anything (a restart would have only new
// keys) until he signs in again. A registration that went clears it. The
// device database is opened the first time, never at Start, so nothing is
// made on disk until he signs in.
func (m *Messenger) device(ctx context.Context, gen int, cli msgrClient, fbid int64) (dev *wastore.Device, registerMs int64, err error) {
	m.dmu.Lock()
	defer m.dmu.Unlock()
	if m.container == nil {
		db, err := store.OpenDevice(filepath.Join(m.dir, Name, "device.db"), m.key)
		if err != nil {
			return nil, 0, err
		}
		c := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
		if err := c.Upgrade(ctx); err != nil {
			db.Close()
			return nil, 0, errInternal
		}
		m.db, m.container = db, c
	}
	devices, err := m.container.GetAllDevices(ctx)
	if err != nil {
		return nil, 0, errInternal
	}
	user := strconv.FormatInt(fbid, 10)
	if m.replacing != nil && m.pendingFor != fbid {
		m.replacing = nil // another account's sign-in: nothing to replace
	}
	if m.replacing == nil {
		for _, d := range devices {
			if d.ID != nil && d.ID.User == user && d.ID.Server == watypes.MessengerServer {
				m.dev = d
				cli.SetDevice(d)
				return d, 0, nil
			}
		}
	}

	dev = m.pending
	if dev == nil || m.pendingFor != fbid {
		dev = m.container.NewDevice()
		m.pending, m.pendingFor = dev, fbid
	}
	cli.SetDevice(dev)
	if dev.ID == nil {
		asked := false
		err := m.request(ctx, gen, func(ctx context.Context) error {
			asked = true
			if dev.FacebookUUID == uuid.Nil {
				// asked here, past the pace's wait, when the page's configs
				// are in place
				dev.FacebookUUID = m.sessionDeviceID(cli)
			}
			began := m.now()
			err := cli.RegisterE2EE(ctx, fbid)
			registerMs = m.now().Sub(began).Milliseconds()
			return err
		})
		switch {
		case err != nil && !asked:
			return nil, 0, err // refused before Meta was asked
		case err != nil, dev.ID == nil:
			return nil, 0, errRegistrationUnclear
		}
	}
	if old := m.replacing; old != nil && old.ID != nil {
		// the row only (its sessions, keys and identities go with it): the
		// object stays as the old socket last saw it
		if err := m.container.DeleteDevice(ctx, old); err != nil {
			return nil, 0, errRegistrationUnclear
		}
	}
	if err := dev.Save(ctx); err != nil {
		return nil, 0, errRegistrationUnclear
	}
	m.replacing = nil
	m.pending, m.pendingFor = nil, 0
	m.dev = dev
	if kind, _, ok := m.keptBlock(); ok && kind == wire.RegistrationUnclear {
		_ = m.keep("block", "") // the device is registered and saved now
	}
	return dev, registerMs, nil
}

// sessionDeviceID is the Facebook device ID a new device registers with:
// the session's, as mautrix-meta's connector takes it (a native session's,
// or the web page's device ID; realClient.SessionDeviceID), else one made
// here once and kept (deviceUUIDKey), so no registration uses an ID
// RegisterE2EE makes up on the spot.
func (m *Messenger) sessionDeviceID(cli msgrClient) uuid.UUID {
	if id := cli.SessionDeviceID(); id != uuid.Nil {
		return id
	}
	if id, err := uuid.Parse(m.value(deviceUUIDKey)); err == nil && id != uuid.Nil {
		return id
	}
	id := uuid.New()
	_ = m.keep(deviceUUIDKey, id.String())
	return id
}

// reregister replaces his Messenger device for run gen, once ever: the
// self-check found this device's identity missing from his account's ICDC
// list, which senders check a device against. The day is kept under
// reregisteredKey before anything is asked, so it never runs twice, after
// a restart either. The encrypted socket is disconnected, and a new device
// (new keys, the session's Facebook device ID) is registered, saved in
// place of the old one and connected (device, connectE2EE). Its first
// connect runs the socket's self-check again and asks again for what could
// not be read (askAgain), which reaches the new device. A failure is what
// a failed first registration is (e2eeSetupFailed): an unclear one keeps
// the registration-unclear block, and the same new keys are registered
// again only at a later sign-in in this helper. began reports whether it
// was attempted; err is the failure that ended the run.
func (m *Messenger) reregister(ctx context.Context, gen int) (began bool, err error) {
	if m.value(reregisteredKey) != "" {
		return false, nil
	}
	m.mu.Lock()
	cli, fbid := m.cli, m.fbid
	ok := gen == m.gen && !m.stopped && cli != nil
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	if err := m.keep(reregisteredKey, m.now().UTC().Format(time.DateOnly)); err != nil {
		m.diagLine("re-registration: not attempted, its marker could not be kept")
		return false, nil
	}
	m.diagLine("re-registration: attempted, with a new device, new keys and the session's device ID")
	m.mu.Lock()
	old := m.e2ee
	if gen == m.gen {
		m.e2ee = nil
	}
	m.mu.Unlock()
	if old != nil {
		old.Disconnect()
	}
	m.e2eeUp(gen, false)
	m.dmu.Lock()
	prev := m.dev
	var prevID *watypes.JID
	if prev != nil && prev.ID != nil {
		id := *prev.ID
		prevID = &id
	}
	m.replacing, m.pending, m.pendingFor = prev, nil, fbid
	m.dmu.Unlock()
	m.checked.Store(false) // the new socket's first connect checks and asks again
	if err := m.connectE2EE(ctx, gen, cli, fbid); err != nil {
		m.diagLine("re-registration: failed (" + string(wire.KindOf(err)) + "); not tried again until a new sign-in")
		m.e2eeSetupFailed(gen, err)
		return true, err
	}
	m.dmu.Lock()
	now := m.dev
	m.dmu.Unlock()
	if now == prev {
		m.diagLine("re-registration: nothing registered, the saved device was kept")
		return true, nil
	}
	changed := prevID == nil || now == nil || now.ID == nil || now.ID.Device != prevID.Device
	m.diagLine(fmt.Sprintf("re-registration: registered, device ID changed %v", changed))
	return true, nil
}

// e2eeSetupFailed ends run gen after its encrypted side could not be set
// up. A registration whose answer was lost may have made a device: it is
// kept as the session's block, so nothing registers again (with new keys,
// after a restart) until he signs in again.
func (m *Messenger) e2eeSetupFailed(gen int, err error) {
	if wire.KindOf(err) == wire.RegistrationUnclear {
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
}

// connectE2EE sets up the encrypted side for run gen: his device (device),
// then the encrypted socket. The state goes live when both sockets are up.
func (m *Messenger) connectE2EE(ctx context.Context, gen int, cli msgrClient, fbid int64) error {
	_, registerMs, err := m.device(ctx, gen, cli, fbid)
	if err != nil {
		return err
	}
	m.cmu.Lock()
	m.counts.Registered = true
	m.counts.RegisterMs = registerMs
	m.cmu.Unlock()

	e, err := cli.PrepareE2EE()
	if err != nil {
		return errInternal
	}
	if wc, ok := e.(waClient); ok {
		// the library's warnings about decrypting go to diagnostics.txt, without numbers (diag.go)
		wc.Client.Log = m.diagLogger()
	}
	// and libsignal's, which say why a message failed ("no valid sessions"
	// is all whatsmeow says); WhatsApp's land here too, as libsignal has one
	// logger for the helper
	quiet.SignalTo(func(line string) { m.diag("signal: " + line) })
	e.AddEventHandlerWithSuccessStatus(m.e2eeHandler(gen, e))
	m.mu.Lock()
	if gen != m.gen || m.stopped {
		m.mu.Unlock()
		return errNotConnected
	}
	m.e2ee = e
	m.mu.Unlock()
	return m.request(ctx, gen, func(ctx context.Context) error { return e.Connect() })
}

// e2eeHandler is the encrypted socket's event handler for run gen: the
// connection's state, and the encrypted chats' lines, receipts and
// undecryptable messages, each written to the store before it returns.
// whatsmeow (with SynchronousAck) acks a message or receipt only when every
// handler reports true, so one not kept for a passing reason (a store
// error, or a run that has ended) comes again at the next connect. One that
// can never be kept (it panics, or the store refuses it) is acked, kept as
// a hidden notice where it can be, so it does not come back at every
// connect for ever (review M2). An undecryptable message is acked by
// whatsmeow before it is handed over, so it never comes again either way.
//
// The socket's own ups and downs count only while it is the socket in
// force: one a re-registration replaced (reregister) says nothing of the
// new one.
func (m *Messenger) e2eeHandler(gen int, self e2eeClient) func(evt any) bool {
	return func(evt any) (ok bool) {
		defer func() {
			if recover() != nil {
				ok = true // a state event that panicked: nothing to keep
			}
		}()
		if !m.current(gen) {
			switch evt.(type) {
			case *events.FBMessage, *events.Receipt, *events.UndecryptableMessage:
				return false
			}
			return true
		}
		switch e := evt.(type) {
		case *events.Connected:
			if !m.isE2EE(self) {
				return true
			}
			m.e2eeUp(gen, true)
			m.mu.Lock()
			client := m.e2ee
			m.mu.Unlock()
			if client != nil && m.checked.CompareAndSwap(false, true) {
				go func() {
					// a socket replaced under it must not end the helper
					defer func() { _ = recover() }()
					m.selfCheck(gen, client)
					if m.isE2EE(client) { // not a socket a re-registration replaced
						m.askAgain(gen, client)
					}
				}()
			}
		case *events.Disconnected:
			if m.isE2EE(self) {
				m.e2eeUp(gen, false)
			}
		case *events.FBMessage:
			m.liveEncryptedReceived.Add(1)
			return m.keptOr(e.Info, func() (bool, error) { return m.onFBMessage(e) })
		case *events.Receipt:
			return m.keptOr(watypes.MessageInfo{}, func() (bool, error) { return m.onEncReceipt(e) })
		case *events.UndecryptableMessage:
			m.liveUndecryptable.Add(1)
			return m.keptOr(watypes.MessageInfo{}, func() (bool, error) { return m.onUndecryptable(e) })
		case *events.TemporaryBan:
			// Meta suspended the encrypted device: a suspension of
			// Messenger, as a ban on the main socket is. Nothing is asked
			// until he signs in again (review I1).
			m.e2eeEnded(gen)
			m.pace.Fail(meta.Banned)
			m.pushedBack(gen, meta.Banned)
		case *events.LoggedOut:
			// Meta logged the encrypted device out: the session is over.
			m.e2eeEnded(gen)
			m.pace.Fail(meta.ReconnectNeeded)
			m.pushedBack(gen, meta.ReconnectNeeded)
		case events.PermanentDisconnect:
			// Meta ended the encrypted session otherwise (an unknown device,
			// a token it would not refresh). Nothing re-registers or
			// reconnects on its own, and no history is asked until a new
			// sign-in; groups stay up.
			m.e2eeEnded(gen)
		}
		return true
	}
}

// keptOr is a handler's answer for a write: true when it was kept, telling
// Pllum there is something to re-read when it wrote anything. A write that
// can never succeed (a panic, or the store refusing it) is acked too, with
// a hidden notice under the message's ID where info names one; only a
// passing failure is not.
func (m *Messenger) keptOr(info watypes.MessageInfo, write func() (bool, error)) bool {
	wrote, err, panicked := safeWrite(write)
	if !panicked && err == nil {
		if wrote {
			m.changed()
		}
		return true
	}
	if !panicked && wire.KindOf(err) != wire.BadRequest {
		return false
	}
	if info.ID != "" {
		if wrote, _, _ := safeWrite(func() (bool, error) { return m.onUndecryptable(&events.UndecryptableMessage{Info: info}) }); wrote {
			m.changed()
		}
	}
	return true
}

// safeWrite runs write, turning a panic into panicked.
func safeWrite(write func() (bool, error)) (wrote bool, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			wrote, err, panicked = false, nil, true
		}
	}()
	wrote, err = write()
	return wrote, err, false
}

// e2eeEnded records that Meta ended run gen's encrypted session: failed,
// and no history asked, until a new sign-in.
func (m *Messenger) e2eeEnded(gen int) {
	m.cmu.Lock()
	m.counts.E2EEFailed = true
	m.cmu.Unlock()
	m.mu.Lock()
	if gen == m.gen && !m.stopped {
		m.e2eeFailed = true
	}
	m.mu.Unlock()
	m.e2eeUp(gen, false)
}

// encryptedState is `status`'s encrypted: failed once Meta ended the
// encrypted session or its registration failed, live while its socket is
// up, connecting while a sign-in is in force, else notRegistered (no
// encrypted session in force). m.mu is held.
func (m *Messenger) encryptedState() string {
	m.cmu.Lock()
	up := m.counts.E2EEConnected
	m.cmu.Unlock()
	switch {
	case m.e2eeFailed:
		return encFailed
	case up:
		return encLive
	case m.cli != nil:
		return encConnecting
	}
	return encNotRegistered
}

// The words of `status`'s encrypted.
const (
	encNotRegistered = "notRegistered"
	encConnecting    = "connecting"
	encLive          = "live"
	encFailed        = "failed"
)

// e2eeUp records the encrypted socket going up or down for run gen.
func (m *Messenger) e2eeUp(gen int, up bool) { m.socketUp(gen, true, up) }

// isE2EE reports whether e is the encrypted socket in force.
func (m *Messenger) isE2EE(e e2eeClient) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.e2ee == e
}

// e2eeConnected reports whether run gen's encrypted socket is up.
func (m *Messenger) e2eeConnected() bool {
	m.cmu.Lock()
	defer m.cmu.Unlock()
	return m.counts.E2EEConnected
}

// encryptedEnded reports whether Meta has ended the encrypted session of
// the run in force: then no history is asked, encrypted or not.
func (m *Messenger) encryptedEnded() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.e2eeFailed
}
