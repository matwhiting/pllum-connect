package messenger

import (
	"context"
	"path/filepath"
	"strconv"

	"go.mau.fi/whatsmeow"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// The encrypted side: Messenger's end-to-end encrypted chats run over
// whatsmeow in Messenger mode, as a device Pllum registers once with Meta
// (ICDC) and keeps in <dir>/messenger/device.db, encrypted under the store
// key as WhatsApp's is.

// e2eeClient is what the adapter uses of the whatsmeow client: its events,
// and the socket. waClient wraps *whatsmeow.Client; tests use a fake.
type e2eeClient interface {
	AddEventHandler(h func(evt any))
	Connect() error
	Disconnect()
}

// waClient is whatsmeow's client, its event handler taking a plain func.
type waClient struct{ *whatsmeow.Client }

func (c waClient) AddEventHandler(h func(evt any)) { c.Client.AddEventHandler(h) }

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
// registered now (once, timed). It holds dmu throughout, so two runs never
// each make one. A registration that fails (Meta's answer lost, or the save
// after it) keeps its unsaved device, keys and Facebook device ID, for the
// next sign-in in this helper to register again: Meta finds the same keys
// and keeps one device, where new keys would add a second. Its error is
// unclear: it may have happened, and signing in again is not the fix. The
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
	for _, d := range devices {
		if d.ID != nil && d.ID.User == user && d.ID.Server == watypes.MessengerServer {
			m.dev = d
			cli.SetDevice(d)
			return d, 0, nil
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
			began := m.now()
			err := cli.RegisterE2EE(ctx, fbid)
			registerMs = m.now().Sub(began).Milliseconds()
			return err
		})
		switch {
		case err != nil && !asked:
			return nil, 0, err // refused before Meta was asked
		case err != nil, dev.ID == nil:
			return nil, 0, errUnclear
		}
	}
	if err := dev.Save(ctx); err != nil {
		return nil, 0, errUnclear
	}
	m.pending, m.pendingFor = nil, 0
	m.dev = dev
	return dev, registerMs, nil
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
	e.AddEventHandler(m.e2eeHandler(gen))
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
// connection's state, and the gate's live counts. It reads no message.
func (m *Messenger) e2eeHandler(gen int) func(evt any) {
	return func(evt any) {
		defer func() { _ = recover() }()
		if !m.current(gen) {
			return
		}
		switch evt.(type) {
		case *events.Connected:
			m.e2eeUp(gen, true)
		case *events.Disconnected:
			m.e2eeUp(gen, false)
		case *events.FBMessage:
			m.liveEncryptedReceived.Add(1)
		case *events.UndecryptableMessage:
			m.liveUndecryptable.Add(1)
		case events.PermanentDisconnect:
			// Meta ended the encrypted session (logged out, an unknown
			// device, a token it would not refresh). Nothing re-registers or
			// reconnects on its own: the gate records it.
			m.cmu.Lock()
			m.counts.E2EEFailed = true
			m.cmu.Unlock()
			m.e2eeUp(gen, false)
		}
	}
}

// e2eeUp records the encrypted socket going up or down for run gen.
func (m *Messenger) e2eeUp(gen int, up bool) { m.socketUp(gen, true, up) }

// e2eeConnected reports whether run gen's encrypted socket is up.
func (m *Messenger) e2eeConnected() bool {
	m.cmu.Lock()
	defer m.cmu.Unlock()
	return m.counts.E2EEConnected
}
