package messenger

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/whatsmeow/util/keys"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// His encrypted history: Messenger keeps a backup of the encrypted chats
// that his six-digit PIN opens. With it, Pllum enrolls a history device
// (kept in the store's meta, under "messenger.history") that reads the
// backup. The steps are connector login_history.go's at mautrix-meta
// v0.2610.0: GetBackup; then RecoverPIN, GetVirtualDevice, DeriveEpochs,
// PrepareDevice and AddDevice.
//
// The PIN is his: typed into Pllum, passed once inside `restoreHistory`, and
// never kept, logged, retried or echoed. Its bytes are zeroed as soon as the
// call returns, and so is the recovery code it yields.

// The history states of `encryptedHistory`. Empty is "not looked yet".
const (
	historyNoBackup = "noBackup"
	historyNeedsPIN = "needsPIN"
	historyRestored = "restored"
	historySkipped  = "skipped"
)

// samplePage is how many lines the gate's one page of history asks for.
const samplePage = 50

// historyClient is what the adapter uses of the secure-history client.
// realHistory wraps *securehistory.Client; tests use a fake.
type historyClient interface {
	GetBackup(ctx context.Context) (*securehistory.Backup, error)
	RecoverPIN(ctx context.Context, backup *securehistory.Backup, pin []byte) (*securehistory.Recovery, error)
	GetVirtualDevice(ctx context.Context, backup *securehistory.Backup, userID int64, code []byte) (virtualDevice, error)
	AddDevice(ctx context.Context, enrollment *securehistory.Enrollment) (*securehistory.Device, error)
	FetchMessages(ctx context.Context, device *securehistory.Device, threadID string, before bool, timestampMS int64, count int) (*securehistory.MessageRange, error)
	// Decrypt is MessageRange.Decrypt, here so a test can stand in for it.
	Decrypt(r *securehistory.MessageRange, device *securehistory.Device, threadID string, selfUserID int64, isGroup bool) ([]*securehistory.DecryptedMessage, error)
	RegistrationID() string
}

// virtualDevice is the backup's virtual device, from which the history
// device is made. *securehistory.VirtualDevice satisfies it.
type virtualDevice interface {
	DeriveEpochs() (map[string]securehistory.Epoch, error)
	PrepareDevice(backupID, registrationID string, armadillo *keys.KeyPair, epochs map[string]securehistory.Epoch) (*securehistory.Enrollment, error)
}

var _ virtualDevice = (*securehistory.VirtualDevice)(nil)

type realHistory struct{ c *securehistory.Client }

func (h realHistory) GetBackup(ctx context.Context) (*securehistory.Backup, error) {
	return h.c.GetBackup(ctx)
}

func (h realHistory) RecoverPIN(ctx context.Context, b *securehistory.Backup, pin []byte) (*securehistory.Recovery, error) {
	return h.c.RecoverPIN(ctx, b, pin)
}

func (h realHistory) GetVirtualDevice(ctx context.Context, b *securehistory.Backup, userID int64, code []byte) (virtualDevice, error) {
	v, err := h.c.GetVirtualDevice(ctx, b, userID, code)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (h realHistory) AddDevice(ctx context.Context, e *securehistory.Enrollment) (*securehistory.Device, error) {
	return h.c.AddDevice(ctx, e)
}

func (h realHistory) FetchMessages(ctx context.Context, d *securehistory.Device, threadID string, before bool, ts int64, count int) (*securehistory.MessageRange, error) {
	return h.c.FetchMessages(ctx, d, threadID, before, ts, count)
}

func (h realHistory) Decrypt(r *securehistory.MessageRange, d *securehistory.Device, threadID string, self int64, isGroup bool) ([]*securehistory.DecryptedMessage, error) {
	return r.Decrypt(d, threadID, self, isGroup)
}

func (h realHistory) RegistrationID() string { return h.c.RegistrationID }

// HistoryState is "noBackup", "needsPIN", "restored" or "skipped"; empty
// until the run has looked (after the encrypted socket is set up).
func (m *Messenger) HistoryState() string {
	m.cmu.Lock()
	defer m.cmu.Unlock()
	return m.history
}

func (m *Messenger) setHistory(state string) {
	m.cmu.Lock()
	defer m.cmu.Unlock()
	m.history = state
	m.counts.HistoryState = state
}

// checkHistory looks for his backup once the encrypted side is set up: a
// history device kept from an earlier run means restored; otherwise one
// GetBackup says whether there is a backup for his PIN to open.
func (m *Messenger) checkHistory(ctx context.Context, gen int, cli msgrClient, fbid int64) {
	if d := m.historyDevice(); d != nil && d.ID != "" {
		m.setHistory(historyRestored)
		m.sample(ctx, gen, cli, fbid)
		return
	}
	h := cli.History()
	var backup *securehistory.Backup
	var none bool
	err := m.request(ctx, gen, func(ctx context.Context) error {
		b, err := h.GetBackup(ctx)
		if errors.Is(err, securehistory.ErrNoPINBackup) {
			none = true
			return nil
		}
		backup = b
		return err
	})
	if !m.current(gen) {
		return
	}
	switch {
	case none:
		m.setHistory(historyNoBackup)
	case err != nil:
		// As the library's connector: a backup it couldn't check may still
		// be there, so the PIN is asked for, and RestoreHistory looks again.
		m.setHistory(historyNeedsPIN)
	default:
		m.hmu.Lock()
		m.backup = backup
		m.hmu.Unlock()
		m.setHistory(historyNeedsPIN)
	}
}

// RestoreHistory opens his backup with the PIN he typed, once: an empty PIN
// skips. Meta is asked nothing unless the PIN is six digits, the encrypted
// socket is up and there is a backup; a wrong PIN is pinWrong (with the
// attempts Meta says remain), none left is pinLimit, and nothing is retried.
// The PIN's bytes are zeroed before it returns, whatever happens.
func (m *Messenger) RestoreHistory(ctx context.Context, pin []byte) error {
	defer clear(pin)
	m.hmu.Lock()
	defer m.hmu.Unlock()

	if m.HistoryState() == historyRestored {
		return nil
	}
	if len(pin) == 0 {
		m.setHistory(historySkipped)
		return nil
	}
	if !sixDigits(pin) {
		return errBadRequest
	}
	m.mu.Lock()
	gen, cli, fbid := m.gen, m.cli, m.fbid
	m.mu.Unlock()
	m.dmu.Lock()
	dev := m.dev
	m.dmu.Unlock()
	if cli == nil || fbid == 0 || dev == nil || dev.IdentityKey == nil || !m.e2eeConnected() {
		return errNotConnected
	}
	if d := m.historyDevice(); d != nil {
		// Enrolled before, but Meta's answer to AddDevice never came: it
		// may or may not have taken it.
		return &wire.Error{Kind: wire.Unclear}
	}
	h := cli.History()

	backup := m.backup
	if backup == nil {
		var none bool
		err := m.request(ctx, gen, func(ctx context.Context) error {
			b, err := h.GetBackup(ctx)
			if errors.Is(err, securehistory.ErrNoPINBackup) {
				none = true
				return nil
			}
			backup = b
			return err
		})
		if err != nil {
			return err
		}
		if none {
			m.setHistory(historyNoBackup)
			return &wire.Error{Kind: wire.NotFound}
		}
		m.backup = backup
	}

	var recovery *securehistory.Recovery
	var wrong *securehistory.RecoveryError
	err := m.request(ctx, gen, func(ctx context.Context) error {
		r, err := h.RecoverPIN(ctx, backup, pin)
		if errors.As(err, &wrong) {
			return nil // Meta answered: the PIN is wrong
		}
		recovery = r
		return err
	})
	clear(pin)
	if err != nil {
		return err
	}
	if wrong != nil {
		return pinError(wrong)
	}
	if recovery == nil {
		return errInternal
	}
	defer clear(recovery.Code)

	var virtual virtualDevice
	err = m.request(ctx, gen, func(ctx context.Context) (err error) {
		virtual, err = h.GetVirtualDevice(ctx, backup, fbid, recovery.Code)
		return err
	})
	if err != nil {
		return err
	}
	epochs, err := virtual.DeriveEpochs()
	if err != nil {
		return errInternal
	}
	enrollment, err := virtual.PrepareDevice(backup.ID, h.RegistrationID(), dev.IdentityKey, epochs)
	if err != nil || enrollment == nil || enrollment.Device == nil {
		return errInternal
	}
	// Kept before Meta is asked, as the connector does: if the answer is
	// lost, the device's keys are not.
	if err := m.keepHistory(enrollment.Device); err != nil {
		return errInternal
	}
	var added *securehistory.Device
	var rejected *securehistory.EnrollmentError
	err = m.request(ctx, gen, func(ctx context.Context) error {
		d, err := h.AddDevice(ctx, enrollment)
		if errors.As(err, &rejected) {
			return nil // Meta answered: it would not take the device
		}
		added = d
		return err
	})
	if err != nil {
		return err
	}
	if rejected != nil || added == nil || added.ID == "" {
		_ = m.keepHistory(nil)
		return &wire.Error{Kind: wire.Refused}
	}
	if err := m.keepHistory(added); err != nil {
		return errInternal
	}
	m.setHistory(historyRestored)
	// The gate's sample runs on the run's context, not this request's.
	m.mu.Lock()
	runCtx := m.runCtx
	if m.stopped || gen != m.gen || runCtx == nil {
		m.mu.Unlock()
		return nil
	}
	m.work.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.work.Done()
		m.sample(runCtx, gen, cli, fbid)
	}()
	return nil
}

// pinError is the wire error for Meta's answer to a wrong PIN: pinLimit when
// it says none are left, else pinWrong with the attempts it says remain.
func pinError(e *securehistory.RecoveryError) error {
	if e.AttemptsRemaining != nil && *e.AttemptsRemaining <= 0 {
		return &wire.Error{Kind: wire.PinLimit}
	}
	out := &wire.Error{Kind: wire.PinWrong}
	if e.AttemptsRemaining != nil {
		n := *e.AttemptsRemaining
		out.AttemptsRemaining = &n
	}
	return out
}

// sixDigits reports whether pin is a Messenger PIN: six ASCII digits.
func sixDigits(pin []byte) bool {
	if len(pin) != 6 {
		return false
	}
	for _, b := range pin {
		if b < '0' || b > '9' {
			return false
		}
	}
	return true
}

// historyDevice is the history device kept in the store, or nil. One with an
// empty ID was prepared but never confirmed by Meta.
func (m *Messenger) historyDevice() *securehistory.Device {
	v, ok, err := m.st.NetworkValue(Name, "history")
	if err != nil || !ok || v == "" {
		return nil
	}
	var d securehistory.Device
	if json.Unmarshal([]byte(v), &d) != nil {
		return nil
	}
	return &d
}

// keepHistory keeps the history device in the store's meta (nil removes it).
func (m *Messenger) keepHistory(d *securehistory.Device) error {
	if d == nil {
		return m.st.PutNetworkValue(Name, "history", "")
	}
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return m.st.PutNetworkValue(Name, "history", string(b))
}

// sample is the gate's read of his restored history: one page of the newest
// encrypted one-to-one, decrypted and counted (never kept or shown). Only
// with --gate; once per run.
func (m *Messenger) sample(ctx context.Context, gen int, cli msgrClient, fbid int64) {
	if !m.gate {
		return
	}
	m.cmu.Lock()
	thread := m.newestOneToOne
	m.cmu.Unlock()
	device := m.historyDevice()
	if thread == 0 || device == nil || device.ID == "" {
		return
	}
	h := cli.History()
	threadID := strconv.FormatInt(thread, 10)
	var page *securehistory.MessageRange
	err := m.request(ctx, gen, func(ctx context.Context) (err error) {
		page, err = h.FetchMessages(ctx, device, threadID, true, m.now().Add(time.Minute).UnixMilli(), samplePage)
		return err
	})
	if err != nil || page == nil {
		return
	}
	if page.Epochs != nil && len(page.Epochs.Edges) > 0 {
		// Meta moved his backup on to newer keys: kept, as the connector does.
		if d, err := device.WithEpochs(page.Epochs); err == nil {
			device = d
			_ = m.keepHistory(d)
		}
	}
	lines, err := h.Decrypt(page, device, threadID, fbid, false)
	if err != nil || !m.current(gen) {
		return
	}
	m.cmu.Lock()
	m.counts.RestoredMessages = len(lines)
	m.cmu.Unlock()
}
