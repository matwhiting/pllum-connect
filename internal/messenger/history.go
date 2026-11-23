package messenger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupMessage"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/store"
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
	// A line of the page that cannot be decrypted is left out (the library
	// passes it over), so a page never fails as a whole.
	Decrypt(ctx context.Context, r *securehistory.MessageRange, device *securehistory.Device, threadID string, selfUserID int64, isGroup bool) []*securehistory.DecryptedMessage
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

func (h realHistory) Decrypt(ctx context.Context, r *securehistory.MessageRange, d *securehistory.Device, threadID string, self int64, isGroup bool) []*securehistory.DecryptedMessage {
	return r.Decrypt(ctx, d, threadID, self, isGroup)
}

func (h realHistory) RegistrationID() string { return h.c.RegistrationID }

// passedOver writes to the diagnostics file, in counts only, how many of a
// backup page's lines the library could not decrypt and left out (got is
// how many Decrypt gave). A line the library skips on purpose (none, or
// tagged visibility metadata) is not one of the page's lines.
func (m *Messenger) passedOver(page *securehistory.MessageRange, got int) {
	n := 0
	for _, a := range page.Messages {
		if a != nil && (a.Protobufs == nil || !slices.Contains(a.Protobufs.Tags, "EB_VISIBILITY_METADATA")) {
			n++
		}
	}
	if n > got {
		m.diagLine(fmt.Sprintf("backup page: %d of %d lines could not be read", n-got, n))
	}
}

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
	// The gate's sample, and the first read of his backup, run on the run's
	// context, not this request's. The read waits for the first sign-in's
	// own backfill, which starts it itself when it ends after this.
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
		defer func() { _ = recover() }()
		m.sample(runCtx, gen, cli, fbid)
		if m.value("backfilled") != "" {
			m.encryptedBackfill(runCtx, gen, cli, fbid)
			m.catchUp(runCtx, gen, cli, fbid)
		}
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
	// Meta moved his backup on to newer keys: kept, as the connector does.
	device = m.saveEpochs(device, page)
	lines := h.Decrypt(ctx, page, device, threadID, fbid, false)
	m.passedOver(page, len(lines))
	if !m.current(gen) {
		return
	}
	m.cmu.Lock()
	m.counts.RestoredMessages = len(lines)
	m.cmu.Unlock()
}

// Reading his restored backup. Once his history is restored, each
// encrypted chat's backup is read from the newest back, a page of
// encPageSize lines at a time (FetchMessages, then Decrypt), every request
// through the pace, and each line is stored with source sync under its
// whatsmeow message ID, so a line restored and also received live is one
// row:
//
//   - at the first restore (or the first sign-in after it), each encrypted
//     chat active in the last 90 days gets up to encHistoryPages pages,
//     newest chat first, encBackfillPages in all. Each chat's count, the
//     pages so far and the end ("ebackfilled") are kept as they go, so a
//     rate limit, Meta pushing back or a quit carries on at the next
//     sign-in and nothing is asked twice; a chat whose page fails with an
//     error of its own is passed over. Never with --gate (the gate only
//     counts);
//   - when Pllum reads to the oldest line of an encrypted chat
//     (FetchOlder), one more page of its backup, until the backup has
//     nothing older ("encDone.<chat>"); then its older thread's.
//
// The first page is asked from a minute from now, as the library's
// connector does; each next one from where the last ended
// ("ecursor.<chat>").
//
// Nothing of the backup is asked twice the same way (review I2):
//
//   - a page that does not reach back past where it was asked from or
//     cannot be read, or a request Meta fails, holds the chat
//     ("estalled.<chat>", kept across restarts), for 6 h the first time,
//     24 h the second and 7 days after that ("estallN.<chat>"); a line of
//     a page that cannot be decrypted is left out by the library and the
//     rest of the page kept, and the diagnostics file says how many
//     (passedOver);
//   - two failed requests in a row, across any chats, pause the whole
//     backup the same way ("epause", "epauseN"; "efails" counts them), and
//     end the first restore's read: a failure of the whole backup is asked
//     at most twice, not once per chat. Any answer Meta gives ends the run
//     of failures and starts the pause's growth over; a page that reaches
//     back starts its chat's holds over;
//   - "thread not found" means that chat has no backup: it is done;
//   - "no longer available" or "a different backup" means the backup his
//     PIN opened is gone: the history
//     device is dropped and the history state is needsPIN again, so
//     nothing of it is asked until he restores it again;
//   - every request that reached Meta counts towards the first restore's
//     cap, failures included; reading to the oldest line (FetchOlder) asks
//     at most onDemandPages pages in a day, across every chat ("eod");
//   - nothing is asked while Meta has ended the encrypted session.
//
// A page that moves Meta's backup on to newer keys has them kept, as the
// connector does.

const (
	encPageSize      = 50  // lines one page of the backup asks for
	encHistoryPages  = 2   // pages of one chat's backup at the first restore
	encBackfillPages = 100 // pages of the whole backup at the first restore
	onDemandPages    = 100 // pages of the backup FetchOlder may ask in a day
	encFailsInARow   = 2   // failed requests in a row that pause the whole backup
)

// holdLadder is how long the nth hold in a row lasts: 6 h, 24 h, then 7
// days.
func holdLadder(n int) time.Duration {
	switch {
	case n <= 1:
		return stallHold
	case n == 2:
		return 24 * time.Hour
	}
	return 7 * 24 * time.Hour
}

// encPaging reports whether reading to the oldest line of chat asks for its
// backup: his history is restored, the chat has an encrypted side, and its
// backup is not done.
func (m *Messenger) encPaging(chat string) bool {
	return m.HistoryState() == historyRestored && m.hasEncryptedSide(chat) && m.value("encDone."+chat) == ""
}

// encThread is the thread a chat's backup is read under (the chat's user
// part), and whether it is a group's.
func encThread(chat string) (thread string, group bool) {
	if t, ok := strings.CutSuffix(chat, groupSuffix); ok {
		return t, true
	}
	return strings.TrimSuffix(chat, oneToOneSuffix), false
}

// encStall holds a chat's backup, longer each time in a row: nothing of it
// is asked until the hold ends, across restarts.
func (m *Messenger) encStall(chat string) {
	n, _ := strconv.Atoi(m.value("estallN." + chat))
	n++
	_ = m.keep("estallN."+chat, strconv.Itoa(n))
	_ = m.keep("estalled."+chat, strconv.FormatInt(m.now().Add(holdLadder(n)).UnixMilli(), 10))
}

// encHeld reports whether a chat's backup is held.
func (m *Messenger) encHeld(chat string) bool {
	until, err := strconv.ParseInt(m.value("estalled."+chat), 10, 64)
	return err == nil && m.now().UnixMilli() < until
}

// encMoved records a page of a chat's backup that reached back: its holds
// start over.
func (m *Messenger) encMoved(chat string) {
	_ = m.keep("estalled."+chat, "")
	_ = m.keep("estallN."+chat, "")
}

// encDone records that a chat's backup has nothing older.
func (m *Messenger) encDone(chat string) {
	_ = m.keep("encDone."+chat, "1")
	m.encMoved(chat)
	m.maybeComplete(chat)
}

// encFailed records a request of the backup that Meta failed; two in a row
// pause the whole backup, longer each time.
func (m *Messenger) encFailed() {
	m.efmu.Lock()
	defer m.efmu.Unlock()
	f, _ := strconv.Atoi(m.value("efails"))
	f++
	_ = m.keep("efails", strconv.Itoa(f))
	if f < encFailsInARow {
		return
	}
	n, _ := strconv.Atoi(m.value("epauseN"))
	n++
	_ = m.keep("epauseN", strconv.Itoa(n))
	_ = m.keep("epause", strconv.FormatInt(m.now().Add(holdLadder(n)).UnixMilli(), 10))
}

// encAnswered records a request of the backup that Meta answered: the run
// of failures, and the growing pause, start over.
func (m *Messenger) encAnswered() {
	m.efmu.Lock()
	defer m.efmu.Unlock()
	_ = m.keep("efails", "")
	_ = m.keep("epauseN", "")
}

// encPaused reports whether the whole backup is paused after failures.
func (m *Messenger) encPaused() bool {
	until, err := strconv.ParseInt(m.value("epause"), 10, 64)
	return err == nil && m.now().UnixMilli() < until
}

// The answers of the library's FetchMessages that say something lasting,
// by their text (the library gives them as plain errors).
const (
	answerNone = iota
	answerNoThread
	answerGone
)

func backupAnswer(err error) int {
	if err == nil {
		return answerNone
	}
	e := err.Error()
	switch {
	case strings.Contains(e, "archive thread was not found"):
		return answerNoThread
	case strings.Contains(e, "archive is no longer available"),
		strings.Contains(e, "belongs to a different backup"):
		return answerGone
	}
	return answerNone
}

// dropHistory forgets the history device after Meta said the backup it
// opened is gone: the history state is needsPIN again, and nothing of the
// backup is asked until his PIN restores it again.
func (m *Messenger) dropHistory() {
	_ = m.keepHistory(nil)
	m.setHistory(historyNeedsPIN)
	m.hmu.Lock()
	m.backup = nil // a restore asks for the backup afresh
	m.hmu.Unlock()
}

// onDemand takes one of the day's pages of the backup FetchOlder may ask:
// false when the day's are spent. give puts back one not asked.
func (m *Messenger) onDemand() bool {
	m.odmu.Lock()
	defer m.odmu.Unlock()
	start, n := m.onDemandDay()
	if n >= onDemandPages {
		return false
	}
	_ = m.keep("eod", strconv.FormatInt(start, 10)+"\n"+strconv.Itoa(n+1))
	return true
}

func (m *Messenger) giveOnDemand() {
	m.odmu.Lock()
	defer m.odmu.Unlock()
	start, n := m.onDemandDay()
	if n > 0 {
		_ = m.keep("eod", strconv.FormatInt(start, 10)+"\n"+strconv.Itoa(n-1))
	}
}

// onDemandDay is the day the on-demand pages are counted in (it starts at
// the first of them) and how many it has had. m.odmu is held.
func (m *Messenger) onDemandDay() (start int64, n int) {
	now := m.now().UnixMilli()
	s, c, ok := strings.Cut(m.value("eod"), "\n")
	start, err1 := strconv.ParseInt(s, 10, 64)
	n, err2 := strconv.Atoi(c)
	if !ok || err1 != nil || err2 != nil || now-start >= (24*time.Hour).Milliseconds() || now < start {
		return now, 0
	}
	return start, n
}

// fetchEncPage reads one page of a chat's backup, older than the last, and
// stores its lines. asked reports whether the request reached Meta.
func (m *Messenger) fetchEncPage(ctx context.Context, gen int, cli msgrClient, fbid int64, chat string) (added int, out pageOutcome, asked bool, err error) {
	if m.encryptedEnded() {
		return 0, pageHeld, false, errNotConnected
	}
	lock := m.chatLock(chat)
	lock.Lock()
	defer lock.Unlock()
	if m.value("encDone."+chat) != "" || m.encHeld(chat) || m.encPaused() {
		return 0, pageHeld, false, nil
	}
	device := m.historyDevice()
	if device == nil || device.ID == "" || fbid == 0 || m.HistoryState() != historyRestored {
		return 0, pageHeld, false, nil
	}
	thread, group := encThread(chat)
	ref := m.now().Add(time.Minute).UnixMilli()
	if v, err := strconv.ParseInt(m.value("ecursor."+chat), 10, 64); err == nil && v > 0 {
		ref = v
	}
	h := cli.History()
	var page *securehistory.MessageRange
	var answer error
	err = m.request(ctx, gen, func(ctx context.Context) (err error) {
		asked = true
		page, err = h.FetchMessages(ctx, device, thread, true, ref, encPageSize)
		answer = err
		return err
	})
	if err != nil {
		if !asked {
			return 0, pageHeld, false, err
		}
		switch backupAnswer(answer) {
		case answerNoThread:
			m.encAnswered()
			m.encDone(chat)
			return 0, pageDone, true, nil
		case answerGone:
			m.encAnswered()
			m.dropHistory()
			return 0, pageHeld, true, errNotFound
		}
		// As fetchPage (review N1): a request that fails in the library
		// would fail the same way at the next read, so it is held, longer
		// each time; no connection, a rate limit or Meta pushing back is
		// not, as the pace holds the last two.
		if k := kindOf(err); ctx.Err() == nil && (k == wire.Internal || k == wire.Timeout) {
			m.encStall(chat)
			m.encFailed()
		}
		return 0, pageHeld, true, err
	}
	if page == nil || page.Range == nil || page.Range.HasMoreBefore == nil {
		m.encStall(chat)
		m.encFailed()
		return 0, pageStall, true, errInternal
	}
	device = m.saveEpochs(device, page)
	msgs := h.Decrypt(ctx, page, device, thread, fbid, group)
	m.passedOver(page, len(msgs))
	added, ok := m.storeArchive(chat, thread, group, fbid, msgs)
	if !ok {
		m.encStall(chat)
		m.encFailed()
		return added, pageStall, true, errInternal
	}
	m.encAnswered()
	next, more := page.Range.Before, *page.Range.HasMoreBefore
	switch {
	case !more:
		m.encDone(chat)
		return added, pageDone, true, nil
	case next <= 0 || next >= ref:
		// It did not reach back past where it was asked from: as the
		// connector's "history cursor did not move backwards".
		m.encStall(chat)
		return added, pageStall, true, nil
	}
	_ = m.keep("ecursor."+chat, strconv.FormatInt(next, 10))
	m.encMoved(chat)
	return added, pageMoved, true, nil
}

// saveEpochs keeps the newer keys a page of the backup moved on to, as the
// connector does, and is the device to decrypt it with.
func (m *Messenger) saveEpochs(device *securehistory.Device, page *securehistory.MessageRange) *securehistory.Device {
	if page.Epochs == nil || len(page.Epochs.Edges) == 0 {
		return device
	}
	m.epmu.Lock()
	defer m.epmu.Unlock()
	held := m.historyDevice()
	if held == nil || held.ID != device.ID {
		return device
	}
	d, err := held.WithEpochs(page.Epochs)
	if err != nil {
		return device
	}
	_ = m.keepHistory(d)
	return d
}

// storeArchive stores a page of his backup in chat: each line as whatsmeow
// would have given it live (the latest of its edits, and each person's
// latest reaction, from its supplementals), with source sync. ok is false
// when a line could not be read or stored.
func (m *Messenger) storeArchive(chat, thread string, group bool, fbid int64, msgs []*securehistory.DecryptedMessage) (added int, ok bool) {
	changed := false
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	jid := watypes.NewJID(thread, watypes.MessengerServer)
	if group {
		jid = watypes.NewJID(thread, watypes.GroupServer)
	}
	ok = true
	for _, dm := range msgs {
		if dm == nil || dm.Message == nil {
			continue
		}
		evt, err := decodeArchive(dm.Message.Message, dm.Message.TimestampMS, jid, fbid)
		if err != nil {
			ok = false
			continue
		}
		if evt == nil {
			continue
		}
		paths := map[string]string{}
		for _, a := range dm.Attachments {
			if a.ObjectID != "" && a.URL != "" {
				paths[a.ObjectID] = a.URL
			}
		}
		if len(paths) == 0 {
			paths = nil
		}
		w := m.encPrep(chat, evt, store.SourceSync, paths)
		w = m.withSupplementals(chat, w, evt, dm, jid, fbid)
		n, c, err := m.applyEnc(chat, w)
		added += n
		changed = changed || c
		if err != nil {
			ok = false
		}
	}
	if changed {
		m.changed()
	}
	return added, ok
}

// withSupplementals adds a backed-up line's supplementals to what it
// writes: its latest edit (by its sender) and each person's latest
// reaction. Anything else among them is passed over. Every edit and
// reaction of the line among them, the latest or not, a reaction taken
// back included, settles the line kept under its own ID, if it is one
// that couldn't be read (applyEnc).
func (m *Messenger) withSupplementals(chat string, w encWrites, parent *events.FBMessage, dm *securehistory.DecryptedMessage, jid watypes.JID, fbid int64) encWrites {
	type latest struct {
		at   int64
		text string
		set  bool
	}
	var edit latest
	reactions := map[string]latest{}
	for _, s := range dm.Supplementals {
		if s == nil || s.Message == nil || s.Message.GetMetadata() == nil {
			continue
		}
		at := s.Message.GetMetadata().GetTimestampMS()
		evt, err := decodeArchive(s.Message, at, jid, fbid)
		if err != nil || evt == nil {
			continue
		}
		c := evt.GetConsumerApplication().GetPayload().GetContent()
		switch {
		case c.GetReactionMessage() != nil:
			r := c.GetReactionMessage()
			if t := r.GetKey().GetID(); t != "" && t != parent.Info.ID {
				continue
			}
			w.settle = append(w.settle, settleOwn(evt.Info.ID, parent.Info.ID)...)
			actor := evt.Info.Sender.User
			prev, seen := reactions[actor]
			if !seen || at > prev.at || at == prev.at && r.GetText() == "" {
				reactions[actor] = latest{at: at, text: r.GetText(), set: true}
			}
		case c.GetEditMessage() != nil:
			e := c.GetEditMessage()
			if t := e.GetKey().GetID(); t != "" && t != parent.Info.ID || evt.Info.Sender.User != parent.Info.Sender.User || e.GetMessage() == nil {
				continue
			}
			w.settle = append(w.settle, settleOwn(evt.Info.ID, parent.Info.ID)...)
			if !edit.set || at > edit.at {
				edit = latest{at: at, text: e.GetMessage().GetText(), set: true}
			}
		}
	}
	if edit.set {
		w.puts = append(w.puts, store.Message{ID: parent.Info.ID, ChatID: chat, Text: edit.text, Edited: true, Source: store.SourceSync})
	}
	for actor, r := range reactions {
		if r.text == "" {
			continue
		}
		id, _, self := m.fbSender(watypes.NewJID(actor, watypes.MessengerServer), false)
		w.reactions = append(w.reactions, encReaction{msgID: parent.Info.ID, r: store.Reaction{Emoji: r.text, SenderID: id, FromMe: self}})
	}
	return w
}

// decodeArchive is one backed-up message as whatsmeow would have given it
// live, as the connector's decodeArchiveMessage reads it: nil (and no
// error) for a transport event, which is no line. A message whose content
// whatsmeow can't decode keeps its metadata and shows as a hidden notice.
func decodeArchive(msg *waArmadilloBackupMessage.BackupMessage, at int64, chat watypes.JID, self int64) (*events.FBMessage, error) {
	if msg == nil {
		return nil, errInternal
	}
	var dec events.FBMessage
	switch p := msg.GetPayload().(type) {
	case *waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage:
		var app waMsgApplication.MessageApplication
		if err := proto.Unmarshal(p.EncryptedTransportMessage, &app); err != nil {
			return nil, errInternal
		}
		dec, _ = whatsmeow.DecodeFBApplication(&app)
		dec.FBApplication = &app
	case *waArmadilloBackupMessage.BackupMessage_EncryptedTransportEvent,
		*waArmadilloBackupMessage.BackupMessage_EncryptedTransportLocallyTransformedMessage:
		return nil, nil
	case *waArmadilloBackupMessage.BackupMessage_MiTransportAdminMessage:
		// an admin notice: no content
	default:
		return nil, errInternal
	}
	md := msg.GetMetadata()
	if md.GetMessageID() == "" || md.GetSenderID() == "" || at < 0 {
		return nil, errInternal
	}
	dec.Info = watypes.MessageInfo{
		MessageSource: watypes.MessageSource{
			Chat:     chat,
			Sender:   watypes.NewJID(md.GetSenderID(), watypes.MessengerServer),
			IsFromMe: md.GetSenderID() == strconv.FormatInt(self, 10),
			IsGroup:  chat.Server == watypes.GroupServer,
		},
		ID:        md.GetMessageID(),
		Timestamp: time.UnixMilli(at),
	}
	return &dec, nil
}

// encryptedBackfill reads, at the first restore, up to encHistoryPages
// pages of the backup of each encrypted chat active in the last 90 days,
// newest first, encBackfillPages requests in all (failures included),
// keeping its progress as it goes. Two failures in a row end it: the whole
// backup is failing. One runs at a time; never with --gate.
func (m *Messenger) encryptedBackfill(ctx context.Context, gen int, cli msgrClient, fbid int64) {
	if m.gate || m.HistoryState() != historyRestored || m.value("ebackfilled") != "" {
		return
	}
	if !m.ebActive.CompareAndSwap(false, true) {
		return
	}
	defer m.ebActive.Store(false)
	chats, ok := m.activeChats()
	if !ok {
		return
	}
	total, _ := strconv.Atoi(m.value("ebfPages"))
	for _, chat := range chats {
		if total >= encBackfillPages {
			break
		}
		if ctx.Err() != nil || !m.current(gen) {
			return
		}
		if !m.hasEncryptedSide(chat) {
			continue
		}
		n, _ := strconv.Atoi(m.value("ebf." + chat))
		for n < encHistoryPages && total < encBackfillPages {
			_, out, asked, err := m.fetchEncPage(ctx, gen, cli, fbid, chat)
			if asked {
				total++
				_ = m.keep("ebfPages", strconv.Itoa(total))
			}
			switch {
			case m.HistoryState() != historyRestored:
				return // the backup is gone: a restore again starts afresh
			case m.encPaused():
				// The whole backup is failing: the first restore's read
				// ends, and reading to the oldest line asks again once the
				// pause ends.
				_ = m.keep("ebf."+chat, strconv.Itoa(n))
				_ = m.keep("ebackfilled", "1")
				return
			}
			if err != nil {
				if k := kindOf(err); !m.current(gen) || ctx.Err() != nil || k != wire.Internal && k != wire.Timeout {
					return // carried on at the next sign-in
				}
				n = encHistoryPages // passed over
				break
			}
			if out != pageMoved {
				n = encHistoryPages
				break
			}
			n++
			_ = m.keep("ebf."+chat, strconv.Itoa(n))
		}
		_ = m.keep("ebf."+chat, strconv.Itoa(n))
	}
	if m.current(gen) && ctx.Err() == nil {
		_ = m.keep("ebackfilled", "1")
		// it read each active chat's newest page: that is today's catch-up (catchup.go)
		_ = m.keep("ecatchup", strconv.FormatInt(m.now().UnixMilli(), 10))
	}
}
