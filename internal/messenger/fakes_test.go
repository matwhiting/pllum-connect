package messenger

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow/proto/waAdv"
	wastore "go.mau.fi/whatsmeow/store"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/keys"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// Every name, ID, cookie, PIN and text in these tests is invented.

// testKey is an invented store key.
var testKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(0x60 + i)
	}
	return k
}()

// His invented account, and the invented threads of his inbox.
const (
	selfFBID int64 = 2000001

	aliceFBID int64 = 2000011 // an encrypted one-to-one, and her older thread
	bobFBID   int64 = 2000022 // an encrypted one-to-one, the newest
	carolFBID int64 = 2000033 // an encrypted one-to-one
	groupOne  int64 = 8800000000000001
	groupTwo  int64 = 8800000000000002
	encGroup  int64 = 9900000000000001 // an encrypted group
	daveFBID  int64 = 2000044          // an older thread moved to the cutover folder

	canary = "canary-msgr-5e1a-invented"
)

// validSession is an invented Facebook web session with every named cookie.
func validSession() network.Session {
	return network.Session{"xs": "xs-invented-1", "c_user": "2000001", "datr": "datr-invented-1"}
}

// inbox is an invented first page of his inbox: two groups, three encrypted
// one-to-ones (Bob's newest), an encrypted group, and four pointers from an
// older thread to an encrypted one (Alice's twice, by two rows).
func inbox() *table.LSTable {
	thread := func(key int64, tt table.ThreadType, at time.Time, folder string) *table.LSDeleteThenInsertThread {
		return &table.LSDeleteThenInsertThread{ThreadKey: key, ThreadType: tt, LastActivityTimestampMs: at.UnixMilli(),
			FolderName: folder, ThreadName: canary + " name", Snippet: canary + " snippet"}
	}
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	return &table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
			thread(groupOne, table.GROUP_THREAD, base.Add(-5*time.Hour), "inbox"),
			thread(groupTwo, table.GROUP_THREAD, base.Add(-6*time.Hour), "inbox"),
			thread(aliceFBID, table.ENCRYPTED_OVER_WA_ONE_TO_ONE, base.Add(-2*time.Hour), "inbox"),
			thread(bobFBID, table.ENCRYPTED_OVER_WA_ONE_TO_ONE, base.Add(-time.Hour), "inbox"),
			thread(carolFBID, table.ENCRYPTED_OVER_WA_ONE_TO_ONE, base.Add(-3*time.Hour), "inbox"),
			thread(encGroup, table.ENCRYPTED_OVER_WA_GROUP, base.Add(-4*time.Hour), "inbox"),
			// Alice's older, unencrypted thread, cut over to her encrypted one
			thread(aliceFBID, table.ONE_TO_ONE, base.Add(-30*time.Hour), "e2ee_cutover"),
		},
		LSVerifyHybridThreadExists: []*table.LSVerifyHybridThreadExists{
			{ThreadKey: aliceFBID, ThreadJID: aliceFBID, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE},
			{ThreadKey: bobFBID, ThreadJID: bobFBID, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE},
		},
		LSUpdateThreadAuthorityAndMappingWithOTIDFromJID: []*table.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID{
			{ThreadKey: carolFBID, ThreadJID: carolFBID},
		},
		LSMoveThreadToE2EECutoverFolder: []*table.LSMoveThreadToE2EECutoverFolder{{ThreadKey: daveFBID}},
	}
}

// fakeUser is his invented account as LoadMessagesPage gives it.
type fakeUser struct{ fbid int64 }

func (u fakeUser) GetUsername() string  { return canary + ".user" }
func (u fakeUser) GetName() string      { return canary + " name" }
func (u fakeUser) GetAvatarURL() string { return "" }
func (u fakeUser) GetFBID() int64       { return u.fbid }

// fakeMeta stands in for Meta: the client, the encrypted socket and the
// secure-history service. It never touches the network.
type fakeMeta struct {
	mu        sync.Mutex
	table     *table.LSTable
	loadErr   error
	loads     int
	registers int
	clients   []*fakeClient
	e2ee      *fakeE2EE
	history   *fakeHistory
}

func newFakeMeta() *fakeMeta {
	return &fakeMeta{table: inbox(), e2ee: &fakeE2EE{}, history: &fakeHistory{}}
}

func (f *fakeMeta) newClient(ck *cookies.Cookies) msgrClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &fakeClient{f: f, ck: ck}
	f.clients = append(f.clients, c)
	return c
}

type fakeClient struct {
	f   *fakeMeta
	ck  *cookies.Cookies
	dev *wastore.Device
}

func (c *fakeClient) LoadMessagesPage(ctx context.Context) (types.UserInfo, *table.LSTable, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.loads++
	if c.f.loadErr != nil {
		return nil, nil, c.f.loadErr
	}
	return fakeUser{fbid: selfFBID}, c.f.table, nil
}

func (c *fakeClient) GetCookies() *cookies.Cookies { return c.ck }

func (c *fakeClient) SetDevice(dev *wastore.Device) { c.dev = dev }

// RegisterE2EE does what the library's does to the device once Meta has
// answered: an ID on the Messenger server, and the placeholder account.
func (c *fakeClient) RegisterE2EE(ctx context.Context, fbid int64) error {
	c.f.mu.Lock()
	c.f.registers++
	c.f.mu.Unlock()
	c.dev.ID = &watypes.JID{User: "2000001", Device: 7, Server: watypes.MessengerServer}
	c.dev.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignatureKey: make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		DeviceSignature:     make([]byte, 64),
	}
	return nil
}

func (c *fakeClient) PrepareE2EE() (e2eeClient, error) { return c.f.e2ee, nil }

func (c *fakeClient) History() historyClient { return c.f.history }

// fakeE2EE is the encrypted socket: Connect says connected, and a test
// delivers events through it.
type fakeE2EE struct {
	mu       sync.Mutex
	handlers []func(evt any)
	connects int
}

func (e *fakeE2EE) AddEventHandler(h func(evt any)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, h)
}

func (e *fakeE2EE) Connect() error {
	e.mu.Lock()
	e.connects++
	e.mu.Unlock()
	go e.dispatch(&events.Connected{})
	return nil
}

func (e *fakeE2EE) Disconnect() {}

func (e *fakeE2EE) dispatch(evt any) {
	e.mu.Lock()
	hs := append([]func(any){}, e.handlers...)
	e.mu.Unlock()
	for _, h := range hs {
		h(evt)
	}
}

// fakeHistory is Meta's secure-history service. RecoverPIN copies the PIN it
// is given (so a test can say the right one was passed) and keeps the slice
// itself (so a test can say it was zeroed after).
type fakeHistory struct {
	mu           sync.Mutex
	backupErr    error
	recoverErr   error
	recoverCalls int
	gotPIN       string
	pinSlice     []byte
	code         []byte
	added        int
	fetches      int
	sample       int // how many lines one page of history decrypts to
}

func (h *fakeHistory) GetBackup(ctx context.Context) (*securehistory.Backup, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.backupErr != nil {
		return nil, h.backupErr
	}
	return &securehistory.Backup{ID: "backup-invented-1"}, nil
}

func (h *fakeHistory) RecoverPIN(ctx context.Context, backup *securehistory.Backup, pin []byte) (*securehistory.Recovery, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recoverCalls++
	h.gotPIN = string(pin)
	h.pinSlice = pin
	if h.recoverErr != nil {
		return nil, h.recoverErr
	}
	h.code = []byte("recovery-code-invented-1")
	return &securehistory.Recovery{Code: h.code}, nil
}

func (h *fakeHistory) GetVirtualDevice(ctx context.Context, backup *securehistory.Backup, userID int64, code []byte) (virtualDevice, error) {
	return fakeVirtual{}, nil
}

func (h *fakeHistory) AddDevice(ctx context.Context, e *securehistory.Enrollment) (*securehistory.Device, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.added++
	d := *e.Device
	d.ID = "history-device-invented-1"
	return &d, nil
}

func (h *fakeHistory) FetchMessages(ctx context.Context, device *securehistory.Device, threadID string, before bool, ts int64, count int) (*securehistory.MessageRange, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fetches++
	no := false
	return &securehistory.MessageRange{Range: &securehistory.MessageRangeInfo{HasMoreBefore: &no}}, nil
}

func (h *fakeHistory) Decrypt(r *securehistory.MessageRange, device *securehistory.Device, threadID string, self int64, isGroup bool) ([]*securehistory.DecryptedMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*securehistory.DecryptedMessage, h.sample)
	for i := range out {
		out[i] = &securehistory.DecryptedMessage{ID: canary + "-line"}
	}
	return out, nil
}

func (h *fakeHistory) RegistrationID() string { return "registration-invented-1" }

type fakeVirtual struct{}

func (fakeVirtual) DeriveEpochs() (map[string]securehistory.Epoch, error) {
	return map[string]securehistory.Epoch{"epoch-invented-1": {ID: "epoch-invented-1", RootKey: []byte("root-invented-1")}}, nil
}

func (fakeVirtual) PrepareDevice(backupID, registrationID string, armadillo *keys.KeyPair, epochs map[string]securehistory.Epoch) (*securehistory.Enrollment, error) {
	return &securehistory.Enrollment{
		Device: &securehistory.Device{BackupID: backupID, StoragePrivateKey: []byte("storage-invented-1"), Epochs: epochs},
		Info:   &securehistory.DeviceInfo{BackupID: backupID},
	}, nil
}

// harness is a messenger network over a store in a temporary folder, on the
// fakes, with the gate on and a pace that never sleeps.
type harness struct {
	m   *Messenger
	f   *fakeMeta
	st  *store.Store
	dir string

	emu    sync.Mutex
	events []map[string]any
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{f: newFakeMeta(), dir: filepath.Join(t.TempDir(), "connect")}
	st, err := store.Open(h.dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	h.st = st
	emit := network.Emit(func(e map[string]any) {
		h.emu.Lock()
		defer h.emu.Unlock()
		h.events = append(h.events, e)
	})
	h.m = New(st, h.dir, testKey, emit.For(Name)).WithGate()
	h.m.newClient = h.f.newClient
	h.m.pace = &meta.Pace{Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := h.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		h.m.Stop()
		st.Close()
	})
	return h
}

// signIn logs in with the invented session and waits for the encrypted
// connection and the history check.
func (h *harness) signIn(t *testing.T) {
	t.Helper()
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the encrypted connection and the history check", func() bool {
		c := h.m.GateCounts().(GateCounts)
		return c.Registered && c.E2EEConnected && c.HistoryState != ""
	})
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
