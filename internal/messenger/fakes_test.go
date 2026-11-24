package messenger

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow"
	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	wastore "go.mau.fi/whatsmeow/store"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/keys"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/testkit"
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

// thread is an invented thread as a listing gives it, in sync group 1 under
// the inbox (parent -1).
func thread(key int64, tt table.ThreadType, at time.Time, folder string) *table.LSDeleteThenInsertThread {
	return &table.LSDeleteThenInsertThread{ThreadKey: key, ThreadType: tt, LastActivityTimestampMs: at.UnixMilli(),
		FolderName: folder, ThreadName: canary + " name", Snippet: canary + " snippet", ParentThreadKey: -1, SyncGroup: 1}
}

// threadsPage is one page of a folder's threads, with the range Meta gives
// for it: more says there are older threads, from the oldest on this page.
func threadsPage(f folderKey, more bool, threads ...*table.LSDeleteThenInsertThread) *table.LSTable {
	tbl := &table.LSTable{}
	minKey, minAt := int64(0), int64(9999999999999)
	for _, t := range threads {
		t.SyncGroup, t.ParentThreadKey = f.group, f.parent
		tbl.LSDeleteThenInsertThread = append(tbl.LSDeleteThenInsertThread, t)
		if t.LastActivityTimestampMs < minAt {
			minKey, minAt = t.ThreadKey, t.LastActivityTimestampMs
		}
	}
	tbl.LSUpsertSyncGroupThreadsRange = []*table.LSUpsertSyncGroupThreadsRange{{
		SyncGroup: f.group, ParentThreadKey: f.parent, HasMoreBefore: more, MinThreadKey: minKey, MinLastActivityTimestampMS: minAt,
	}}
	return tbl
}

// history is a page of an invented thread's lines, with the range Meta gives
// for it: more says there are older lines.
func history(key int64, more bool, lines ...*table.LSUpsertMessage) *table.LSTable {
	tbl := &table.LSTable{LSUpsertMessage: lines}
	r := &table.LSInsertNewMessageRange{ThreadKey: key, HasMoreBefore: more, MinTimestampMs: 1 << 62, MaxTimestampMs: 0}
	for _, l := range lines {
		l.ThreadKey = key
		if l.TimestampMs < r.MinTimestampMs {
			r.MinTimestampMs, r.MinMessageId = l.TimestampMs, l.MessageId
		}
		if l.TimestampMs > r.MaxTimestampMs {
			r.MaxTimestampMs, r.MaxMessageId = l.TimestampMs, l.MessageId
		}
	}
	r.MinTimestampMsTemplate, r.MaxTimestampMsTemplate = r.MinTimestampMs, r.MaxTimestampMs
	tbl.LSInsertNewMessageRange = []*table.LSInsertNewMessageRange{r}
	return tbl
}

// line is an invented line of an unencrypted thread.
func line(id string, sender int64, at time.Time, text string) *table.LSUpsertMessage {
	return &table.LSUpsertMessage{MessageId: id, SenderId: sender, TimestampMs: at.UnixMilli(), Text: text}
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
	inflight  *atomic.Int32 // events the fakes are still handing over on goroutines of their own
	table     *table.LSTable
	loadErr   error
	loads     int
	registers int
	clients   []*fakeClient
	e2ee      *fakeE2EE
	history   *fakeHistory

	// holdConnect keeps the main socket from saying it connected until a
	// test dispatches the event itself.
	holdConnect bool
	connects    int
	disconnects int // main clients told to disconnect
	// registerErrs fail the next registrations, in order (Meta's answer
	// lost). regKeys and regUUIDs are the identity key and Facebook device
	// UUID each registration was made with.
	registerErrs []error
	regKeys      [][32]byte
	regUUIDs     []uuid.UUID
	// icdc is the account's signed list of device identities as Meta holds
	// it, and icdcSeq its seq: each registration that goes adds its
	// identity and moves the seq on, as the library's does, unless icdcSkip
	// (the next registrations that do not land in it) says not. icdcErr
	// fails the next fetch of it. sessionUUID is the Messenger Lite
	// device ID SessionDeviceID gives (uuid.Nil, as for a web cookie
	// session: none, and RegisterE2EE makes one). sameDevice gives
	// every registration device 7, where each would get the next number.
	icdc        [][32]byte
	icdcSeq     int
	icdcSkip    int
	icdcErr     error
	sessionUUID uuid.UUID
	sameDevice  bool
	// e2eeNext are the encrypted clients PrepareE2EE gives next, in order;
	// after them, e2ee again.
	e2eeNext []*fakeE2EE
	// folders are the thread pages FetchThreadsTask gives for each folder
	// (sync group and parent thread key), in order; after them, an empty
	// answer. endless gives every folder a new page every time.
	folders map[folderKey][]*table.LSTable
	endless bool
	fetches int
	// threadTasks and messageTasks are the reads asked, in order.
	threadTasks  []socket.FetchThreadsTask
	messageTasks []socket.FetchMessagesTask
	// older are the pages of a thread's history FetchMessagesTask gives, in
	// order, delivered on the main socket as Meta does; after them, a range
	// saying there is nothing older. inResponse puts them in the task's own
	// answer instead. silent answers nothing at all.
	older      map[int64][]*table.LSTable
	olderFn    func(key int64) *table.LSTable // when set, every thread's next page
	inResponse bool
	silent     bool
	posted     int // thread pages handed back for the library's sync cursors
	// taskErr fails the reads it returns an error for.
	taskErr func(task socket.Task) error

	// The writes (fakeWriter): every task ExecuteTasks was handed, write
	// or not; the encrypted sends and read receipts; the files asked of
	// the encrypted side (by direct path) and of the CDN (by address); the
	// uploads. writeErrs fails a write by its name (a task's type, or the
	// method's); answer is Meta's answer to a send (by default it takes it
	// as "mid.$invented-sent-<otid>"); files are the bytes each direct path
	// or address gives.
	tasks       []socket.Task
	fbSends     []fbSend
	marks       []markCall
	fbDownloads []string
	cdnGets     []string
	uploads     int
	fbUploads   []whatsmeow.MediaType
	writeErrs   map[string]error
	answer      func(t *socket.SendMessageTask) *table.LSTable
	hook        func(name string) // called once a write is answered, outside the fake's lock
	files       map[string][]byte

	// clock is the time the pace runs on; calls are the requests made, with
	// the time each went.
	clock *fakeClock
	calls []call
}

// call is one request made of the fake, and when.
type call struct {
	name string
	at   time.Time
}

func newFakeMeta() *fakeMeta {
	inflight := &atomic.Int32{}
	f := &fakeMeta{inflight: inflight, table: inbox(), e2ee: &fakeE2EE{inflight: inflight}, history: &fakeHistory{},
		folders: map[folderKey][]*table.LSTable{}, older: map[int64][]*table.LSTable{}, clock: newFakeClock(),
		writeErrs: map[string]error{}, files: map[string][]byte{},
		icdc: [][32]byte{inventedIdentity(0x11)}, icdcSeq: 3}
	f.history.rec = func(name string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.record(name)
	}
	return f
}

// record notes a request; f.mu is held.
func (f *fakeMeta) record(name string) {
	f.calls = append(f.calls, call{name: name, at: f.clock.Now()})
}

// requests are the requests made so far.
func (f *fakeMeta) requests() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call{}, f.calls...)
}

// fakeClock is the time the pace and the network's timers run on: Sleep
// moves it on at once, and a timer runs when the clock reaches it.
type fakeClock = testkit.Clock

func newFakeClock() *fakeClock {
	return testkit.NewClock(time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC))
}

// gaps fails unless every request went at least 2 s after the one before.
func gaps(t *testing.T, calls []call) {
	t.Helper()
	sorted := append([]call{}, calls...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].at.Before(sorted[j].at) })
	for i := 1; i < len(sorted); i++ {
		if d := sorted[i].at.Sub(sorted[i-1].at); d < 2*time.Second {
			t.Fatalf("%s went %v after %s", sorted[i].name, d, sorted[i-1].name)
		}
	}
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

	hmu     sync.Mutex
	handler messagix.EventHandler
}

func (c *fakeClient) SetEventHandler(h messagix.EventHandler) {
	c.hmu.Lock()
	defer c.hmu.Unlock()
	c.handler = h
}

// Connect starts the main socket: it says connected at once, unless the
// test holds it.
func (c *fakeClient) Connect(ctx context.Context) error {
	c.f.mu.Lock()
	c.f.connects++
	c.f.record("Connect")
	hold := c.f.holdConnect
	c.f.mu.Unlock()
	if !hold {
		goDispatch(c.f.inflight, func() { c.dispatch(&messagix.ConnectedEvent{}) })
	}
	return nil
}

func (c *fakeClient) Disconnect() {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.disconnects++
}

// dispatch delivers a main-socket event, as messagix's loop does.
func (c *fakeClient) dispatch(evt any) {
	c.hmu.Lock()
	h := c.handler
	c.hmu.Unlock()
	if h != nil {
		h(context.Background(), evt)
	}
}

// PostHandlePublishResponse is where the real client takes the sync
// cursors a page's answer carries; the fake counts the calls.
func (c *fakeClient) PostHandlePublishResponse(tbl *table.LSTable) {
	c.f.mu.Lock()
	c.f.posted++
	c.f.mu.Unlock()
}

func (c *fakeClient) GetCursor(db int64) string {
	return "cursor-invented-" + strconv.FormatInt(db, 10)
}

// ExecuteTasks answers the two reads: a page of a folder's threads in the
// answer, and a page of a thread's history on the main socket.
func (c *fakeClient) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	c.f.mu.Lock()
	if len(tasks) != 1 {
		c.f.mu.Unlock()
		return nil, errors.New("invented: one task at a time")
	}
	switch t := tasks[0].(type) {
	case *socket.FetchThreadsTask:
		c.f.record("FetchThreadsTask")
		c.f.threadTasks = append(c.f.threadTasks, *t)
		c.f.fetches++
	case *socket.FetchMessagesTask:
		c.f.record("FetchMessagesTask")
		c.f.messageTasks = append(c.f.messageTasks, *t)
	default:
		c.f.mu.Unlock()
		return nil, errors.New("invented: a task the reads never send")
	}
	if c.f.taskErr != nil {
		if err := c.f.taskErr(tasks[0]); err != nil {
			c.f.mu.Unlock()
			return nil, err
		}
	}
	switch t := tasks[0].(type) {
	case *socket.FetchThreadsTask:
		defer c.f.mu.Unlock()
		key := folderKey{group: int64(t.SyncGroup), parent: t.ParentThreadKey}
		if c.f.endless {
			k := int64(7000000000000000 + c.f.fetches)
			at := c.f.clock.Now().Add(-time.Duration(c.f.fetches) * time.Minute)
			page := threadsPage(key, true, thread(k, table.GROUP_THREAD, at, "inbox"))
			// A listed thread comes with its newest line, as Meta's pages do.
			h := history(k, true, line(fmt.Sprintf("mid.$invented-e%d", c.f.fetches), aliceFBID, at, "x"))
			page.LSUpsertMessage, page.LSInsertNewMessageRange = h.LSUpsertMessage, h.LSInsertNewMessageRange
			return page, nil
		}
		pages := c.f.folders[key]
		if len(pages) == 0 {
			return &table.LSTable{}, nil
		}
		c.f.folders[key] = pages[1:]
		return pages[0], nil
	case *socket.FetchMessagesTask:
		var tbl *table.LSTable
		if c.f.olderFn != nil {
			tbl = c.f.olderFn(t.ThreadKey)
		} else if pages := c.f.older[t.ThreadKey]; len(pages) > 0 {
			tbl, c.f.older[t.ThreadKey] = pages[0], pages[1:]
		} else {
			tbl = &table.LSTable{LSUpdateExistingMessageRange: []*table.LSUpdateExistingMessageRange{
				{ThreadKey: t.ThreadKey, TimestampMS: t.ReferenceTimestampMs}}}
		}
		inResponse, silent := c.f.inResponse, c.f.silent
		c.f.mu.Unlock()
		switch {
		case silent:
			return &table.LSTable{}, nil
		case inResponse:
			return tbl, nil
		}
		goDispatch(c.f.inflight, func() { c.dispatch(tbl) })
		return &table.LSTable{}, nil
	}
	c.f.mu.Unlock()
	return nil, nil
}

// last is the newest client made.
func (f *fakeMeta) last() *fakeClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clients[len(f.clients)-1]
}

func (c *fakeClient) LoadMessagesPage(ctx context.Context) (types.UserInfo, *table.LSTable, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.loads++
	c.f.record("LoadMessagesPage")
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
	if c.dev.FacebookUUID == uuid.Nil {
		c.dev.FacebookUUID = uuid.New()
	}
	c.f.mu.Lock()
	c.f.registers++
	c.f.record("RegisterE2EE")
	c.f.regKeys = append(c.f.regKeys, *c.dev.IdentityKey.Pub)
	c.f.regUUIDs = append(c.f.regUUIDs, c.dev.FacebookUUID)
	var err error
	if len(c.f.registerErrs) > 0 {
		err, c.f.registerErrs = c.f.registerErrs[0], c.f.registerErrs[1:]
	}
	c.f.mu.Unlock()
	if err != nil {
		return err
	}
	c.f.mu.Lock()
	device := uint16(6 + c.f.registers)
	if c.f.sameDevice {
		device = 7
	}
	if c.f.icdcSkip > 0 {
		c.f.icdcSkip--
	} else {
		c.f.icdc = append(c.f.icdc, *c.dev.IdentityKey.Pub)
		c.f.icdcSeq++
	}
	c.f.mu.Unlock()
	c.dev.ID = &watypes.JID{User: "2000001", Device: device, Server: watypes.MessengerServer}
	c.dev.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignatureKey: make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		DeviceSignature:     make([]byte, 64),
	}
	return nil
}

// FetchICDC gives the identity list Meta holds now, and its seq.
func (c *fakeClient) FetchICDC(ctx context.Context, fbid int64, deviceUUID uuid.UUID) ([][32]byte, int, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	c.f.record("FetchICDC")
	if err := c.f.icdcErr; err != nil {
		c.f.icdcErr = nil
		return nil, 0, err
	}
	return append([][32]byte{}, c.f.icdc...), c.f.icdcSeq, nil
}

// SessionDeviceID is the session's invented Messenger Lite device ID, if any.
func (c *fakeClient) SessionDeviceID() uuid.UUID {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	return c.f.sessionUUID
}

// PrepareE2EE gives the next encrypted client waiting, else the one there is.
func (c *fakeClient) PrepareE2EE() (e2eeClient, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	if len(c.f.e2eeNext) > 0 {
		c.f.e2ee, c.f.e2eeNext = c.f.e2eeNext[0], c.f.e2eeNext[1:]
		if c.f.e2ee.inflight == nil {
			c.f.e2ee.inflight = c.f.inflight
		}
	}
	return c.f.e2ee, nil
}

// inventedSessionUUID is an invented Messenger Lite device ID.
var inventedSessionUUID = uuid.MustParse("5e5510e0-0000-4000-8000-00000000c0de")

// inventedIdentity is an invented identity key of another of his devices.
func inventedIdentity(b byte) (k [32]byte) {
	for i := range k {
		k[i] = b
	}
	return k
}

func (c *fakeClient) History() historyClient { return c.f.history }

// goDispatch hands an event over on a goroutine of its own, as the real
// sockets do, counted in inflight until it has been handled.
func goDispatch(inflight *atomic.Int32, dispatch func()) {
	inflight.Add(1)
	go func() {
		defer inflight.Add(-1)
		dispatch()
	}()
}

// fakeE2EE is the encrypted socket: Connect says connected (unless the
// test holds it), and a test delivers events through it.
type fakeE2EE struct {
	inflight    *atomic.Int32
	mu          sync.Mutex
	handlers    []func(evt any) bool
	connects    int
	disconnects int
	hold        bool
	connectErr  error // fails the next Connect
}

func (e *fakeE2EE) AddEventHandlerWithSuccessStatus(h func(evt any) bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = append(e.handlers, h)
}

func (e *fakeE2EE) Connect() error {
	e.mu.Lock()
	e.connects++
	hold := e.hold
	if err := e.connectErr; err != nil {
		e.connectErr = nil
		e.mu.Unlock()
		return err
	}
	e.mu.Unlock()
	if !hold {
		goDispatch(e.inflight, func() { e.dispatch(&events.Connected{}) })
	}
	return nil
}

func (e *fakeE2EE) Disconnect() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.disconnects++
}

// dispatch delivers an event as whatsmeow does, and reports whether every
// handler took it (whatsmeow acks only then).
func (e *fakeE2EE) dispatch(evt any) bool {
	e.mu.Lock()
	hs := append([]func(any) bool{}, e.handlers...)
	e.mu.Unlock()
	ok := true
	for _, h := range hs {
		if !h(evt) {
			ok = false
		}
	}
	return ok
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

	// pages, when set, are each thread's pages of the backup, in order;
	// after them (or for a thread with none) an empty page that ends it.
	// asks are the references each thread was asked from; rec notes each
	// fetch among the fake's requests, for the pace.
	pages map[string][]fakePage
	asks  map[string][]int64
	given map[*securehistory.MessageRange][]*securehistory.DecryptedMessage
	rec   func(name string)
	// failWith fails every page of the backup with Meta's answer; fail
	// fails one thread's. endless gives every thread a new page, reaching
	// back an hour, every time.
	failWith error
	fail     map[string]error
	endless  bool
}

// fakePage is one page of an invented backup: its lines, and its range.
// unmoved gives a range that does not reach back past the reference.
type fakePage struct {
	msgs    []*securehistory.DecryptedMessage
	before  int64
	more    bool
	unmoved bool
	// unreadable is how many more lines the page holds that Decrypt
	// leaves out, as the library passes over a line it cannot decrypt.
	unreadable int
}

// script gives a thread's pages of the backup.
func (h *fakeHistory) script(thread string, pages ...fakePage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pages == nil {
		h.pages = map[string][]fakePage{}
	}
	h.pages[thread] = append(h.pages[thread], pages...)
}

// asked are the references a thread's backup was asked from.
func (h *fakeHistory) asked(thread string) []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int64{}, h.asks[thread]...)
}

// total is how many pages of the backup were asked in all.
func (h *fakeHistory) total() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fetches
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
	if h.rec != nil {
		h.rec("FetchMessages")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fetches++
	no := false
	if h.pages == nil {
		return &securehistory.MessageRange{Range: &securehistory.MessageRangeInfo{HasMoreBefore: &no}}, nil
	}
	if h.asks == nil {
		h.asks = map[string][]int64{}
		h.given = map[*securehistory.MessageRange][]*securehistory.DecryptedMessage{}
	}
	h.asks[threadID] = append(h.asks[threadID], ts)
	if h.failWith != nil {
		return nil, h.failWith
	}
	if err := h.fail[threadID]; err != nil {
		return nil, err
	}
	if h.endless {
		yes := true
		r := &securehistory.MessageRange{BackupID: device.BackupID, Range: &securehistory.MessageRangeInfo{HasMoreBefore: &yes, HasMoreAfter: &no, Before: ts - time.Hour.Milliseconds()}}
		h.given[r] = nil
		return r, nil
	}
	p := fakePage{}
	if pages := h.pages[threadID]; len(pages) > 0 {
		p, h.pages[threadID] = pages[0], pages[1:]
	}
	more := p.more
	next := p.before
	if p.unmoved {
		next = ts
	}
	r := &securehistory.MessageRange{BackupID: device.BackupID, Range: &securehistory.MessageRangeInfo{HasMoreBefore: &more, HasMoreAfter: &no, Before: next}}
	for range len(p.msgs) + p.unreadable {
		r.Messages = append(r.Messages, &securehistory.ArchiveMessage{})
	}
	h.given[r] = p.msgs
	return r, nil
}

func (h *fakeHistory) Decrypt(_ context.Context, r *securehistory.MessageRange, device *securehistory.Device, threadID string, self int64, isGroup bool) []*securehistory.DecryptedMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.pages != nil {
		return h.given[r]
	}
	out := make([]*securehistory.DecryptedMessage, h.sample)
	for i := range out {
		out[i] = &securehistory.DecryptedMessage{ID: canary + "-line"}
	}
	return out
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
	log *testkit.Events
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessOver(t, filepath.Join(t.TempDir(), "connect"), newFakeMeta())
}

// newHarnessOver is a new network (a new helper process, as it were) over
// an existing folder, on the given fakes.
func newHarnessOver(t *testing.T, dir string, f *fakeMeta) *harness {
	t.Helper()
	h := &harness{f: f, dir: dir, log: &testkit.Events{}}
	st, err := store.Open(h.dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	h.st = st
	emit := network.Emit(h.log.Emit)
	h.m = New(st, h.dir, testKey, emit.For(Name)).WithGate()
	h.m.newClient = h.f.newClient
	h.m.newWriter = func(msgrClient, e2eeClient) msgrWriter { return fakeWriter{f} }
	h.m.newDownloader = func(msgrClient) msgrDownloader { return fakeWriter{f} }
	h.m.now = f.clock.Now
	h.m.afterFunc = f.clock.AfterFunc
	h.m.pageWait = 2 * time.Second
	h.m.pace = &meta.Pace{Now: f.clock.Now, Sleep: f.clock.Sleep}
	st.SetClock(f.clock.Now)
	if err := h.m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.close)
	return h
}

// close stops the network and closes its store; safe to call twice.
func (h *harness) close() {
	h.m.Stop()
	h.st.Close()
}

// state is the network's state and kind now.
func (h *harness) state() (network.State, string) {
	if e := h.log.LastState(); e != nil {
		k, _ := e["kind"].(string)
		return e["state"].(network.State), k
	}
	return network.NotConnected, ""
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
	testkit.Eventually(t, what, ok)
}

// fbSend is one encrypted send as SendFBMessage was asked it.
type fbSend struct {
	to   watypes.JID
	msg  armadillo.RealMessageApplicationSub
	meta *waMsgApplication.MessageApplication_Metadata
	id   watypes.MessageID
}

// markCall is one encrypted read receipt as MarkRead was asked it.
type markCall struct {
	ids          []watypes.MessageID
	chat, sender watypes.JID
}

// fakeWriter is Meta's side of every write, and of the files fetched.
type fakeWriter struct{ f *fakeMeta }

// taskName is a task's type, as the fake records it.
func taskName(task socket.Task) string {
	switch task.(type) {
	case *socket.SendMessageTask:
		return "SendMessageTask"
	case *socket.SendReactionTask:
		return "SendReactionTask"
	case *socket.ThreadMarkReadTask:
		return "ThreadMarkReadTask"
	case *socket.MuteThreadTask:
		return "MuteThreadTask"
	}
	return fmt.Sprintf("%T", task)
}

// setErr fails the write called name with err (nil clears it).
func (f *fakeMeta) setErr(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErrs[name] = err
}

// count is how many requests called name were made.
func (f *fakeMeta) count(name string) int {
	n := 0
	for _, c := range f.requests() {
		if c.name == name {
			n++
		}
	}
	return n
}

// written are the tasks ExecuteTasks was handed, by name.
func (f *fakeMeta) written() []socket.Task {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]socket.Task{}, f.tasks...)
}

// after calls the test's hook for a write just answered.
func (f *fakeMeta) after(name string) {
	f.mu.Lock()
	hook := f.hook
	f.mu.Unlock()
	if hook != nil {
		hook(name)
	}
}

func (w fakeWriter) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	f := w.f
	if len(tasks) == 1 {
		defer f.after(taskName(tasks[0]))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks = append(f.tasks, tasks...)
	if len(tasks) != 1 {
		f.record("ExecuteTasks")
		return nil, errors.New("invented: one task at a time")
	}
	name := taskName(tasks[0])
	f.record(name)
	if err := f.writeErrs[name]; err != nil {
		return nil, err
	}
	if t, ok := tasks[0].(*socket.SendMessageTask); ok {
		if f.answer != nil {
			return f.answer(t), nil
		}
		otid := strconv.FormatInt(t.Otid, 10)
		return &table.LSTable{LSReplaceOptimsiticMessage: []*table.LSReplaceOptimsiticMessage{
			{OfflineThreadingId: otid, MessageId: "mid.$invented-sent-" + otid}}}, nil
	}
	return &table.LSTable{}, nil
}

func (w fakeWriter) SendFBMessage(ctx context.Context, to watypes.JID, message armadillo.RealMessageApplicationSub, metadata *waMsgApplication.MessageApplication_Metadata, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	f := w.f
	defer f.after("SendFBMessage")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("SendFBMessage")
	s := fbSend{to: to, msg: message, meta: metadata}
	if len(extra) == 1 {
		s.id = extra[0].ID
	}
	f.fbSends = append(f.fbSends, s)
	if err := f.writeErrs["SendFBMessage"]; err != nil {
		return whatsmeow.SendResponse{}, err
	}
	return whatsmeow.SendResponse{Timestamp: f.clock.Now(), ID: s.id}, nil
}

func (w fakeWriter) MarkRead(ctx context.Context, ids []watypes.MessageID, timestamp time.Time, chat, sender watypes.JID, receiptTypeExtra ...watypes.ReceiptType) error {
	f := w.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("MarkRead")
	f.marks = append(f.marks, markCall{ids: append([]watypes.MessageID{}, ids...), chat: chat, sender: sender})
	return f.writeErrs["MarkRead"]
}

func (w fakeWriter) DownloadFB(ctx context.Context, transport *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error) {
	f := w.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("DownloadFB")
	f.fbDownloads = append(f.fbDownloads, transport.GetDirectPath())
	if err := f.writeErrs["DownloadFB"]; err != nil {
		return nil, err
	}
	data, ok := f.files[transport.GetDirectPath()]
	if !ok {
		return nil, whatsmeow.ErrMediaDownloadFailedWith404
	}
	return data, nil
}

func (w fakeWriter) Upload(ctx context.Context, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error) {
	f := w.f
	defer f.after("Upload")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Upload")
	f.uploads++
	if err := f.writeErrs["Upload"]; err != nil {
		return nil, err
	}
	fbid := int64(5550000000000001)
	return &types.MercuryUploadResponse{Payload: types.MediaPayloads{RealMetadata: &types.FileMetadata{ImageID: types.StringOrInt(fbid)}}}, nil
}

// Download is the CDN: the file at an address, or a 404.
func (w fakeWriter) Download(ctx context.Context, url string) ([]byte, error) {
	f := w.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("Download")
	f.cdnGets = append(f.cdnGets, url)
	if err := f.writeErrs["Download"]; err != nil {
		return nil, err
	}
	data, ok := f.files[url]
	if !ok {
		return nil, statusError(404)
	}
	return data, nil
}

// UploadFB is WhatsApp's media servers for an encrypted file: each upload
// gets a direct path and object ID of its own.
func (w fakeWriter) UploadFB(ctx context.Context, data []byte, mediaType whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	f := w.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("UploadFB")
	f.fbUploads = append(f.fbUploads, mediaType)
	if err := f.writeErrs["UploadFB"]; err != nil {
		return whatsmeow.UploadResponse{}, err
	}
	n := len(f.fbUploads)
	return whatsmeow.UploadResponse{DirectPath: fmt.Sprintf("/v/t62.invented/upload-%d", n), ObjectID: fmt.Sprintf("object-upload-%d", n),
		MediaKey: []byte("key-invented"), FileSHA256: []byte("sha-invented"), FileEncSHA256: []byte("enc-invented"), FileLength: uint64(len(data))}, nil
}
