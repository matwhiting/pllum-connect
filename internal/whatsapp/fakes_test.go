package whatsapp

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waCommon"
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
)

// Every name, number, JID, ID and text in these tests is invented.

// testKey is an invented store key.
var testKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(0x40 + i)
	}
	return k
}()

var (
	selfJID   = types.NewJID("10000000099", types.DefaultUserServer)
	selfLID   = types.NewJID("100000000000099", types.HiddenUserServer)
	aliceJID  = types.NewJID("10000000001", types.DefaultUserServer)
	aliceLID  = types.NewJID("100000000000001", types.HiddenUserServer)
	bobJID    = types.NewJID("10000000002", types.DefaultUserServer)
	groupJID  = types.NewJID("120000000000000001", types.GroupServer)
	listJID   = types.NewJID("1700000001", types.BroadcastServer)
	newsJID   = types.NewJID("120000000000000009", types.NewsletterServer)
	botJID    = types.NewJID("867051314767696", types.BotServer)
	statusJID = types.StatusBroadcastJID
)

// base is an invented moment, a whole second: 2026-01-01T00:00:00Z.
const base int64 = 1767225600000

// fakeClient stands in for whatsmeow's client: the adapter's events are
// dispatched to it by the test, and nothing reaches the network.
type fakeClient struct {
	mu          sync.Mutex
	dev         *wastore.Device
	parser      *whatsmeow.Client // a real client that never connects, for ParseWebMessage
	handlers    []whatsmeow.EventHandlerWithSuccessStatus
	qr          chan whatsmeow.QRChannelItem
	qrCtx       context.Context
	connects    int
	disconnects int
	connected   bool
	connectErr  error
	downloads   int
	history     map[string]*waHistorySync.HistorySync // by the notification's direct path
	block       chan struct{}                         // when set, a download waits for it or its context
}

func newFakeClient(dev *wastore.Device) *fakeClient {
	return &fakeClient{
		dev:     dev,
		parser:  whatsmeow.NewClient(dev, waLog.Noop),
		history: map[string]*waHistorySync.HistorySync{},
	}
}

func (f *fakeClient) AddEventHandlerWithSuccessStatus(h whatsmeow.EventHandlerWithSuccessStatus) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers = append(f.handlers, h)
	return uint32(len(f.handlers))
}

func (f *fakeClient) GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dev.ID != nil {
		return nil, whatsmeow.ErrQRStoreContainsID
	}
	f.qr = make(chan whatsmeow.QRChannelItem, 8)
	f.qrCtx = ctx
	return f.qr, nil
}

func (f *fakeClient) Connect() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	if f.connectErr != nil {
		return f.connectErr
	}
	f.connected = true
	return nil
}

func (f *fakeClient) Disconnect() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnects++
	f.connected = false
}

func (f *fakeClient) IsConnected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}

// DownloadHistorySync gives the history the test filed under the
// notification's direct path, and (as whatsmeow does with synchronous
// storage) keeps its number–LID mappings and push names in the device store.
func (f *fakeClient) DownloadHistorySync(ctx context.Context, notif *waE2E.HistorySyncNotification, synchronousStorage bool) (*waHistorySync.HistorySync, error) {
	f.mu.Lock()
	f.downloads++
	block := f.block
	hs := f.history[notif.GetDirectPath()]
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if hs == nil {
		return nil, whatsmeow.ErrMediaDownloadFailedWith404
	}
	if synchronousStorage {
		for _, m := range hs.GetPhoneNumberToLidMappings() {
			pn, _ := types.ParseJID(m.GetPnJID())
			lid, _ := types.ParseJID(m.GetLidJID())
			_ = f.dev.LIDs.PutLIDMapping(ctx, lid, pn)
		}
		for _, p := range hs.GetPushnames() {
			jid, _ := types.ParseJID(p.GetID())
			_, _, _ = f.dev.Contacts.PutPushName(ctx, jid, p.GetPushname())
		}
	}
	return proto.Clone(hs).(*waHistorySync.HistorySync), nil
}

func (f *fakeClient) ParseWebMessage(chat types.JID, webMsg *waWeb.WebMessageInfo) (*events.Message, error) {
	return f.parser.ParseWebMessage(chat, webMsg)
}

// dispatch hands evt to the adapter as whatsmeow would, and reports whether
// every handler said it was handled (whatsmeow acks only then).
func (f *fakeClient) dispatch(evt any) bool {
	f.mu.Lock()
	hs := append([]whatsmeow.EventHandlerWithSuccessStatus(nil), f.handlers...)
	f.mu.Unlock()
	ok := true
	for _, h := range hs {
		ok = h(evt) && ok
	}
	return ok
}

func (f *fakeClient) counts() (connects, disconnects, downloads int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects, f.disconnects, f.downloads
}

// fakeClock runs timers only when the test moves it on.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Time
	f       func()
	stopped bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.UnixMilli(base + 86_400_000)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		was := !t.stopped
		t.stopped = true
		return was
	}
}

// Advance moves the clock on by d, running each timer that falls due, in
// order, outside the clock's lock.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	end := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		sort.SliceStable(c.timers, func(i, j int) bool { return c.timers[i].at.Before(c.timers[j].at) })
		var due *fakeTimer
		for _, t := range c.timers {
			if !t.stopped && !t.at.After(end) {
				due = t
				break
			}
		}
		if due == nil {
			c.now = end
			c.mu.Unlock()
			return
		}
		due.stopped = true
		if due.at.After(c.now) {
			c.now = due.at
		}
		c.mu.Unlock()
		due.f()
	}
}

// eventLog keeps every event the adapter emits.
type eventLog struct {
	mu  sync.Mutex
	all []map[string]any
}

func (l *eventLog) emit(ev map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.all = append(l.all, ev)
}

func (l *eventLog) named(name string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, ev := range l.all {
		if ev["event"] == name {
			out = append(out, ev)
		}
	}
	return out
}

func (l *eventLog) lastState() map[string]any {
	s := l.named("state")
	if len(s) == 0 {
		return nil
	}
	return s[len(s)-1]
}

// rig is one adapter over a temporary store, with fake clients and a fake
// clock.
type rig struct {
	t       *testing.T
	dir     string
	st      *store.Store
	w       *WhatsApp
	clock   *fakeClock
	log     *eventLog
	mu      sync.Mutex
	clients []*fakeClient
	onMake  func(*fakeClient)
}

// newRig makes an adapter; paired puts a linked device (selfJID) in the
// device database first, as a link in an earlier run would have.
func newRig(t *testing.T, paired bool) *rig {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "connect")
	st, err := store.Open(dir, testKey)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if paired {
		pairDevice(t, dir)
	}
	r := &rig{t: t, dir: dir, st: st}
	r.fresh()
	return r
}

// fresh makes a new adapter on the rig's store and folder, as a restart does.
func (r *rig) fresh() {
	r.clock = newFakeClock()
	r.log = &eventLog{}
	w := New(r.st, r.dir, testKey, network.Emit(r.log.emit).For("whatsapp"))
	w.clock = r.clock
	w.newClient = func(dev *wastore.Device) waClient {
		fc := newFakeClient(dev)
		r.mu.Lock()
		r.clients = append(r.clients, fc)
		on := r.onMake
		r.mu.Unlock()
		if on != nil {
			on(fc)
		}
		return fc
	}
	r.w = w
	r.t.Cleanup(w.Stop)
}

// restart stops the adapter and starts a new one on the same store.
func (r *rig) restart() {
	r.w.Stop()
	r.mu.Lock()
	r.clients = nil
	r.mu.Unlock()
	r.fresh()
	r.start()
}

func (r *rig) start() {
	r.t.Helper()
	if err := r.w.Start(context.Background()); err != nil {
		r.t.Fatalf("Start: %v", err)
	}
}

// client is the newest fake client the adapter made.
func (r *rig) client() *fakeClient {
	r.t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.clients) == 0 {
		r.t.Fatalf("the adapter made no client")
	}
	return r.clients[len(r.clients)-1]
}

// live starts a paired adapter and brings it to live.
func (r *rig) live() *fakeClient {
	r.t.Helper()
	r.start()
	c := r.client()
	c.dispatch(&events.Connected{})
	c.dispatch(&events.OfflineSyncCompleted{})
	return c
}

// send dispatches evt and fails the test if the adapter did not handle it.
func (r *rig) send(evt any) {
	r.t.Helper()
	if !r.client().dispatch(evt) {
		r.t.Fatalf("the adapter did not handle a %T", evt)
	}
}

func (r *rig) messages(chatID string) []store.Message {
	r.t.Helper()
	page, err := r.st.Messages("whatsapp", chatID, "", 200)
	if err != nil {
		r.t.Fatalf("Messages: %v", err)
	}
	return page.Items
}

func (r *rig) message(chatID, id string) store.Message {
	r.t.Helper()
	for _, m := range r.messages(chatID) {
		if m.ID == id {
			return m
		}
	}
	r.t.Fatalf("no line %s in the chat", id)
	return store.Message{}
}

func (r *rig) chat(chatID string) store.Chat {
	r.t.Helper()
	c, ok, err := r.st.Chat("whatsapp", chatID)
	if err != nil || !ok {
		r.t.Fatalf("Chat: ok %v, err %v", ok, err)
	}
	return c
}

func (r *rig) chatIDs() []string {
	r.t.Helper()
	page, err := r.st.Chats("whatsapp", 0, "", 200)
	if err != nil {
		r.t.Fatal(err)
	}
	var ids []string
	for _, c := range page.Items {
		ids = append(ids, c.ID)
	}
	sort.Strings(ids)
	return ids
}

// pairDevice saves a linked device in <dir>/whatsapp/device.db. Pairing gives
// a device its signed identity; invented bytes stand in.
func pairDevice(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenDevice(filepath.Join(dir, "whatsapp", "device.db"), testKey)
	if err != nil {
		t.Fatalf("OpenDevice: %v", err)
	}
	defer db.Close()
	c := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := c.Upgrade(ctx); err != nil {
		t.Fatalf("Upgrade: %v", err)
	}
	dev := c.NewDevice()
	linkDevice(t, dev)
}

// linkDevice gives dev the identity pairing would, and saves it.
func linkDevice(t *testing.T, dev *wastore.Device) {
	t.Helper()
	jid := selfJID
	jid.Device = 7
	dev.ID = &jid
	dev.LID = types.NewADJID(selfLID.User, 0, 7)
	dev.LID.Server = types.HiddenUserServer
	dev.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte("invented-details"),
		AccountSignatureKey: make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		DeviceSignature:     make([]byte, 64),
	}
	if err := dev.Save(context.Background()); err != nil {
		t.Fatalf("saving the device: %v", err)
	}
}

// eventually waits up to five seconds for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// msg builds a message event as whatsmeow dispatches it.
func msg(chat, sender types.JID, id string, ms int64, fromMe bool, m *waE2E.Message) *events.Message {
	evt := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chat,
				Sender:   sender,
				IsFromMe: fromMe,
				IsGroup:  chat.Server == types.GroupServer,
			},
			ID:        id,
			Timestamp: time.UnixMilli(ms),
		},
		RawMessage: m,
	}
	return evt.UnwrapRaw()
}

// textFrom is an incoming text from sender in chat.
func textFrom(chat, sender types.JID, id string, ms int64, body string) *events.Message {
	return msg(chat, sender, id, ms, false, &waE2E.Message{Conversation: proto.String(body)})
}

// textMine is a text he sent from his phone.
func textMine(chat types.JID, id string, ms int64, body string) *events.Message {
	return msg(chat, selfJID, id, ms, true, &waE2E.Message{Conversation: proto.String(body)})
}

// webMsg is one history message as the phone sends it.
func webMsg(chat, sender types.JID, id string, ms int64, fromMe bool, m *waE2E.Message) *waHistorySync.HistorySyncMsg {
	wm := &waWeb.WebMessageInfo{
		Key:              &waCommon.MessageKey{RemoteJID: proto.String(chat.String()), FromMe: proto.Bool(fromMe), ID: proto.String(id)},
		Message:          m,
		MessageTimestamp: proto.Uint64(uint64(ms / 1000)),
	}
	if chat.Server == types.GroupServer && !fromMe {
		wm.Key.Participant = proto.String(sender.String())
	}
	return &waHistorySync.HistorySyncMsg{Message: wm}
}

// historyNotif is the protocol message that tells of a history blob, filed
// under path in the fake.
func historyNotif(id, path string, ms int64) *events.Message {
	return msg(selfJID, selfJID, id, ms, true, &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		Type: waE2E.ProtocolMessage_HISTORY_SYNC_NOTIFICATION.Enum(),
		HistorySyncNotification: &waE2E.HistorySyncNotification{
			DirectPath: proto.String(path),
			SyncType:   waE2E.HistorySyncType_INITIAL_BOOTSTRAP.Enum(),
		},
	}})
}

var errCutOff = errors.New("cut off")
