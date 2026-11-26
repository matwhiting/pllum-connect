package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every name, number and JID in these tests is invented.

// tripClient passes every call to a fake client until it is armed; once
// armed, any call of any kind (a send, a receipt, a download, a connect, even
// IsConnected) fails the test. It names each method itself rather than
// embedding one, so a method added to waClient must be added here too.
type tripClient struct {
	t     *testing.T
	inner *fakeClient

	mu    sync.Mutex
	armed bool
	hits  []string
}

var _ waClient = (*tripClient)(nil)

func (c *tripClient) hit(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.armed {
		c.hits = append(c.hits, method)
	}
}

func (c *tripClient) arm(on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.armed = on
}

func (c *tripClient) calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.hits...)
}

func (c *tripClient) AddEventHandlerWithSuccessStatus(h whatsmeow.EventHandlerWithSuccessStatus) uint32 {
	c.hit("AddEventHandlerWithSuccessStatus")
	return c.inner.AddEventHandlerWithSuccessStatus(h)
}

func (c *tripClient) GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	c.hit("GetQRChannel")
	return c.inner.GetQRChannel(ctx)
}

func (c *tripClient) Connect() error { c.hit("Connect"); return c.inner.Connect() }

func (c *tripClient) Disconnect() { c.hit("Disconnect"); c.inner.Disconnect() }

func (c *tripClient) IsConnected() bool { c.hit("IsConnected"); return c.inner.IsConnected() }

func (c *tripClient) DownloadHistorySync(ctx context.Context, n *waE2E.HistorySyncNotification, sync bool) (*waHistorySync.HistorySync, error) {
	c.hit("DownloadHistorySync")
	return c.inner.DownloadHistorySync(ctx, n, sync)
}

func (c *tripClient) ParseWebMessage(chat types.JID, m *waWeb.WebMessageInfo) (*events.Message, error) {
	c.hit("ParseWebMessage")
	return c.inner.ParseWebMessage(chat, m)
}

func (c *tripClient) SendProtocolMessageReceipt(ctx context.Context, id types.MessageID, kind types.ReceiptType) error {
	c.hit("SendProtocolMessageReceipt")
	return c.inner.SendProtocolMessageReceipt(ctx, id, kind)
}

func (c *tripClient) BuildReaction(chat, sender types.JID, id types.MessageID, reaction string) *waE2E.Message {
	c.hit("BuildReaction")
	return c.inner.BuildReaction(chat, sender, id, reaction)
}

func (c *tripClient) GenerateMessageID() types.MessageID {
	c.hit("GenerateMessageID")
	return c.inner.GenerateMessageID()
}

func (c *tripClient) SendMessage(ctx context.Context, to types.JID, m *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	c.hit("SendMessage")
	return c.inner.SendMessage(ctx, to, m, extra...)
}

func (c *tripClient) Upload(ctx context.Context, data []byte, kind whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	c.hit("Upload")
	return c.inner.Upload(ctx, data, kind)
}

func (c *tripClient) MarkRead(ctx context.Context, ids []types.MessageID, ts time.Time, chat, sender types.JID, extra ...types.ReceiptType) error {
	c.hit("MarkRead")
	return c.inner.MarkRead(ctx, ids, ts, chat, sender, extra...)
}

func (c *tripClient) SendAppState(ctx context.Context, patch appstate.PatchInfo) error {
	c.hit("SendAppState")
	return c.inner.SendAppState(ctx, patch)
}

func (c *tripClient) Download(ctx context.Context, m whatsmeow.DownloadableMessage) ([]byte, error) {
	c.hit("Download")
	return c.inner.Download(ctx, m)
}

func (c *tripClient) SendMediaRetryReceipt(ctx context.Context, info *types.MessageInfo, mediaKey []byte) error {
	c.hit("SendMediaRetryReceipt")
	return c.inner.SendMediaRetryReceipt(ctx, info, mediaKey)
}

func (c *tripClient) Logout(ctx context.Context) error { c.hit("Logout"); return c.inner.Logout(ctx) }

func (c *tripClient) IsOnWhatsApp(ctx context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
	c.hit("IsOnWhatsApp")
	return c.inner.IsOnWhatsApp(ctx, phones)
}

// tripRig is a paired, live adapter whose client is a tripClient.
func tripRig(t *testing.T) (*rig, *tripClient) {
	t.Helper()
	r := newRig(t, true)
	trip := &tripClient{t: t}
	r.w.newClient = func(dev *wastore.Device) waClient {
		fc := newFakeClient(dev)
		r.mu.Lock()
		r.clients = append(r.clients, fc)
		r.mu.Unlock()
		trip.inner = fc
		return trip
	}
	c := r.live()
	// Start connects in its own goroutine: wait for that Connect, so it
	// is not taken for the adapter's doing once a test arms the trip.
	eventually(t, "the adapter's own connect", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.connects > 0
	})
	return r, trip
}

// contacts calls Contacts with the client armed, and fails the test if the
// adapter touched the client.
func contacts(t *testing.T, r *rig, trip *tripClient) []network.Contact {
	t.Helper()
	trip.arm(true)
	got, err := r.w.Contacts(context.Background())
	trip.arm(false)
	if err != nil {
		t.Fatalf("Contacts: %v", err)
	}
	if calls := trip.calls(); len(calls) != 0 {
		t.Fatalf("Contacts called the WhatsApp client: %v", calls)
	}
	return got
}

func TestContactsListsTheLocalStoreAndSendsNothing(t *testing.T) {
	r, trip := tripRig(t)
	ctx := context.Background()
	dev := r.w.device()
	carol := types.NewJID("10000000003", types.DefaultUserServer)
	// Alice is in his phone's contacts with a full name; Bob only with a
	// first name, and has a push name; Carol is a business he never saved.
	if err := dev.Contacts.PutContactName(ctx, aliceJID, "Alice", "Alice Example"); err != nil {
		t.Fatal(err)
	}
	if err := dev.Contacts.PutContactName(ctx, bobJID, "Bob", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dev.Contacts.PutPushName(ctx, bobJID, "Bobby"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dev.Contacts.PutBusinessName(ctx, carol, "Invented Bakery"); err != nil {
		t.Fatal(err)
	}
	before := r.client()
	c0, d0, h0 := before.counts()
	w0 := before.writes()

	got := contacts(t, r, trip)

	want := []network.Contact{
		{ID: aliceJID.String(), Phone: "+10000000001", Name: "Alice Example"},
		{ID: bobJID.String(), Phone: "+10000000002", Name: "Bob", PushName: "Bobby"},
		{ID: carol.String(), Phone: "+10000000003", BusinessName: "Invented Bakery"},
	}
	if len(got) != len(want) {
		t.Fatalf("Contacts = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("item %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	c1, d1, h1 := before.counts()
	if c1 != c0 || d1 != d0 || h1 != h0 || before.writes() != w0 || len(before.sentReceipts()) != 0 {
		t.Errorf("the fake recorded a call: connects %d→%d, disconnects %d→%d, downloads %d→%d, writes %d→%d, receipts %v",
			c0, c1, d0, d1, h0, h1, w0, before.writes(), before.sentReceipts())
	}
	for _, e := range r.log.All() {
		if e["event"] != "state" {
			t.Errorf("Contacts emitted %v", e)
		}
	}
}

func TestContactsKeysALIDByItsNumberWhenKnown(t *testing.T) {
	r, trip := tripRig(t)
	ctx := context.Background()
	dev := r.w.device()
	if _, _, err := dev.Contacts.PutPushName(ctx, aliceLID, "Ali"); err != nil {
		t.Fatal(err)
	}
	// A LID whose number the device has never learnt carries no phone: its
	// digits are not a number.
	strangerLID := types.NewJID("100000000000007", types.HiddenUserServer)
	if _, _, err := dev.Contacts.PutPushName(ctx, strangerLID, "Someone"); err != nil {
		t.Fatal(err)
	}
	got := contacts(t, r, trip)
	if len(got) != 2 {
		t.Fatalf("Contacts = %+v, want Alice by her LID and the stranger", got)
	}
	if got[0] != (network.Contact{ID: aliceLID.String(), PushName: "Ali"}) {
		t.Errorf("Alice before her number is known = %+v", got[0])
	}
	if got[1] != (network.Contact{ID: strangerLID.String(), PushName: "Someone"}) {
		t.Errorf("the stranger = %+v", got[1])
	}

	if err := dev.LIDs.PutLIDMapping(ctx, aliceLID, aliceJID); err != nil {
		t.Fatal(err)
	}
	got = contacts(t, r, trip)
	// sorted by ID: the stranger's LID sorts before Alice's number
	if len(got) != 2 || got[1] != (network.Contact{ID: aliceJID.String(), Phone: "+10000000001", PushName: "Ali"}) {
		t.Fatalf("Alice once her number is known = %+v", got)
	}

	// Stored under both: one item, each side's empty fields filled from the
	// other's.
	if err := dev.Contacts.PutContactName(ctx, aliceJID, "Alice", "Alice Example"); err != nil {
		t.Fatal(err)
	}
	got = contacts(t, r, trip)
	if len(got) != 2 {
		t.Fatalf("Contacts = %+v, want Alice once", got)
	}
	if got[1] != (network.Contact{ID: aliceJID.String(), Phone: "+10000000001", Name: "Alice Example", PushName: "Ali"}) {
		t.Errorf("Alice under both = %+v", got[1])
	}
}

func TestContactsLeavesOutSelfGroupsAndChannels(t *testing.T) {
	r, trip := tripRig(t)
	ctx := context.Background()
	dev := r.w.device()
	bot := types.NewJID("13135550002", types.DefaultUserServer) // in the range WhatsApp gives its bots
	for _, j := range []types.JID{selfJID, selfLID, groupJID, listJID, newsJID, botJID, bot, statusJID} {
		if _, _, err := dev.Contacts.PutPushName(ctx, j, "Invented"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := dev.Contacts.PutPushName(ctx, bobJID, "Bobby"); err != nil {
		t.Fatal(err)
	}
	got := contacts(t, r, trip)
	if len(got) != 1 || got[0].ID != bobJID.String() {
		t.Fatalf("Contacts = %+v, want only Bob", got)
	}
}

func TestContactsBeforeSignInIsNotConnected(t *testing.T) {
	r := newRig(t, false)
	for _, when := range []string{"before Start", "with no linked device"} {
		if when != "before Start" {
			r.start()
		}
		_, err := r.w.Contacts(context.Background())
		var we *wire.Error
		if !errors.As(err, &we) || we.Kind != wire.NotConnected {
			t.Fatalf("Contacts %s = %#v, want a *wire.Error of kind notConnected", when, err)
		}
	}
}

// failingContacts is a contact store whose listing fails with err.
type failingContacts struct {
	wastore.ContactStore
	err error
}

func (f failingContacts) GetAllContacts(context.Context) (map[types.JID]types.ContactInfo, error) {
	return nil, f.err
}

func TestAContactStoreThatWillNotReadIsInternalAndKeepsItsCause(t *testing.T) {
	r, trip := tripRig(t)
	dev := r.w.device()
	// The cause's text stands in for a database error that quotes a row.
	cause := errors.New("invented cause naming 10000000001@s.whatsapp.net")
	held := dev.Contacts
	dev.Contacts = failingContacts{ContactStore: held, err: cause}
	defer func() { dev.Contacts = held }()

	trip.arm(true)
	_, err := r.w.Contacts(context.Background())
	trip.arm(false)
	if kindOf(err) != wire.Internal {
		t.Fatalf("Contacts = %v, want internal", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("the error lost its cause: %#v", err)
	}
	if text := fmt.Sprint(err); strings.Contains(text, "10000000001") || strings.Contains(text, "invented cause") {
		t.Errorf("the error's text carries the cause's: %q", text)
	}
	if calls := trip.calls(); len(calls) != 0 {
		t.Errorf("Contacts called the WhatsApp client: %v", calls)
	}
}
