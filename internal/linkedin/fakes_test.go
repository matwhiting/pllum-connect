package linkedin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"
	"go.mau.fi/util/jsontime"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/testkit"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every URN, name, cookie and text in these tests is invented, and no
// request reaches LinkedIn: every client the network makes is a fakeClient
// over one fakeLinkedIn.

// testKey is an invented store key.
var testKey = func() (k [32]byte) {
	for i := range k {
		k[i] = byte(0x60 + i)
	}
	return k
}()

// start is an invented moment: 2026-03-01T09:00:00Z.
var start = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

// His profile, two people and an organisation.
const (
	selfID  = "ACoAAInventedSelf0001"
	aliceID = "ACoAAInventedAlice001"
	bobID   = "ACoAAInventedBob00001"
	otherID = "ACoAAInventedOther001" // another account than his
	orgID   = "90000001"
)

// convURN is an invented conversation URN; n tells them apart.
func convURN(n int) string {
	return fmt.Sprintf("urn:li:msg_conversation:(urn:li:fsd_profile:%s,2-aW52ZW50ZWQt%04d)", selfID, n)
}

// msgURN is an invented message URN.
func msgURN(n int) string {
	return fmt.Sprintf("urn:li:msg_message:(urn:li:fsd_profile:%s,2-bWVzc2FnZS%06d)", selfID, n)
}

func participantURN(id string) linkedingo.URN {
	return linkedingo.NewURN("urn:li:msg_messagingParticipant:urn:li:fsd_profile:" + id)
}

// validSession is an invented web session with both required cookies and a
// few optional ones.
func validSession() network.Session {
	return network.Session{
		"li_at":      "li-at-invented-1",
		"JSESSIONID": `"ajax:0000000000000000001"`,
		"bcookie":    `"v=2&invented-1"`,
		"lidc":       `"b=invented-1"`,
		"lang":       "v=2&lang=en-us",
	}
}

// fakeClock moves only when something sleeps on it or a test advances it.
type fakeClock = testkit.Clock

func newFakeClock() *fakeClock { return testkit.NewClock(start) }

// call is one call a fake client was asked to make.
type call struct {
	name string
	at   time.Time
	arg  string
	// cat is the folder a GetConversationsWithNextCursor call asked for.
	cat string
}

// fakeLinkedIn stands in for LinkedIn, scripted by each test.
type fakeLinkedIn struct {
	mu       sync.Mutex
	clock    *fakeClock
	clients  []*fakeClient
	calls    []call
	inFlight int
	maxIn    int

	profileID string
	errs      map[string]error // a call fails with it, by name
	// errsOnce fail the next call of that name only.
	errsOnce map[string]error

	// syncPage answers GetConversationsBySyncToken (with the token the client
	// holds); newToken is the token the handler is given after it.
	syncPage func(token string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation]
	newToken string
	// cursorPage answers GetConversationsWithNextCursor for the INBOX
	// folder, and archivePage for ARCHIVE; a folder with no answer set is
	// empty. foldersErr fails a folder's calls (by its name), and
	// foldersErrAfter fails them from the nth call of that folder on.
	cursorPage  func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]
	archivePage func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]
	foldersErr  map[string]error
	// before answers GetMessagesBefore; prev answers GetMessagesWithPrevCursor.
	before func(conv string, before time.Time, count int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]
	prev   func(conv, cursor string, count int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]
	// convErr, when set, fails a GetMessagesBefore or
	// GetMessagesWithPrevCursor call (by name) of one conversation with
	// what it gives, after the call is recorded.
	convErr func(name, conv string) error
	// rotate, when set, is applied to the client's jar on every request: the
	// cookies LinkedIn sets as it answers.
	rotate func(jar *linkedingo.StringCookieJar, n int)
	nreq   int

	// send, when set, answers SendMessage (after it is recorded and any
	// error set for it given); otherwise a sent message with a new URN.
	send  func(c *fakeClient, ctx context.Context, conv string, body linkedingo.SendMessageBody, rc []linkedingo.SendRenderContent, txn string) (*linkedingo.MessageSentResponse, error)
	sends []sentMessage
	nsent int
	// uploads are the files UploadMedia was given.
	uploads []upload
	// download, when set, answers DownloadHTTP; otherwise an empty 200.
	download func(url string) (*http.Response, error)
}

// sentMessage is one SendMessage call's arguments.
type sentMessage struct {
	conv string
	body linkedingo.SendMessageBody
	rc   []linkedingo.SendRenderContent
	txn  string
}

// upload is one UploadMedia call's arguments.
type upload struct {
	kind        linkedingo.MediaUploadType
	filename    string
	contentType string
	size        int
	data        []byte
}

// uploadedAsset is the asset URN every invented upload answers with.
const uploadedAsset = "urn:li:digitalmediaAsset:InventedAsset0001"

// sentNow is LinkedIn's answer to a send: the message, his, under a new
// invented URN.
func (f *fakeLinkedIn) sentNow(conv string, body linkedingo.SendMessageBody) *linkedingo.MessageSentResponse {
	f.mu.Lock()
	f.nsent++
	n := f.nsent
	f.mu.Unlock()
	return &linkedingo.MessageSentResponse{Data: linkedingo.Message{
		EntityURN:               linkedingo.NewURN(msgURN(800000 + n)),
		Body:                    linkedingo.AttributedText{Text: body.Text},
		DeliveredAt:             ms(f.clock.Now()),
		Sender:                  me(),
		MessageBodyRenderFormat: linkedingo.MessageBodyRenderFormatDefault,
		ConversationURN:         linkedingo.NewURN(conv),
	}}
}

func (f *fakeLinkedIn) sendsNow() []sentMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentMessage(nil), f.sends...)
}

func (f *fakeLinkedIn) uploadsNow() []upload {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]upload(nil), f.uploads...)
}

func newFakeLinkedIn(clock *fakeClock) *fakeLinkedIn {
	return &fakeLinkedIn{
		clock:     clock,
		profileID: selfID,
		errs:      map[string]error{},
		errsOnce:  map[string]error{},
		newToken:  "sync-token-invented-new",
	}
}

func (f *fakeLinkedIn) newClient(self linkedingo.URN, jar *linkedingo.StringCookieJar, userAgent, syncToken string, h linkedingo.Handlers) liClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &fakeClient{f: f, self: self, jar: jar, ua: userAgent, token: syncToken, h: h, sessID: uuid.New()}
	f.clients = append(f.clients, c)
	return c
}

func (f *fakeLinkedIn) setErr(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs[name] = err
}

func (f *fakeLinkedIn) failOnce(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errsOnce[name] = err
}

// record notes a call and gives the error set for it, if any.
func (f *fakeLinkedIn) record(c *fakeClient, name, arg string) error {
	return f.recordIn(c, name, arg, "")
}

// recordIn is record for a call that names a folder.
func (f *fakeLinkedIn) recordIn(c *fakeClient, name, arg, cat string) error {
	f.mu.Lock()
	f.calls = append(f.calls, call{name: name, at: f.clock.Now(), arg: arg, cat: cat})
	err := f.errs[name]
	if e, ok := f.foldersErr[cat]; ok && cat != "" {
		err = e
	}
	if e, ok := f.errsOnce[name]; ok {
		err = e
		delete(f.errsOnce, name)
	}
	f.nreq++
	n := f.nreq
	rotate := f.rotate
	f.mu.Unlock()
	if rotate != nil && err == nil && c != nil {
		rotate(c.jar, n)
	}
	return err
}

// failConv is the error convErr gives a call of conv, if any.
func (f *fakeLinkedIn) failConv(name, conv string) error {
	f.mu.Lock()
	fail := f.convErr
	f.mu.Unlock()
	if fail == nil {
		return nil
	}
	return fail(name, conv)
}

func (f *fakeLinkedIn) enter() {
	f.mu.Lock()
	f.inFlight++
	f.maxIn = max(f.maxIn, f.inFlight)
	f.mu.Unlock()
}

func (f *fakeLinkedIn) leave() {
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
}

// callsNow is a copy of every call so far.
func (f *fakeLinkedIn) callsNow() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *fakeLinkedIn) count(name string) int {
	n := 0
	for _, c := range f.callsNow() {
		if c.name == name {
			n++
		}
	}
	return n
}

// countIn is the number of listing requests made for a folder.
func (f *fakeLinkedIn) countIn(cat string) int {
	n := 0
	for _, c := range f.callsNow() {
		if c.name == "GetConversationsWithNextCursor" && c.cat == cat {
			n++
		}
	}
	return n
}

// setFolderErr fails every listing request of a folder with err.
func (f *fakeLinkedIn) setFolderErr(cat string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.foldersErr == nil {
		f.foldersErr = map[string]error{}
	}
	f.foldersErr[cat] = err
}

func (f *fakeLinkedIn) namesSince(i int) []string {
	calls := f.callsNow()
	var out []string
	for _, c := range calls[min(i, len(calls)):] {
		out = append(out, c.name)
	}
	return out
}

func (f *fakeLinkedIn) last() *fakeClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.clients) == 0 {
		return nil
	}
	return f.clients[len(f.clients)-1]
}

// fakeClient is one client the network made.
type fakeClient struct {
	f      *fakeLinkedIn
	self   linkedingo.URN
	jar    *linkedingo.StringCookieJar
	ua     string
	token  string
	h      linkedingo.Handlers
	sessID uuid.UUID

	mu        sync.Mutex
	connected bool
	ctx       context.Context
}

var _ liClient = (*fakeClient)(nil)

func (c *fakeClient) GetCurrentUserProfile(ctx context.Context) (*linkedingo.UserProfile, error) {
	c.f.enter()
	defer c.f.leave()
	if err := c.f.record(c, "GetCurrentUserProfile", ""); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	id := c.f.profileID
	c.f.mu.Unlock()
	return &linkedingo.UserProfile{MiniProfile: linkedingo.MiniProfile{
		FirstName:     "Invented",
		LastName:      "Self",
		EntityURN:     linkedingo.NewURN("urn:li:fs_miniProfile:" + id),
		DashEntityURN: linkedingo.NewURN("urn:li:fsd_profile:" + id),
	}}, nil
}

func (c *fakeClient) GetConversationsBySyncToken(ctx context.Context) (*linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation], error) {
	c.f.enter()
	defer c.f.leave()
	if err := c.f.record(c, "GetConversationsBySyncToken", c.token); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	page, token := c.f.syncPage, c.f.newToken
	c.f.mu.Unlock()
	resp := &linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation]{}
	if page != nil {
		resp = page(c.token)
	}
	resp.Metadata.NewSyncToken = token
	c.token = token
	if c.h.ConversationsSyncToken != nil {
		c.h.ConversationsSyncToken(ctx, token)
	}
	return resp, nil
}

func (c *fakeClient) GetConversationsUpdatedBefore(ctx context.Context, updatedBefore time.Time) (*linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation], error) {
	_ = c.f.record(c, "GetConversationsUpdatedBefore", "")
	return nil, fmt.Errorf("LinkedIn ignores lastUpdatedBefore: never call it")
}

func (c *fakeClient) GetConversationsWithNextCursor(ctx context.Context, category, cursor string) (*linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation], error) {
	c.f.enter()
	defer c.f.leave()
	if err := c.f.recordIn(c, "GetConversationsWithNextCursor", cursor, category); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	var page func(string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]
	switch category {
	case "INBOX":
		page = c.f.cursorPage
	case "ARCHIVE":
		page = c.f.archivePage
	}
	c.f.mu.Unlock()
	if page == nil {
		return &linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]{}, nil
	}
	return page(cursor), nil
}

func (c *fakeClient) GetMessagesBefore(ctx context.Context, conv linkedingo.URN, before time.Time, count int) (*linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message], error) {
	c.f.enter()
	defer c.f.leave()
	if err := c.f.record(c, "GetMessagesBefore", conv.String()); err != nil {
		return nil, err
	}
	if err := c.f.failConv("GetMessagesBefore", conv.String()); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	page := c.f.before
	c.f.mu.Unlock()
	if page == nil {
		return &linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]{}, nil
	}
	return page(conv.String(), before, count), nil
}

func (c *fakeClient) GetMessagesWithPrevCursor(ctx context.Context, conv linkedingo.URN, prevCursor string, count int) (*linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message], error) {
	c.f.enter()
	defer c.f.leave()
	if err := c.f.record(c, "GetMessagesWithPrevCursor", conv.String()+" "+prevCursor); err != nil {
		return nil, err
	}
	if err := c.f.failConv("GetMessagesWithPrevCursor", conv.String()); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	page := c.f.prev
	c.f.mu.Unlock()
	if page == nil {
		return &linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]{}, nil
	}
	return page(conv.String(), prevCursor, count), nil
}

func (c *fakeClient) DownloadHTTP(ctx context.Context, url string) (*http.Response, error) {
	if err := c.f.record(c, "DownloadHTTP", url); err != nil {
		return nil, err
	}
	c.f.mu.Lock()
	download := c.f.download
	c.f.mu.Unlock()
	if download != nil {
		return download(url)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func (c *fakeClient) RealtimeConnect(ctx context.Context) error {
	if err := c.f.record(c, "RealtimeConnect", ""); err != nil {
		return err
	}
	c.mu.Lock()
	c.connected, c.ctx = true, ctx
	c.mu.Unlock()
	return nil
}

func (c *fakeClient) RealtimeDisconnect() {
	_ = c.f.record(c, "RealtimeDisconnect", "")
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
}

func (c *fakeClient) IsLoggedIn() bool {
	_ = c.f.record(c, "IsLoggedIn", "")
	return true
}

func (c *fakeClient) SendMessage(ctx context.Context, conv linkedingo.URN, body linkedingo.SendMessageBody, rc []linkedingo.SendRenderContent, txn string) (*linkedingo.MessageSentResponse, error) {
	c.f.enter()
	defer c.f.leave()
	c.f.mu.Lock()
	c.f.sends = append(c.f.sends, sentMessage{conv: conv.String(), body: body, rc: rc, txn: txn})
	send := c.f.send
	c.f.mu.Unlock()
	if err := c.f.record(c, "SendMessage", conv.String()); err != nil {
		return nil, err
	}
	if send != nil {
		return send(c, ctx, conv.String(), body, rc, txn)
	}
	return c.f.sentNow(conv.String(), body), nil
}

func (c *fakeClient) UploadMedia(ctx context.Context, t linkedingo.MediaUploadType, filename, contentType string, size int, r io.Reader) (linkedingo.URN, error) {
	data, _ := io.ReadAll(r)
	c.f.mu.Lock()
	c.f.uploads = append(c.f.uploads, upload{kind: t, filename: filename, contentType: contentType, size: size, data: data})
	c.f.mu.Unlock()
	if err := c.f.record(c, "UploadMedia", filename); err != nil {
		return linkedingo.URN{}, err
	}
	return linkedingo.NewURN(uploadedAsset), nil
}

func (c *fakeClient) SendReaction(ctx context.Context, msg linkedingo.URN, emoji string) error {
	return c.f.record(c, "SendReaction", msg.String()+" "+emoji)
}

func (c *fakeClient) RemoveReaction(ctx context.Context, msg linkedingo.URN, emoji string) error {
	return c.f.record(c, "RemoveReaction", msg.String()+" "+emoji)
}

func (c *fakeClient) MarkConversationRead(ctx context.Context, convs ...linkedingo.URN) (*linkedingo.MarkThreadReadResponse, error) {
	return &linkedingo.MarkThreadReadResponse{}, c.f.record(c, "MarkConversationRead", urnList(convs))
}

func (c *fakeClient) MarkConversationUnread(ctx context.Context, convs ...linkedingo.URN) (*linkedingo.MarkThreadReadResponse, error) {
	return &linkedingo.MarkThreadReadResponse{}, c.f.record(c, "MarkConversationUnread", urnList(convs))
}

func urnList(us []linkedingo.URN) string {
	var out []string
	for _, u := range us {
		out = append(out, u.String())
	}
	return strings.Join(out, ",")
}

func (c *fakeClient) Logout(ctx context.Context) error {
	return c.f.record(c, "Logout", "")
}

// dispatch hands a realtime event to the client's handlers, as the
// library's stream does.
func (c *fakeClient) dispatch(evt any) {
	ctx := context.Background()
	switch e := evt.(type) {
	case *linkedingo.ClientConnection:
		if e.SessID == uuid.Nil {
			e.SessID = c.sessID
		}
		c.h.ClientConnection(ctx, e)
	case *linkedingo.DecoratedEvent:
		c.h.DecoratedEvent(ctx, e)
	case heartbeat:
		c.h.Heartbeat(ctx)
	case transient:
		c.h.TransientDisconnect(ctx, e.err)
	case badCredentials:
		c.h.BadCredentials(ctx, e.err)
	default:
		panic(fmt.Sprintf("unknown event %T", evt))
	}
}

type heartbeat struct{}
type transient struct{ err error }
type badCredentials struct{ err error }

// statusErr is LinkedIn answering with code.
func statusErr(code int) error { return &linkedingo.ResponseError{StatusCode: code} }

// events records what the network emitted.
type events = testkit.Events

// rig is one network over a fresh store, a fake clock and a fake LinkedIn.
type rig struct {
	t     *testing.T
	l     *LinkedIn
	f     *fakeLinkedIn
	clock *fakeClock
	st    *store.Store
	dir   string
	ev    *events
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "connect")
	st, err := store.Open(dir, testKey)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	clock := newFakeClock()
	r := &rig{t: t, f: newFakeLinkedIn(clock), clock: clock, st: st, dir: dir}
	r.restart()
	return r
}

// restart makes a new network over the same store, as a restart of the
// helper does, stopping the old one.
func (r *rig) restart() {
	r.t.Helper()
	if r.l != nil {
		r.l.Stop()
	}
	r.ev = &events{}
	r.l = New(r.st, r.dir, testKey, r.ev.Emit, Options{NewClient: r.f.newClient, Clock: r.clock})
	if err := r.l.Start(context.Background()); err != nil {
		r.t.Fatalf("Start: %v", err)
	}
	r.t.Cleanup(r.l.Stop)
}

func (r *rig) login(s network.Session) error {
	return r.l.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: s})
}

// keep sets one of the network's stored values, as an earlier run left it.
func (r *rig) keep(name, value string) {
	r.t.Helper()
	if err := r.st.PutNetworkValue(Name, name, value); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) value(name string) string {
	v, _, _ := r.st.NetworkValue(Name, name)
	return v
}

// live signs in (as an account that has signed in before, with a sync
// token) and waits until it is live, its catch-up and history done.
func (r *rig) live() *fakeClient {
	r.t.Helper()
	if r.value("self") == "" {
		r.keep("self", selfID)
	}
	if r.value("syncToken") == "" {
		r.keep("syncToken", "sync-token-invented-0")
	}
	r.keep("listed", "1")
	r.keep("listedFolders", "2")
	if err := r.login(validSession()); err != nil {
		r.t.Fatalf("Login: %v", err)
	}
	r.waitState(network.Live, "")
	r.idle()
	return r.f.last()
}

// waitState waits until the network's state is s with kind.
func (r *rig) waitState(s network.State, kind wire.Kind) {
	r.t.Helper()
	eventually(r.t, fmt.Sprintf("state %s/%s (have %v)", s, kind, r.ev.States()), func() bool {
		st, k := r.l.stateNow()
		return st == s && k == kind
	})
}

// idle waits until the network's background work has nothing left to do.
func (r *rig) idle() {
	r.t.Helper()
	eventually(r.t, "the background work to settle", func() bool { return r.l.idleNow() })
}

func (r *rig) chat(id string) store.Chat {
	r.t.Helper()
	c, ok, err := r.st.Chat(Name, id)
	if err != nil || !ok {
		r.t.Fatalf("no chat %q: %v", id, err)
	}
	return c
}

func (r *rig) lines(chat string) []store.Message {
	r.t.Helper()
	var out []store.Message
	before := ""
	for range 100 {
		p, err := r.st.Messages(Name, chat, before, 200)
		if err != nil {
			r.t.Fatalf("Messages: %v", err)
		}
		out = append(p.Items, out...)
		if !p.HasMore {
			return out
		}
		before = p.OldestCursor
	}
	return out
}

// eventually waits for cond (testkit.Eventually).
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testkit.Eventually(t, what, cond)
}

// never checks cond stays false for a short real time.
func never(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testkit.Never(t, what, cond)
}

// Builders of LinkedIn's shapes.

func ms(t time.Time) jsontime.UnixMilli { return jsontime.UM(t) }

func person(id, first, last string) linkedingo.MessagingParticipant {
	return linkedingo.MessagingParticipant{
		EntityURN: participantURN(id),
		ParticipantType: linkedingo.ParticipantType{Member: &linkedingo.MemberParticipantInfo{
			FirstName: linkedingo.AttributedText{Text: first},
			LastName:  linkedingo.AttributedText{Text: last},
		}},
	}
}

func me() linkedingo.MessagingParticipant { return person(selfID, "Invented", "Self") }

func org(id, name string) linkedingo.MessagingParticipant {
	return linkedingo.MessagingParticipant{
		EntityURN: linkedingo.NewURN("urn:li:msg_messagingParticipant:urn:li:fsd_company:" + id),
		ParticipantType: linkedingo.ParticipantType{Organization: &linkedingo.OrganizationParticipantInfo{
			Name: linkedingo.AttributedText{Text: name},
		}},
	}
}

// conv is a one-to-one with Alice, last active at.
func conv(n int, at time.Time) linkedingo.Conversation {
	return linkedingo.Conversation{
		EntityURN:                linkedingo.NewURN(convURN(n)),
		LastActivityAt:           ms(at),
		ConversationParticipants: []linkedingo.MessagingParticipant{me(), person(aliceID, "Alice", "Invented")},
		Categories:               []string{"INBOX", "PRIMARY_INBOX"},
	}
}

// text is a plain message in conversation n.
func text(n, id int, from linkedingo.MessagingParticipant, at time.Time, body string) linkedingo.Message {
	return linkedingo.Message{
		EntityURN:               linkedingo.NewURN(msgURN(id)),
		Body:                    linkedingo.AttributedText{Text: body},
		DeliveredAt:             ms(at),
		Sender:                  from,
		MessageBodyRenderFormat: linkedingo.MessageBodyRenderFormatDefault,
		Conversation:            linkedingo.Conversation{EntityURN: linkedingo.NewURN(convURN(n))},
	}
}

func messagePage(prev string, msgs ...linkedingo.Message) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
	return &linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message]{
		Metadata: linkedingo.MessageMetadata{PrevCursor: prev},
		Elements: msgs,
	}
}

func syncPageOf(convs []linkedingo.Conversation, deleted ...string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
	p := &linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation]{Elements: convs}
	for _, d := range deleted {
		var u linkedingo.DeletedURN
		u.Conversation.EntityURN = linkedingo.NewURN(d)
		p.Metadata.DeletedURNs = append(p.Metadata.DeletedURNs, u)
	}
	return p
}

func cursorPageOf(next string, convs ...linkedingo.Conversation) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
	return &linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]{
		Metadata: linkedingo.ConversationCursorMetadata{NextCursor: next},
		Elements: convs,
	}
}

// topic is a realtime topic URN of the form LinkedIn sends.
func topic(name string) linkedingo.URN {
	return linkedingo.NewURN("urn:li-realtime:" + name + ":urn:li-realtime:myself")
}

func messageEvent(m linkedingo.Message) *linkedingo.DecoratedEvent {
	return &linkedingo.DecoratedEvent{
		Topic: topic(linkedingo.RealtimeEventTopicMessages),
		Payload: linkedingo.DecoratedEventPayload{Data: linkedingo.DecoratedEventData{
			DecoratedMessage: &linkedingo.DecoratedMessage{Result: m},
		}},
	}
}

func sortedNames(calls []call) []string {
	var out []string
	for _, c := range calls {
		out = append(out, c.name)
	}
	sort.Strings(out)
	return out
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func joined(list []string) string { return strings.Join(list, ", ") }
