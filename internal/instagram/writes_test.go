package instagram

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every ID, key and text in these tests is invented.

// signedIn is a harness signed in, its socket connected (catchingUp).
func signedIn(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	return h
}

// send sends m (to Alice's chat when it names none) and waits for the send
// to settle.
func (h *harness) send(m network.Outgoing) string {
	h.t.Helper()
	if m.ChatID == "" {
		m.ChatID = aliceChat
	}
	id, err := h.g.Send(context.Background(), m)
	if err != nil {
		h.t.Fatalf("Send: %v", err)
	}
	h.settle()
	return id
}

func (h *harness) outgoing(id string) store.Outgoing {
	h.t.Helper()
	o, ok, err := h.st.Outgoing(Name, id)
	if err != nil || !ok {
		h.t.Fatalf("outgoing %s: ok %v, err %v", id, ok, err)
	}
	return o
}

// incoming stores a line from Alice in her chat.
func (h *harness) incoming(id string, at time.Time) {
	h.t.Helper()
	if err := h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: textMsg(id, aliceFBI, at, "invented "+id)})); err != nil {
		h.t.Fatal(err)
	}
	if _, ok := h.lines(aliceChat)[id]; !ok {
		h.t.Fatalf("line %s was not stored", id)
	}
}

// echo is his message as the socket brings it back.
func echo(id, otid, text string, at time.Time) *slidetypes.Delta {
	m := textMsg(id, selfFBID, at, text)
	m.OfflineThreadingID = otid
	return delta(aliceThread, &slidetypes.NewMessageEvent{Message: m})
}

func otidOf(t *testing.T, id string) string {
	t.Helper()
	n, ok := strings.CutPrefix(id, "otid:")
	if !ok || n == "" {
		t.Fatalf("send ID %q is not an offline threading ID", id)
	}
	return n
}

// writes are the names of the write requests (and the reads they need)
// made since the n-th call.
func (m *fakeMeta) namesSince(n int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, c := range m.calls[n:] {
		switch c.name {
		case "FetchThreadID", "Upload", "SendMessage", "SendMedia", "SendReaction", "MarkRead", "MarkReadValidation",
			"MuteThread", "Download", "PaginateMessages":
			out = append(out, c.name)
		}
	}
	return out
}

func (m *fakeMeta) mark() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// serveIG runs the helper's server over an Instagram on fake Meta m, and is
// a function that asks it one request and gives the answer.
func serveIG(t *testing.T, m *fakeMeta, clock *fakeClock) (ask func(method string, params map[string]any) map[string]any, ig func() *Instagram) {
	t.Helper()
	var gmu sync.Mutex
	var g *Instagram
	dir := filepath.Join(t.TempDir(), "connect")
	deps := server.Deps{
		Dir:  dir,
		Open: func(key [32]byte) (*store.Store, error) { return store.Open(dir, key) },
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			gmu.Lock()
			defer gmu.Unlock()
			g = New(st, dir, key, emit.For(Name))
			g.clock = clock
			g.newClient = m.newClient
			return []network.Network{g}
		},
		Now: clock.Now,
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	served := make(chan error, 1)
	go func() { served <- server.Serve(context.Background(), inR, outW, deps); outW.Close() }()
	lines := make(chan map[string]any, 512)
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			var v map[string]any
			if json.Unmarshal(sc.Bytes(), &v) == nil {
				lines <- v
			}
		}
		close(lines)
	}()
	t.Cleanup(func() {
		inW.Close()
		select {
		case <-served:
		case <-time.After(6 * time.Second):
			t.Errorf("Serve did not return at end of input")
		}
		for range lines {
		}
	})
	answers := map[string]map[string]any{}
	n := 0
	ask = func(method string, params map[string]any) map[string]any {
		t.Helper()
		n++
		id := "r" + strings.Repeat("x", n)
		params["network"] = Name
		b, _ := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
		if _, err := inW.Write(append(b, '\n')); err != nil {
			t.Fatalf("writing %s: %v", method, err)
		}
		deadline := time.After(10 * time.Second)
		for {
			if a, ok := answers[id]; ok {
				return a
			}
			select {
			case v, ok := <-lines:
				if !ok {
					t.Fatalf("the output ended before %s was answered", method)
				}
				if rid, _ := v["id"].(string); rid != "" {
					answers[rid] = v
				}
			case <-deadline:
				t.Fatalf("no answer to %s", method)
			}
		}
	}
	ask("hello", map[string]any{"protocol": 1, "storeKey": hex.EncodeToString(testKey[:]), "app": "0.0.1-test"})
	delete(answers, "rx")
	return ask, func() *Instagram {
		gmu.Lock()
		defer gmu.Unlock()
		return g
	}
}

// Review Focus 3: a send answers at once with an ID of its own making (the
// offline threading ID), and sendStatus then reports it sent with lineID,
// Meta's message ID, which is the ID the line is stored under.
func TestSendReturnsAThreadingIDAndSettlesToMetasID(t *testing.T) {
	clock := newFakeClock()
	m := newFakeMeta(clock)
	ask, ig := serveIG(t, m, clock)
	session := map[string]any{}
	for k, v := range validSession() {
		session[k] = v
	}
	ask("login", map[string]any{"method": "session", "session": session})
	eventually(t, "the socket to connect", func() bool { return m.count("Connect") > 0 })
	eventually(t, "the backfill", func() bool { return ig().busy.Load() == 0 })
	_ = m.dispatch(&slidetypes.Connected{})

	a := ask("send", map[string]any{"chatID": aliceChat, "text": "invented hello", "clientID": "key-1"})
	res, _ := a["result"].(map[string]any)
	id, _ := res["messageID"].(string)
	otid := otidOf(t, id)
	eventually(t, "the send to settle", func() bool { return ig().busy.Load() == 0 })

	want := sentID(otid)
	for _, q := range []map[string]any{{"messageID": id}, {"clientID": "key-1"}} {
		st, _ := ask("sendStatus", q)["result"].(map[string]any)
		if st["state"] != "sent" || st["lineID"] != want {
			t.Errorf("sendStatus %v = %v, want sent with lineID %s", q, st, want)
		}
	}
	if args := m.argsOf("SendMessage"); len(args) != 1 || !strings.HasPrefix(args[0], otid+" "+aliceThread+" ") {
		t.Errorf("SendMessage was asked %v, want once, with the threading ID and Alice's thread", args)
	}

	// The echo carrying the threading ID is the same line, not a second one.
	_ = m.dispatch(echo(want, otid, "invented hello", clock.Now()))
	page, _ := ask("messages", map[string]any{"chatID": aliceChat, "limit": 50})["result"].(map[string]any)
	items, _ := page["items"].([]any)
	var mine []string
	for _, it := range items {
		if l, _ := it.(map[string]any); l["fromMe"] == true {
			mine = append(mine, l["id"].(string))
		}
	}
	if !slices.Equal(mine, []string{want}) {
		t.Errorf("his lines are %v, want only %s", mine, want)
	}

	// The same key again (its answer was lost) is the same send, sent once.
	again, _ := ask("send", map[string]any{"chatID": aliceChat, "text": "invented hello", "clientID": "key-1"})["result"].(map[string]any)
	if again["messageID"] != id {
		t.Errorf("a repeated send gave %v, want %s", again["messageID"], id)
	}
	eventually(t, "nothing more", func() bool { return ig().busy.Load() == 0 })
	if n := m.count("SendMessage"); n != 1 {
		t.Errorf("SendMessage was asked %d times, want 1", n)
	}
}

// cutOff sends once with SendMessage timing out, then restarts on the same
// store over the same Meta, signed in again.
func cutOff(t *testing.T) (h *harness, id string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "connect")
	h = openHarness(t, dir, nil)
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	h.m.setErr("SendMessage", context.DeadlineExceeded)
	id = h.send(network.Outgoing{Text: "invented cut off", ClientID: "key-cut"})
	if st := h.g.SendState(id); st.State != network.SendSending {
		t.Fatalf("before the restart the send is %+v, want sending", st)
	}
	h.close()

	m := h.m
	m.setErr("SendMessage", nil)
	h = openHarness(t, dir, m)
	return h, id
}

// Review Focus 2: a send that may have reached Meta when the helper stopped
// is never sent again, by itself or by the same key asked again.
func TestAMetaSendCutOffByARestartIsNotSentAgain(t *testing.T) {
	h, id := cutOff(t)
	if st := h.g.SendState(id); st.State != network.SendUnknown {
		t.Errorf("after the restart the send is %+v, want unknown", st)
	}
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	h.g.Nudge()
	h.clock.Advance(10 * time.Minute)
	h.settle()
	again, err := h.g.Send(context.Background(), network.Outgoing{ChatID: aliceChat, Text: "invented cut off", ClientID: "key-cut"})
	if err != nil || again != id {
		t.Errorf("the key asked again gave %q, %v; want %s", again, err, id)
	}
	h.settle()
	if n := h.m.count("SendMessage"); n != 1 {
		t.Errorf("SendMessage was asked %d times, want 1", n)
	}
	if st := h.g.SendState(id); st.State != network.SendUnknown {
		t.Errorf("the send is %+v, want still unknown", st)
	}
}

func TestALateEchoStillSettlesIt(t *testing.T) {
	h, id := cutOff(t)
	h.login(validSession())
	_ = h.m.dispatch(&slidetypes.Connected{})
	const late = "mid.$invented-late-1"
	if err := h.m.dispatch(echo(late, otidOf(t, id), "invented cut off", h.clock.Now())); err != nil {
		t.Fatal(err)
	}
	if st := h.g.SendState(id); st.State != network.SendSent {
		t.Errorf("after the echo the send is %+v, want sent", st)
	}
	if o := h.outgoing(id); o.LineID != late {
		t.Errorf("lineID = %q, want %s", o.LineID, late)
	}
	if l, ok := h.lines(aliceChat)[late]; !ok || !l.FromMe {
		t.Errorf("the echo's line is %+v (held %v), want his", l, ok)
	}
}

// writePNG writes a small invented photo and is its path.
func writePNG(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "invented.png")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := png.Encode(f, image.NewRGBA(image.Rect(0, 0, 4, 3))); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return path
}

func TestTextWithAFileIsTwoSendsTextFirst(t *testing.T) {
	h := signedIn(t)
	from := h.m.mark()
	id := h.send(network.Outgoing{Text: "invented caption", File: writePNG(t), ClientID: "key-file"})
	otid := otidOf(t, id)

	got := h.m.namesSince(from)
	if want := []string{"FetchThreadID", "Upload", "SendMessage", "SendMedia"}; !slices.Equal(got, want) {
		t.Errorf("requests %v, want %v (the upload first, so a failed one sends nothing; then the text, then the photo)", got, want)
	}
	text := h.m.argsOf("SendMessage")
	media := h.m.argsOf("SendMedia")
	if len(text) != 1 || len(media) != 1 {
		t.Fatalf("SendMessage %v, SendMedia %v; want one each", text, media)
	}
	textOTID, _, _ := strings.Cut(text[0], " ")
	if textOTID == otid || !strings.HasSuffix(text[0], "invented caption") {
		t.Errorf("the text went as %q: want its own threading ID and the text", text[0])
	}
	if want := otid + " " + aliceThread + " 5550000000000001 "; media[0] != want {
		t.Errorf("the photo went as %q, want %q", media[0], want)
	}
	gaps(t, h.m.requests())

	o := h.outgoing(id)
	if o.State != store.OutSent || o.LineID != sentID(otid) {
		t.Errorf("the send is %s with lineID %q, want sent with the photo's %s", o.State, o.LineID, sentID(otid))
	}
	lines := h.lines(aliceChat)
	tl, tok := lines[sentID(textOTID)]
	pl, pok := lines[sentID(otid)]
	if !tok || !pok || !tl.FromMe || !pl.FromMe || tl.Text != "invented caption" || pl.Kind != "photo" {
		t.Fatalf("the two lines are %+v (%v) and %+v (%v)", tl, tok, pl, pok)
	}
	if tl.TimeMs >= pl.TimeMs {
		t.Errorf("the text (%d) is not before the photo (%d)", tl.TimeMs, pl.TimeMs)
	}
	// His own photo opens without a download.
	if _, err := h.g.Media(context.Background(), aliceChat, sentID(otid)); err != nil {
		t.Errorf("Media of his photo = %v", err)
	}
	if n := h.m.count("Download"); n != 0 {
		t.Errorf("his own photo was downloaded %d times", n)
	}
}

func TestAFailureBeforeMetaIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fail     string
		err      error
		file     bool
		sendsOut int
	}{
		{"the thread's ID cannot be fetched", "FetchThreadID", errors.New("invented failure"), false, 0},
		{"the upload fails", "Upload", errors.New("invented failure"), true, 0},
		{"Meta turns the send down", "SendMessage", &types.ErrorResponse{ErrorCode: 1545999, ErrorSummary: "invented"}, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := signedIn(t)
			h.m.setErr(tc.fail, tc.err)
			out := network.Outgoing{Text: "invented"}
			if tc.file {
				out.File = writePNG(t)
			}
			id := h.send(out)
			if st := h.g.SendState(id); st.State != network.SendFailed || st.Kind != wire.Refused {
				t.Errorf("the send is %+v, want failed refused", st)
			}
			if n := h.m.count("SendMessage") + h.m.count("SendMedia"); n != tc.sendsOut {
				t.Errorf("%d sends reached Meta, want %d", n, tc.sendsOut)
			}
		})
	}
}

func TestATimeoutStaysSending(t *testing.T) {
	h := signedIn(t)
	h.m.setErr("SendMessage", context.DeadlineExceeded)
	id := h.send(network.Outgoing{Text: "invented slow"})
	h.m.setErr("SendMessage", nil)
	_ = h.m.dispatch(&slidetypes.Connected{})
	h.g.Nudge()
	h.clock.Advance(time.Hour)
	h.settle()
	if st := h.g.SendState(id); st.State != network.SendSending {
		t.Errorf("the send is %+v, want sending", st)
	}
	if n := h.m.count("SendMessage"); n != 1 {
		t.Errorf("SendMessage was asked %d times, want 1 (a write never tries itself again)", n)
	}
}

// Review Focus 1: once Meta asks him to confirm it is him, no write (nor
// the read a write needs, nor a media fetch) asks Meta anything.
func TestACheckpointRefusesEveryWrite(t *testing.T) {
	h := signedIn(t)
	h.incoming("mid.$invented-in1", start)
	if err := h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: photoMsg("mid.$invented-p1", aliceFBI, start.Add(time.Second), "https://cdn.invalid/p1.jpg")})); err != nil {
		t.Fatal(err)
	}
	h.m.setErr("FetchThreadID", httpclient.ErrCheckpointRequired)
	id := h.send(network.Outgoing{Text: "invented"})
	if st := h.g.SendState(id); st.State != network.SendFailed || st.Kind != wire.Refused {
		t.Errorf("the send is %+v, want failed refused", st)
	}
	if got := h.lastState(); got != "reconnectNeeded/checkpoint" {
		t.Fatalf("the state is %q, want reconnectNeeded/checkpoint", got)
	}
	from := h.m.mark()
	ctx := context.Background()
	_, err := h.g.Send(ctx, network.Outgoing{ChatID: aliceChat, Text: "invented"})
	checks := map[string]error{
		"Send":     err,
		"React":    h.g.React(ctx, aliceChat, "mid.$invented-in1", "👍", "txn-cp"),
		"MarkRead": h.g.MarkRead(ctx, aliceChat, ""),
		"SetMuted": h.g.SetMuted(ctx, aliceChat, true),
	}
	_, checks["Media"] = h.g.Media(ctx, aliceChat, "mid.$invented-p1")
	for name, err := range checks {
		if kindErr(err) != wire.Checkpoint {
			t.Errorf("%s = %v, want checkpoint", name, err)
		}
	}
	// Marking unread is kept here only, so it may go on.
	if err := h.g.MarkUnread(ctx, aliceChat); err != nil {
		t.Errorf("MarkUnread = %v", err)
	}
	h.settle()
	if got := h.m.namesSince(from); len(got) != 0 {
		t.Errorf("requests went after the checkpoint: %v", got)
	}
}

func TestReactAndRemove(t *testing.T) {
	h := signedIn(t)
	const target = "mid.$invented-r1"
	h.incoming(target, start)
	ctx := context.Background()
	// Removing a reaction he does not have asks Meta nothing at all.
	from := h.m.mark()
	if err := h.g.React(ctx, aliceChat, target, "", "txn-0"); err != nil {
		t.Fatalf("removing none = %v", err)
	}
	if got := h.m.namesSince(from); len(got) != 0 {
		t.Errorf("removing a reaction he does not have asked %v", got)
	}
	if err := h.g.React(ctx, aliceChat, target, "❤️", "txn-1"); err != nil {
		t.Fatalf("React = %v", err)
	}
	if err := h.g.React(ctx, aliceChat, target, "", "txn-2"); err != nil {
		t.Fatalf("removing = %v", err)
	}
	want := []string{"created ❤️ " + target + " " + aliceThread, "deleted ❤️ " + target + " " + aliceThread}
	if got := h.m.argsOf("SendReaction"); !slices.Equal(got, want) {
		t.Errorf("SendReaction %v, want %v", got, want)
	}
	if r := h.lines(aliceChat)[target].Reactions; len(r) != 0 {
		t.Errorf("after the removal the line holds %+v", r)
	}
}

func TestReactionIsStoredAsHis(t *testing.T) {
	h := signedIn(t)
	const target = "mid.$invented-r2"
	h.incoming(target, start)
	if err := h.g.React(context.Background(), aliceChat, target, "😂", "txn-1"); err != nil {
		t.Fatal(err)
	}
	r := h.lines(aliceChat)[target].Reactions
	if len(r) != 1 || r[0].Emoji != "😂" || !r[0].FromMe {
		t.Errorf("the line holds %+v, want his 😂", r)
	}
}

func TestARepeatedTransactionSendsNothing(t *testing.T) {
	h := signedIn(t)
	const target = "mid.$invented-r1"
	h.incoming(target, start)
	for range 2 {
		if err := h.g.React(context.Background(), aliceChat, target, "👍", "txn-same"); err != nil {
			t.Fatalf("React = %v", err)
		}
	}
	if n := h.m.count("SendReaction"); n != 1 {
		t.Errorf("SendReaction was asked %d times, want 1", n)
	}
}

func TestReactingToABeeperLineIsNotOnNetwork(t *testing.T) {
	h := signedIn(t)
	from := h.m.mark()
	err := h.g.React(context.Background(), aliceChat, "beeper:4242", "👍", "txn-b")
	if kindErr(err) != wire.NotOnNetwork {
		t.Errorf("React = %v, want notOnNetwork", err)
	}
	if got := h.m.namesSince(from); len(got) != 0 {
		t.Errorf("requests went: %v", got)
	}
}

func TestMarkReadStopsAtThrough(t *testing.T) {
	h := signedIn(t)
	for i, id := range []string{"mid.$invented-a1", "mid.$invented-a2", "mid.$invented-a3"} {
		h.incoming(id, start.Add(time.Duration(i+1)*time.Second))
	}
	ctx := context.Background()
	if err := h.g.MarkRead(ctx, aliceChat, "mid.$invented-a2"); err != nil {
		t.Fatalf("MarkRead = %v", err)
	}
	if got, want := h.m.argsOf("MarkRead"), []string{"mid.$invented-a2 " + aliceLong + " item="}; !slices.Equal(got, want) {
		t.Errorf("MarkRead %v, want %v", got, want)
	}
	a2 := start.Add(2 * time.Second).UnixMilli()
	if got, want := h.m.argsOf("MarkReadValidation"), []string{"mid.$invented-a2 " + aliceLong + " " + itoa(a2)}; !slices.Equal(got, want) {
		t.Errorf("MarkReadValidation %v, want %v", got, want)
	}
	if u := h.chat(aliceChat).Unread; u != 1 {
		t.Errorf("unread = %d, want 1 (the newer line stays unread)", u)
	}
	if err := h.g.MarkRead(ctx, aliceChat, ""); err != nil {
		t.Fatalf("MarkRead to the newest = %v", err)
	}
	if got := h.m.argsOf("MarkRead"); len(got) != 2 || !strings.HasPrefix(got[1], "mid.$invented-a3 ") {
		t.Errorf("MarkRead %v, want the newest second", got)
	}
	if u := h.chat(aliceChat).Unread; u != 0 {
		t.Errorf("unread = %d, want 0", u)
	}
	if n := h.m.count("FetchThreadID"); n != 1 {
		t.Errorf("FetchThreadID was asked %d times, want once for the chat", n)
	}
}

func TestMarkUnreadSendsNothing(t *testing.T) {
	h := signedIn(t)
	h.incoming("mid.$invented-u1", start)
	ctx := context.Background()
	if err := h.g.MarkRead(ctx, aliceChat, ""); err != nil {
		t.Fatal(err)
	}
	from := h.m.mark()
	if err := h.g.MarkUnread(ctx, aliceChat); err != nil {
		t.Fatalf("MarkUnread = %v", err)
	}
	h.settle()
	if got := h.m.namesSince(from); len(got) != 0 {
		t.Errorf("requests went: %v", got)
	}
	if u := h.chat(aliceChat).Unread; u != 1 {
		t.Errorf("unread = %d, want 1", u)
	}
}

func TestMuteForeverAndUnmute(t *testing.T) {
	h := signedIn(t)
	ctx := context.Background()
	if err := h.g.SetMuted(ctx, aliceChat, true); err != nil {
		t.Fatal(err)
	}
	if !h.chat(aliceChat).Muted {
		t.Errorf("the chat is not muted")
	}
	if err := h.g.SetMuted(ctx, aliceChat, false); err != nil {
		t.Fatal(err)
	}
	if h.chat(aliceChat).Muted {
		t.Errorf("the chat is still muted")
	}
	got := h.m.argsOf("MuteThread")
	if len(got) != 2 || !strings.HasPrefix(got[0], aliceThread+" -1 otid=") || !strings.HasPrefix(got[1], aliceThread+" 0 otid=") {
		t.Errorf("MuteThread %v, want -1 then 0 on Alice's thread", got)
	}
}

// The adapter can make exactly the writes the global constraints allow,
// and nothing else: igWriter is its only way to write, and igClient (the
// reads) and igDownloader hold no write.
func TestOnlyTheAllowedWritesExist(t *testing.T) {
	names := func(v any) []string {
		typ := reflect.TypeOf(v).Elem()
		var out []string
		for i := range typ.NumMethod() {
			out = append(out, typ.Method(i).Name)
		}
		sort.Strings(out)
		return out
	}
	want := []string{"MarkRead", "MarkReadValidation", "MuteThread", "SendMedia", "SendMessage", "SendReaction", "Upload"}
	if got := names((*igWriter)(nil)); !slices.Equal(got, want) {
		t.Errorf("igWriter has %v, want exactly %v", got, want)
	}
	if got := names((*igDownloader)(nil)); !slices.Equal(got, []string{"Download"}) {
		t.Errorf("igDownloader has %v, want only Download", got)
	}
	for _, m := range names((*igClient)(nil)) {
		for _, w := range []string{"Send", "Mark", "Mute", "Upload", "Edit", "Unsend", "Delete", "Pin", "Typing", "Block", "Leave", "Add", "Remove", "Accept", "Register"} {
			if strings.HasPrefix(m, w) {
				t.Errorf("igClient holds a write: %s", m)
			}
		}
	}
}
