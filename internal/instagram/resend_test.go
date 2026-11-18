package instagram

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Fix round 1 (task-A3-review.md I1-I3, M2). Every value here is invented.

// I1: the library must never POST a send again by itself. With NoRetry it
// asks once (MakeRequestOnce) instead of up to six times.
func TestTheSendMutationsAreAskedOnce(t *testing.T) {
	for _, name := range []string{"IGDirectTextSendMutation", "IGDirectMediaSendMutation"} {
		doc, ok := graphql.GraphQLDocs[name]
		if !ok {
			t.Fatalf("no GraphQL doc %s", name)
		}
		if !doc.NoRetry {
			t.Errorf("%s may be retried by the library", name)
		}
		if doc.DocID == "" || doc.FriendlyName != name {
			t.Errorf("%s lost its other fields: %+v", name, doc)
		}
	}
}

// I1: what a failed send may have done. "refused" is safe to send again, so
// only what certainly never reached Meta, or what Meta said no to, is.
func TestWhatMayHaveReachedMeta(t *testing.T) {
	requestFailed := func(op string, err error) error {
		return fmt.Errorf("%w: %w", httpclient.ErrRequestFailed,
			&url.Error{Op: "Post", URL: "https://meta.invalid/api/graphql/", Err: &net.OpError{Op: op, Net: "tcp", Err: err}})
	}
	status := func(code int) error { return fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, code) }
	for _, tc := range []struct {
		name  string
		err   error
		maybe bool
	}{
		{"a refused connection (dial)", requestFailed("dial", syscall.ECONNREFUSED), false},
		{"no route to Meta (dial)", requestFailed("dial", &net.DNSError{Err: "no such host", Name: "meta.invalid"}), false},
		{"a dial that timed out", requestFailed("dial", context.DeadlineExceeded), false},
		{"a connection reset after the body went", requestFailed("read", syscall.ECONNRESET), true},
		{"a write that broke off", requestFailed("write", syscall.EPIPE), true},
		{"a 500", status(500), true},
		{"a 503", status(503), true},
		{"a 502 after the library's retries", fmt.Errorf("%w: %w", httpclient.ErrMaxRetriesReached, status(502)), true},
		{"a 400", status(400), false},
		{"a 404", status(404), false},
		{"a 429", httpclient.ErrRateLimited, false},
		{"Meta's error answer", &types.ErrorResponse{ErrorCode: 1545999, ErrorSummary: "invented"}, false},
		{"Meta's GraphQL error", errors.Join(&types.GraphQLError{Message: "invented", Code: 1545999}), false},
		{"a checkpoint", httpclient.ErrCheckpointRequired, false},
		{"a suspended account", httpclient.ErrAccountSuspended, false},
		{"no answer in time", context.DeadlineExceeded, true},
		{"the helper stopping", context.Canceled, true},
		{"an answer it could not read", fmt.Errorf("%w: %w", httpclient.ErrResponseReadFailed, errors.New("invented")), true},
		{"an answer it does not understand", errors.New("failed to unmarshal invented"), true},
	} {
		if got := unsure(tc.err); got != tc.maybe {
			t.Errorf("%s: may have gone = %v, want %v", tc.name, got, tc.maybe)
		}
	}
}

// I2: once the text of a text-and-file send went, sending it again (the
// same key: Try again) sends only the file.
func TestATextWithAFileWhoseFileFailsIsNotResentAsText(t *testing.T) {
	h := signedIn(t)
	h.m.setErr("SendMedia", &types.ErrorResponse{ErrorCode: 1545999, ErrorSummary: "invented"})
	out := network.Outgoing{ChatID: aliceChat, Text: "invented caption", File: writePNG(t), ClientID: "key-tf"}
	id := h.send(out)
	if st := h.g.SendState(id); st.State != network.SendFailed {
		t.Fatalf("after the file failed the send is %+v, want failed", st)
	}
	h.m.setErr("SendMedia", nil)
	again, err := h.g.Send(context.Background(), out)
	if err != nil || again != id {
		t.Fatalf("Try again gave %q, %v; want the same send %s", again, err, id)
	}
	h.settle()
	if n := h.m.count("SendMessage"); n != 1 {
		t.Errorf("the text was sent %d times, want once", n)
	}
	if n := h.m.count("SendMedia"); n != 2 {
		t.Errorf("SendMedia was asked %d times, want 2 (the failed one, then the file alone)", n)
	}
	if st := h.g.SendState(id); st.State != network.SendSent {
		t.Errorf("after Try again the send is %+v, want sent", st)
	}
	otid := otidOf(t, id)
	if o := h.outgoing(id); o.LineID != sentID(otid) {
		t.Errorf("lineID = %q, want the file's %s", o.LineID, sentID(otid))
	}
	texts := 0
	for _, l := range h.lines(aliceChat) {
		if l.FromMe && l.Text == "invented caption" {
			texts++
		}
	}
	if texts != 1 {
		t.Errorf("his text shows %d times, want once", texts)
	}
}

// fakeOnce stands in for the client's MakeRequestOnce: one scripted answer,
// and every request counted.
type fakeOnce struct {
	calls   int
	method  string
	url     string
	ctype   string
	payload []byte
	status  int
	body    string
	err     error
}

func (f *fakeOnce) MakeRequestOnce(ctx context.Context, url, method string, headers http.Header, payload []byte, contentType types.ContentType) (*http.Response, []byte, error) {
	f.calls++
	f.method, f.url, f.ctype, f.payload = method, url, headers.Get("content-type"), payload
	if f.err != nil {
		return nil, nil, f.err
	}
	return &http.Response{StatusCode: f.status}, []byte(f.body), nil
}

// I3: an upload is one request, whatever Meta answers, and its answer
// becomes a fixed kind (a rate limit one the pace backs off on).
func TestAnUploadIsAskedOnce(t *testing.T) {
	media := &httpclient.MercuryUploadMedia{Filename: "invented.png", MimeType: "image/png", MediaData: []byte("invented bytes")}
	const at = "https://upload.invalid/ajax/mercury/upload.php?invented=1"
	for _, tc := range []struct {
		name string
		f    *fakeOnce
		kind wire.Kind
	}{
		{"Meta's error answer", &fakeOnce{status: 200, body: `for (;;);{"error":1545999,"errorSummary":"invented","errorDescription":"invented"}`}, wire.Refused},
		{"Meta asks him to sign in again", &fakeOnce{status: 200, body: `for (;;);{"error":1357001,"errorSummary":"invented"}`}, meta.ReconnectNeeded},
		{"a consent page", &fakeOnce{status: 200, body: `for (;;);{"redirect":"https://www.instagram.invalid/consent/?x=1"}`}, wire.Checkpoint},
		{"an answer with no file", &fakeOnce{status: 200, body: `for (;;);{"payload":{}}`}, wire.Refused},
		{"a 429", &fakeOnce{err: httpclient.ErrRateLimited}, wire.RateLimited},
		{"a 500", &fakeOnce{err: fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 500)}, wire.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := meta.UploadOnce(context.Background(), tc.f, at, http.Header{}, media)
			if tc.f.calls != 1 {
				t.Errorf("%d requests, want exactly 1", tc.f.calls)
			}
			if err == nil || resp != nil {
				t.Fatalf("uploadOnce = %+v, %v; want an error", resp, err)
			}
			if got := kindOf(err); got != tc.kind {
				t.Errorf("kind %q, want %q", got, tc.kind)
			}
		})
	}

	t.Run("a photo", func(t *testing.T) {
		f := &fakeOnce{status: 200, body: `for (;;);{"payload":{"uploadID":"invented","metadata":[{"image_id":5550000000000002,"filename":"invented.png"}]}}`}
		resp, err := meta.UploadOnce(context.Background(), f, at, http.Header{}, media)
		if err != nil || f.calls != 1 {
			t.Fatalf("uploadOnce = %v after %d requests", err, f.calls)
		}
		if resp.Payload.RealMetadata == nil || resp.Payload.RealMetadata.GetFbId() != 5550000000000002 {
			t.Errorf("the attachment is %+v", resp.Payload.RealMetadata)
		}
		if f.method != http.MethodPost || f.url != at || !strings.HasPrefix(f.ctype, "multipart/form-data; boundary=----WebKitFormBoundary") {
			t.Errorf("asked %s %s with %q", f.method, f.url, f.ctype)
		}
		if !bytes.Contains(f.payload, []byte(`name="farr"; filename="invented.png"`)) || !bytes.Contains(f.payload, media.MediaData) {
			t.Errorf("the body is not the library's multipart form: %q", f.payload)
		}
	})
}

// M2: a failed reaction tried again sends the emoji he chose this time.
func TestARetriedReactionSendsTheNewEmoji(t *testing.T) {
	h := signedIn(t)
	const target = "mid.$invented-r3"
	h.incoming(target, start)
	h.m.setErr("SendReaction", &types.ErrorResponse{ErrorCode: 1545999, ErrorSummary: "invented"})
	if err := h.g.React(context.Background(), aliceChat, target, "👍", "txn-again"); err == nil {
		t.Fatalf("the refused reaction answered nil")
	}
	h.m.setErr("SendReaction", nil)
	if err := h.g.React(context.Background(), aliceChat, target, "❤️", "txn-again"); err != nil {
		t.Fatalf("React again = %v", err)
	}
	got := h.m.argsOf("SendReaction")
	if len(got) != 2 || !strings.HasPrefix(got[1], "created ❤️ ") {
		t.Errorf("SendReaction %v, want the heart second", got)
	}
	if r := h.lines(aliceChat)[target].Reactions; len(r) != 1 || r[0].Emoji != "❤️" {
		t.Errorf("the line holds %+v, want his heart", r)
	}
}
