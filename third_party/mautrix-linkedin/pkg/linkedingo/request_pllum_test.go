// Pllum Connect's tests of its one-attempt send (PLLUM-CHANGES.md, change
// 2). Every request goes to a counting round-tripper: nothing leaves the
// Mac, and every URN, cookie and text here is invented.

package linkedingo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// countingTransport answers each request with the next of its answers (the
// last one again once they run out) and counts the requests. A nil answer
// is the network failing.
type countingTransport struct {
	mu      sync.Mutex
	n       int
	answers []*countedAnswer
}

type countedAnswer struct {
	status int
	body   string
}

var errInventedNetwork = errors.New("invented network failure")

func (t *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	i := min(t.n, len(t.answers)-1)
	t.n++
	a := t.answers[i]
	t.mu.Unlock()
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}
	if a == nil {
		return nil, errInventedNetwork
	}
	return &http.Response{
		StatusCode: a.status,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(a.body)),
		Request:    req,
	}, nil
}

func (t *countingTransport) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n
}

// pllumCountingClient is a client over a counting transport giving answers
// in turn, signed in as an invented profile with invented cookies.
func pllumCountingClient(t *testing.T, answers ...*countedAnswer) (*Client, *countingTransport) {
	t.Helper()
	cli, _ := pllumTestClient(t, "")
	rt := &countingTransport{answers: answers}
	cli.http.Transport = rt
	return cli, rt
}

// pllumSend sends an invented message to an invented conversation.
func pllumSend(cli *Client) error {
	conv := NewURN("urn:li:msg_conversation:(urn:li:fsd_profile:ACoAAInventedSelf,2-aW52ZW50ZWQ=)")
	_, err := cli.SendMessage(context.Background(), conv, SendMessageBody{Text: "invented text"}, nil, "origin-token-invented")
	return err
}

func TestPllumSendMessageIsOneAttemptOnANetworkError(t *testing.T) {
	cli, rt := pllumCountingClient(t, nil)
	began := time.Now()
	err := pllumSend(cli)
	took := time.Since(began)
	if err == nil {
		t.Fatal("SendMessage succeeded over a failing network")
	}
	if !errors.Is(err, errInventedNetwork) {
		t.Errorf("SendMessage's error is %v, want the network's", err)
	}
	if n := rt.count(); n != 1 {
		t.Errorf("SendMessage made %d requests, want 1", n)
	}
	if took >= time.Second {
		t.Errorf("SendMessage took %s, want under 1 s (no retry wait)", took)
	}
}

func TestPllumSendMessageIsOneAttemptOnA503(t *testing.T) {
	cli, rt := pllumCountingClient(t, &countedAnswer{status: http.StatusServiceUnavailable}, &countedAnswer{status: http.StatusOK, body: `{}`})
	began := time.Now()
	err := pllumSend(cli)
	took := time.Since(began)
	var re *ResponseError
	if !errors.As(err, &re) || re.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("SendMessage's error is %v, want a *ResponseError with 503", err)
	}
	if n := rt.count(); n != 1 {
		t.Errorf("SendMessage made %d requests, want 1", n)
	}
	if took >= time.Second {
		t.Errorf("SendMessage took %s, want under 1 s (no retry wait)", took)
	}
}

func TestPllumOtherRequestsStillRetry(t *testing.T) {
	profile := `{"miniProfile":{"firstName":"Invented","lastName":"Self","dashEntityUrn":"urn:li:fsd_profile:ACoAAInventedSelf"}}`
	cli, rt := pllumCountingClient(t, nil, &countedAnswer{status: http.StatusOK, body: profile})
	p, err := cli.GetCurrentUserProfile(context.Background())
	if err != nil {
		t.Fatalf("GetCurrentUserProfile: %v", err)
	}
	if p.MiniProfile.FirstName != "Invented" {
		t.Errorf("the profile read is %+v", p.MiniProfile)
	}
	if n := rt.count(); n != 2 {
		t.Errorf("GetCurrentUserProfile made %d requests, want 2 (one failed, one retried)", n)
	}
}
