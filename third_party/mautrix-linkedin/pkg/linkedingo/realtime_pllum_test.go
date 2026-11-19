// Pllum Connect's tests of its heartbeat change (PLLUM-CHANGES.md, change
// 3). Every request goes to a fake round-tripper: nothing leaves the Mac, and
// every URN and cookie here is invented.

package linkedingo

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// heartbeatTransport answers the realtime connect with a stream that stays
// open until its request ends, and every heartbeat with status.
type heartbeatTransport struct {
	status int
}

// openStream is a response body that gives nothing until ctx ends.
type openStream struct{ ctx context.Context }

func (s openStream) Read([]byte) (int, error) {
	<-s.ctx.Done()
	return 0, s.ctx.Err()
}

func (s openStream) Close() error { return nil }

func (t *heartbeatTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}
	if req.Method == http.MethodGet && strings.HasPrefix(req.URL.String(), linkedInRealtimeConnectURL) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {contentTypeTextEventStream}},
			Body:       openStream{ctx: req.Context()},
			Request:    req,
		}, nil
	}
	h := http.Header{"Content-Type": {"application/json"}}
	if t.status == http.StatusFound {
		h.Set("Location", "https://www.linkedin.com/checkpoint/invented")
	}
	return &http.Response{
		StatusCode: t.status,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(`{}`)),
		Request:    req,
	}, nil
}

// pllumRealtimeClient is a client whose heartbeats LinkedIn answers with
// status, and the errors its stream drops with.
func pllumRealtimeClient(t *testing.T, status int) (*Client, chan error) {
	t.Helper()
	cli, _ := pllumTestClient(t, "")
	dropped := make(chan error, 8)
	cli.handlers.TransientDisconnect = func(_ context.Context, err error) { dropped <- err }
	cli.http.Transport = &heartbeatTransport{status: status}
	return cli, dropped
}

func TestPllumAHeartbeatPushedBackStopsTheStream(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests, 999, http.StatusFound} {
		cli, dropped := pllumRealtimeClient(t, status)
		if err := cli.RealtimeConnect(context.Background()); err != nil {
			t.Fatalf("RealtimeConnect: %v", err)
		}
		select {
		case err := <-dropped:
			var re *ResponseError
			if !errors.As(err, &re) || re.StatusCode != status {
				t.Errorf("a heartbeat answered %d dropped the stream with %v, want a *ResponseError with %d", status, err, status)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("a heartbeat answered %d never dropped the stream", status)
		}
		cli.RealtimeDisconnect()
	}
}

func TestPllumAHeartbeatAnsweredKeepsTheStream(t *testing.T) {
	cli, dropped := pllumRealtimeClient(t, http.StatusOK)
	if err := cli.RealtimeConnect(context.Background()); err != nil {
		t.Fatalf("RealtimeConnect: %v", err)
	}
	select {
	case err := <-dropped:
		t.Errorf("a heartbeat answered 200 dropped the stream with %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	cli.RealtimeDisconnect()
}
