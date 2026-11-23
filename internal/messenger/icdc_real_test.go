package messenger

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// Every token, ID and key here is invented, and no request leaves the
// machine: the client's transport is a fake.

// roundTrip is a fake transport.
type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// stateClient is a real messagix client holding invented page configs (as
// a synced client would), its requests answered by rt.
func stateClient(t *testing.T, mqttID string, rt roundTrip) realClient {
	// mqttID is the page's MQTT web device ID, which is never a device ID
	t.Helper()
	ck := &cookies.Cookies{Platform: types.Facebook}
	ck.UpdateValues(map[cookies.MetaCookieName]string{"c_user": "2000001", "xs": "xs-invented-1", "datr": "datr-invented-1"})
	cli := messagix.NewClient(ck, zerolog.Nop(), &messagix.Config{})
	state, err := json.Marshal(map[string]any{
		"Configs": map[string]any{"BrowserConfigTable": map[string]any{
			"MessengerWebInitData": map[string]any{"appId": 77001, "cryptoAuthToken": map[string]any{"encrypted_serialized_cat": "cat-invented-1"}},
			"MqttWebDeviceID":      map[string]any{"clientID": mqttID},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.LoadState(state); err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	cli.GetHTTP().SetTransportOverride(rt)
	return realClient{cli}
}

// The real client's FetchICDC posts the library's form to its ICDC
// endpoint, with no cookie, and reads the list and its seq.
func TestTheRealClientFetchesTheICDCList(t *testing.T) {
	a, b := inventedIdentity(0x31), inventedIdentity(0x32)
	devUUID := uuid.MustParse("0000000a-0000-4000-8000-000000000001")
	var got url.Values
	var path, cookie string
	c := stateClient(t, inventedSessionUUID.String(), func(r *http.Request) (*http.Response, error) {
		path, cookie = r.URL.Host+r.URL.Path, r.Header.Get("Cookie")
		body, _ := io.ReadAll(r.Body)
		got, _ = url.ParseQuery(string(body))
		resp := `{"device_identities":["` + base64.RawURLEncoding.EncodeToString(a[:]) + `","` + base64.RawURLEncoding.EncodeToString(b[:]) + `"],"icdc_seq":6,"status":200}`
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(resp)), Header: http.Header{}, Request: r}, nil
	})
	ids, seq, err := c.FetchICDC(context.Background(), 2000001, devUUID)
	if err != nil {
		t.Fatalf("FetchICDC: %v", err)
	}
	if len(ids) != 2 || ids[0] != a || ids[1] != b || seq != 6 {
		t.Errorf("FetchICDC gave %d identities and seq %d", len(ids), seq)
	}
	if path != "reg-e2ee.facebook.com/v2/fb_icdc_fetch" || cookie != "" {
		t.Errorf("the request went to %q with a cookie %v", path, cookie != "")
	}
	if got.Get("fbid") != "2000001" || got.Get("fb_cat") != "cat-invented-1" || got.Get("app_id") != "77001" || got.Get("device_id") != devUUID.String() {
		t.Errorf("the form was %v", got)
	}
	if id := c.SessionDeviceID(); id != uuid.Nil {
		t.Errorf("a web session's SessionDeviceID is %v, want none (the page's MQTT ID is not a device ID)", id)
	}
}

// A 429 is a rate limit the pace holds off for; another refusal, or an
// answer that is not a list, is an error.
func TestTheRealClientsICDCRefusals(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		limit  bool
	}{
		{429, `{}`, true},
		{500, `{}`, false},
		{200, `{"status":400}`, false},
		{200, `not json`, false},
		{200, `{"device_identities":["c2hvcnQ"],"status":200}`, false},
	} {
		c := stateClient(t, "not-a-uuid", func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: http.Header{}, Request: r}, nil
		})
		_, _, err := c.FetchICDC(context.Background(), 2000001, uuid.Nil)
		if err == nil || errors.Is(err, httpclient.ErrRateLimited) != tc.limit {
			t.Errorf("status %d %q: err %v", tc.status, tc.body, err)
		}
	}
}

// A client that has not loaded the page has no configs: nothing is asked.
func TestTheRealClientWithoutThePageAsksNothing(t *testing.T) {
	ck := &cookies.Cookies{Platform: types.Facebook}
	cli := messagix.NewClient(ck, zerolog.Nop(), &messagix.Config{})
	asked := false
	cli.GetHTTP().SetTransportOverride(roundTrip(func(r *http.Request) (*http.Response, error) {
		asked = true
		return nil, errors.New("invented: no network in tests")
	}))
	c := realClient{cli}
	if _, _, err := c.FetchICDC(context.Background(), 2000001, uuid.Nil); err == nil || asked {
		t.Errorf("FetchICDC without the page: err %v, asked %v", err, asked)
	}
	if id := c.SessionDeviceID(); id != uuid.Nil {
		t.Errorf("SessionDeviceID without the page is %v", id)
	}
}
