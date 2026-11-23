package messenger

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"
)

// The account's ICDC list, read the way messagix's RegisterE2EE reads it
// (its unexported fetchICDC and doE2EERequest, in e2ee-register.go of
// mautrix-meta v0.2610.0), from exported parts only: the page's configs
// come from DumpState (the state mautrix-meta's connector caches), the
// endpoint from GetEndpoint, the HTTP client from GetHTTP, and the answer
// is messagix's own ICDCFetchResponse. The request carries the account's
// FBID, the crypto auth token and app ID from the page, and a device ID,
// and no cookie, as the library's does; nothing of it, or of the answer,
// is logged or kept.

// errNoPageConfigs: the client has not loaded and synced the messages page
// yet, so its configs (the crypto auth token, the app ID, the page's device
// ID) are not there to read.
var errNoPageConfigs = errors.New("messenger: the page's configs are not loaded")

// pageConfigs are the configs the client took from the messages page, as
// DumpState gives them (it answers nothing before the main socket has
// synced once).
func (c realClient) pageConfigs() (*types.SchedulerJSDefineConfig, error) {
	raw, err := c.Client.DumpState()
	if err != nil || len(raw) == 0 {
		return nil, errNoPageConfigs
	}
	var state struct{ Configs *httpclient.Configs }
	if err := json.Unmarshal(raw, &state); err != nil || state.Configs == nil || state.Configs.BrowserConfigTable == nil {
		return nil, errNoPageConfigs
	}
	return state.Configs.BrowserConfigTable, nil
}

// FetchICDC is the account's ICDC list: every device identity in it, and
// its seq. A rate limit is httpclient.ErrRateLimited, so the pace holds
// off; any other answer that is not the list is an error with no detail.
func (c realClient) FetchICDC(ctx context.Context, fbid int64, deviceUUID uuid.UUID) ([][32]byte, int, error) {
	cfg, err := c.pageConfigs()
	if err != nil {
		return nil, 0, err
	}
	init := cfg.MessengerWebInitData
	if init.CryptoAuthToken.EncryptedSerializedCat == "" || init.AppID == 0 {
		return nil, 0, errNoPageConfigs
	}
	body := url.Values{
		"fbid":      {strconv.FormatInt(fbid, 10)},
		"fb_cat":    {init.CryptoAuthToken.EncryptedSerializedCat},
		"app_id":    {strconv.FormatInt(init.AppID, 10)},
		"device_id": {deviceUUID.String()},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Client.GetEndpoint("icdc_fetch"), strings.NewReader(body.Encode()))
	if err != nil {
		return nil, 0, errors.New("messenger: the ICDC request could not be made")
	}
	// the library's headers for it (doE2EERequest)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("accept-language", "en-US,en;q=0.9")
	req.Header.Set("User-Agent", useragent.UserAgent)
	req.Header.Set("sec-fetch-dest", "empty")
	req.Header.Set("sec-fetch-mode", "cors")
	req.Header.Set("sec-fetch-site", "cross-site")
	req.Header.Set("origin", c.Client.GetEndpoint("base_url"))
	req.Header.Set("referer", c.Client.GetEndpoint("messages")+"/")
	resp, err := c.Client.GetHTTP().GetHTTPClient().Do(req)
	if err != nil {
		return nil, 0, err // a network error: kindOf reads it as offline or a timeout
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, 0, httpclient.ErrRateLimited
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, 0, errors.New("messenger: the ICDC list was refused")
	case err != nil:
		return nil, 0, errors.New("messenger: the ICDC list was cut off")
	}
	var r messagix.ICDCFetchResponse
	if err := json.Unmarshal(data, &r); err != nil || (r.Status != 0 && r.Status != http.StatusOK) {
		return nil, 0, errors.New("messenger: the ICDC answer was not a list")
	}
	ids, err := r.DeviceIdentityBytes()
	if err != nil {
		return nil, 0, errors.New("messenger: the ICDC list held an identity that is not a key")
	}
	return ids, r.ICDCSeq, nil
}

// SessionDeviceID is the session's Facebook device ID, as mautrix-meta's
// connector takes it before registering: a Messenger Lite (native)
// session's suggested one (nil for a web cookie session, whose client has
// no MessengerLite), else the device ID the messages page gave the web
// session, which messagix's own socket uses (configs.go's updateSocketIDs:
// IGDMqttWebDeviceID, then MqttWebDeviceID). uuid.Nil when neither is a
// UUID.
func (c realClient) SessionDeviceID() uuid.UUID {
	if id := c.Client.MessengerLite.GetSuggestedDeviceID(); id != uuid.Nil {
		return id
	}
	cfg, err := c.pageConfigs()
	if err != nil {
		return uuid.Nil
	}
	id, err := uuid.Parse(cmp.Or(cfg.IGDMqttWebDeviceID.ClientID, cfg.MqttWebDeviceID.ClientID))
	if err != nil {
		return uuid.Nil
	}
	return id
}
