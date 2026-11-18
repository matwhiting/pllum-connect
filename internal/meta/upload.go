package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"

	"github.com/google/go-querystring/query"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/util/random"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// A file sent to Meta (Instagram's or Messenger's unencrypted chats) is
// uploaded first and named by its ID in the message. The library's own
// upload (httpclient.SendMercuryUploadRequest) repeats itself up to five
// more times, sleeping 3 to 15 s where the context cannot cut it short,
// whenever Meta's answer is one it does not know; anti-goal 1 forbids
// asking again after Meta said no. Upload is a one-shot copy of it.

var (
	errNoClient   = &wire.Error{Kind: wire.NotConnected}
	errBadUpload  = &wire.Error{Kind: wire.BadRequest}
	errUnreadable = &wire.Error{Kind: wire.Internal}
	errRefused    = &wire.Error{Kind: wire.Refused}
)

// UploadClient is the part of a signed-in Meta client (instameow's or
// messagix's) an upload uses.
type UploadClient interface {
	GetHTTP() *httpclient.HTTPClient
	GetEndpoint(name string) string
}

// Upload uploads a file for a thread (named by its thread key, as
// mautrix-meta's mediadl.ReuploadFileToMeta names it), asked once. It is
// the library's SendMercuryUploadRequest (httpclient/mercury.go, mautrix-meta
// v0.2610.0, AGPL-3.0 like this helper) with its retry loop taken out: the
// same URL, headers and body, sent with MakeRequestOnce.
func Upload(ctx context.Context, c UploadClient, threadID int64, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error) {
	h := c.GetHTTP()
	if h == nil {
		return nil, errNoClient
	}
	values, err := query.Values(h.NewHTTPQuery())
	if err != nil {
		return nil, errUnreadable
	}
	url := c.GetEndpoint("media_upload") + values.Encode()
	headers := h.BuildHeaders(true, false)
	headers.Set("accept", "*/*")
	headers.Set("origin", c.GetEndpoint("base_url"))
	headers.Set("referer", c.GetEndpoint("thread")+strconv.FormatInt(threadID, 10)+"/")
	headers.Set("priority", "u=1, i")
	headers.Set("sec-fetch-dest", "empty")
	headers.Set("sec-fetch-mode", "cors")
	headers.Set("sec-fetch-site", "same-origin") // header is required
	return UploadOnce(ctx, h, url, headers, media)
}

// OnceRequester is the one request an upload makes: the client's
// MakeRequestOnce (*httpclient.HTTPClient), which never repeats itself.
type OnceRequester interface {
	MakeRequestOnce(ctx context.Context, url, method string, headers http.Header, payload []byte, contentType types.ContentType) (*http.Response, []byte, error)
}

// UploadOnce posts the upload's multipart body to url once and reads
// Meta's answer. An error answer becomes a fixed kind: an ended session
// (1357053, 1357001 "please log in") stops the pace like any other; a
// consent page is a checkpoint; anything else is refused. A 429 is the
// library's ErrRateLimited, which the pace backs off on.
func UploadOnce(ctx context.Context, r OnceRequester, url string, headers http.Header, media *httpclient.MercuryUploadMedia) (*types.MercuryUploadResponse, error) {
	payload, contentType, err := mercuryPayload(media)
	if err != nil {
		return nil, errBadUpload
	}
	headers.Set("content-type", contentType)
	_, body, err := r.MakeRequestOnce(ctx, url, http.MethodPost, headers, payload, types.NONE)
	if err != nil {
		return nil, err
	}
	var resp *types.MercuryUploadResponse
	if err := json.Unmarshal(bytes.TrimPrefix(body, httpclient.AntiJSPrefix), &resp); err != nil || resp == nil {
		return nil, errUnreadable
	}
	if resp.ErrorCode != 0 {
		switch resp.ErrorCode {
		case 1357053:
			return nil, &resp.ErrorResponse // an ended session, to the network's kindOf
		case types.ErrPleaseLogIn.ErrorCode:
			return nil, httpclient.ErrTokenInvalidated
		}
		return nil, errRefused
	}
	if strings.Contains(resp.Redirect, "/consent/") {
		return nil, httpclient.ErrConsentRequired
	}
	if uploadMetadata(resp) != nil {
		return nil, errRefused
	}
	return resp, nil
}

// uploadMetadata reads the uploaded file's IDs: the library's
// parseMetadata (httpclient/mercury.go, v0.2610.0), copied.
func uploadMetadata(resp *types.MercuryUploadResponse) error {
	if len(resp.Payload.Metadata) == 0 {
		return errRefused
	}
	switch resp.Payload.Metadata[0] {
	case '[':
		var list []types.FileMetadata
		if err := json.Unmarshal(resp.Payload.Metadata, &list); err != nil || len(list) == 0 {
			return errRefused
		}
		resp.Payload.RealMetadata = &list[0]
	case '{':
		var byIndex map[string]types.FileMetadata
		if err := json.Unmarshal(resp.Payload.Metadata, &byIndex); err != nil {
			return errRefused
		}
		first := byIndex["0"]
		resp.Payload.RealMetadata = &first
	default:
		return errRefused
	}
	return nil
}

// mercuryPayload is the upload's multipart body and its content type: the
// library's newMercuryMediaPayload (httpclient/mercury.go, v0.2610.0),
// copied.
func mercuryPayload(media *httpclient.MercuryUploadMedia) ([]byte, string, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.SetBoundary("----WebKitFormBoundary" + random.String(16)); err != nil {
		return nil, "", err
	}
	if media.IsVoiceClip {
		if err := w.WriteField("voice_clip", "true"); err != nil {
			return nil, "", err
		}
		if media.WaveformData != nil {
			waveform, err := json.Marshal(media.WaveformData)
			if err != nil {
				return nil, "", err
			}
			if err := w.WriteField("voice_clip_waveform_data", string(waveform)); err != nil {
				return nil, "", err
			}
		}
	}
	part := make(textproto.MIMEHeader)
	part.Set("Content-Disposition", fmt.Sprintf(`form-data; name="farr"; filename="%s"`, media.Filename))
	part.Set("Content-Type", media.MimeType)
	pw, err := w.CreatePart(part)
	if err != nil {
		return nil, "", err
	}
	if _, err := pw.Write(media.MediaData); err != nil {
		return nil, "", err
	}
	if err := w.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), w.FormDataContentType(), nil
}
