package meta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// MediaLimit is the largest file Download fetches.
const MediaLimit = 32 << 20

// StatusError is a CDN's answer other than 200 (and 429, which is
// httpclient.ErrRateLimited, so the pace backs off).
type StatusError int

func (e StatusError) Error() string { return "status " + strconv.Itoa(int(e)) }

// Download fetches a file from Meta's CDN at url with the signed-in
// client's HTTP client h (no cookies: the CDN address is signed), with the
// headers mautrix-meta's media downloads send for an image
// (msgconv/mediadl). It is a read: the caller makes it through the pace.
// A 429 is httpclient.ErrRateLimited; any other answer but 200 is a
// StatusError; a file over MediaLimit is tooLarge.
func Download(ctx context.Context, h *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &wire.Error{Kind: wire.NotFound}
	}
	req.Header.Set("Accept", "image/avif,image/webp,*/*")
	req.Header.Set("Sec-Fetch-Dest", "image")
	req.Header.Set("Sec-Fetch-Mode", "no-cors")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("User-Agent", useragent.UserAgent)
	req.Header.Set("sec-ch-ua", useragent.SecCHUserAgent)
	req.Header.Set("sec-ch-ua-platform", useragent.SecCHPlatform)
	resp, err := h.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %w", httpclient.ErrRequestFailed, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, httpclient.ErrRateLimited
	case resp.StatusCode != http.StatusOK:
		return nil, StatusError(resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MediaLimit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", httpclient.ErrResponseReadFailed, err)
	}
	if len(data) > MediaLimit {
		return nil, &wire.Error{Kind: wire.TooLarge}
	}
	return data, nil
}

// Answered reports whether a download failed with an answer from the CDN
// (a StatusError), not for want of one.
func Answered(err error) bool {
	var s StatusError
	return errors.As(err, &s)
}

// Expired reports whether a download failed because the CDN no longer
// takes its address (403, 404, 410).
func Expired(err error) bool {
	var s StatusError
	return errors.As(err, &s) && (s == 403 || s == 404 || s == 410)
}
