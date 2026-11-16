// Package meta holds what the networks that sign in through a website
// (Instagram, Messenger) share: the sign-in pages Pllum shows, the cookie
// sessions that come out of them, and the pace at which the helper may talk
// to Meta. It imports only the leaf packages of mautrix-meta's messagix, for
// their cookie lists and user agent, never its connectors.
package meta

import (
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"
)

// SignInPage is what Pllum needs to show a network's sign-in page in a web
// view and take the session out of it. It is the result of `signIn`, so the
// JSON names are the contract's (PROTOCOL.md).
type SignInPage struct {
	// URL is the page to open.
	URL string `json:"url"`
	// DonePattern is a regular expression on the full URL: when the window's
	// address matches it, he is signed in and the cookies can be read.
	DonePattern string `json:"donePattern"`
	// CookieDomain is the domain the cookies are read from.
	CookieDomain string `json:"cookieDomain"`
	// Required are the cookies without which there is no session; Optional
	// are the ones to take when they are there.
	Required []string `json:"required"`
	Optional []string `json:"optional"`
	// UserAgent is the one the helper presents to Meta, so the window and
	// the helper look like one browser.
	UserAgent string `json:"userAgent"`
}

// Names is every cookie the page names, required first.
func (p SignInPage) Names() []string {
	return append(append([]string{}, p.Required...), p.Optional...)
}

// InstagramPage and FacebookPage copy the web sign-in steps of mautrix-meta
// v0.2610.0: pkg/igconnector/login.go (URL, done pattern and cookie domain
// for Instagram) and pkg/connector/login.go (the same for Facebook, in its
// Facebook branch). The cookie lists are the library's own, converted below;
// a test holds them equal.
var (
	InstagramPage = SignInPage{
		URL:          "https://www.instagram.com/accounts/login/",
		DonePattern:  `^https://www\.instagram\.com/(?:direct/(?:inbox/|t/[0-9]+/)?)?(?:\?.*)?$`,
		CookieDomain: "instagram.com",
		Required:     names(cookies.IGRequiredCookies),
		Optional:     names(cookies.IGOptionalCookies),
		UserAgent:    useragent.UserAgent,
	}
	FacebookPage = SignInPage{
		URL:          "https://www.facebook.com/",
		DonePattern:  `^https://www\.facebook\.com/(?:messages/(?:e2ee/)?(?:t/[0-9]+/?)?)?(?:\?.*)?$`,
		CookieDomain: "facebook.com",
		Required:     names(cookies.FBRequiredCookies),
		Optional:     []string{},
		UserAgent:    useragent.UserAgent,
	}
)

func names(list []cookies.MetaCookieName) []string {
	out := make([]string, len(list))
	for i, n := range list {
		out[i] = string(n)
	}
	return out
}
