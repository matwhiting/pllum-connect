package linkedin

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
)

// userAgent is the one browser the window and the helper both are: this
// Mac's Safari 27.2, as the sign-in spike used it on 2026-11-19. The window
// sets it as its customUserAgent, and every request the helper makes carries
// it alone, so no Sec-Ch-* header goes (Safari sends none) and the helper
// never presents itself as Chrome on Linux (the library's default).
const userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.2 Safari/605.1.15"

// SignInPage is LinkedIn's page for Pllum's sign-in window: linkedin.com's
// own sign-in, done once the feed or messaging loads, with li_at and
// JSESSIONID required and the other cookies the spike found at the signed-in
// page taken when they are there (names only were recorded, 2026-11-19).
func SignInPage() meta.SignInPage {
	return meta.SignInPage{
		URL:          "https://www.linkedin.com/login",
		DonePattern:  `^https://www\.linkedin\.com/(?:feed|messaging)(?:/.*)?(?:\?.*)?$`,
		CookieDomain: "linkedin.com",
		Required:     []string{"li_at", "JSESSIONID"},
		Optional: []string{"bcookie", "bscookie", "lidc", "liap", "lang", "timezone", "__cf_bm", "dfpfpt",
			"fptctx2", "sdui_ver", "aam_uuid", "AMCV_14215E3D5995C57C0A495C55%40AdobeOrg", "AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg"},
		UserAgent: userAgent,
	}
}

// browserHeaders are the headers every request carries in place of the
// library's: the page's User-Agent and nothing else.
func browserHeaders(ua string) http.Header {
	return http.Header{"User-Agent": {ua}}
}

// named is the session's named cookies (the page's; anything else is
// dropped), and whether both required ones are there with a value.
func named(s network.Session) (network.Session, bool) {
	p := SignInPage()
	out := network.Session{}
	for _, name := range p.Names() {
		if v := s[name]; v != "" {
			out[name] = v
		}
	}
	for _, name := range p.Required {
		if out[name] == "" {
			return nil, false
		}
	}
	return out, true
}

// sessionHash is a SHA-256 of a session's required cookies: what tells the
// session LinkedIn ended from a new sign-in, with no cookie value kept.
func sessionHash(s network.Session) string {
	h := sha256.New()
	for _, name := range SignInPage().Required {
		h.Write([]byte(name + "=" + s[name] + "\x00"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// jarOf is a cookie jar holding the session as the window took it, for every
// linkedin.com host. A value in double quotes (JSESSIONID's) is held as a
// quoted cookie: it goes out with its quotes, as a browser sends it, and the
// CSRF token the library reads from it is the value inside them.
func jarOf(s network.Session) *linkedingo.StringCookieJar {
	jar := linkedingo.NewEmptyStringCookieJar()
	cookies := make([]*http.Cookie, 0, len(s))
	for name, v := range s {
		c := &http.Cookie{Name: name, Value: v, Domain: ".linkedin.com", Path: "/", Secure: true}
		if len(v) >= 2 && strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) {
			c.Value, c.Quoted = v[1:len(v)-1], true
		}
		cookies = append(cookies, c)
	}
	jar.SetCookies(linkedingo.CookieBaseURL, cookies)
	return jar
}

// jarValue is a cookie's value as the window would give it (in its quotes,
// if it has them), the one set last when LinkedIn has set it more than once;
// "" for none.
func jarValue(jar *linkedingo.StringCookieJar, name string) string {
	v := ""
	for _, c := range jar.Cookies(linkedingo.CookieBaseURL) {
		if c.Name != name {
			continue
		}
		v = c.Value
		if c.Quoted {
			v = `"` + v + `"`
		}
	}
	return v
}
