// Pllum Connect's tests of its changes to this package (PLLUM-CHANGES.md).
// Every request goes to a fake round-tripper: nothing leaves the Mac, and
// every URN, cookie and cursor here is invented.

package linkedingo

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// recordingTransport answers every request with body and keeps the requests.
type recordingTransport struct {
	mu   sync.Mutex
	reqs []*http.Request
	body string
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.reqs = append(t.reqs, req)
	t.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(t.body)),
		Request:    req,
	}, nil
}

// pllumTestClient is a client over a recording transport, signed in as an
// invented profile with invented cookies.
func pllumTestClient(t *testing.T, body string) (*Client, *recordingTransport) {
	t.Helper()
	jar := NewEmptyStringCookieJar()
	jar.SetCookies(CookieBaseURL, []*http.Cookie{
		{Name: "li_at", Value: "li-at-invented"},
		{Name: LinkedInCookieJSESSIONID, Value: `"ajax:0000000000000000001"`},
	})
	cli := NewClient(context.Background(), NewURN("urn:li:fsd_profile:ACoAAInventedSelf"), jar, "", "", "", Handlers{})
	rt := &recordingTransport{body: body}
	cli.http.Transport = rt
	return cli, rt
}

const pllumCursorPage = `{"data":{"messengerConversationsByCategoryQuery":{
	"metadata":{"nextCursor":"next-cursor-invented-2"},
	"elements":[{"entityUrn":"urn:li:msg_conversation:(urn:li:fsd_profile:ACoAAInventedSelf,2-aW52ZW50ZWQ=)","lastActivityAt":1767225600000}]
}}}`

func TestPllumConversationsWithNextCursorSendsTheCursor(t *testing.T) {
	cli, rt := pllumTestClient(t, pllumCursorPage)
	cursor := "cursor-invented=1/+x"
	page, err := cli.GetConversationsWithNextCursor(context.Background(), "PRIMARY_INBOX", cursor)
	if err != nil {
		t.Fatalf("GetConversationsWithNextCursor: %v", err)
	}
	if len(rt.reqs) != 1 {
		t.Fatalf("%d requests, want 1", len(rt.reqs))
	}
	q := rt.reqs[0].URL.RawQuery
	if !strings.Contains(q, "queryId="+graphQLQueryIDMessengerConversationsWithNextCursor) {
		t.Errorf("the request is not the by-category query: %s", q)
	}
	if want := "nextCursor:" + url.QueryEscape(cursor); !strings.Contains(q, want) {
		t.Errorf("the request does not carry %q: %s", want, q)
	}
	for _, want := range []string{
		"count:20",
		"mailboxUrn:" + url.QueryEscape("urn:li:fsd_profile:ACoAAInventedSelf"),
		"query:(predicateUnions:List((conversationCategoryPredicate:(category:PRIMARY_INBOX))))",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("the request does not carry %q: %s", want, q)
		}
	}
	if strings.Contains(q, "lastUpdatedBefore") {
		t.Errorf("the request carries lastUpdatedBefore, which LinkedIn ignores: %s", q)
	}
	if page == nil || page.Metadata.NextCursor != "next-cursor-invented-2" || len(page.Elements) != 1 {
		t.Errorf("page = %+v, want the next cursor and one conversation", page)
	}
}

func TestPllumConversationsWithNoCursorIsTheFirstPage(t *testing.T) {
	cli, rt := pllumTestClient(t, pllumCursorPage)
	if _, err := cli.GetConversationsWithNextCursor(context.Background(), "PRIMARY_INBOX", ""); err != nil {
		t.Fatalf("GetConversationsWithNextCursor: %v", err)
	}
	if q := rt.reqs[0].URL.RawQuery; strings.Contains(q, "nextCursor") {
		t.Errorf("the first page carries a cursor: %s", q)
	}
}

func TestPllumAUserAgentAloneSendsNoSecChHeaders(t *testing.T) {
	cli, rt := pllumTestClient(t, pllumCursorPage)
	const ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.2 Safari/605.1.15"
	cli.SetBrowserHeaders(http.Header{"User-Agent": {ua}})
	if _, err := cli.GetConversationsWithNextCursor(context.Background(), "PRIMARY_INBOX", ""); err != nil {
		t.Fatalf("GetConversationsWithNextCursor: %v", err)
	}
	h := rt.reqs[0].Header
	if got := h.Get("User-Agent"); got != ua {
		t.Errorf("User-Agent = %q, want the page's", got)
	}
	for name := range h {
		if strings.HasPrefix(strings.ToLower(name), "sec-ch-") {
			t.Errorf("the request carries %s", name)
		}
	}
}

func TestPllumConversationsWithNextCursorSendsTheCategory(t *testing.T) {
	for _, category := range []string{"INBOX", "ARCHIVE", "PRIMARY_INBOX"} {
		cli, rt := pllumTestClient(t, pllumCursorPage)
		if _, err := cli.GetConversationsWithNextCursor(context.Background(), category, "cursor-invented-1"); err != nil {
			t.Fatalf("%s: GetConversationsWithNextCursor: %v", category, err)
		}
		q := rt.reqs[0].URL.RawQuery
		want := "query:(predicateUnions:List((conversationCategoryPredicate:(category:" + category + "))))"
		if !strings.Contains(q, want) {
			t.Errorf("%s: the request does not carry %q: %s", category, want, q)
		}
		for _, other := range []string{"INBOX", "ARCHIVE", "PRIMARY_INBOX"} {
			if other != category && strings.Contains(q, "category:"+other+")") {
				t.Errorf("%s: the request also names %s: %s", category, other, q)
			}
		}
	}
}

func TestPllumConversationsWithNoCategoryIsThePrimaryInbox(t *testing.T) {
	cli, rt := pllumTestClient(t, pllumCursorPage)
	if _, err := cli.GetConversationsWithNextCursor(context.Background(), "", ""); err != nil {
		t.Fatalf("GetConversationsWithNextCursor: %v", err)
	}
	if q := rt.reqs[0].URL.RawQuery; !strings.Contains(q, "category:PRIMARY_INBOX)") {
		t.Errorf("an empty category is not the primary inbox: %s", q)
	}
}
