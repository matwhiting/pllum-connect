package network

import (
	"fmt"
	"strings"
	"testing"
)

// The cookie value is invented.
const canaryCookie = "cookie-canary-5d1e"

func TestASessionNeverPrintsItsCookies(t *testing.T) {
	s := Session{"sessionid": canaryCookie, "csrftoken": canaryCookie + "-csrf"}
	p := LoginParams{Method: LoginSession, Session: s}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%T %v"} {
		for name, v := range map[string]any{"Session": s, "LoginParams": p, "*LoginParams": &p, "[]any": []any{s}} {
			got := fmt.Sprintf(verb, v)
			if strings.Contains(got, canaryCookie) || strings.Contains(got, "sessionid") {
				t.Errorf("%s of a %s prints the session: %s", verb, name, got)
			}
		}
	}
	if got := fmt.Sprint(s); got != "[session redacted]" {
		t.Errorf("Sprint = %q, want [session redacted]", got)
	}
	if got := s.String() + " " + s.GoString(); got != "[session redacted] [session redacted]" {
		t.Errorf("String and GoString = %q", got)
	}
	// It is still a map to the code that uses it.
	if s["sessionid"] != canaryCookie || len(s) != 2 {
		t.Errorf("the session lost its cookies")
	}
}
