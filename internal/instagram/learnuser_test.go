package instagram

import (
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// A page whose sender comes with a user_dict no thread listed (someone who
// left a group, say) stores without waiting for ever: the sender is learned
// before the page's batch, so the line, and a reaction of theirs later in
// the page, carry their IGID (quality review of Batch B, C-1).
func TestAPageFromASenderNoThreadListedLearnsThemFirst(t *testing.T) {
	h := newHarness(t)
	const fbid int64 = 1000077
	const igid = "17800000000000077"
	first := textMsg("mid.$invented-learn-1", fbid, start.Add(-2*time.Minute), "invented hello")
	first.Sender = &slidetypes.MessageSender{Name: "Former Member",
		UserDict: slidetypes.User{ID: igid, InteropMessagingUserFBID: fbid, FullName: "Former Member"}}
	second := textMsg("mid.$invented-learn-2", aliceFBI, start.Add(-time.Minute), "invented reply")
	second.Reactions = []*slidetypes.Reaction{{Reaction: "👍", SenderFBID: fbid}}

	done := make(chan error, 1)
	go func() {
		_, err := h.g.storeMessages(aliceThread, []slidetypes.Node[*slidetypes.Message]{{Node: first}, {Node: second}}, store.SourceSync)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("storeMessages: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("storeMessages hung")
	}

	page, err := h.st.Messages(Name, aliceThread, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("the chat holds %d lines", len(page.Items))
	}
	if got := page.Items[0]; got.SenderID != igid || got.SenderName != "Former Member" {
		t.Fatalf("the first line is from %q (%q); want %q", got.SenderID, got.SenderName, igid)
	}
	if rs := page.Items[1].Reactions; len(rs) != 1 || rs[0].SenderID != igid {
		t.Fatalf("the reaction is from %+v; want %q", rs, igid)
	}
	if v, ok, _ := h.st.NetworkValue(Name, "user.1000077"); !ok || v != igid {
		t.Fatalf("learned %q, %v", v, ok)
	}
}
