package linkedin

import (
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"
	"go.mau.fi/util/jsontime"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// liveWith signs in and returns the client, its catch-up having stored a
// one-to-one with Alice (conversation 1).
func liveWith(t *testing.T) (*rig, *fakeClient) {
	t.Helper()
	r := newRig(t)
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	return r, r.live()
}

func alice() linkedingo.MessagingParticipant { return person(aliceID, "Alice", "Invented") }

// one is the only line of conversation 1 with the given ID.
func one(t *testing.T, r *rig, id int) store.Message {
	t.Helper()
	var found []store.Message
	for _, m := range r.lines(convURN(1)) {
		if m.ID == msgURN(id) {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d lines %d, want 1", len(found), id)
	}
	return found[0]
}

func TestAChatIsNamedForThePersonOrTheOrganisation(t *testing.T) {
	r, c := liveWith(t)
	ch := r.chat(convURN(1))
	if ch.Kind != "single" || ch.Title != "Alice Invented" || ch.Phone != "" {
		t.Errorf("chat = %+v, want a single named for Alice", ch)
	}
	if len(ch.Participants) != 2 {
		t.Fatalf("participants = %+v", ch.Participants)
	}
	for _, p := range ch.Participants {
		if p.ID != selfID && p.ID != aliceID {
			t.Errorf("participant ID %q is not a profile ID", p.ID)
		}
		if p.IsSelf != (p.ID == selfID) {
			t.Errorf("participant %q isSelf = %v", p.ID, p.IsSelf)
		}
	}

	g := conv(2, start)
	g.GroupChat = true
	g.Title = "Invented Group"
	g.ConversationParticipants = append(g.ConversationParticipants, person(bobID, "Bob", "Invented"))
	o := conv(3, start)
	o.ConversationParticipants = []linkedingo.MessagingParticipant{me(), org(orgID, "Invented Org")}
	c.dispatch(convEvent(g))
	c.dispatch(convEvent(o))
	if ch := r.chat(convURN(2)); ch.Kind != "group" || ch.Title != "Invented Group" || len(ch.Participants) != 3 {
		t.Errorf("group = %+v", ch)
	}
	if ch := r.chat(convURN(3)); ch.Title != "Invented Org" {
		t.Errorf("an organisation's chat is titled %q", ch.Title)
	}
}

func convEvent(c linkedingo.Conversation) *linkedingo.DecoratedEvent {
	return &linkedingo.DecoratedEvent{
		Topic: topic(linkedingo.RealtimeEventTopicConversations),
		Payload: linkedingo.DecoratedEventPayload{Data: linkedingo.DecoratedEventData{
			DecoratedConversation: &linkedingo.DecoratedConversation{Result: c},
		}},
	}
}

func TestARecalledMessageIsDeleted(t *testing.T) {
	r, c := liveWith(t)
	c.dispatch(messageEvent(text(1, 10, alice(), start, "invented to recall")))
	m := text(1, 10, alice(), start, "")
	m.MessageBodyRenderFormat = linkedingo.MessageBodyRenderFormatRecalled
	c.dispatch(messageEvent(m))
	got := one(t, r, 10)
	if !got.Deleted || got.Text != "" {
		t.Errorf("recalled line = %+v, want deleted with no text", got)
	}
	// A line first seen recalled is one deleted line too.
	first := text(1, 11, alice(), start, "")
	first.MessageBodyRenderFormat = linkedingo.MessageBodyRenderFormatRecalled
	c.dispatch(messageEvent(first))
	if got := one(t, r, 11); !got.Deleted {
		t.Errorf("a line first seen recalled = %+v", got)
	}
}

func TestAnEditKeepsItsURNAndChangesItsText(t *testing.T) {
	r, c := liveWith(t)
	c.dispatch(messageEvent(text(1, 20, alice(), start, "invented first")))
	m := text(1, 20, alice(), start, "invented second")
	m.MessageBodyRenderFormat = linkedingo.MessageBodyRenderFormatEdited
	c.dispatch(messageEvent(m))
	got := one(t, r, 20)
	if got.Text != "invented second" || !got.Edited || got.TimeMs != start.UnixMilli() {
		t.Errorf("edited line = %+v", got)
	}
	// First seen edited, in a page.
	m = text(1, 21, alice(), start.Add(time.Second), "invented already edited")
	m.MessageBodyRenderFormat = linkedingo.MessageBodyRenderFormatEdited
	r.l.storeMessages([]linkedingo.Message{m}, store.SourceSync)
	if got := one(t, r, 21); got.Text != "invented already edited" || !got.Edited {
		t.Errorf("a line first seen edited = %+v", got)
	}
}

func TestASystemMessageIsHidden(t *testing.T) {
	r, c := liveWith(t)
	m := text(1, 30, alice(), start, "invented joined the conversation")
	m.MessageBodyRenderFormat = linkedingo.MessageBodyRenderFormatSystem
	c.dispatch(messageEvent(m))
	got := one(t, r, 30)
	if got.Kind != "notice" || got.Hidden == nil || !*got.Hidden {
		t.Errorf("system line = %+v, want a hidden notice", got)
	}
	if r.chat(convURN(1)).Unread != 0 {
		t.Errorf("a system line counts as unread")
	}
}

func TestAForwardedMessageIsLabelled(t *testing.T) {
	r, c := liveWith(t)
	m := text(1, 40, alice(), start, "")
	m.RenderContent = []linkedingo.RenderContent{{ForwardedMessage: &linkedingo.ForwardedMessage{
		ForwardedBody: linkedingo.AttributedText{Text: "invented forwarded words"},
	}}}
	c.dispatch(messageEvent(m))
	if got := one(t, r, 40); got.Text != "[forwarded]\ninvented forwarded words" || got.Kind != "text" {
		t.Errorf("forwarded line = %+v", got)
	}
}

func TestASharedPostWithNoTextIsLabelled(t *testing.T) {
	r, c := liveWith(t)
	m := text(1, 50, alice(), start, "")
	m.RenderContent = []linkedingo.RenderContent{{HostURNData: &linkedingo.HostURNData{
		Type: "FEED_UPDATE", HostURN: linkedingo.NewURN("urn:li:activity:7000000000000000001"),
	}}}
	c.dispatch(messageEvent(m))
	if got := one(t, r, 50); got.Text != "[shared post]" {
		t.Errorf("shared post = %+v", got)
	}
	// With text, the text.
	m = text(1, 51, alice(), start, "invented look at this")
	m.RenderContent = []linkedingo.RenderContent{{HostURNData: &linkedingo.HostURNData{Type: "FEED_UPDATE"}}}
	c.dispatch(messageEvent(m))
	if got := one(t, r, 51); got.Text != "invented look at this" {
		t.Errorf("shared post with text = %+v", got)
	}
}

func TestAnInMailWithNoTextButAnAttachmentIsALine(t *testing.T) {
	r, c := liveWith(t)
	m := text(1, 60, alice(), start, "")
	m.RenderContent = []linkedingo.RenderContent{{File: &linkedingo.FileAttachment{
		Name: "invented.pdf", MediaType: "application/pdf", URL: "https://media.invented.example/file",
	}}}
	c.dispatch(messageEvent(m))
	got := one(t, r, 60)
	if got.Kind != "file" || len(got.Media) != 1 || got.Media[0].Kind != "file" {
		t.Errorf("attachment-only line = %+v", got)
	}
	if got.Hidden != nil && *got.Hidden {
		t.Errorf("attachment-only line is hidden")
	}
	for _, rc := range []struct {
		content linkedingo.RenderContent
		kind    string
	}{
		{linkedingo.RenderContent{VectorImage: &linkedingo.VectorImage{RootURL: "https://media.invented.example/", Artifacts: []linkedingo.VectorArtifact{{Width: 640, Height: 480, FileIdentifyingURLPathSegment: "p"}}}}, "photo"},
		{linkedingo.RenderContent{ExternalMedia: &linkedingo.ExternalMedia{Media: linkedingo.ExternalProxyImage{URL: "https://media.invented.example/gif", OriginalWidth: 200, OriginalHeight: 100}}}, "photo"},
		{linkedingo.RenderContent{Video: &linkedingo.VideoPlayMetadata{}}, "video"},
		{linkedingo.RenderContent{Audio: &linkedingo.AudioMetadata{URL: "https://media.invented.example/voice"}}, "voiceNote"},
	} {
		m := text(1, 61, alice(), start, "")
		m.EntityURN = linkedingo.NewURN(msgURN(61 + len(rc.kind)))
		m.RenderContent = []linkedingo.RenderContent{rc.content}
		c.dispatch(messageEvent(m))
		got := one(t, r, 61+len(rc.kind))
		if got.Kind != rc.kind || len(got.Media) != 1 || got.Media[0].Kind != rc.kind {
			t.Errorf("%s line = %+v", rc.kind, got)
		}
	}
	// Nothing at all is still never an empty line.
	empty := text(1, 70, alice(), start, "")
	c.dispatch(messageEvent(empty))
	if got := one(t, r, 70); got.Hidden == nil || !*got.Hidden {
		t.Errorf("an empty message = %+v, want hidden", got)
	}
}

func TestAReplyNamesTheOriginal(t *testing.T) {
	r, c := liveWith(t)
	c.dispatch(messageEvent(text(1, 80, alice(), start, "invented original")))
	m := text(1, 81, me(), start.Add(time.Second), "invented reply")
	m.RenderContent = []linkedingo.RenderContent{{RepliedMessageContent: &linkedingo.RepliedMessage{
		OriginalMessage: linkedingo.Message{EntityURN: linkedingo.NewURN(msgURN(80))},
	}}}
	c.dispatch(messageEvent(m))
	got := one(t, r, 81)
	if got.ReplyTo != msgURN(80) || !got.FromMe || got.Text != "invented reply" {
		t.Errorf("reply = %+v", got)
	}
}

func TestTheSameMessageFromRealtimeAndAPageIsOneRow(t *testing.T) {
	r, c := liveWith(t)
	m := text(1, 90, me(), start, "invented both ways")
	c.dispatch(messageEvent(m))
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(time.Hour))})
	}
	r.f.before = func(string, time.Time, int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		return messagePage("", m)
	}
	c.dispatch(convDeleteless())
	r.idle()
	one(t, r, 90)
	// And the other way round.
	m2 := text(1, 91, alice(), start.Add(2*time.Second), "invented page first")
	r.l.storeMessages([]linkedingo.Message{m2}, store.SourceSync)
	c.dispatch(messageEvent(m2))
	one(t, r, 91)
}

func TestReactionsCarryViewerReactedAsFromMe(t *testing.T) {
	r, c := liveWith(t)
	m := text(1, 100, alice(), start, "invented react")
	m.ReactionSummaries = []linkedingo.ReactionSummary{
		{Emoji: "👍", Count: 2, ViewerReacted: true},
		{Emoji: "🎉", Count: 1},
	}
	c.dispatch(messageEvent(m))
	got := one(t, r, 100).Reactions
	if len(got) != 2 {
		t.Fatalf("reactions = %+v", got)
	}
	for _, x := range got {
		if x.SenderID != "" {
			t.Errorf("reaction %s has sender %q", x.Emoji, x.SenderID)
		}
		if x.FromMe != (x.Emoji == "👍") {
			t.Errorf("reaction %s fromMe = %v", x.Emoji, x.FromMe)
		}
	}
	// The realtime summary: 🎉 gone, ❤️ his.
	summary := func(emoji string, count int, viewer, added bool) *linkedingo.DecoratedEvent {
		return &linkedingo.DecoratedEvent{
			Topic: topic(linkedingo.RealtimeEventTopicMessageReactionSummaries),
			Payload: linkedingo.DecoratedEventPayload{Data: linkedingo.DecoratedEventData{
				DecoratedReactionSummary: &linkedingo.DecoratedReactionSummary{Result: linkedingo.RealtimeReactionSummary{
					ReactionAdded:   added,
					Message:         linkedingo.Message{EntityURN: linkedingo.NewURN(msgURN(100))},
					ReactionSummary: linkedingo.ReactionSummary{Emoji: emoji, Count: count, ViewerReacted: viewer},
				}},
			}},
		}
	}
	c.dispatch(summary("🎉", 0, false, false))
	c.dispatch(summary("❤️", 1, true, true))
	got = one(t, r, 100).Reactions
	var emojis []string
	for _, x := range got {
		emojis = append(emojis, x.Emoji)
		if x.Emoji == "❤️" && !x.FromMe {
			t.Errorf("his ❤️ is not fromMe")
		}
	}
	slices.Sort(emojis)
	if !slices.Equal(emojis, []string{"❤️", "👍"}) {
		t.Errorf("reactions = %v", emojis)
	}
}

func TestASeenReceiptMarksHisLinesSeen(t *testing.T) {
	r, c := liveWith(t)
	c.dispatch(messageEvent(text(1, 110, me(), start, "invented mine 1")))
	c.dispatch(messageEvent(text(1, 111, me(), start.Add(time.Second), "invented mine 2")))
	c.dispatch(messageEvent(text(1, 112, me(), start.Add(2*time.Second), "invented mine 3")))
	seenAt := start.Add(time.Minute)
	c.dispatch(&linkedingo.DecoratedEvent{
		Topic: topic(linkedingo.RealtimeEventTopicMessageSeenReceipts),
		Payload: linkedingo.DecoratedEventPayload{Data: linkedingo.DecoratedEventData{
			DecoratedSeenReceipt: &linkedingo.DecoratedSeenReceipt{Result: linkedingo.SeenReceipt{
				SeenAt:            jsontime.UM(seenAt),
				Message:           linkedingo.Message{EntityURN: linkedingo.NewURN(msgURN(111))},
				SeenByParticipant: alice(),
			}},
		}},
	})
	if got := one(t, r, 110).SeenMs; got != seenAt.UnixMilli() {
		t.Errorf("line 1 seen %d, want %d", got, seenAt.UnixMilli())
	}
	if got := one(t, r, 111).SeenMs; got != seenAt.UnixMilli() {
		t.Errorf("line 2 seen %d", got)
	}
	if got := one(t, r, 112).SeenMs; got != 0 {
		t.Errorf("line 3, after the one seen, is seen")
	}
}

func TestLastReadAtClearsUnread(t *testing.T) {
	r, c := liveWith(t)
	c.dispatch(messageEvent(text(1, 120, alice(), start, "invented unread")))
	if n := r.chat(convURN(1)).Unread; n != 1 {
		t.Fatalf("unread = %d, want 1", n)
	}
	cv := conv(1, start)
	cv.LastReadAt = ms(start.Add(time.Second))
	c.dispatch(convEvent(cv))
	if n := r.chat(convURN(1)).Unread; n != 0 {
		t.Errorf("unread = %d after LastReadAt passed it, want 0", n)
	}
	// Read, with no LastReadAt: read through its last activity.
	c.dispatch(messageEvent(text(1, 121, alice(), start.Add(time.Minute), "invented second")))
	cv = conv(1, start.Add(time.Minute))
	cv.Read = true
	c.dispatch(convEvent(cv))
	if n := r.chat(convURN(1)).Unread; n != 0 {
		t.Errorf("unread = %d for a read chat, want 0", n)
	}
}

func TestOtherTopicsAreIgnored(t *testing.T) {
	r, c := liveWith(t)
	n := len(r.f.callsNow())
	c.dispatch(&linkedingo.DecoratedEvent{Topic: topic(linkedingo.RealtimeEventTopicTypingIndicators)})
	c.dispatch(&linkedingo.DecoratedEvent{Topic: topic(linkedingo.RealtimeEventTopicTabBadgeUpdate)})
	c.dispatch(&linkedingo.DecoratedEvent{Topic: topic(linkedingo.RealtimeEventTopicPresenceStatus)})
	r.idle()
	if names := r.f.namesSince(n); len(names) != 0 {
		t.Errorf("an ignored topic made calls: %v", names)
	}
}

func TestSpamIsLeftOut(t *testing.T) {
	r, c := liveWith(t)
	spam := conv(9, start)
	spam.Categories = []string{"SPAM"}
	c.dispatch(convEvent(spam))
	m := text(9, 900, alice(), start, "invented spam")
	m.Conversation = spam
	c.dispatch(messageEvent(m))
	if _, ok, _ := r.st.Chat(Name, convURN(9)); ok {
		t.Errorf("a spam conversation became a chat")
	}
}

func TestALinkedInParticipantCarriesItsPublicID(t *testing.T) {
	r, c := liveWith(t)
	g := conv(2, start)
	g.GroupChat = true
	g.Title = "Invented Group"
	bob := person(bobID, "Bob", "Invented")
	bob.ParticipantType.Member.ProfileURL = "https://www.linkedin.com/in/invented-person-12ab/"
	// A URL that names the member by their profile ID is not a public ID.
	other := person(otherID, "Other", "Invented")
	other.ParticipantType.Member.ProfileURL = "https://www.linkedin.com/in/" + otherID
	g.ConversationParticipants = append(g.ConversationParticipants, bob, other, org(orgID, "Invented Org"))
	c.dispatch(convEvent(g))

	got := map[string]string{}
	for _, p := range r.chat(convURN(2)).Participants {
		got[p.ID] = p.Handle
	}
	want := map[string]string{selfID: "", aliceID: "", bobID: "invented-person-12ab", otherID: "", orgID: ""}
	if len(got) != len(want) {
		t.Fatalf("participants = %v", got)
	}
	for id, h := range want {
		if got[id] != h {
			t.Errorf("participant %s has handle %q, want %q", id, got[id], h)
		}
	}
}

func TestPublicIDReadsOnlyAnInProfileURL(t *testing.T) {
	for _, c := range []struct{ url, want string }{
		{"https://www.linkedin.com/in/invented-person-12ab/", "invented-person-12ab"},
		{"https://www.linkedin.com/in/invented-person-12ab", "invented-person-12ab"},
		{"https://linkedin.com/in/invented-person-12ab?trk=invented", "invented-person-12ab"},
		{"https://www.linkedin.com/in/invented-person-12ab/?miniProfileUrn=x", "invented-person-12ab"},
		{"https://www.linkedin.com/in/inv%C3%A9nted-person", "invénted-person"},
		{"https://nz.linkedin.com/in/invented-person-12ab/", "invented-person-12ab"},
		{"https://www.linkedin.com/company/x", ""},
		{"https://www.linkedin.com/in/", ""},
		{"https://www.linkedin.com/in", ""},
		{"", ""},
		{"https://www.example.com/in/invented-person-12ab/", ""},
		{"https://notlinkedin.com/in/invented-person-12ab/", ""},
		{"https://www.linkedin.com.example.com/in/invented-person-12ab/", ""},
		{"ftp://www.linkedin.com/in/invented-person-12ab/", ""},
		{"/in/invented-person-12ab/", ""},
		{"not a url %zz", ""},
	} {
		if got := publicID(c.url); got != c.want {
			t.Errorf("publicID(%q) = %q, want %q", c.url, got, c.want)
		}
	}
}
