package instagram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

func kindErr(err error) wire.Kind {
	var e *wire.Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

func TestASessionLoginStoresTheMailbox(t *testing.T) {
	h := newHarness(t)
	h.m.mailbox = mailboxOf(false,
		threadWith(aliceFBI, aliceThread, alice(), start.Add(-time.Hour)),
		group(start.Add(-2*time.Hour)))
	h.login(validSession())

	if n := h.m.count("LoadIndex"); n != 1 {
		t.Fatalf("LoadIndex ran %d times, want 1", n)
	}
	p := h.m.last().params
	if !p.DisableTyping || p.LogRedactedLoginResponses || p.Log.GetLevel() != zerolog.Disabled {
		t.Errorf("the client is not quiet: typing off %v, redacted login logs %v, log level %v",
			p.DisableTyping, p.LogRedactedLoginResponses, p.Log.GetLevel())
	}
	if p.Cookies == nil || p.Cookies.Get(cookies.IGCookieSessionID) != "sess-invented-1" || p.Cookies.Get("rur") != "rur-invented-1" {
		t.Errorf("the client was not given the session")
	}

	one := h.chat(aliceChat)
	if one.Kind != "single" || one.Title != "Alice Example" || one.LastActivityMs != start.Add(-time.Hour).UnixMilli() {
		t.Errorf("the one-to-one is %+v", one)
	}
	if len(one.Participants) != 2 || one.Participants[0].ID != aliceIG || one.Participants[0].IsSelf ||
		one.Participants[1].ID != selfIG || !one.Participants[1].IsSelf {
		t.Errorf("the one-to-one's participants are %+v, want Alice's IGID then his, marked self", one.Participants)
	}
	grp := h.chat(groupChat)
	if grp.Kind != "group" || grp.Title != "Invented Group" || len(grp.Participants) != 3 {
		t.Errorf("the group is %+v", grp)
	}
	if st := h.g.Status(); st.SelfChatID != "" {
		t.Errorf("selfChatID is %q, want none", st.SelfChatID)
	}
	if h.m.count("Connect") != 1 {
		t.Errorf("the socket was not connected after the inbox")
	}
	if got := h.named("session"); len(got) != 0 {
		t.Errorf("the session login was given came back: %v", got)
	}

	// Subscribed: catching up, then live at the first sequence ID update.
	if err := h.m.dispatch(&slidetypes.Connected{}); err != nil {
		t.Fatal(err)
	}
	if h.lastState() != "catchingUp" {
		t.Errorf("after Connected the state is %s", h.lastState())
	}
	if err := h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 101, Timestamp: start}); err != nil {
		t.Fatal(err)
	}
	if got := h.states(); !slices.Equal(got, []string{"connecting", "catchingUp", "live"}) {
		t.Errorf("states %v, want connecting, catchingUp, live", got)
	}
	// Dropped: offline (the library reconnects); subscribed again with no
	// update: live after 60 s.
	if err := h.m.dispatch(&slidetypes.Disconnected{Error: errors.New("invented drop")}); err != nil {
		t.Errorf("a dropped socket stopped the library reconnecting: %v", err)
	}
	if h.lastState() != "offline" {
		t.Errorf("after Disconnected the state is %s", h.lastState())
	}
	_ = h.m.dispatch(&slidetypes.Connected{})
	h.clock.Advance(59 * time.Second)
	if h.lastState() != "catchingUp" {
		t.Errorf("live before 60 s")
	}
	h.clock.Advance(2 * time.Second)
	if h.lastState() != "live" {
		t.Errorf("not live 60 s after subscribing: %s", h.lastState())
	}
}

func TestMissingRequiredCookiesIsBadRequestBeforeAnyRequest(t *testing.T) {
	for name, session := range map[string]network.Session{
		"no sessionid":    {"csrftoken": "csrf-invented-1", "ds_user_id": "4000000099"},
		"empty csrftoken": {"sessionid": "sess-invented-1", "csrftoken": "", "ds_user_id": "4000000099"},
		"no ds_user_id":   {"sessionid": "sess-invented-1", "csrftoken": "csrf-invented-1", "rur": "rur-invented-1"},
		"nothing named":   {"xs": "x-invented"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			err := h.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: session})
			if kindErr(err) != wire.BadRequest {
				t.Errorf("Login = %v, want badRequest", err)
			}
			if got := h.states(); !slices.Equal(got, []string{"notConnected/badRequest"}) {
				t.Errorf("states %v, want notConnected/badRequest", got)
			}
			if len(h.m.clients) != 0 || len(h.m.calls) != 0 {
				t.Errorf("a client was made (%d) or a call made (%v)", len(h.m.clients), h.m.calls)
			}
		})
	}
	h := newHarness(t)
	if err := h.g.Login(context.Background(), network.LoginParams{Method: network.LoginQR}); kindErr(err) != wire.Unsupported {
		t.Errorf("a QR login = %v, want unsupported", err)
	}
}

func TestLoadIndexErrorsBecomeKinds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		state   string
		blocked wire.Kind // what the pace now refuses with ("" for nothing)
	}{
		{"challenge", httpclient.ErrChallengeRequired, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"checkpoint", httpclient.ErrCheckpointRequired, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"consent", httpclient.ErrConsentRequired, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"redirected to a checkpoint", httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired, URL: "https://invalid.invalid/"}, "reconnectNeeded/checkpoint", wire.Checkpoint},
		{"session ended", fmt.Errorf("%w: invented", httpclient.ErrTokenInvalidated), "reconnectNeeded", meta.ReconnectNeeded},
		{"suspended", httpclient.RedirectedError{Type: httpclient.ErrAccountSuspended, URL: "https://invalid.invalid/"}, "banned", meta.Banned},
		{"rate limited", httpclient.ErrRateLimited, "notConnected/rateLimited", wire.RateLimited},
		{"no connection", fmt.Errorf("%w: invented", httpclient.ErrRequestFailed), "notConnected/offline", ""},
		{"anything else", errors.New("invented"), "notConnected/internal", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, auth := range []bool{false, true} {
				h := newHarness(t)
				h.m.indexErr = tc.err
				h.m.authFirst = auth // the client also dispatches AuthError for some
				h.login(validSession())
				if got := h.states(); !slices.Equal(got, []string{"connecting", tc.state}) {
					t.Errorf("auth event %v: states %v, want connecting, %s", auth, got, tc.state)
				}
				k, blocked := h.g.pace.Blocked()
				if tc.blocked == "" && blocked || tc.blocked != "" && k != tc.blocked {
					t.Errorf("auth event %v: the pace is blocked by %q (%v), want %q", auth, k, blocked, tc.blocked)
				}
				if h.m.count("Connect") != 0 {
					t.Errorf("auth event %v: the socket connected after a failed sign-in", auth)
				}
			}
		})
	}
}

func TestACheckpointStopsEveryRequestUntilANewLogin(t *testing.T) {
	h := newHarness(t)
	h.m.mailbox = mailboxOf(true, threadWith(aliceFBI, aliceThread, alice(), start.Add(-time.Hour)))
	h.m.authFirst = true
	h.m.mailboxPage = func(n int, cursor string) (*slidetypes.PaginateMailboxResponse, error) {
		return nil, httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired, URL: "https://invalid.invalid/"}
	}
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(true, "older-1", textMsg("mid.$invented0001", aliceFBI, start.Add(-time.Hour), "hello")), nil
	}
	h.login(validSession())

	if h.lastState() != "reconnectNeeded/checkpoint" {
		t.Fatalf("after the checkpoint the state is %s", h.lastState())
	}
	if h.m.count("Disconnect") == 0 {
		eventually(t, "the socket to be disconnected", func() bool { return h.m.count("Disconnect") > 0 })
	}
	calls := len(h.m.requests())
	connects := h.m.count("Connect")

	// Nothing more is asked of Meta: not the rest of the backfill, not an
	// older page, not a reconnect, not after hours.
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); kindErr(err) != wire.Checkpoint {
		t.Errorf("FetchOlder = %v, want checkpoint", err)
	}
	h.g.Nudge()
	_ = h.m.dispatch(&slidetypes.Disconnected{Error: errors.New("invented drop")})
	_ = h.m.dispatch(&slidetypes.Delta{ThreadIGID: "340282366841710300000000000000077",
		Data: &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented0099", bobFBID, start, "new")}})
	h.clock.Advance(48 * time.Hour)
	h.g.Nudge()
	h.settle()
	if got := len(h.m.requests()); got != calls {
		t.Errorf("%d requests after the checkpoint, want none: %v", got-calls, h.m.requests()[calls:])
	}
	if h.m.count("Connect") != connects {
		t.Errorf("the socket reconnected after the checkpoint")
	}
	if h.lastState() != "reconnectNeeded/checkpoint" {
		t.Errorf("the state moved without a login: %s", h.lastState())
	}

	// A checkpoint arriving on the socket, not a request, does the same.
	h2 := newHarness(t)
	h2.login(validSession())
	_ = h2.m.dispatch(&slidetypes.AuthError{Error: httpclient.ErrChallengeRequired})
	if h2.lastState() != "reconnectNeeded/checkpoint" {
		t.Errorf("an AuthError on the socket left the state %s", h2.lastState())
	}
	if _, err := h2.g.FetchOlder(context.Background(), aliceChat); kindErr(err) != wire.Checkpoint {
		t.Errorf("FetchOlder after a socket AuthError = %v, want checkpoint", err)
	}

	// The same cookies sent again (Pllum's start-up login) ask nothing.
	_ = h.g.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()})
	h.settle()
	if got := len(h.m.requests()); got != calls {
		t.Errorf("the stopped session, sent again, made %d requests", got-calls)
	}

	// He signs in again in the window (a new session): a new login asks
	// again.
	h.m.authFirst = false
	h.m.mailboxPage = func(n int, cursor string) (*slidetypes.PaginateMailboxResponse, error) {
		return &slidetypes.PaginateMailboxResponse{Mailbox: &slidetypes.MailboxPage{}}, nil
	}
	fresh := validSession()
	fresh["sessionid"] = "sess-invented-2"
	h.login(fresh)
	if got := len(h.m.requests()); got == calls {
		t.Errorf("a new login made no requests")
	}
	if h.m.count("Connect") == connects {
		t.Errorf("a new login did not connect the socket")
	}
	if _, blocked := h.g.pace.Blocked(); blocked {
		t.Errorf("the pace is still blocked after a new login")
	}
}

func TestALiveMessageIsStoredUnderMetasID(t *testing.T) {
	h := newHarness(t)
	h.login(validSession())

	in := textMsg("mid.$cAAAinvented0001", aliceFBI, start.Add(time.Minute), "hello from Alice")
	in.Sender = &slidetypes.MessageSender{Name: "Alice Example", IGID: aliceIG}
	reply := textMsg("mid.$cAAAinvented0002", selfFBID, start.Add(2*time.Minute), "hi Alice")
	reply.RepliedToMessageID = "mid.$cAAAinvented0001"
	for _, m := range []*slidetypes.Message{in, reply} {
		if err := h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: m})); err != nil {
			t.Fatal(err)
		}
	}
	lines := h.lines(aliceChat)
	got, ok := lines["mid.$cAAAinvented0001"]
	if !ok {
		t.Fatalf("the message is not stored under Meta's ID: %v", lines)
	}
	if got.Text != "hello from Alice" || got.SenderID != aliceIG || got.FromMe || got.Kind != "text" ||
		got.TimeMs != start.Add(time.Minute).UnixMilli() || got.SenderName != "Alice Example" {
		t.Errorf("the line is %+v", got)
	}
	mine := lines["mid.$cAAAinvented0002"]
	if !mine.FromMe || mine.SenderID != selfIG || mine.ReplyTo != "mid.$cAAAinvented0001" {
		t.Errorf("his reply is %+v", mine)
	}
	if len(h.named("changed")) == 0 {
		t.Errorf("no changed event")
	}
	if c := h.chat(aliceChat); c.Unread != 1 || c.LastMessageID != "mid.$cAAAinvented0002" {
		t.Errorf("the chat is %+v", c)
	}

	// A message in a thread never listed: the thread is fetched once (through
	// the pace) and the message lands under the thread key, not Meta's chat ID.
	const bobThread = "340282366841710300000000000000022"
	h.m.threadInfo = func(igChatID string) (*slidetypes.ThreadInfoResponse, error) {
		if igChatID != bobThread {
			return nil, errors.New("unknown thread")
		}
		th := threadWith(bobFBID, bobThread, bob(), start.Add(3*time.Minute))
		th.SlideMessages = &slidetypes.SlideMessages{PageInfo: slidetypes.PageInfo{HasNextPage: true, EndCursor: "bob-older"}}
		return &slidetypes.ThreadInfoResponse{ThreadInfo: slidetypes.WrappedThreadInfo{AsIGDirectThread: th}}, nil
	}
	if err := h.m.dispatch(delta(bobThread, &slidetypes.NewMessageEvent{Message: textMsg("mid.$cAAAinvented0003", bobFBID, start.Add(3*time.Minute), "new chat")})); err != nil {
		t.Fatal(err)
	}
	bobChat := fmt.Sprint(bobFBID)
	if l, ok := h.lines(bobChat)["mid.$cAAAinvented0003"]; !ok || l.SenderID != bobIG {
		t.Errorf("the new chat's message is %+v (held %v)", l, ok)
	}
	if c := h.chat(bobChat); c.Title != "bob.example" {
		t.Errorf("the new chat's title is %q", c.Title)
	}
	if _, ok, _ := h.st.Chat(Name, bobThread); ok {
		t.Errorf("a chat was made under Meta's chat ID")
	}
	_ = h.m.dispatch(delta(bobThread, &slidetypes.NewMessageEvent{Message: textMsg("mid.$cAAAinvented0004", bobFBID, start.Add(4*time.Minute), "again")}))
	if n := h.m.count("GetThread"); n != 1 {
		t.Errorf("GetThread ran %d times, want 1", n)
	}
}

func TestReactionsEditsAndDeletes(t *testing.T) {
	h := newHarness(t)
	h.login(validSession())
	const id = "mid.$cAAAinvented0010"
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: textMsg(id, aliceFBI, start, "first words")}))

	_ = h.m.dispatch(delta(aliceThread, &slidetypes.CreateReactionEvent{MessageID: id,
		Reaction: slidetypes.Reaction{Reaction: "❤️", SenderFBID: aliceFBI, LogMessageID: "mid.$react-invented-1"}}))
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.CreateReactionEvent{MessageID: id,
		Reaction: slidetypes.Reaction{Reaction: "😂", SenderFBID: selfFBID, LogMessageID: "mid.$react-invented-2"}}))
	l := h.lines(aliceChat)[id]
	if len(l.Reactions) != 2 {
		t.Fatalf("reactions %+v, want two", l.Reactions)
	}
	for _, r := range l.Reactions {
		if r.Emoji == "❤️" && (r.SenderID != aliceIG || r.FromMe) || r.Emoji == "😂" && (r.SenderID != selfIG || !r.FromMe) {
			t.Errorf("reaction %+v", r)
		}
	}
	// One removed by its sender, the other named only by its reaction
	// message.
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.DeleteReactionEvent{MessageID: id, Reaction: slidetypes.Reaction{SenderFBID: aliceFBI}}))
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.DeleteReactionEvent{MessageID: "mid.$delete-invented", Reaction: slidetypes.Reaction{LogMessageID: "mid.$react-invented-2"}}))
	if l := h.lines(aliceChat)[id]; len(l.Reactions) != 0 {
		t.Errorf("reactions after removing both: %+v", l.Reactions)
	}

	_ = h.m.dispatch(delta(aliceThread, &slidetypes.EditMessageEvent{MessageID: id, TextBody: "second words"}))
	if l := h.lines(aliceChat)[id]; l.Text != "second words" || !l.Edited {
		t.Errorf("after the edit the line is %+v", l)
	}
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.DeleteMessageEvent{MessageID: id}))
	page, _ := h.st.Messages(Name, aliceChat, "", 50)
	for _, m := range page.Items {
		if m.ID == id && (!m.Deleted || m.Text != "") {
			t.Errorf("after the unsend the line is %+v", m)
		}
	}
}

func TestReadReceiptsAndMarkReadEvents(t *testing.T) {
	h := newHarness(t)
	h.login(validSession())
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-r1", selfFBID, start.Add(time.Minute), "mine")}))
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-r2", aliceFBI, start.Add(2*time.Minute), "theirs")}))
	if c := h.chat(aliceChat); c.Unread != 1 {
		t.Fatalf("unread %d, want 1", c.Unread)
	}

	// Alice read up to his line.
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.ReadReceiptEvent{ReadReceipt: slidetypes.ReadReceipt{
		ParticipantFBID: aliceFBI, WatermarkTimestampMS: ms(start.Add(90 * time.Second))}}))
	if l := h.lines(aliceChat)["mid.$invented-r1"]; l.SeenMs != start.Add(90*time.Second).UnixMilli() {
		t.Errorf("his line's seenMs is %d", l.SeenMs)
	}
	// He read hers elsewhere.
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.MarkReadEvent{ReadTimestampMS: ms(start.Add(2 * time.Minute))}))
	if c := h.chat(aliceChat); c.Unread != 0 {
		t.Errorf("after MarkRead unread is %d", c.Unread)
	}
	// He marked it unread on his phone: kept here.
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.MarkUnreadEvent{MarkedAsUnread: true}))
	if c := h.chat(aliceChat); c.Unread != 1 {
		t.Errorf("after MarkUnread unread is %d", c.Unread)
	}
	// His own read receipt (another device) reads it again.
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.ReadReceiptEvent{ReadReceipt: slidetypes.ReadReceipt{
		ParticipantFBID: selfFBID, WatermarkTimestampMS: ms(start.Add(3 * time.Minute))}}))
	if c := h.chat(aliceChat); c.Unread != 0 {
		t.Errorf("after his own receipt unread is %d", c.Unread)
	}
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.MuteThreadEvent{IsMutedNow: true}))
	if !h.chat(aliceChat).Muted {
		t.Errorf("the mute did not land")
	}
	_ = h.m.dispatch(delta(aliceThread, &slidetypes.MuteThreadEvent{IsMutedNow: false}))
	if h.chat(aliceChat).Muted {
		t.Errorf("the unmute did not land")
	}
}

func TestPostsReelsStoriesAndCallsAreLabels(t *testing.T) {
	h := newHarness(t)
	h.login(validSession())
	at := start
	n := 0
	put := func(content slidetypes.MessageContent, typeName, contentType string, admin bool) store.Message {
		t.Helper()
		n++
		at = at.Add(time.Second)
		id := fmt.Sprintf("mid.$invented-share-%02d", n)
		m := &slidetypes.Message{ID: id, SenderFBID: aliceFBI, TimestampMS: ms(at), ContentType: contentType,
			Content: slidetypes.MessageContentWrapper{TypeName: typeName, Content: content}}
		var data slidetypes.DeltaEvent = &slidetypes.NewMessageEvent{Message: m}
		if admin {
			data = &slidetypes.AdminMessageEvent{Message: m}
		}
		if err := h.m.dispatch(delta(aliceThread, data)); err != nil {
			t.Fatal(err)
		}
		page, err := h.st.Messages(Name, aliceChat, "", 200)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range page.Items {
			if l.ID == id {
				return l
			}
		}
		t.Fatalf("message %s not stored", id)
		return store.Message{}
	}
	xma := func(url, text string) *slidetypes.MessageContentXMA {
		return &slidetypes.MessageContentXMA{XMA: &slidetypes.XMAContent{TargetURL: url, TargetID: "3000000000000000001"}, XMATextBody: text}
	}
	for _, tc := range []struct {
		name    string
		content slidetypes.MessageContent
		want    string
	}{
		{"post", xma("https://www.instagram.com/p/INVENTED1/", ""), "[post]"},
		{"reel", xma("https://www.instagram.com/reel/INVENTED2/", ""), "[reel]"},
		{"story", xma("https://www.instagram.com/stories/invented.user/3000000000000000002/", ""), "[story]"},
		{"story reply", xma("https://www.instagram.com/stories/invented.user/3000000000000000003/", "nice view"), "[story]\nnice view"},
		{"link", xma("https://l.facebook.com/l.php?u=https%3A%2F%2Fexample.invalid%2Fpage", ""), "https://example.invalid/page"},
		{"text with a link", &slidetypes.MessageContentXMA{XMA: &slidetypes.XMAContent{TargetURL: "https://example.invalid/"}, XMATextBody: "see https://example.invalid/"}, "see https://example.invalid/"},
	} {
		l := put(tc.content, "SlideMessageXMAContent", "", false)
		if l.Text != tc.want || l.Kind != "text" || *l.Hidden {
			t.Errorf("%s: %q (%s, hidden %v), want %q", tc.name, l.Text, l.Kind, *l.Hidden, tc.want)
		}
	}
	call := put(&slidetypes.MessageContentAdminText{TextFragments: []slidetypes.TextFragment{{Plaintext: "Video chat ended"}}},
		"SlideMessageAdminText", "IG_VIDEO_CALL_XMAT", false)
	if call.Text != "[call]" || *call.Hidden {
		t.Errorf("a call is %q (hidden %v)", call.Text, *call.Hidden)
	}
	notice := put(&slidetypes.MessageContentAdminText{TextFragments: []slidetypes.TextFragment{{Plaintext: "Alice named the group"}}},
		"SlideMessageAdminText", "", true)
	if notice.Kind != "notice" || !*notice.Hidden {
		t.Errorf("an admin message is %+v, want a hidden notice", notice)
	}

	photo := put(&slidetypes.MessageContentImage{Attachments: []*slidetypes.Attachment{{
		AttachmentCDNURL: "https://cdn.invalid/photo-invented.jpg", PreviewWidth: 640, PreviewHeight: 480}}},
		"SlideMessageImageContent", "", false)
	if photo.Kind != "photo" || len(photo.Media) != 1 || photo.Media[0].Width != 640 || !photo.Media[0].HasFile {
		t.Errorf("a photo is %+v", photo)
	}
	raw, err := h.st.Raw(Name, aliceChat, photo.ID)
	if err != nil || !strings.Contains(string(raw), "photo-invented.jpg") || !json.Valid(raw) {
		t.Errorf("the photo's raw is %s (%v), want its content JSON", raw, err)
	}
	for _, tc := range []struct {
		content  slidetypes.MessageContent
		typeName string
		kind     string
	}{
		{&slidetypes.MessageContentVideo{Videos: []*slidetypes.Attachment{{AttachmentCDNURL: "https://cdn.invalid/v.mp4"}}}, "SlideMessageVideosContent", "video"},
		{&slidetypes.MessageContentAudio{AudioAttachments: []*slidetypes.AudioAttachment{{AttachmentCDNURL: "https://cdn.invalid/a.mp4"}}}, "SlideMessageAudiosContent", "voiceNote"},
		{&slidetypes.MessageContentSticker{PreviewURL: "https://cdn.invalid/s.webp"}, "SlideMessageStoreStickerContent", "sticker"},
	} {
		l := put(tc.content, tc.typeName, "", false)
		if l.Kind != tc.kind {
			t.Errorf("%s is %s", tc.typeName, l.Kind)
		}
		if raw, _ := h.st.Raw(Name, aliceChat, l.ID); !json.Valid(raw) || !strings.Contains(string(raw), "cdn.invalid") {
			t.Errorf("%s keeps no content JSON: %s", tc.typeName, raw)
		}
	}
}

func TestARestartResumesFromTheSavedSeqID(t *testing.T) {
	h := newHarness(t)
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(false, ""), nil // the first backfill completes
	}
	h.login(validSession())
	saved := start.Add(-5 * time.Minute).Truncate(time.Millisecond)
	if err := h.m.dispatch(&slidetypes.SeqIDUpdate{SeqID: 4242, Timestamp: saved}); err != nil {
		t.Fatal(err)
	}
	dir, m := h.dir, h.m
	h.close()

	// With the client's saved state, a restart loads it and connects from
	// the saved seq: no LoadIndex, which would subscribe from now and drop
	// what came while Pllum was closed.
	again := openHarness(t, dir, m)
	indexes := m.count("LoadIndex")
	again.login(validSession())
	p := m.last().params
	if p.SeqID != 4242 || !p.SeqIDTS.Equal(saved) {
		t.Errorf("the client was given seq %d at %v, want 4242 at %v", p.SeqID, p.SeqIDTS, saved)
	}
	if m.last().state == nil || !strings.Contains(string(m.last().state), "invented") {
		t.Errorf("the saved client state was not loaded for the same account")
	}
	if n := m.count("LoadIndex") - indexes; n != 0 {
		t.Errorf("a restart with saved state ran LoadIndex %d times, want 0", n)
	}
	if got := m.argsOf("Connect"); got[len(got)-1] != "4242" {
		t.Errorf("the socket subscribed from seq %s, want 4242", got[len(got)-1])
	}
	again.close()

	// A state the client refuses (too old) loads the inbox instead.
	m.stateErr = errors.New("cached state is too old")
	stale := openHarness(t, dir, m)
	indexes = m.count("LoadIndex")
	stale.login(validSession())
	if m.count("LoadIndex")-indexes != 1 {
		t.Errorf("a refused state did not load the inbox")
	}
	stale.close()
	m.stateErr = nil

	// Another account's state is never loaded, and it loads the inbox.
	other := openHarness(t, dir, m)
	s := validSession()
	s["ds_user_id"] = "4000000098"
	indexes = m.count("LoadIndex")
	other.login(s)
	if m.last().state != nil {
		t.Errorf("another account's state was loaded")
	}
	if m.count("LoadIndex")-indexes != 1 {
		t.Errorf("another account did not load the inbox")
	}
}

func TestBackfillIsPacedAndStopsAtItsLimits(t *testing.T) {
	// An inbox that never ends, one recent thread a page, each with a
	// history that never ends.
	h := newHarness(t)
	h.m.mailbox = mailboxOf(true, threadWith(aliceFBI, aliceThread, alice(), start.Add(-time.Hour)))
	h.m.mailboxPage = func(n int, cursor string) (*slidetypes.PaginateMailboxResponse, error) {
		key := int64(2000000 + n)
		th := threadWith(key, fmt.Sprintf("3402823668417103999000000%08d", n), &slidetypes.User{InteropMessagingUserFBID: key, ID: fmt.Sprintf("1784%011d", n)}, start.Add(-time.Duration(n)*time.Hour))
		page := &slidetypes.MailboxPage{}
		page.ThreadsByFolder.Edges = []slidetypes.Node[slidetypes.WrappedThreadInfo]{{Node: slidetypes.WrappedThreadInfo{AsIGDirectThread: th}}}
		page.ThreadsByFolder.PageInfo = slidetypes.PageInfo{HasNextPage: true, EndCursor: fmt.Sprintf("inbox-cursor-%d", n+1)}
		return &slidetypes.PaginateMailboxResponse{Mailbox: page}, nil
	}
	pagesBy := map[string]int{}
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		pagesBy[igChatID]++
		n := pagesBy[igChatID]
		return messagesPage(true, fmt.Sprintf("older-%d", n),
			textMsg(fmt.Sprintf("mid.$invented-%s-%d", igChatID[len(igChatID)-4:], n), aliceFBI, start.Add(-time.Duration(n)*time.Minute), "invented")), nil
	}
	h.login(validSession())

	if n := h.m.count("PaginateMailbox"); n != 40 {
		t.Errorf("%d inbox pages, want 40", n)
	}
	if len(pagesBy) != 41 {
		t.Errorf("%d threads had their messages fetched, want 41", len(pagesBy))
	}
	for id, n := range pagesBy {
		if n != 5 {
			t.Errorf("thread …%s: %d message pages, want 5", id[len(id)-4:], n)
		}
	}
	gaps(t, h.m.requests())
	// Each page carried on from the last one's cursor.
	if c := h.m.calls; !slices.ContainsFunc(c, func(c call) bool { return c.name == "PaginateMessages" && strings.HasSuffix(c.arg, " older-4") }) {
		t.Errorf("no page went on from a cursor")
	}

	// It stops at a thread older than 365 days, and fetches no messages for
	// threads quiet for more than 90.
	h2 := newHarness(t)
	h2.m.mailbox = mailboxOf(true, threadWith(aliceFBI, aliceThread, alice(), start.Add(-100*24*time.Hour)))
	h2.m.mailboxPage = func(n int, cursor string) (*slidetypes.PaginateMailboxResponse, error) {
		age := 200 * 24 * time.Hour
		if n == 3 {
			age = 400 * 24 * time.Hour
		}
		th := threadWith(int64(3000000+n), fmt.Sprintf("34028236684171030000000001%07d", n), bob(), start.Add(-age))
		page := &slidetypes.MailboxPage{}
		page.ThreadsByFolder.Edges = []slidetypes.Node[slidetypes.WrappedThreadInfo]{{Node: slidetypes.WrappedThreadInfo{AsIGDirectThread: th}}}
		page.ThreadsByFolder.PageInfo = slidetypes.PageInfo{HasNextPage: true, EndCursor: "next"}
		return &slidetypes.PaginateMailboxResponse{Mailbox: page}, nil
	}
	h2.m.messagePage = h.m.messagePage
	h2.login(validSession())
	if n := h2.m.count("PaginateMailbox"); n != 3 {
		t.Errorf("%d inbox pages past a year-old thread, want 3", n)
	}
	if n := h2.m.count("PaginateMessages"); n != 0 {
		t.Errorf("%d message pages for threads quiet over 90 days, want 0", n)
	}

	// And at the end of the inbox: no next page, no request for one.
	h3 := newHarness(t)
	h3.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		return messagesPage(false, ""), nil
	}
	h3.login(validSession())
	if n := h3.m.count("PaginateMailbox"); n != 0 {
		t.Errorf("%d inbox pages after the last, want 0", n)
	}
	if !h3.chat(aliceChat).HistoryComplete {
		t.Errorf("a thread with no earlier page is not historyComplete")
	}

	// The next sign-in resumes from the saved state: no request at all (the
	// socket replays what it missed).
	before := len(h3.m.requests())
	h3.login(validSession())
	if got := h3.m.requests()[before:]; len(got) != 0 {
		t.Errorf("a second sign-in with saved state made %v, want none", got)
	}
	// Without it, the inbox is loaded and only threads active since are
	// asked again: none here.
	if err := h3.st.PutNetworkValue(Name, "state", ""); err != nil {
		t.Fatal(err)
	}
	h3.g.values.Drop()
	before = len(h3.m.requests())
	h3.login(validSession())
	if got := h3.m.requests()[before:]; len(got) != 1 || got[0].name != "LoadIndex" {
		t.Errorf("a second sign-in without saved state made %v, want LoadIndex only", got)
	}
}

func TestOlderPagesSetsHistoryCompleteAtTheStart(t *testing.T) {
	h := newHarness(t)
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		if cursor == "" {
			return messagesPage(true, "older-1", textMsg("mid.$invented-o1", aliceFBI, start.Add(-time.Hour), "newest")), nil
		}
		if cursor != "older-1" {
			return nil, fmt.Errorf("unexpected cursor")
		}
		return messagesPage(false, "", textMsg("mid.$invented-o0", selfFBID, start.Add(-48*time.Hour), "the very first")), nil
	}
	// The backfill reads the newest page only (the next fails).
	older := false
	h.m.messagePage = func(f func(string, string) (*slidetypes.PaginateMessagesResponse, error)) func(string, string) (*slidetypes.PaginateMessagesResponse, error) {
		return func(id, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
			if !older && cursor != "" {
				return nil, httpclient.ErrRequestFailed
			}
			return f(id, cursor)
		}
	}(h.m.messagePage)
	h.login(validSession())
	older = true
	if h.chat(aliceChat).HistoryComplete {
		t.Fatalf("complete before the start was reached")
	}
	changed := len(h.named("changed"))

	added, err := h.g.FetchOlder(context.Background(), aliceChat)
	if err != nil || added != 1 {
		t.Fatalf("FetchOlder = %d, %v; want 1 line", added, err)
	}
	if !h.chat(aliceChat).HistoryComplete {
		t.Errorf("no earlier page, and the chat is not historyComplete")
	}
	if _, ok := h.lines(aliceChat)["mid.$invented-o0"]; !ok {
		t.Errorf("the older line was not stored")
	}
	h.clock.Advance(time.Second)
	if len(h.named("changed")) == changed {
		t.Errorf("FetchOlder sent no changed event")
	}

	// A chat Meta never listed (only in Beeper's copy) asks nothing.
	calls := len(h.m.requests())
	if _, err := h.g.FetchOlder(context.Background(), "unplaced:invented"); kindErr(err) != wire.NotFound {
		t.Errorf("FetchOlder of an unknown chat = %v, want notFound", err)
	}
	if len(h.m.requests()) != calls {
		t.Errorf("FetchOlder of an unknown chat made a request")
	}
}

func TestRotatedCookiesEmitASessionEvent(t *testing.T) {
	h := newHarness(t)
	pages := 0
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		pages++
		return messagesPage(true, fmt.Sprintf("older-%d", pages), textMsg(fmt.Sprintf("mid.$invented-k%d", pages), aliceFBI, start.Add(-time.Duration(pages)*time.Hour), "x")), nil
	}
	h.login(validSession())
	if got := h.named("session"); len(got) != 0 {
		t.Fatalf("the session login gave came back unchanged: %v", got)
	}
	ck := h.m.last().params.Cookies

	// Meta rotates rur on a response: the next request after it sends the
	// whole named set, once.
	ck.Set("rur", "rur-invented-2")
	ck.Set("xs", "not-a-named-cookie")
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); err != nil {
		t.Fatal(err)
	}
	got := h.named("session")
	if len(got) != 1 {
		t.Fatalf("%d session events after a rotation, want 1", len(got))
	}
	want := map[string]string{"sessionid": "sess-invented-1", "csrftoken": "csrf-invented-1",
		"ds_user_id": "4000000099", "rur": "rur-invented-2", "mid": "mid-invented-1"}
	if s, _ := got[0]["session"].(map[string]string); fmt.Sprint(s) != fmt.Sprint(want) {
		t.Errorf("the session event is %v, want %v", s, want)
	}

	// Another rotation within the minute waits for it.
	ck.Set("rur", "rur-invented-3")
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); err != nil {
		t.Fatal(err)
	}
	if n := len(h.named("session")); n != 1 {
		t.Errorf("%d session events within a minute, want 1", n)
	}
	h.clock.Advance(time.Minute)
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); err != nil {
		t.Fatal(err)
	}
	if got := h.named("session"); len(got) != 2 || got[1]["session"].(map[string]string)["rur"] != "rur-invented-3" {
		t.Errorf("after the minute: %v", got)
	}
}

func TestARateLimitHoldsEveryRequestUntilItEnds(t *testing.T) {
	h := newHarness(t)
	limited := true
	h.m.messagePage = func(igChatID, cursor string) (*slidetypes.PaginateMessagesResponse, error) {
		if limited {
			return nil, httpclient.ErrRateLimited
		}
		return messagesPage(true, "older-1", textMsg("mid.$invented-l1", aliceFBI, start.Add(-time.Hour), "x")), nil
	}
	h.m.threadInfo = func(igChatID string) (*slidetypes.ThreadInfoResponse, error) {
		return &slidetypes.ThreadInfoResponse{ThreadInfo: slidetypes.WrappedThreadInfo{
			AsIGDirectThread: threadWith(bobFBID, igChatID, bob(), start)}}, nil
	}
	h.login(validSession()) // the backfill's first page is rate-limited
	_ = h.m.dispatch(&slidetypes.Connected{})
	if h.lastState() != "catchingUp" {
		t.Fatalf("a rate limit on the history changed the state: %s", h.lastState())
	}
	calls := len(h.m.requests())

	// Nothing goes for 15 minutes: not an older page, not a new thread.
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); kindErr(err) != wire.RateLimited {
		t.Errorf("FetchOlder during the block = %v, want rateLimited", err)
	}
	_ = h.m.dispatch(delta("340282366841710300000000000000022", &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-l2", bobFBID, start, "new")}))
	h.clock.Advance(14 * time.Minute)
	_ = h.m.dispatch(delta("340282366841710300000000000000022", &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-l3", bobFBID, start, "new")}))
	if got := h.m.requests()[calls:]; len(got) != 0 {
		t.Errorf("requests during the block: %v", got)
	}

	// After it, they go again.
	h.clock.Advance(2 * time.Minute)
	limited = false
	if _, err := h.g.FetchOlder(context.Background(), aliceChat); err != nil {
		t.Errorf("FetchOlder after the block = %v", err)
	}
	_ = h.m.dispatch(delta("340282366841710300000000000000022", &slidetypes.NewMessageEvent{Message: textMsg("mid.$invented-l4", bobFBID, start, "new")}))
	if h.m.count("GetThread") != 1 {
		t.Errorf("a new thread was not fetched after the block")
	}
}

func TestAnInstagramParticipantCarriesItsUsername(t *testing.T) {
	h := newHarness(t)
	named := &slidetypes.User{InteropMessagingUserFBID: aliceFBI, ID: aliceIG, FullName: "Alice Example", Username: "invented.user"}
	nameOnly := &slidetypes.User{InteropMessagingUserFBID: bobFBID, ID: bobIG, FullName: "Bob Example"}
	h.m.mailbox = mailboxOf(false,
		threadWith(aliceFBI, aliceThread, named, start.Add(-time.Hour)),
		threadWith(bobFBID, "340282366841710300000000000000022", nameOnly, start.Add(-2*time.Hour)))
	h.login(validSession())

	if p := h.chat(aliceChat).Participants; len(p) != 2 || p[0].ID != aliceIG || p[0].Handle != "invented.user" || p[1].Handle != "" {
		t.Errorf("the participants with a username are %+v", p)
	}
	if p := h.chat(strconv.FormatInt(bobFBID, 10)).Participants; len(p) != 2 || p[0].ID != bobIG || p[0].Handle != "" {
		t.Errorf("the participants with only a full name are %+v", p)
	}
}
