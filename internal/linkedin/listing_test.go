package linkedin

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-linkedin/pkg/linkedingo"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The listing of the folders: INBOX, then ARCHIVE, each by cursor. Every
// URN, cursor and text here is invented, and every request goes to the fake.

type cursorPageFn = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation]

// folderFeed is a folder of pages chats long: page p (cursor prefix+p; the
// first has none) holds one chat numbered base+p, last active days[p] days
// back (the last page's list ends with no next cursor).
func folderFeed(prefix string, base int, days []int) cursorPageFn {
	return func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		p := 0
		if cursor != "" {
			fmt.Sscanf(strings.TrimPrefix(cursor, prefix), "%d", &p)
		}
		next := ""
		if p+1 < len(days) {
			next = fmt.Sprintf("%s%d", prefix, p+1)
		}
		return cursorPageOf(next, conv(base+p, start.Add(-time.Duration(days[p])*24*time.Hour)))
	}
}

// listed is the (folder, cursor) of every listing request made, in order.
func listed(r *rig) []string {
	var out []string
	for _, c := range r.f.callsNow() {
		if c.name == "GetConversationsWithNextCursor" {
			out = append(out, c.cat+":"+c.arg)
		}
	}
	return out
}

func connect(t *testing.T, r *rig) {
	t.Helper()
	if err := r.login(validSession()); err != nil {
		t.Fatal(err)
	}
	r.waitState(network.Live, "")
	r.idle()
}

func TestTheListingAsksInboxThenArchiveByCursor(t *testing.T) {
	r := newRig(t)
	// INBOX: the third page is over a year back, so it is the last.
	r.f.cursorPage = folderFeed("i-invented-", 10, []int{1, 5, 400, 500})
	// ARCHIVE: the second page is over a year back.
	r.f.archivePage = folderFeed("a-invented-", 50, []int{30, 380, 600})
	connect(t, r)

	want := []string{"INBOX:", "INBOX:i-invented-1", "INBOX:i-invented-2", "ARCHIVE:", "ARCHIVE:a-invented-1"}
	if got := listed(r); !slices.Equal(got, want) {
		t.Errorf("listing requests\n%v\nwant\n%v", got, want)
	}
	for _, n := range []int{10, 11, 12, 50, 51} {
		r.chat(convURN(n))
	}
	for _, n := range []int{13, 52} {
		if _, ok, _ := r.st.Chat(Name, convURN(n)); ok {
			t.Errorf("chat %d, past a folder's stop, was listed", n)
		}
	}
	if r.value("listed") == "" || r.value("listedFolders") != "2" || r.value("listCursor") != "" {
		t.Errorf("marks: listed=%q listedFolders=%q listCursor=%q", r.value("listed"), r.value("listedFolders"), r.value("listCursor"))
	}
}

func TestEachFolderStopsAtFortyPages(t *testing.T) {
	r := newRig(t)
	page := 0
	endless := func(prefix string, base int) cursorPageFn {
		return func(string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
			page++
			return cursorPageOf(fmt.Sprintf("%s%d", prefix, page), conv(base+page, start.Add(-time.Duration(page)*time.Minute)))
		}
	}
	r.f.cursorPage = endless("i-invented-", 1000)
	r.f.archivePage = endless("a-invented-", 3000)
	connect(t, r)
	if i, a := r.f.countIn("INBOX"), r.f.countIn("ARCHIVE"); i != 40 || a != 40 {
		t.Errorf("%d INBOX and %d ARCHIVE pages, want 40 of each", i, a)
	}
}

func TestARefusedFolderIsSkippedAndTheOthersGoOn(t *testing.T) {
	refusals := map[string]error{"a 400": statusErr(400), "no GraphQL data": linkedingo.ErrMissingGraphQLData}
	for name, refusal := range refusals {
		for _, refused := range []string{"ARCHIVE", "INBOX"} {
			t.Run(name+" on "+refused, func(t *testing.T) {
				r := newRig(t)
				r.f.cursorPage = folderFeed("i-invented-", 10, []int{1, 2})
				r.f.archivePage = folderFeed("a-invented-", 50, []int{3, 4})
				r.f.setFolderErr(refused, refusal)
				connect(t, r)

				kept, lost := []int{10, 11}, []int{50, 51}
				if refused == "INBOX" {
					kept, lost = lost, kept
				}
				for _, n := range kept {
					r.chat(convURN(n))
				}
				for _, n := range lost {
					if _, ok, _ := r.st.Chat(Name, convURN(n)); ok {
						t.Errorf("chat %d of the refused folder was listed", n)
					}
				}
				if got := r.f.countIn(refused); got != 1 {
					t.Errorf("the refused folder was asked %d times, want once", got)
				}
				if st, k := r.l.stateNow(); st != network.Live || k != "" {
					t.Errorf("state = %s/%s, want live", st, k)
				}
				if r.value("listedFolders") != "2" {
					t.Errorf("a refused folder left the listing undone")
				}
			})
		}
	}
}

func TestAPauseDuringTheSecondFolderStopsEverything(t *testing.T) {
	for _, code := range []int{403, 429, 999} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			r := newRig(t)
			r.f.cursorPage = folderFeed("i-invented-", 10, []int{1, 2})
			r.f.archivePage = folderFeed("a-invented-", 50, []int{3, 4})
			r.f.setFolderErr("ARCHIVE", statusErr(code))
			if err := r.login(validSession()); err != nil {
				t.Fatal(err)
			}
			r.waitState(network.Offline, wire.RateLimited)
			r.idle()
			until, blocked := r.l.pause.blocked()
			if !blocked {
				t.Fatalf("a %d on the second folder did not pause", code)
			}
			// INBOX's chats landed; ARCHIVE was asked once; nothing else
			// goes until the pause ends.
			r.chat(convURN(10))
			r.chat(convURN(11))
			if r.f.countIn("ARCHIVE") != 1 {
				t.Errorf("ARCHIVE asked %d times, want once", r.f.countIn("ARCHIVE"))
			}
			if r.value("listedFolders") != "" || r.value("listed") != "" {
				t.Errorf("an unfinished listing was marked done (listed=%q listedFolders=%q)", r.value("listed"), r.value("listedFolders"))
			}
			if got := r.value("listCursor"); got != "ARCHIVE:" {
				t.Errorf("listCursor = %q, want the second folder's start", got)
			}
			n := len(r.f.callsNow())
			r.clock.Advance(14 * time.Minute)
			never(t, "a request went during the pause", func() bool { return len(r.f.callsNow()) > n })
			if now := r.clock.Now(); !now.Before(until) {
				t.Fatalf("the clock passed the pause's end")
			}
		})
	}
}

func TestAnInterruptedListingResumesInItsFolder(t *testing.T) {
	r := newRig(t)
	r.keep("listCursor", "ARCHIVE:a-invented-1")
	r.f.cursorPage = folderFeed("i-invented-", 10, []int{1, 2})
	r.f.archivePage = folderFeed("a-invented-", 50, []int{3, 4, 5})
	connect(t, r)
	want := []string{"ARCHIVE:a-invented-1", "ARCHIVE:a-invented-2"}
	if got := listed(r); !slices.Equal(got, want) {
		t.Errorf("listing requests\n%v\nwant\n%v", got, want)
	}
}

func TestAnAccountListedBeforeListsTheFoldersOnce(t *testing.T) {
	r := newRig(t)
	// As an earlier version left it: signed in, a token, listed, no
	// listedFolders, and chat 1 stored.
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-7")
	r.keep("listed", "1")
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	r.f.cursorPage = folderFeed("i-invented-", 10, []int{1, 2})
	r.f.archivePage = folderFeed("a-invented-", 50, []int{3, 4})
	var liveAt time.Time
	r.f.before = func(c string, _ time.Time, _ int) *linkedingo.CollectionResponse[linkedingo.MessageMetadata, linkedingo.Message] {
		if st, _ := r.l.stateNow(); st != network.Live {
			t.Errorf("messages of %s fetched in state %s, want live (chats first)", c, st)
		} else if liveAt.IsZero() {
			liveAt = r.clock.Now()
		}
		return messagePage("")
	}
	connect(t, r)

	want := []string{"INBOX:", "INBOX:i-invented-1", "ARCHIVE:", "ARCHIVE:a-invented-1"}
	if got := listed(r); !slices.Equal(got, want) {
		t.Fatalf("listing requests\n%v\nwant\n%v", got, want)
	}
	for _, n := range []int{10, 11, 50, 51} {
		r.chat(convURN(n))
	}
	if r.value("listedFolders") != "2" || r.value("listCursor") != "" {
		t.Errorf("marks: listedFolders=%q listCursor=%q", r.value("listedFolders"), r.value("listCursor"))
	}
	// The messages of the new chats come behind, from the background work.
	asked := map[string]bool{}
	for _, c := range r.f.callsNow() {
		if c.name == "GetMessagesBefore" {
			asked[c.arg] = true
		}
	}
	for _, n := range []int{10, 11, 50, 51} {
		if !asked[convURN(n)] {
			t.Errorf("the messages of chat %d were not fetched behind the listing", n)
		}
	}

	// Not again at the next start.
	n := len(r.f.callsNow())
	r.restart()
	connect(t, r)
	if names := r.f.namesSince(n); slices.Contains(names, "GetConversationsWithNextCursor") {
		t.Errorf("a second start listed the folders again: %v", names)
	}
}

func TestAnAccountListedBeforeWithADroppedTokenListsOnce(t *testing.T) {
	r := newRig(t)
	r.keep("self", selfID)
	r.keep("syncToken", "sync-token-invented-7")
	r.keep("listed", "1")
	r.f.failOnce("GetConversationsBySyncToken", statusErr(400))
	r.f.cursorPage = folderFeed("i-invented-", 10, []int{1})
	r.f.archivePage = folderFeed("a-invented-", 50, []int{2})
	connect(t, r)
	if got, want := listed(r), []string{"INBOX:", "ARCHIVE:"}; !slices.Equal(got, want) {
		t.Errorf("listing requests %v, want one pass over each folder %v", got, want)
	}
}

func TestAGapListingStopsAtTheSameStoredActivityInEveryFolder(t *testing.T) {
	r := newRig(t)
	r.f.syncPage = func(string) *linkedingo.CollectionResponse[linkedingo.ConversationSyncMetadata, linkedingo.Conversation] {
		return syncPageOf([]linkedingo.Conversation{conv(1, start.Add(-time.Hour))})
	}
	c := r.live()
	c.dispatch(&linkedingo.ClientConnection{})
	r.idle()

	// A new realtime session: INBOX brings chat 2, newer than the newest
	// stored (an hour ago); ARCHIVE's first page is older than that, so
	// ARCHIVE is asked once, and not paged on to chat 52.
	r.f.cursorPage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		if cursor == "" {
			return cursorPageOf("i-invented-1", conv(2, start.Add(20*time.Minute)))
		}
		return cursorPageOf("", conv(1, start.Add(-time.Hour)))
	}
	r.f.archivePage = func(cursor string) *linkedingo.CollectionResponse[linkedingo.ConversationCursorMetadata, linkedingo.Conversation] {
		if cursor != "" {
			t.Errorf("ARCHIVE paged past the newest stored activity, to %q", cursor)
		}
		return cursorPageOf("a-invented-1", conv(51, start.Add(-3*time.Hour)))
	}
	c.dispatch(&linkedingo.ClientConnection{SessID: [16]byte{9, 8, 7}})
	r.idle()
	want := []string{"INBOX:", "INBOX:i-invented-1", "ARCHIVE:"}
	if got := listed(r); !slices.Equal(got, want) {
		t.Errorf("listing requests\n%v\nwant\n%v", got, want)
	}
	r.chat(convURN(2))
}

func TestTheListingKeepsItsPaceAcrossFolders(t *testing.T) {
	r := newRig(t)
	r.f.cursorPage = folderFeed("i-invented-", 10, []int{1, 2, 3})
	r.f.archivePage = folderFeed("a-invented-", 50, []int{4, 5, 6})
	connect(t, r)
	var last time.Time
	var n int
	var crossed bool
	for _, c := range r.f.callsNow() {
		if c.name != "GetConversationsWithNextCursor" {
			continue
		}
		n++
		if !last.IsZero() {
			if gap := c.at.Sub(last); gap < 2*time.Second || gap > 4*time.Second {
				t.Errorf("%s:%s went %v after the request before it, want 2 to 4 s", c.cat, c.arg, gap)
			}
			if c.cat == "ARCHIVE" && c.arg == "" {
				crossed = true
			}
		}
		last = c.at
	}
	if n != 6 || !crossed {
		t.Errorf("%d listing requests, crossed into ARCHIVE: %v; want 6 and true", n, crossed)
	}
	if r.f.maxIn > 1 {
		t.Errorf("%d requests at once, want one at a time", r.f.maxIn)
	}
}
