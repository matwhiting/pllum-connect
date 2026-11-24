package messenger

import (
	"cmp"
	"maps"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// The gate (Task B0): before anything else of Messenger is built, Matthew
// signs in once with the helper started with --gate, and these counts say
// whether Pllum can read his encrypted chats. `gateCounts` is a hidden
// method the server answers only with --gate. Counts only: numbers,
// booleans and historyState's fixed words, never an ID, a name or text.

// GateCounts is what `gateCounts` answers.
type GateCounts struct {
	// Loaded is true once the messages page has loaded.
	Loaded bool `json:"loaded"`
	// Groups are the unencrypted group threads in his inbox.
	Groups int `json:"groups"`
	// ListPages are the pages of threads fetched after the messages page's
	// first listing, every folder together; ListComplete says every folder's
	// listing ended (Meta had no more, or it reached a thread over a year
	// old), ListCapped that one stopped at its page cap with more to come.
	ListPages    int  `json:"listPages"`
	ListComplete bool `json:"listComplete"`
	ListCapped   bool `json:"listCapped"`
	// EncryptedPointers are the older threads that point at an encrypted
	// one (a hybrid thread mapping, or the e2ee cutover folder), each once.
	EncryptedPointers int `json:"encryptedPointers"`
	// EncryptedThreads are the encrypted threads (one-to-ones and groups);
	// EncryptedOneToOnes the one-to-ones among them.
	EncryptedThreads   int `json:"encryptedThreads"`
	EncryptedOneToOnes int `json:"encryptedOneToOnes"`
	// Registered: Pllum is a Messenger device, registered now or before.
	// RegisterMs is how long this run's registration took (0 when the device
	// was registered before).
	Registered bool  `json:"registered"`
	RegisterMs int64 `json:"registerMs"`
	// MainConnected: the main (lightspeed) socket is up.
	MainConnected bool `json:"mainConnected"`
	// E2EEConnected: the encrypted socket is up. E2EEFailed: Meta ended the
	// encrypted session during this run.
	E2EEConnected bool `json:"e2eeConnected"`
	E2EEFailed    bool `json:"e2eeFailed"`
	// HistoryState is encryptedHistory's state (empty until looked at), and
	// RestoredMessages how many lines one page of the newest encrypted
	// one-to-one's restored history decrypted to.
	HistoryState     string `json:"historyState"`
	RestoredMessages int    `json:"restoredMessages"`
	// The encrypted messages that arrived on the socket since the helper
	// started, and those that could not be decrypted.
	LiveEncryptedReceived int64 `json:"liveEncryptedReceived"`
	LiveUndecryptable     int64 `json:"liveUndecryptable"`

	// The breakdown (Task B1), so a gap against Beeper's counts can be
	// explained without an ID: every thread listed (an older thread and
	// its encrypted one apart), by the folder Meta files it in (its folder
	// name, a fixed word), by thread type, by sync group (1 the inbox, 95
	// message requests), and how many were last active over a year ago.
	// FolderStops is how each folder's listing ended (complete,
	// yearReached, capped, failed). OtherParents are the parents besides
	// the inbox that hold threads (Marketplace, communities), and
	// InOtherParents the threads under them. NoActivity are threads listed
	// with no last activity (a bare "this thread exists").
	FolderStops         map[string]int `json:"folderStops"`
	ByFolder            map[string]int `json:"byFolder"`
	ByType              map[string]int `json:"byType"`
	BySyncGroup         map[string]int `json:"bySyncGroup"`
	OlderThanYear       int            `json:"olderThanYear"`
	OlderThanYearByType map[string]int `json:"olderThanYearByType"`
	OtherParents        int            `json:"otherParents"`
	InOtherParents      int            `json:"inOtherParents"`
	NoActivity          int            `json:"noActivity"`

	// OneToOneChats and GroupChats are the distinct chats the threads listed
	// make, after aliasing (review I3): one-to-ones (<FBID>@msgr, encrypted
	// or not) and groups (unencrypted and <id>@g.us). These, not the
	// encrypted one-to-ones alone, compare with Beeper's one-to-ones and
	// groups. Spam, folders and Marketplace or page threads are in neither.
	OneToOneChats int `json:"oneToOneChats"`
	GroupChats    int `json:"groupChats"`
}

// GateCounts is the gate's counts now.
func (m *Messenger) GateCounts() any {
	yearAgo := m.clock.Now().Add(-yearReach).UnixMilli()
	m.gauge.mu.Lock()
	c := m.gauge.counts
	c.FolderStops = maps.Clone(m.gauge.counts.FolderStops)
	if c.FolderStops == nil {
		c.FolderStops = map[string]int{}
	}
	c.ByFolder, c.ByType, c.BySyncGroup, c.OlderThanYearByType = map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}
	for t, s := range m.gauge.listing.threads {
		tw := typeWord(t.encrypted, s.tt)
		c.ByFolder[folderWord(s.folder)]++
		c.ByType[tw]++
		c.BySyncGroup[groupWord(s.group)]++
		if s.parent != 0 && s.parent != topParent {
			c.InOtherParents++
		}
		switch {
		case s.lastMs <= 0:
			c.NoActivity++
		case s.lastMs < yearAgo:
			c.OlderThanYear++
			c.OlderThanYearByType[tw]++
		}
	}
	for p := range m.gauge.listing.parents {
		if p != topParent {
			c.OtherParents++
		}
	}
	type placed struct {
		key    int64
		tt     table.ThreadType
		folder string
	}
	threads := make([]placed, 0, len(m.gauge.listing.threads))
	for t, s := range m.gauge.listing.threads {
		threads = append(threads, placed{t.key, s.tt, s.folder})
	}
	m.gauge.mu.Unlock()
	ones, groups := map[string]bool{}, map[string]bool{}
	for _, t := range threads {
		p, ok := placeThread(t.key, t.tt)
		if !ok || t.folder == folderSpam {
			continue
		}
		chat := p.chat
		if !p.enc {
			if got, ok := m.st.AliasTarget(Name, keyID(t.key)); ok {
				chat = got
			}
		}
		switch {
		case strings.HasSuffix(chat, oneToOneSuffix):
			ones[chat] = true
		case strings.HasSuffix(chat, groupSuffix) || p.kind == "group":
			groups[chat] = true
		}
	}
	c.OneToOneChats, c.GroupChats = len(ones), len(groups)
	c.LiveEncryptedReceived = m.gauge.liveEncryptedReceived.Load()
	c.LiveUndecryptable = m.gauge.liveUndecryptable.Load()
	return c
}

// knownFolders are the folder names the library's connector knows; any
// other is counted as otherFolder, so no unexpected string leaves.
var knownFolders = map[string]bool{
	"inbox": true, "other": true, "spam": true, "pending": true, "montage": true, "hidden": true, "legacy": true,
	"disabled": true, "page_background": true, "page_done": true, "blocked": true, "community": true,
	"restricted": true, "bc_partnership": true, "e2ee_cutover": true, "e2ee_cutover_archived": true,
	"e2ee_cutover_pending": true, "e2ee_cutover_other": true, "interop": true, "archived": true,
	"ai_active": true, "salsa_restricted": true, "messenger_marketing_message": true,
}

func folderWord(folder string) string {
	switch {
	case folder == "":
		return "noFolder"
	case knownFolders[folder]:
		return folder
	}
	return "otherFolder"
}

func groupWord(group int64) string {
	switch group {
	case 1:
		return "1"
	case 95:
		return "95"
	}
	return "otherGroup"
}

// typeWord is a fixed name for a listed thread's type.
func typeWord(encrypted bool, tt table.ThreadType) string {
	switch {
	case encrypted && tt == table.ENCRYPTED_OVER_WA_GROUP:
		return "encryptedGroup"
	case encrypted:
		return "encryptedOneToOne"
	case tt == table.COMMUNITY_GROUP:
		return "communityGroup"
	case tt == table.MARKETPLACE:
		return "marketplace"
	case tt == table.FOLDER || tt == table.COMMUNITY_FOLDER || tt == table.COMMUNITY_CHANNEL_CATEGORY:
		return "folder"
	case tt.IsOneToOne():
		return "oneToOne"
	case isGroup(tt):
		return "group"
	}
	return "otherType"
}

// cutoverFolder is the folder prefix of an older thread moved aside when its
// chat became end-to-end encrypted (connector events.go: e2ee_cutover and
// its _archived, _pending and _other forms).
const cutoverFolder = "e2ee_cutover"

// listing is what the gate and the listing have seen of his inbox: every
// thread (an older thread and its encrypted one apart) with where it was
// found, every older thread that points at an encrypted one, every parent
// (folder) a thread or the messages page names, and how far each folder's
// listing reaches. It only grows, from the messages page, the pages and the
// main socket's tables, so a thread repeated is counted once.
type listing struct {
	threads  map[listedThread]*seenThread
	pointers map[int64]bool
	parents  map[int64]bool
	ranges   map[folderKey]folderRange
	// configParents are the parents the messages page names
	// (LSThreadsRangesQuery): the only ones the library lists sync group 95
	// under.
	configParents map[int64]bool
}

type listedThread struct {
	key       int64
	encrypted bool
}

// seenThread is where a thread was found, and its last activity.
type seenThread struct {
	tt            table.ThreadType
	folder        string
	group, parent int64
	lastMs        int64
}

// addTable adds a table's threads to the listing and the gate's counts,
// and keeps the newest encrypted one-to-one (the one whose history the
// gate samples).
func (m *Messenger) addTable(tbl *table.LSTable) {
	if tbl == nil {
		return
	}
	m.gauge.mu.Lock()
	defer m.gauge.mu.Unlock()
	l := &m.gauge.listing
	if l.threads == nil {
		l.threads, l.pointers = map[listedThread]*seenThread{}, map[int64]bool{}
		l.parents, l.ranges = map[int64]bool{}, map[folderKey]folderRange{}
		l.configParents = map[int64]bool{}
	}
	c := &m.gauge.counts
	add := func(key int64, tt table.ThreadType, folder string, group, parent, lastMs int64) {
		if key == 0 {
			return
		}
		if strings.HasPrefix(folder, cutoverFolder) && !tt.IsWhatsApp() {
			l.pointers[key] = true
		}
		if tt == table.ENCRYPTED_OVER_WA_ONE_TO_ONE && (lastMs > m.gauge.newestAt || m.gauge.newestOneToOne == 0) {
			m.gauge.newestOneToOne, m.gauge.newestAt = key, lastMs
		}
		if parent != 0 {
			l.parents[parent] = true
		}
		if tt == table.FOLDER || tt == table.COMMUNITY_FOLDER {
			l.parents[key] = true
		}
		t := listedThread{key, tt.IsWhatsApp()}
		if s := l.threads[t]; s != nil {
			s.lastMs = max(s.lastMs, lastMs)
			s.folder = cmp.Or(folder, s.folder)
			s.group = cmp.Or(group, s.group)
			s.parent = cmp.Or(parent, s.parent)
			if tt != table.UNKNOWN_THREAD_TYPE {
				s.tt = tt
			}
			return
		}
		l.threads[t] = &seenThread{tt: tt, folder: folder, group: group, parent: parent, lastMs: lastMs}
		switch {
		case tt.IsWhatsApp():
			c.EncryptedThreads++
			if tt == table.ENCRYPTED_OVER_WA_ONE_TO_ONE {
				c.EncryptedOneToOnes++
			}
		case isGroup(tt):
			c.Groups++
		}
	}
	for _, t := range tbl.LSDeleteThenInsertThread {
		add(t.ThreadKey, t.ThreadType, t.FolderName, t.SyncGroup, t.ParentThreadKey, t.LastActivityTimestampMs)
	}
	for _, t := range tbl.LSUpdateOrInsertThread {
		add(t.ThreadKey, t.ThreadType, t.FolderName, t.SyncGroup, t.ParentThreadKey, t.LastActivityTimestampMs)
	}
	for _, t := range tbl.LSVerifyThreadExists {
		add(t.ThreadKey, t.ThreadType, t.FolderName, t.SyncGroup, t.ParentThreadKey, 0)
	}
	for _, p := range tbl.LSVerifyHybridThreadExists {
		l.pointers[p.ThreadKey] = true
	}
	for _, p := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
		l.pointers[p.ThreadKey] = true
	}
	for _, p := range tbl.LSMoveThreadToE2EECutoverFolder {
		l.pointers[p.ThreadKey] = true
	}
	delete(l.pointers, 0)
	for _, q := range tbl.LSThreadsRangesQuery {
		if q.ParentThreadKey != 0 {
			l.parents[q.ParentThreadKey] = true
			l.configParents[q.ParentThreadKey] = true
		}
	}
	for _, f := range tbl.LSUpsertFolder {
		if f.ThreadKey != 0 {
			l.parents[f.ThreadKey] = true
		}
	}
	for _, r := range tbl.LSUpsertSyncGroupThreadsRange {
		l.ranges[folderKey{r.SyncGroup, r.ParentThreadKey}] = folderRange{minKey: r.MinThreadKey, minAt: r.MinLastActivityTimestampMS, more: r.HasMoreBefore}
		if r.ParentThreadKey != 0 {
			l.parents[r.ParentThreadKey] = true
		}
	}
	for _, r := range tbl.LSUpsertInboxThreadsRange {
		l.ranges[folderKey{r.SyncGroup, topParent}] = folderRange{minKey: r.MinThreadKey, minAt: r.MinLastActivityTimestampMs, more: r.HasMoreBefore}
	}
	c.EncryptedPointers = len(l.pointers)
	c.Loaded = true
}

// rangeOf is how far a folder's listing reaches, as Meta last said.
func (m *Messenger) rangeOf(f folderKey) (folderRange, bool) {
	m.gauge.mu.Lock()
	defer m.gauge.mu.Unlock()
	r, ok := m.gauge.listing.ranges[f]
	return r, ok
}

// listed records how a listing of every folder ended.
func (m *Messenger) listed(complete, capped bool) {
	m.gauge.mu.Lock()
	defer m.gauge.mu.Unlock()
	m.gauge.counts.ListComplete, m.gauge.counts.ListCapped = complete, capped
}

// isGroup reports whether an unencrypted thread type is a group chat.
func isGroup(tt table.ThreadType) bool {
	switch tt {
	case table.GROUP_THREAD, table.TINCAN_GROUP_DISAPPEARING, table.CARRIER_MESSAGING_GROUP, table.COMMUNITY_GROUP, table.XAC_GROUP:
		return true
	}
	return false
}
