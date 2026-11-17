package messenger

import (
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
	// ListPages are the older pages of threads fetched after the messages
	// page's first listing; ListComplete says the listing ended, ListCapped
	// that it stopped at maxThreadPages with more to come.
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
}

// GateCounts is the gate's counts now.
func (m *Messenger) GateCounts() any {
	m.cmu.Lock()
	c := m.counts
	m.cmu.Unlock()
	c.LiveEncryptedReceived = m.liveEncryptedReceived.Load()
	c.LiveUndecryptable = m.liveUndecryptable.Load()
	return c
}

// maxThreadPages is the most older pages of threads one run fetches.
const maxThreadPages = 20

// cutoverFolder is the folder prefix of an older thread moved aside when its
// chat became end-to-end encrypted (connector events.go: e2ee_cutover and
// its _archived, _pending and _other forms).
const cutoverFolder = "e2ee_cutover"

// listing is what the gate has seen of his inbox: every thread (an older
// thread and its encrypted one apart), and every older thread that points
// at an encrypted one. It only grows, from the messages page, the older
// pages and the main socket's tables, so a thread repeated is counted once.
type listing struct {
	threads  map[listedThread]bool
	pointers map[int64]bool
}

type listedThread struct {
	key       int64
	encrypted bool
}

// addTable adds a table's threads to the gate's counts, and keeps the newest
// encrypted one-to-one (the one whose history the gate samples).
func (m *Messenger) addTable(tbl *table.LSTable) {
	if tbl == nil {
		return
	}
	m.cmu.Lock()
	defer m.cmu.Unlock()
	l := &m.listing
	if l.threads == nil {
		l.threads, l.pointers = map[listedThread]bool{}, map[int64]bool{}
	}
	c := &m.counts
	add := func(key int64, tt table.ThreadType, folder string, lastMs int64) {
		if key == 0 {
			return
		}
		if strings.HasPrefix(folder, cutoverFolder) && !tt.IsWhatsApp() {
			l.pointers[key] = true
		}
		if tt == table.ENCRYPTED_OVER_WA_ONE_TO_ONE && (lastMs > m.newestAt || m.newestOneToOne == 0) {
			m.newestOneToOne, m.newestAt = key, lastMs
		}
		t := listedThread{key, tt.IsWhatsApp()}
		if l.threads[t] {
			return
		}
		l.threads[t] = true
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
		add(t.ThreadKey, t.ThreadType, t.FolderName, t.LastActivityTimestampMs)
	}
	for _, t := range tbl.LSUpdateOrInsertThread {
		add(t.ThreadKey, t.ThreadType, t.FolderName, t.LastActivityTimestampMs)
	}
	for _, t := range tbl.LSVerifyThreadExists {
		add(t.ThreadKey, t.ThreadType, t.FolderName, 0)
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
	c.EncryptedPointers = len(l.pointers)
	c.Loaded = true
}

// listed records how far the older pages of his inbox got.
func (m *Messenger) listed(pages int, complete, capped bool) {
	m.cmu.Lock()
	defer m.cmu.Unlock()
	m.counts.ListPages, m.counts.ListComplete, m.counts.ListCapped = pages, complete, capped
}

// isGroup reports whether an unencrypted thread type is a group chat.
func isGroup(tt table.ThreadType) bool {
	switch tt {
	case table.GROUP_THREAD, table.TINCAN_GROUP_DISAPPEARING, table.CARRIER_MESSAGING_GROUP, table.COMMUNITY_GROUP, table.XAC_GROUP:
		return true
	}
	return false
}
