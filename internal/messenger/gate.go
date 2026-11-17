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
	// Groups are the unencrypted group threads on the page's first listing.
	Groups int `json:"groups"`
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

// cutoverFolder is the folder prefix of an older thread moved aside when its
// chat became end-to-end encrypted (connector events.go: e2ee_cutover and
// its _archived, _pending and _other forms).
const cutoverFolder = "e2ee_cutover"

// list counts the messages page's first listing for the gate, and finds the
// newest encrypted one-to-one (the one whose history the gate samples).
func (m *Messenger) list(tbl *table.LSTable) {
	type thread struct {
		key       int64
		encrypted bool
	}
	var c GateCounts
	var newest, newestAt int64
	seen := map[thread]bool{}
	pointers := map[int64]bool{}
	add := func(key int64, tt table.ThreadType, folder string, lastMs int64) {
		if key == 0 {
			return
		}
		if strings.HasPrefix(folder, cutoverFolder) && !tt.IsWhatsApp() {
			pointers[key] = true
		}
		t := thread{key, tt.IsWhatsApp()}
		if seen[t] {
			return
		}
		seen[t] = true
		switch {
		case tt.IsWhatsApp():
			c.EncryptedThreads++
			if tt == table.ENCRYPTED_OVER_WA_ONE_TO_ONE {
				c.EncryptedOneToOnes++
				if lastMs > newestAt || newest == 0 {
					newest, newestAt = key, lastMs
				}
			}
		case isGroup(tt):
			c.Groups++
		}
	}
	if tbl != nil {
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
			pointers[p.ThreadKey] = true
		}
		for _, p := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
			pointers[p.ThreadKey] = true
		}
		for _, p := range tbl.LSMoveThreadToE2EECutoverFolder {
			pointers[p.ThreadKey] = true
		}
	}
	delete(pointers, 0)
	c.EncryptedPointers = len(pointers)
	c.Loaded = true

	m.cmu.Lock()
	defer m.cmu.Unlock()
	c.Registered, c.RegisterMs, c.E2EEConnected, c.E2EEFailed = m.counts.Registered, m.counts.RegisterMs, m.counts.E2EEConnected, m.counts.E2EEFailed
	c.HistoryState, c.RestoredMessages = m.counts.HistoryState, m.counts.RestoredMessages
	m.counts = c
	m.newestOneToOne = newest
}

// isGroup reports whether an unencrypted thread type is a group chat.
func isGroup(tt table.ThreadType) bool {
	switch tt {
	case table.GROUP_THREAD, table.TINCAN_GROUP_DISAPPEARING, table.CARRIER_MESSAGING_GROUP, table.COMMUNITY_GROUP, table.XAC_GROUP:
		return true
	}
	return false
}
