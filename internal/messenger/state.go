package messenger

import (
	"database/sql"
	"sync"
	"sync/atomic"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"

	"github.com/matwhiting/pllum-connect/internal/network"
)

// The Messenger's state besides what m.mu guards (the session, the run,
// the sockets, the state, the redials): each group below is guarded by its
// own mu, which guards that group's fields and nothing else. m.mu is held
// while gauge.mu is taken (the counts follow the sockets). devStore.mu and
// restore.mu are also held across requests to Meta (one registration, one
// restore at a time), so m.mu and the others are taken under them.

// deviceStore is the whatsmeow device store (e2ee.go).
type deviceStore struct {
	mu        sync.Mutex
	db        *sql.DB
	container *sqlstore.Container
	dev       *wastore.Device // his registered, saved device
	// pending is a device made for pendingFor whose registration or save
	// failed in this helper: the next sign-in registers its keys again.
	pending    *wastore.Device
	pendingFor int64
	// replacing is the device a re-registration replaces (reregister):
	// while it is set, the pending device is registered in its place and
	// its row deleted once that has gone.
	replacing *wastore.Device
}

// restoreState is his backup as the history check found it (history.go):
// mu is also held through a whole RestoreHistory, so there is one restore
// at a time.
type restoreState struct {
	mu     sync.Mutex
	backup *securehistory.Backup
}

// backupBook serialises the read-change-write of the backup's bookkeeping
// kept in the store (enc_history.go): the history device's keys as pages
// move them on, the run of failed backup requests and its pause, and the
// day's pages asked on demand.
type backupBook struct {
	mu sync.Mutex
}

// diagFile is diagnostics.txt (diag.go): one line written at a time.
type diagFile struct {
	mu sync.Mutex
}

// fills are the chats whose newest backup page is due (catchup.go): due two
// minutes on, again an hour on. Both are in memory only: a restart forgets
// them, and the daily catch-up still reads the chat.
type fills struct {
	mu    sync.Mutex
	due   map[string]bool
	again map[string]bool
}

// gauge is what the gate counts (gate.go), and the history state it
// reports (history.go).
type gauge struct {
	mu             sync.Mutex
	counts         GateCounts
	listing        listing
	newestOneToOne int64  // the thread key of the newest encrypted one-to-one, 0 for none
	newestAt       int64  // its last activity, in ms
	history        string // historyState: "", noBackup, needsPIN, restored, skipped

	liveEncryptedReceived atomic.Int64
	liveUndecryptable     atomic.Int64
}

// runMemo is what the network remembers in memory only, this run (and
// across runs, never across a restart): people's names from contact rows,
// which chat a line seen lately is in (for an edit that names only the
// line), the FetchOlder waiting for each thread's page, and the lock that
// keeps one history fetch per chat at a time.
type runMemo struct {
	mu          sync.Mutex
	names       map[int64]string
	recent      map[string]string // a line seen → its chat
	recentOrder []string
	waiters     map[int64]chan rangeNote // a thread key → the FetchOlder waiting for its page
	locks       map[string]*sync.Mutex
}

// sends are the sends waiting their turn (send.go): one goroutine sends
// them in order; resume is held while a failed send is put back in line, so
// it is put there once.
type sends struct {
	outbox network.Outbox
	resume sync.Mutex
	busy   atomic.Int64 // sends in line or going
}
