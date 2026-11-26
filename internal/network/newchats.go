package network

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Recipient is a person a new message is for, by one exact address: their
// number (Phone, "+" and digits) or their ID on the network (UserID).
// Exactly one is set.
type Recipient struct {
	Phone  string
	UserID string
}

// Valid reports whether exactly one address is set, and a number is "+"
// and 6 to 15 digits.
func (r Recipient) Valid() bool {
	if (r.Phone == "") == (r.UserID == "") {
		return false
	}
	if r.Phone == "" {
		return true
	}
	digits, ok := strings.CutPrefix(r.Phone, "+")
	if !ok || len(digits) < 6 || len(digits) > 15 {
		return false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// WhatsAppNewChatsPerDay is how many new chats WhatsApp starts in a local
// day.
const WhatsAppNewChatsPerDay = 10

// NewChatHoldFor is how long new chats wait after a refusal or a pushback.
const NewChatHoldFor = 24 * time.Hour

// The values NewChats keeps in the network's Values.
const (
	newChatDayKey  = "nc.day"  // <YYYY-MM-DD>\n<new chats that local day>
	newChatHoldKey = "nc.hold" // <until, Unix ms>\n<kind>
)

// NewChats gates a network's new chats: at most PerDay a local day, and
// none for a day after a refusal or a pushback (Hold). Both are kept in the
// network's values, so a restart keeps them. Its methods are safe from any
// goroutine.
type NewChats struct {
	Values *Values
	Clock  Clock
	PerDay int

	mu sync.Mutex
}

var (
	errDailyCap    = &wire.Error{Kind: wire.DailyCap}
	errNewChatHold = &wire.Error{Kind: wire.NewChatHold}
	errNewChatsKey = &wire.Error{Kind: wire.Internal}
)

// Take counts one more new chat today, before any request is made for it:
// newChatHold during a hold, dailyCap once the day's are used, and internal
// when the count can't be read or kept (none of which counts anything).
func (n *NewChats) Take() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := n.Clock.Now()
	held, err := n.held(now)
	if err != nil {
		return errNewChatsKey
	}
	if held {
		return errNewChatHold
	}
	day, count, err := n.day(now)
	if err != nil {
		return errNewChatsKey
	}
	if count >= n.PerDay {
		return errDailyCap
	}
	if err := n.Values.Keep(newChatDayKey, day+"\n"+strconv.Itoa(count+1)); err != nil {
		return errNewChatsKey
	}
	return nil
}

// Hold stops new chats for NewChatHoldFor from now, kept with kind.
func (n *NewChats) Hold(kind wire.Kind) {
	n.mu.Lock()
	defer n.mu.Unlock()
	until := n.Clock.Now().Add(NewChatHoldFor)
	_ = n.Values.Keep(newChatHoldKey, strconv.FormatInt(until.UnixMilli(), 10)+"\n"+string(kind))
}

// Left is how many new chats today may still start (0 during a hold).
func (n *NewChats) Left() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := n.Clock.Now()
	if held, err := n.held(now); err != nil || held {
		return 0
	}
	_, count, err := n.day(now)
	if err != nil {
		return 0
	}
	return max(n.PerDay-count, 0)
}

// held reports whether a kept hold is in force. One that can't be read
// holds (fail closed).
func (n *NewChats) held(now time.Time) (bool, error) {
	v, err := n.Values.Lookup(newChatHoldKey)
	if err != nil {
		return true, err
	}
	if v == "" {
		return false, nil
	}
	ms, _, _ := strings.Cut(v, "\n")
	until, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return true, nil
	}
	return now.UnixMilli() < until, nil
}

// day is today's local date and how many new chats it has had.
func (n *NewChats) day(now time.Time) (string, int, error) {
	today := now.Local().Format(time.DateOnly)
	v, err := n.Values.Lookup(newChatDayKey)
	if err != nil {
		return today, 0, err
	}
	d, c, ok := strings.Cut(v, "\n")
	if !ok || d != today {
		return today, 0, nil
	}
	count, err := strconv.Atoi(c)
	if err != nil || count < 0 {
		return today, n.PerDay, nil // unreadable: none left today
	}
	return today, count, nil
}
