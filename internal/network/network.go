// Package network is what each network the helper runs implements.
// Reads are answered from the shared store; a network only writes into it.
package network

import (
	"context"

	"github.com/matwhiting/pllum-connect/internal/meta"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Emit writes one event line: {"event":"<name>","network":"<name>",…}.
// It is safe to call from any goroutine.
type Emit func(event map[string]any)

// For is the emit to give the network called name: each event goes out with
// "network" set to name, whatever the network put there. The server drops an
// event whose "network" is not one of its networks, so every network is made
// with its own emit.For(<its Name()>).
func (e Emit) For(name string) Emit {
	return func(event map[string]any) {
		stamped := make(map[string]any, len(event)+1)
		for k, v := range event {
			stamped[k] = v
		}
		stamped["network"] = name
		e(stamped)
	}
}

// State is a network's connection state, as `status` and the `state` event
// report it.
type State string

// The states of protocol 1.
const (
	NotConnected    State = "notConnected"
	Linking         State = "linking"
	Connecting      State = "connecting"
	CatchingUp      State = "catchingUp"
	Live            State = "live"
	Offline         State = "offline"
	ReconnectNeeded State = "reconnectNeeded"
	Outdated        State = "outdated"
	Banned          State = "banned"
)

// Status is what `status` answers. An empty SelfChatID is sent as null.
type Status struct {
	State      State
	SelfChatID string
}

// LoginParams are `login`'s params besides `network`.
type LoginParams struct {
	Method  string            // "qr", or "session" with Session
	Session map[string]string // cookie name → value, for "session"; nil for "qr"
}

// The login methods of protocol 1.
const (
	LoginQR      = "qr"
	LoginSession = "session"
)

// Outgoing is one `send`.
type Outgoing struct {
	ChatID   string
	Text     string
	ReplyTo  string // a message ID, or empty
	File     string // an absolute path, or empty
	FileName string
	// ClientID is the caller's own key for this send: a send repeated with
	// it (its answer was lost) gives the same message ID and sends nothing
	// more, unless the first one failed.
	ClientID string
}

// SendState is what `sendStatus` answers. Kind is set only when State is
// SendFailed.
type SendState struct {
	State string
	Kind  wire.Kind
}

// The send states of protocol 1.
const (
	SendSending = "sending"
	SendSent    = "sent"
	SendFailed  = "failed"
	SendUnknown = "unknown"
)

// Network is one network the helper runs. Errors returned are *wire.Error so
// they carry a fixed kind; anything else is sent as internal.
type Network interface {
	Name() string                    // "whatsapp"
	Start(ctx context.Context) error // connects if signed in; returns at once
	Status() Status                  // State, SelfChatID
	Login(ctx context.Context, p LoginParams) error
	CancelLogin()
	Logout(ctx context.Context) error
	Nudge()
	Send(ctx context.Context, m Outgoing) (messageID string, err error)
	SendState(messageID string) SendState
	// SendFor is the message ID of the send kept under the caller's key,
	// or "" when there is none; an error when it can't say.
	SendFor(clientID string) (string, error)
	React(ctx context.Context, chatID, messageID, emoji, transactionID string) error
	MarkRead(ctx context.Context, chatID, through string) error
	MarkUnread(ctx context.Context, chatID string) error
	SetMuted(ctx context.Context, chatID string, muted bool) error
	Media(ctx context.Context, chatID, messageID string) (path string, err error)
	Stop()
}

// The interfaces below are optional: a network implements the ones it has,
// and server checks for them with a type assertion, answering `unsupported`
// (or doing nothing) for a network without. Step 2's Network is unchanged.

// SignInPager is a network that signs in through a website: `signIn` answers
// with its page, and Pllum shows it and sends the cookies back as a `login`
// of method "session".
type SignInPager interface {
	SignIn() meta.SignInPage
}

// OlderPages is a network that can fetch history older than what the store
// holds. server calls FetchOlder when `messages` reaches the oldest stored
// line of a chat that is not historyComplete, at most once per chat per 10 s,
// and not again while one is running. It returns how many lines it added.
type OlderPages interface {
	FetchOlder(ctx context.Context, chatID string) (added int, err error)
}

// EncryptedHistory is a network whose end-to-end encrypted history is
// restored with a PIN. HistoryState is "noBackup", "needsPIN", "restored" or
// "skipped". RestoreHistory takes the PIN as typed; an empty pin skips the
// restore. A wrong PIN is a *wire.Error of kind PinWrong (with
// AttemptsRemaining when known); too many is PinLimit.
type EncryptedHistory interface {
	HistoryState() string
	RestoreHistory(ctx context.Context, pin []byte) error
}
