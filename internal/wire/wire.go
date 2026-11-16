// Package wire holds the shapes of protocol 1: JSON lines of requests,
// responses and events, and the fixed error kinds. PROTOCOL.md is the
// contract this package follows.
package wire

import (
	"encoding/json"
	"errors"
)

// Protocol is the only protocol version this helper speaks.
const Protocol = 1

// MaxLine is the longest line either side may write, in bytes, not counting
// the newline.
const MaxLine = 16 << 20

// Kind is an error's fixed kind: the only thing an error ever says.
type Kind string

// The error kinds of protocol 1. No other value is ever sent.
const (
	BadRequest     Kind = "badRequest"
	UnknownMethod  Kind = "unknownMethod"
	UnknownNetwork Kind = "unknownNetwork"
	NotConnected   Kind = "notConnected"
	Offline        Kind = "offline"
	NotFound       Kind = "notFound"
	NotOnNetwork   Kind = "notOnNetwork"
	Refused        Kind = "refused"
	Unclear        Kind = "unclear"
	RateLimited    Kind = "rateLimited"
	TooLarge       Kind = "tooLarge"
	MediaGone      Kind = "mediaGone"
	Unsupported    Kind = "unsupported"
	StoreKey       Kind = "storeKey"
	Outdated       Kind = "outdated"
	Timeout        Kind = "timeout"
	Internal       Kind = "internal"

	// Added for networks signed in through a website (PROTOCOL.md).
	// Checkpoint: Meta wants him to confirm it is him (a challenge,
	// checkpoint or consent page). PinWrong and PinLimit are restoreHistory's.
	Checkpoint Kind = "checkpoint"
	PinWrong   Kind = "pinWrong"
	PinLimit   Kind = "pinLimit"
)

// Kinds lists every kind of protocol 1.
var Kinds = []Kind{
	BadRequest, UnknownMethod, UnknownNetwork, NotConnected, Offline, NotFound,
	NotOnNetwork, Refused, Unclear, RateLimited, TooLarge, MediaGone,
	Unsupported, StoreKey, Outdated, Timeout, Internal,
	Checkpoint, PinWrong, PinLimit,
}

// Error is an error that carries only its kind, so nothing private can ride
// along in its text. AttemptsRemaining is the one extra: on PinWrong, how many
// tries are left before PinLimit, when the network knows.
type Error struct {
	Kind              Kind
	AttemptsRemaining *int
}

func (e *Error) Error() string { return string(e.Kind) }

// AttemptsOf is the attempts remaining err carries, for a PinWrong only.
func AttemptsOf(err error) *int {
	var e *Error
	if errors.As(err, &e) && e != nil && e.Kind == PinWrong {
		return e.AttemptsRemaining
	}
	return nil
}

// KindOf is the kind to send for err: the kind of the first *Error in its
// chain, and Internal for anything else. A nil error has no kind.
func KindOf(err error) Kind {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) && e != nil && known(e.Kind) {
		return e.Kind
	}
	return Internal
}

func known(k Kind) bool {
	for _, c := range Kinds {
		if c == k {
			return true
		}
	}
	return false
}

// Request is one line from Pllum.
type Request struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// Response answers one request: Result or Error, never both.
type Response struct {
	ID     string     `json:"id"`
	Result any        `json:"result,omitempty"`
	Error  *ErrorBody `json:"error,omitempty"`
}

// ErrorBody is an error response's body.
type ErrorBody struct {
	Kind              Kind `json:"kind"`
	AttemptsRemaining *int `json:"attemptsRemaining,omitempty"`
}

// Empty is the result of a method that answers {}.
var Empty = struct{}{}
