package meta

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Ended are the messagix errors that mean Meta has ended the session, for
// every network: igconnector's bad credentials (Meta's GraphQL error
// 1357053 is the other), and the redirect loop the library counts as
// permanent.
var ended = []error{httpclient.ErrTokenInvalidated, httpclient.ErrTooManyRedirects}

// KindRules are what a network adds to KindOf.
type KindRules struct {
	// Ended are more errors that mean the session has ended.
	Ended []error
	// Net makes a connection that failed at the network (a net.Error)
	// offline, or timeout when it timed out.
	Net bool
}

// KindOf is the kind of a client error. A checkpoint, challenge or consent
// page is checkpoint; a session Meta has ended is ReconnectNeeded; a
// suspended account is Banned (these two are Pace's, not wire kinds); then
// rateLimited, timeout, offline, the kind a *wire.Error carries, else
// internal.
func KindOf(err error, r KindRules) wire.Kind {
	switch {
	case errors.Is(err, httpclient.ErrChallengeRequired),
		errors.Is(err, httpclient.ErrCheckpointRequired),
		errors.Is(err, httpclient.ErrConsentRequired):
		return wire.Checkpoint
	case isAny(err, ended), isAny(err, r.Ended), IsMetaError(err, 1357053):
		return ReconnectNeeded
	case errors.Is(err, httpclient.ErrAccountSuspended):
		return Banned
	case errors.Is(err, httpclient.ErrRateLimited):
		return wire.RateLimited
	case errors.Is(err, context.DeadlineExceeded):
		return wire.Timeout
	case errors.Is(err, httpclient.ErrRequestFailed):
		return wire.Offline
	}
	if r.Net {
		var ne net.Error
		if errors.As(err, &ne) {
			if ne.Timeout() {
				return wire.Timeout
			}
			return wire.Offline
		}
	}
	var e *wire.Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return wire.Internal
}

func isAny(err error, targets []error) bool {
	for _, t := range targets {
		if errors.Is(err, t) {
			return true
		}
	}
	return false
}

// IsMetaError reports whether err is Meta's error response with code.
func IsMetaError(err error, code int) bool {
	var e *types.ErrorResponse
	return errors.As(err, &e) && e != nil && e.ErrorCode == code
}

// ErrorKind is the wire kind to return for kind: reconnectNeeded and banned
// are states, so a request refused for them is notConnected.
func ErrorKind(kind wire.Kind) wire.Kind {
	switch kind {
	case ReconnectNeeded, Banned:
		return wire.NotConnected
	}
	return kind
}

// Values is where a network keeps its own values in the store
// (network.Values).
type Values interface {
	Get(name string) string
	Keep(name, v string) error
}

// Session is what a network signed in through a Meta website (Instagram,
// Messenger) does with its session and its pace: every request through the
// pace, Meta pushing back blocking it, and the block (with the hashes of
// every version of the session it stopped) and the rate limit kept in the
// store, so neither a restart nor the same cookies sent again asks Meta
// anything. The network keeps its own state and run; Session's callbacks
// reach them.
type Session struct {
	Page   SignInPage
	Pace   *Pace
	Values Values
	// KindOf is the kind of one of the network's client errors.
	KindOf func(error) wire.Kind
	// Terminal reports whether kind stops the session until he signs in
	// again (it has a state, and a kept block).
	Terminal func(kind wire.Kind) bool
}

// Hash is SessionHash for the network's page.
func (s *Session) Hash(session map[string]string) string { return SessionHash(s.Page, session) }

// SessionHash is a SHA-256 of a session's required cookies (p's): what tells
// the session Meta stopped from a new sign-in, with no cookie value kept.
// Optional cookies (rur rotates) do not count.
func SessionHash(p SignInPage, session map[string]string) string {
	h := sha256.New()
	for _, name := range p.Required {
		h.Write([]byte(name + "=" + session[name] + "\x00"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Gate is the error for a request now, when the pace forbids one.
func (s *Session) Gate() error {
	if kind, blocked := s.Pace.Blocked(); blocked {
		return &wire.Error{Kind: ErrorKind(kind)}
	}
	return nil
}

// Request makes one request to Meta through the pace: none while a block
// is in force, then a 2 to 4 s gap after the last, and none once live says
// the run that asks has ended (notConnected). A success starts the rate
// limit's count over and calls answered (the network sends rotated
// cookies); Meta pushing back blocks the pace and calls pushedBack with the
// kind, which for a checkpoint, an ended session or a suspension ends the
// run.
func (s *Session) Request(ctx context.Context, live func() bool, fn func(ctx context.Context) error, answered func(), pushedBack func(kind wire.Kind)) error {
	if err := s.Gate(); err != nil {
		return err
	}
	if err := s.Pace.Wait(ctx); err != nil {
		return &wire.Error{Kind: wire.Timeout}
	}
	if err := s.Gate(); err != nil {
		return err
	}
	if !live() {
		return &wire.Error{Kind: wire.NotConnected}
	}
	err := fn(ctx)
	if err == nil {
		s.Pace.Succeed()
		s.KeepRateLimit()
		answered()
		return nil
	}
	kind := s.KindOf(err)
	s.Pace.Fail(kind)
	s.KeepRateLimit()
	pushedBack(kind)
	return &wire.Error{Kind: ErrorKind(kind)}
}

// KeepBlock keeps the kind that stopped the session and the hashes of every
// version of it.
func (s *Session) KeepBlock(kind wire.Kind, hashes []string) {
	_ = s.Values.Keep("block", string(kind)+"\n"+strings.Join(hashes, ","))
}

// KeptBlock is the kind that stopped a session in this or an earlier run,
// and the hashes of every version of that session; ok is false when none is
// kept.
func (s *Session) KeptBlock() (kind wire.Kind, hashes []string, ok bool) {
	k, list, ok := strings.Cut(s.Values.Get("block"), "\n")
	if !ok || list == "" {
		return "", nil, false
	}
	if !s.Terminal(wire.Kind(k)) {
		return "", nil, false
	}
	return wire.Kind(k), strings.Split(list, ","), true
}

// KeepRateLimit keeps the pace's rate limit (its end and its count) so a
// restart carries it on; nothing when there is none.
func (s *Session) KeepRateLimit() {
	until, step := s.Pace.RateLimit()
	v := ""
	if step > 0 || !until.IsZero() {
		v = strconv.FormatInt(until.UnixMilli(), 10) + "\n" + strconv.Itoa(step)
	}
	_ = s.Values.Keep("rate", v)
}

// KeptRateLimit is the rate limit an earlier run kept.
func (s *Session) KeptRateLimit() (until time.Time, step int, ok bool) {
	ms, n, ok := strings.Cut(s.Values.Get("rate"), "\n")
	if !ok {
		return time.Time{}, 0, false
	}
	untilMs, err1 := strconv.ParseInt(ms, 10, 64)
	step, err2 := strconv.Atoi(n)
	if err1 != nil || err2 != nil {
		return time.Time{}, 0, false
	}
	if untilMs > 0 {
		until = time.UnixMilli(untilMs)
	}
	return until, step, true
}
