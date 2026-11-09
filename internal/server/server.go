// Package server speaks protocol 1 over a pair of streams: it reads one
// request per line, answers each on its own goroutine, and writes responses
// and events one line at a time. See PROTOCOL.md.
package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Version is the helper's version, set at build time with
// -ldflags "-X github.com/matwhiting/pllum-connect/internal/server.Version=…".
// When it is empty, hello reports the module version Go stamped, else "dev".
var Version = ""

// shutdownGrace is how long Serve waits, once input ends, for the networks to
// stop and the requests in flight to finish. The contract allows 5 s in all.
const shutdownGrace = 3 * time.Second

// Deps are what Serve needs to answer hello: the store opened with the key
// hello carries, and the networks made over it.
type Deps struct {
	Dir      string // the data folder; each network keeps its files under it
	Open     func(key [32]byte) (*store.Store, error)
	Networks func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network
}

// Serve answers requests from in until in ends, a shutdown request arrives or
// ctx is cancelled, then stops the networks and returns within 5 s. It
// returns nil for those three, and the read error if in fails otherwise.
func Serve(ctx context.Context, in io.Reader, out io.Writer, deps Deps) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &server{
		ctx:      ctx,
		deps:     deps,
		out:      &lineWriter{w: out},
		networks: map[string]network.Network{},
	}
	lr := &lineReader{lines: make(chan line)}
	go lr.run(in, ctx.Done())

	for {
		select {
		case <-ctx.Done():
			s.stop(cancel)
			return nil
		case l, ok := <-lr.lines:
			if !ok {
				s.stop(cancel)
				if errors.Is(lr.err, io.EOF) {
					return nil
				}
				return lr.err
			}
			if s.handleLine(l) {
				s.stop(cancel)
				return nil
			}
		}
	}
}

type server struct {
	ctx      context.Context
	deps     Deps
	out      *lineWriter
	ready    bool // hello agreed; touched only by Serve's loop
	networks map[string]network.Network
	order    []network.Network
	inFlight sync.WaitGroup
}

// handleLine answers one line, and reports whether the helper should stop.
func (s *server) handleLine(l line) (stop bool) {
	if l.tooLong {
		s.fail("", wire.BadRequest)
		return false
	}
	b := bytes.TrimSpace(l.b)
	if len(b) == 0 {
		return false
	}
	var req wire.Request
	if err := json.Unmarshal(b, &req); err != nil || req.ID == "" || req.Method == "" {
		var idOnly struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(b, &idOnly)
		s.fail(idOnly.ID, wire.BadRequest)
		return false
	}
	params := bytes.TrimSpace(req.Params)
	if len(params) == 0 || bytes.Equal(params, []byte("null")) {
		params = []byte("{}")
	}
	if params[0] != '{' {
		s.fail(req.ID, wire.BadRequest)
		return false
	}

	if req.Method == "hello" {
		s.hello(req.ID, params)
		return false
	}
	if !s.ready {
		s.fail(req.ID, wire.BadRequest)
		return false
	}
	if req.Method == "shutdown" {
		s.answer(req.ID, wire.Empty)
		return true
	}
	h, ok := methods[req.Method]
	if !ok {
		s.fail(req.ID, wire.UnknownMethod)
		return false
	}
	var p struct {
		Network string `json:"network"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Network == "" {
		s.fail(req.ID, wire.BadRequest)
		return false
	}
	n, ok := s.networks[p.Network]
	if !ok {
		s.fail(req.ID, wire.UnknownNetwork)
		return false
	}

	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		result, err := s.call(h, n, params)
		if err != nil {
			s.fail(req.ID, wire.KindOf(err))
			return
		}
		s.answer(req.ID, result)
	}()
	return false
}

// call runs one handler, turning a panic into internal.
func (s *server) call(h handler, n network.Network, params json.RawMessage) (result any, err error) {
	defer func() {
		if recover() != nil {
			result, err = nil, &wire.Error{Kind: wire.Internal}
		}
	}()
	return h(s.ctx, n, params)
}

func (s *server) hello(id string, params json.RawMessage) {
	if s.ready {
		s.fail(id, wire.BadRequest)
		return
	}
	var p struct {
		Protocol *int   `json:"protocol"`
		StoreKey string `json:"storeKey"`
		App      string `json:"app"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Protocol == nil {
		s.fail(id, wire.BadRequest)
		return
	}
	if *p.Protocol != wire.Protocol {
		s.fail(id, wire.Outdated)
		return
	}
	key, ok := parseKey(p.StoreKey)
	if !ok {
		s.fail(id, wire.StoreKey)
		return
	}
	nets, err := s.open(key)
	if err != nil {
		s.fail(id, wire.KindOf(err))
		return
	}

	names := []string{}
	for _, n := range nets {
		if n == nil {
			continue
		}
		if _, dup := s.networks[n.Name()]; dup {
			continue
		}
		s.networks[n.Name()] = n
		s.order = append(s.order, n)
		names = append(names, n.Name())
	}
	s.ready = true
	version, commit := buildInfo()
	s.answer(id, map[string]any{
		"protocol": wire.Protocol,
		"version":  version,
		"commit":   commit,
		"networks": names,
	})

	// Started after hello's answer, so no event comes before it.
	for _, n := range s.order {
		if err := s.start(n); err != nil {
			s.emit(map[string]any{
				"event":   "state",
				"network": n.Name(),
				"state":   n.Status().State,
				"kind":    wire.KindOf(err),
			})
		}
	}
}

// open opens the store and makes the networks, turning a panic into internal.
func (s *server) open(key [32]byte) (nets []network.Network, err error) {
	defer func() {
		if recover() != nil {
			nets, err = nil, &wire.Error{Kind: wire.Internal}
		}
	}()
	if s.deps.Open == nil {
		return nil, &wire.Error{Kind: wire.Internal}
	}
	st, err := s.deps.Open(key)
	if err != nil {
		return nil, err
	}
	if s.deps.Networks == nil {
		return nil, nil
	}
	return s.deps.Networks(st, s.deps.Dir, key, s.emit), nil
}

func (s *server) start(n network.Network) (err error) {
	defer func() {
		if recover() != nil {
			err = &wire.Error{Kind: wire.Internal}
		}
	}()
	return n.Start(s.ctx)
}

// stop cancels every request, stops the networks and waits for both, but
// never longer than shutdownGrace. Nothing is written after it returns.
func (s *server) stop(cancel context.CancelFunc) {
	cancel()
	done := make(chan struct{})
	go func() {
		for _, n := range s.order {
			stopNetwork(n)
		}
		s.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownGrace):
	}
	s.out.close()
}

func stopNetwork(n network.Network) {
	defer func() { _ = recover() }()
	n.Stop()
}

func (s *server) answer(id string, result any) {
	if !s.out.write(wire.Response{ID: id, Result: result}) {
		s.fail(id, wire.Internal)
	}
}

func (s *server) fail(id string, kind wire.Kind) {
	s.out.write(wire.Response{ID: id, Error: &wire.ErrorBody{Kind: kind}})
}

// emit writes one event line; networks call it from any goroutine.
func (s *server) emit(event map[string]any) {
	s.out.write(event)
}

// parseKey reads hello's storeKey: 64 hex digits, 32 bytes.
func parseKey(s string) (key [32]byte, ok bool) {
	if len(s) != hex.EncodedLen(len(key)) {
		return key, false
	}
	if _, err := hex.Decode(key[:], []byte(s)); err != nil {
		return [32]byte{}, false
	}
	return key, true
}

// buildInfo is hello's version and commit.
func buildInfo() (version, commit string) {
	version, commit = Version, "unknown"
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		if version == "" {
			version = "dev"
		}
		return version, commit
	}
	if version == "" {
		version = bi.Main.Version
	}
	if version == "" || version == "(devel)" {
		version = "dev"
	}
	for _, kv := range bi.Settings {
		if kv.Key == "vcs.revision" && kv.Value != "" {
			commit = kv.Value
		}
	}
	return version, commit
}

// lineWriter writes whole lines to out, one at a time, until closed.
type lineWriter struct {
	mu     sync.Mutex
	w      io.Writer
	closed bool
}

// write marshals v and writes it as one line. It reports false only when v
// could not be marshalled; a failed or closed output drops the line quietly
// (Pllum has gone, and end of input follows).
func (lw *lineWriter) write(v any) bool {
	b, err := json.Marshal(v)
	if err != nil {
		return false
	}
	if len(b) > wire.MaxLine {
		return false
	}
	b = append(b, '\n')
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if !lw.closed {
		_, _ = lw.w.Write(b)
	}
	return true
}

func (lw *lineWriter) close() {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.closed = true
}

// line is one input line without its newline, or a marker that the line was
// longer than wire.MaxLine and has been skipped.
type line struct {
	b       []byte
	tooLong bool
}

type lineReader struct {
	lines chan line
	err   error // set before lines is closed
}

// run reads lines until in ends or done closes. An over-long line is skipped
// to its newline without holding more than MaxLine bytes of it.
func (lr *lineReader) run(in io.Reader, done <-chan struct{}) {
	defer close(lr.lines)
	r := bufio.NewReaderSize(in, 64<<10)
	for {
		l, err := readLine(r)
		if err != nil {
			lr.err = err
			return
		}
		select {
		case lr.lines <- l:
		case <-done:
			lr.err = io.EOF
			return
		}
	}
}

func readLine(r *bufio.Reader) (line, error) {
	var buf []byte
	tooLong := false
	for {
		chunk, err := r.ReadSlice('\n')
		if !tooLong {
			if len(buf)+len(chunk) > wire.MaxLine+1 {
				tooLong, buf = true, nil
			} else {
				buf = append(buf, chunk...)
			}
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case err == nil:
			// buf ends with the newline.
		case errors.Is(err, io.EOF) && (len(buf) > 0 || tooLong):
			// A last line with no newline is still a line.
		default:
			return line{}, err
		}
		buf = bytes.TrimSuffix(buf, []byte("\n"))
		buf = bytes.TrimSuffix(buf, []byte("\r"))
		if len(buf) > wire.MaxLine {
			tooLong, buf = true, nil
		}
		return line{b: buf, tooLong: tooLong}, nil
	}
}

// handler answers one method for one network.
type handler func(ctx context.Context, n network.Network, params json.RawMessage) (any, error)

// methods are the network methods; hello and shutdown are Serve's own.
var methods = map[string]handler{
	"status": func(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
		st := n.Status()
		return map[string]any{"state": st.State, "selfChatID": nullable(st.SelfChatID)}, nil
	},
	"login": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			Method  string          `json:"method"`
			Session json.RawMessage `json:"session"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		switch p.Method {
		case network.LoginQR:
		case network.LoginSession:
			if len(p.Session) == 0 || bytes.Equal(p.Session, []byte("null")) {
				return nil, badRequest
			}
		default:
			return nil, badRequest
		}
		return wire.Empty, n.Login(ctx, network.LoginParams{Method: p.Method, Session: p.Session})
	},
	"cancelLogin": func(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
		n.CancelLogin()
		return wire.Empty, nil
	},
	"logout": func(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
		return wire.Empty, n.Logout(ctx)
	},
	"nudge": func(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
		n.Nudge()
		return wire.Empty, nil
	},
	"send": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			ChatID   string `json:"chatID"`
			Text     string `json:"text"`
			ReplyTo  string `json:"replyTo"`
			File     string `json:"file"`
			FileName string `json:"fileName"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.ChatID == "" || (p.File != "" && !filepath.IsAbs(p.File)) {
			return nil, badRequest
		}
		id, err := n.Send(ctx, network.Outgoing{ChatID: p.ChatID, Text: p.Text, ReplyTo: p.ReplyTo, File: p.File, FileName: p.FileName})
		if err != nil {
			return nil, err
		}
		return map[string]any{"messageID": id}, nil
	},
	"sendStatus": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			MessageID string `json:"messageID"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.MessageID == "" {
			return nil, badRequest
		}
		st := n.SendState(p.MessageID)
		r := map[string]any{"state": st.State}
		if st.Kind != "" {
			r["kind"] = wire.KindOf(&wire.Error{Kind: st.Kind})
		}
		return r, nil
	},
	"react": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			ChatID        string  `json:"chatID"`
			MessageID     string  `json:"messageID"`
			Emoji         *string `json:"emoji"`
			TransactionID string  `json:"transactionID"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.ChatID == "" || p.MessageID == "" || p.Emoji == nil || p.TransactionID == "" {
			return nil, badRequest
		}
		return wire.Empty, n.React(ctx, p.ChatID, p.MessageID, *p.Emoji, p.TransactionID)
	},
	"markRead": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			ChatID  string `json:"chatID"`
			Through string `json:"through"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.ChatID == "" {
			return nil, badRequest
		}
		return wire.Empty, n.MarkRead(ctx, p.ChatID, p.Through)
	},
	"markUnread": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		chatID, err := chatIDOf(params)
		if err != nil {
			return nil, err
		}
		return wire.Empty, n.MarkUnread(ctx, chatID)
	},
	"setMuted": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			ChatID string `json:"chatID"`
			Muted  *bool  `json:"muted"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.ChatID == "" || p.Muted == nil {
			return nil, badRequest
		}
		return wire.Empty, n.SetMuted(ctx, p.ChatID, *p.Muted)
	},
	"media": func(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
		var p struct {
			ChatID    string `json:"chatID"`
			MessageID string `json:"messageID"`
		}
		if err := decode(params, &p); err != nil {
			return nil, err
		}
		if p.ChatID == "" || p.MessageID == "" {
			return nil, badRequest
		}
		path, err := n.Media(ctx, p.ChatID, p.MessageID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"path": path}, nil
	},
}

var badRequest = &wire.Error{Kind: wire.BadRequest}

// decode reads params into p; a malformed or mistyped param is badRequest.
func decode(params json.RawMessage, p any) error {
	if err := json.Unmarshal(params, p); err != nil {
		return badRequest
	}
	return nil
}

func chatIDOf(params json.RawMessage) (string, error) {
	var p struct {
		ChatID string `json:"chatID"`
	}
	if err := decode(params, &p); err != nil {
		return "", err
	}
	if p.ChatID == "" {
		return "", badRequest
	}
	return p.ChatID, nil
}

// nullable sends an empty string as null.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
