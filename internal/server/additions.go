package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// The additions to protocol 1 for networks signed in through a website
// (PROTOCOL.md). A network opts into the optional interfaces of package
// network; one without answers `unsupported`.

var unsupported = &wire.Error{Kind: wire.Unsupported}

// ownMethod is the handler for a method that needs the server's store or
// clock as well as the network.
func (s *server) ownMethod(name string) (handler, bool) {
	switch name {
	case "sendStatus":
		return s.sendStatus, true
	case "messages":
		return s.messages, true
	case "signIn":
		return signIn, true
	case "encryptedHistory":
		return encryptedHistory, true
	case "restoreHistory":
		return restoreHistory, true
	case "contacts":
		return contacts, true
	case "gateCounts":
		// hidden: it exists only when the helper is started with --gate
		if s.deps.Gate {
			return gateCounts, true
		}
	}
	return nil, false
}

// sendStatus answers sendStatus: the state of a send, asked by message ID or
// by the caller's key. Once the send is sent, it also carries lineID, the
// line's own ID, when the network has kept one that differs from the ID the
// send was made with.
func (s *server) sendStatus(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
	var p struct {
		MessageID string `json:"messageID"`
		ClientID  string `json:"clientID"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	if (p.MessageID == "") == (p.ClientID == "") {
		return nil, badRequest
	}
	r := map[string]any{}
	id := p.MessageID
	if p.ClientID != "" {
		// asked by the caller's key: the send it names, when there is one
		var err error
		if id, err = n.SendFor(p.ClientID); err != nil {
			return nil, err
		}
		if id == "" {
			return map[string]any{"state": network.SendUnknown}, nil
		}
		r["messageID"] = id
	}
	st := n.SendState(id)
	r["state"] = st.State
	if st.Kind != "" {
		r["kind"] = wire.KindOf(&wire.Error{Kind: st.Kind})
	}
	if st.State == network.SendSent {
		// lineID is optional, so a store that cannot say just leaves it out.
		if o, ok, err := s.store.Outgoing(n.Name(), id); err == nil && ok && o.LineID != "" && o.LineID != id {
			r["lineID"] = o.LineID
		}
	}
	return r, nil
}

// messages answers messages from the store, and, when the page reaches the
// oldest stored line of a chat whose history is not complete, asks the
// network (if it can) to fetch older lines. The answer does not wait for it.
func (s *server) messages(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
	page, chatID, err := messagesPage(s.store, n.Name(), params)
	if err != nil {
		return nil, err
	}
	if !page.HasMore {
		s.askOlder(n, chatID)
	}
	return messagesResult(page), nil
}

// olderState is what the server remembers of a chat's FetchOlder calls.
type olderState struct {
	at      time.Time
	running bool
}

func (s *server) now() time.Time {
	if s.deps.Now != nil {
		return s.deps.Now()
	}
	return time.Now()
}

// askOlder calls the network's FetchOlder for the chat unless the network
// has none, the chat's history is complete, one is already running, or the
// last was under olderEvery ago.
func (s *server) askOlder(n network.Network, chatID string) {
	op, ok := n.(network.OlderPages)
	if !ok {
		return
	}
	c, ok, err := s.store.Chat(n.Name(), chatID)
	if err != nil || !ok || c.HistoryComplete {
		return
	}
	key := n.Name() + "\x00" + c.ID
	now := s.now()
	s.olderMu.Lock()
	st := s.older[key]
	if st.running || (!st.at.IsZero() && now.Sub(st.at) < olderEvery) {
		s.olderMu.Unlock()
		return
	}
	s.older[key] = olderState{at: now, running: true}
	s.olderMu.Unlock()

	s.inFlight.Add(1)
	go func() {
		defer s.inFlight.Done()
		defer func() {
			_ = recover() // a failed fetch is the network's to report, as a state
			s.olderMu.Lock()
			st := s.older[key]
			st.running = false
			s.older[key] = st
			s.olderMu.Unlock()
		}()
		_, _ = op.FetchOlder(s.ctx, c.ID)
	}()
}

// signIn answers a network's sign-in page. The cookie lists go out as lists,
// never null.
func signIn(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
	sp, ok := n.(network.SignInPager)
	if !ok {
		return nil, unsupported
	}
	page := sp.SignIn()
	if page.Required == nil {
		page.Required = []string{}
	}
	if page.Optional == nil {
		page.Optional = []string{}
	}
	return page, nil
}

func encryptedHistory(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
	eh, ok := n.(network.EncryptedHistory)
	if !ok {
		return nil, unsupported
	}
	return map[string]any{"state": eh.HistoryState()}, nil
}

// restoreHistory passes his PIN to the network ("" skips). The PIN goes no
// further than the network's call: it is not kept, logged or echoed.
func restoreHistory(ctx context.Context, n network.Network, params json.RawMessage) (any, error) {
	eh, ok := n.(network.EncryptedHistory)
	if !ok {
		return nil, unsupported
	}
	var p struct {
		PIN *string `json:"pin"`
	}
	if err := decode(params, &p); err != nil {
		return nil, err
	}
	if p.PIN == nil {
		return nil, badRequest
	}
	return wire.Empty, eh.RestoreHistory(ctx, []byte(*p.PIN))
}

// contacts answers a network's own contact store (WhatsApp's linked device),
// read locally; the network makes no request for it. Empty fields go out as
// null, and no contacts as an empty list.
func contacts(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
	cl, ok := n.(network.ContactLister)
	if !ok {
		return nil, unsupported
	}
	list, err := cl.Contacts(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(list))
	for _, c := range list {
		items = append(items, map[string]any{
			"id":           c.ID,
			"phone":        nullable(c.Phone),
			"name":         nullable(c.Name),
			"pushName":     nullable(c.PushName),
			"businessName": nullable(c.BusinessName),
		})
	}
	return map[string]any{"items": items}, nil
}

// gateCounts answers a network's gate counts (counts only), or unsupported
// for a network with no gate. Only registered with --gate (ownMethod).
func gateCounts(ctx context.Context, n network.Network, _ json.RawMessage) (any, error) {
	g, ok := n.(network.Gate)
	if !ok {
		return nil, unsupported
	}
	return g.GateCounts(), nil
}
