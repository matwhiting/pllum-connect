package whatsapp

import (
	"context"
	"sort"

	"go.mau.fi/whatsmeow/types"

	"github.com/matwhiting/pllum-connect/internal/network"
)

var _ network.ContactLister = (*WhatsApp)(nil)

// Contacts lists the people in the linked device's own contact store: the
// names his phone's contacts gave them, their push names and business names,
// as whatsmeow has kept them from what WhatsApp already sent. It reads the
// device database only and never touches the client, so nothing goes to
// WhatsApp. Only people are listed (by number or LID; not him, groups,
// broadcast lists, Channels or bots), each once under its personID, with the
// fields of a LID entry and a number entry for one person folded together.
// Phone is set only from a number, never from a LID's digits.
func (w *WhatsApp) Contacts(ctx context.Context) ([]network.Contact, error) {
	dev := w.device()
	if dev == nil || dev.ID == nil || dev.Contacts == nil {
		return nil, errNotConnected
	}
	all, err := dev.Contacts.GetAllContacts(ctx)
	if err != nil {
		return nil, errInternal
	}
	jids := make([]types.JID, 0, len(all))
	for jid := range all {
		if isPerson(jid) && !w.isSelf(jid) {
			jids = append(jids, jid)
		}
	}
	// A number's entry is read before a LID's, so where both name one person
	// and differ, the number's wins; the order is otherwise the JID's.
	sort.Slice(jids, func(i, j int) bool {
		ni, nj := norm(jids[i]).Server == types.DefaultUserServer, norm(jids[j]).Server == types.DefaultUserServer
		if ni != nj {
			return ni
		}
		return jids[i].String() < jids[j].String()
	})
	byID := map[string]*network.Contact{}
	for _, jid := range jids {
		ci := all[jid]
		id := w.personID(jid)
		c, ok := byID[id]
		if !ok {
			c = &network.Contact{ID: id}
			byID[id] = c
		}
		fill(&c.Phone, w.phoneOf(jid))
		name := ci.FullName
		if name == "" {
			name = ci.FirstName
		}
		fill(&c.Name, name)
		fill(&c.PushName, ci.PushName)
		fill(&c.BusinessName, ci.BusinessName)
	}
	out := make([]network.Contact, 0, len(byID))
	for _, c := range byID {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// isPerson reports whether jid is a person, by number or by LID: not a
// group, broadcast list, status, Channel or bot.
func isPerson(jid types.JID) bool {
	j := norm(jid)
	switch j.Server {
	case types.DefaultUserServer:
		return j.User != "" && !j.IsBot()
	case types.HiddenUserServer:
		return j.User != ""
	}
	return false
}

// fill sets *field to v when it is still empty.
func fill(field *string, v string) {
	if *field == "" {
		*field = v
	}
}
