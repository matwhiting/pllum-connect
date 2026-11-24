package messenger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	watypes "go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Diagnostics for the encrypted side, in <dir>/messenger/diagnostics.txt
// (0600, on his Mac only, started again past diagLimit): the library's
// warnings about decrypting a message, a self-check each time the
// encrypted socket comes up, and its ICDC half and any re-registration in
// each run's setup (each of their requests through the pace), and how many
// of a backup page's lines the library could not decrypt. Every line but
// the self-check's and those (words, yes/no and counts) loses its numbers
// (sanitize), so it
// holds no ID, JID, device number or time but its own timestamp, and never
// a word of a message: the library's warnings name a message by its ID only.

const diagLimit = 64 << 10

var digits = regexp.MustCompile(`[0-9]+`)

// sanitize is a line with every run of digits made "#".
func sanitize(s string) string { return digits.ReplaceAllString(s, "#") }

// diag appends one line, sanitized, under the time it was written.
func (m *Messenger) diag(line string) { m.diagLine(sanitize(line)) }

// diagLine appends a line as it is: only the self-check's and a backup
// page's passed-over lines (passedOver), made of words, yes/no and counts,
// never an ID.
func (m *Messenger) diagLine(line string) {
	m.diagmu.Lock()
	defer m.diagmu.Unlock()
	path := filepath.Join(m.dir, Name, "diagnostics.txt")
	if info, err := os.Stat(path); err == nil && info.Size() > diagLimit-1024 {
		_ = os.Remove(path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = fmt.Fprintf(f, "%s %s\n", m.clock.Now().UTC().Format(time.RFC3339), line)
}

// diagLogger is the library's logger: its warnings and errors about
// decrypting, retrying or keys go to diag; everything else is dropped.
func (m *Messenger) diagLogger() waLog.Logger { return diagLog{m} }

type diagLog struct{ m *Messenger }

var diagWords = []string{"decrypt", "unavailable", "retry", "prekey", "identity", "session"}

func (d diagLog) keep(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	low := strings.ToLower(line)
	if slices.ContainsFunc(diagWords, func(w string) bool { return strings.Contains(low, w) }) {
		d.m.diag(line)
	}
}

func (d diagLog) Warnf(format string, args ...any)  { d.keep(format, args...) }
func (d diagLog) Errorf(format string, args ...any) { d.keep(format, args...) }
func (d diagLog) Infof(string, ...any)              {}
func (d diagLog) Debugf(string, ...any)             {}
func (d diagLog) Sub(string) waLog.Logger           { return d }

// selfCheck asks Meta what it holds for this device and writes the answers
// as yes/no and counts (checkDevice), each time the encrypted socket first
// comes up in this helper. Its other half, the ICDC list, is checkICDC,
// which the run's setup makes in line (icdcCheck).
func (m *Messenger) selfCheck(gen int, e e2eeClient) {
	wc, ok := e.(waClient)
	if !ok || wc.Client == nil || wc.Client.Store == nil || wc.Client.Store.ID == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	m.checkDevice(ctx, gen, wc.Client)
}

// icdcCheck is the self-check's ICDC half, once in each run's setup, right
// after the encrypted side is set up and before anything reads with it:
// checkICDC, and when it finds this device's identity missing from the
// list, the one re-registration ever (reregister), then the list checked
// again for the new device. ok is false when a re-registration failed:
// the run has ended then.
func (m *Messenger) icdcCheck(ctx context.Context, gen int) (ok bool) {
	if !m.checkICDC(ctx, gen) {
		return true
	}
	if m.reregistered() {
		m.diagLine("re-registration: not attempted, it was done once before")
		return true
	}
	began, err := m.reregister(ctx, gen)
	if !began {
		return true
	}
	if err != nil {
		return false
	}
	m.checkICDC(ctx, gen)
	return true
}

// checkICDC fetches his account's ICDC list once (the request registering
// makes first; through the pace) and writes whether this device's identity
// is in it, how many identities it has and its seq, never what it holds.
// It reports whether the identity is surely missing: the list came, has
// identities, and none is this device's.
func (m *Messenger) checkICDC(ctx context.Context, gen int) (missing bool) {
	m.mu.Lock()
	cli, fbid := m.cli, m.fbid
	ok := gen == m.runs.Gen && !m.stopped && cli != nil
	m.mu.Unlock()
	m.dmu.Lock()
	dev := m.dev
	m.dmu.Unlock()
	if !ok || dev == nil || dev.IdentityKey == nil || dev.IdentityKey.Pub == nil {
		return false
	}
	own := *dev.IdentityKey.Pub
	var ids [][32]byte
	seq := 0
	if err := m.request(ctx, gen, func(ctx context.Context) (err error) {
		ids, seq, err = cli.FetchICDC(ctx, fbid, dev.FacebookUUID)
		return err
	}); err != nil {
		m.diagLine("self-check: ICDC list failed (" + string(wire.KindOf(err)) + ")")
		return false
	}
	in := slices.Contains(ids, own)
	m.diagLine(fmt.Sprintf("self-check: ICDC list has %d identities, this device's among them %v; Meta's ICDC seq %d; re-registered before %v",
		len(ids), in, seq, m.reregistered()))
	return !in && len(ids) > 0
}

// checkDevice writes whether Meta's own device list names this device;
// whether the key bundle others are given for it (fetching it uses one
// one-time key) has this device's identity, signed prekey and registration
// ID, and a one-time key held here; how many one-time keys Meta has left;
// and how many devices each of his five latest encrypted one-to-ones lists.
func (m *Messenger) checkDevice(ctx context.Context, gen int, cli *whatsmeow.Client) {
	own := *cli.Store.ID
	listed, devices := false, 0
	var devs []watypes.JID
	if err := m.request(ctx, gen, func(ctx context.Context) (err error) {
		devs, err = cli.GetUserDevices(ctx, []watypes.JID{own.ToNonAD()})
		return err
	}); err != nil {
		m.diag("self-check: own device list failed: " + err.Error())
	} else {
		devices = len(devs)
		listed = slices.ContainsFunc(devs, func(j watypes.JID) bool { return j.Device == own.Device })
	}
	var bundle *prekey.Bundle
	_ = m.request(ctx, gen, func(ctx context.Context) error {
		bundle = cli.DangerousInternals().FetchPreKeysNoError(ctx, []watypes.JID{own})[own]
		return nil
	})
	identity, signed, reg, oneTime := false, false, false, false
	if bundle != nil {
		identity = bundle.IdentityKey().PublicKey().PublicKey() == *cli.Store.IdentityKey.Pub
		signed = bundle.SignedPreKeyID() == cli.Store.SignedPreKey.KeyID && bundle.SignedPreKey().PublicKey() == *cli.Store.SignedPreKey.Pub
		reg = bundle.RegistrationID() == cli.Store.RegistrationID
		if id := bundle.PreKeyID(); id != nil && !id.IsEmpty {
			pk, err := cli.Store.PreKeys.GetPreKey(ctx, id.Value)
			oneTime = err == nil && pk != nil && bundle.PreKey() != nil && bundle.PreKey().PublicKey() == *pk.Pub
		}
	}
	count := -1
	_ = m.request(ctx, gen, func(ctx context.Context) error {
		n, err := cli.DangerousInternals().GetServerPreKeyCount(ctx)
		if err == nil {
			count = n
		}
		return err
	})
	var theirs []int
	if page, err := m.st.Chats(Name, m.clock.Now().Add(-7*24*time.Hour).UnixMilli(), "", 20); err == nil {
		for _, c := range page.Items {
			if len(theirs) == 5 {
				break
			}
			if !strings.HasSuffix(c.ID, oneToOneSuffix) || !m.hasEncryptedSide(c.ID) {
				continue
			}
			user := strings.TrimSuffix(c.ID, oneToOneSuffix)
			var devs []watypes.JID
			err := m.request(ctx, gen, func(ctx context.Context) (err error) {
				devs, err = cli.GetUserDevices(ctx, []watypes.JID{{User: user, Server: watypes.MessengerServer}})
				return err
			})
			if err != nil {
				theirs = append(theirs, -1)
				continue
			}
			theirs = append(theirs, len(devs))
		}
	}
	m.diagLine(fmt.Sprintf("self-check: own devices listed %d, this one among them %v; bundle given for it: found %v, identity matches %v, signed prekey matches %v, registration matches %v, one-time key held here %v; one-time keys left with Meta %d; recent encrypted one-to-ones list %v devices",
		devices, listed, bundle != nil, identity, signed, reg, oneTime, count, theirs))
}

// askAgain sends a retry receipt, once a run, for each message from someone
// in his five latest encrypted one-to-ones that came in the last day and
// could not be read (an undecryptable or unavailable line, or a hidden
// notice kept live before those had kinds of their own): the sender's app
// re-encrypts and sends it again, unseen by them, and the message arriving
// readable takes its line over. The receipt goes to each device the sender
// lists (the device a message came from isn't kept) and carries this
// device's keys.
// How many it asked for goes to diag; how each went, the library's own
// warnings say there.
func (m *Messenger) askAgain(gen int, e e2eeClient) {
	wc, ok := e.(waClient)
	if !ok || wc.Client == nil {
		return
	}
	cli := wc.Client
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	since := m.clock.Now().Add(-24 * time.Hour).UnixMilli()
	page, err := m.st.Chats(Name, since, "", 20)
	if err != nil {
		return
	}
	asked, chats := 0, 0
	for _, c := range page.Items {
		if chats == 5 || asked >= 10 {
			break
		}
		if !strings.HasSuffix(c.ID, oneToOneSuffix) || !m.hasEncryptedSide(c.ID) {
			continue
		}
		chats++
		lines, err := m.st.Messages(Name, c.ID, "", 50)
		if err != nil {
			continue
		}
		user := watypes.JID{User: strings.TrimSuffix(c.ID, oneToOneSuffix), Server: watypes.MessengerServer}
		var devices []watypes.JID
		for _, l := range lines.Items {
			unread := l.Kind == "undecryptable" || l.Kind == "unavailable" || (l.Kind == "notice" && l.Hidden != nil && *l.Hidden && l.Source == "live")
			if l.FromMe || !unread || l.TimeMs < since || asked >= 10 {
				continue
			}
			if devices == nil {
				if err := m.request(ctx, gen, func(ctx context.Context) (err error) {
					devices, err = cli.GetUserDevices(ctx, []watypes.JID{user})
					return err
				}); err != nil || len(devices) == 0 {
					devices = []watypes.JID{user}
				}
			}
			for _, d := range devices {
				node := waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{"id": l.ID, "from": d, "t": strconv.FormatInt(l.TimeMs/1000, 10)}}
				info := &watypes.MessageInfo{MessageSource: watypes.MessageSource{Chat: user, Sender: d}, ID: l.ID, Timestamp: time.UnixMilli(l.TimeMs)}
				_ = m.request(ctx, gen, func(ctx context.Context) error {
					cli.DangerousInternals().SendRetryReceipt(ctx, &node, info, true)
					return nil
				})
			}
			asked++
		}
	}
	m.diagLine(fmt.Sprintf("asked again for %d messages it could not read, in %d chats, each from every device its sender lists", asked, chats))
}
