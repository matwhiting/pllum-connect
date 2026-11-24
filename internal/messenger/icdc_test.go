package messenger

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Every ID, key and session here is invented.

// diagText is the diagnostics file as it reads now.
func (h *harness) diagText(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(h.dir, Name, "diagnostics.txt"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(b)
}

// waitDiag waits until the diagnostics file holds what at least n times.
func (h *harness) waitDiag(t *testing.T, what string, n int) {
	t.Helper()
	eventually(t, what, func() bool { return strings.Count(h.diagText(t), what) >= n })
}

// icdcLines are the self-check's lines about the ICDC list, in order.
func (h *harness) icdcLines(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, l := range strings.Split(h.diagText(t), "\n") {
		if strings.Contains(l, "ICDC list") {
			out = append(out, l)
		}
	}
	return out
}

// savedDevice is what device.db holds of one device.
type savedDevice struct {
	identity [32]byte
	fbUUID   uuid.UUID
	device   uint16
}

// savedDevices are the devices device.db holds; the helper must be stopped.
func savedDevices(t *testing.T, dir string) []savedDevice {
	t.Helper()
	db, err := store.OpenDevice(filepath.Join(dir, Name, "device.db"), testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	devs, err := c.GetAllDevices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []savedDevice
	for _, d := range devs {
		out = append(out, savedDevice{identity: *d.IdentityKey.Pub, fbUUID: d.FacebookUUID, device: d.ID.Device})
	}
	return out
}

// registered is how many registrations Meta was asked for, and with which
// keys and Facebook device IDs.
func (f *fakeMeta) registered() (n int, keys [][32]byte, ids []uuid.UUID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registers, append([][32]byte{}, f.regKeys...), append([]uuid.UUID{}, f.regUUIDs...)
}

// The self-check asks Meta for the account's ICDC list once and writes
// whether this device's identity is in it, the list's size and its seq,
// and nothing else of the list: no identity, in any encoding, and no ID.
// With his identity there, nothing is registered again.
func TestTheSelfCheckLogsWhetherThisDeviceIsInTheICDCList(t *testing.T) {
	h := newHarness(t)
	h.f.icdc = [][32]byte{inventedIdentity(0x11), inventedIdentity(0x22)}
	h.f.icdcSeq = 4
	h.signIn(t)
	h.waitDiag(t, "ICDC list", 1)
	h.settle()

	lines := h.icdcLines(t)
	if len(lines) != 1 {
		t.Fatalf("the ICDC lines are %q, want one", lines)
	}
	if !strings.Contains(lines[0], "ICDC list has 3 identities, this device's among them true") || !strings.Contains(lines[0], "ICDC seq 5") {
		t.Errorf("the ICDC line reads %q", lines[0])
	}
	if n := h.f.count("FetchICDC"); n != 1 {
		t.Errorf("the ICDC list was fetched %d times, want once", n)
	}
	if n, _, _ := h.f.registered(); n != 1 {
		t.Errorf("Meta was asked to register %d times, want 1", n)
	}
	if v := h.m.value(reregisteredKey); v != "" {
		t.Errorf("the re-registration marker is set with his identity in the list")
	}
	if strings.Contains(h.diagText(t), "re-registration") {
		t.Errorf("a re-registration was logged with his identity in the list")
	}

	_, keys, ids := h.f.registered()
	text := h.diagText(t)
	for _, k := range append(keys, inventedIdentity(0x11), inventedIdentity(0x22)) {
		for _, enc := range []string{base64.StdEncoding.EncodeToString(k[:]), base64.RawURLEncoding.EncodeToString(k[:]), hex.EncodeToString(k[:])} {
			if strings.Contains(text, enc) {
				t.Fatalf("an identity from the list reached the diagnostics file")
			}
		}
	}
	for _, s := range []string{"2000001", ids[0].String()} {
		if strings.Contains(text, s) {
			t.Fatalf("an ID reached the diagnostics file")
		}
	}
}

// Identity present: nothing more is registered, and the marker stays unset.
func TestAPresentIdentityRegistersNothingMore(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.waitDiag(t, "ICDC list", 1)
	h.settle()
	h.f.clock.Advance(48 * time.Hour)
	h.settle()
	if n, _, _ := h.f.registered(); n != 1 {
		t.Fatalf("Meta was asked to register %d times, want 1", n)
	}
	if h.m.value(reregisteredKey) != "" {
		t.Errorf("the re-registration marker is set")
	}
	if l := h.icdcLines(t); len(l) != 1 || !strings.Contains(l[0], "among them true") {
		t.Errorf("the ICDC lines are %q", l)
	}
}

// Identity absent: the device is replaced exactly once, by a new device
// (new keys) with the session's Facebook device ID; the old device's row
// is gone, the new one is saved, the marker is set, and the new device's
// first connect checks the list again. A later helper with the marker set
// never re-registers, even with his identity absent again.
func TestAnAbsentIdentityReRegistersOnce(t *testing.T) {
	for _, same := range []bool{false, true} {
		name := "a new device number"
		if same {
			name = "the same device number"
		}
		t.Run(name, func(t *testing.T) {
			f := newFakeMeta()
			f.sameDevice = same
			if same {
				f.sessionUUID = inventedSessionUUID // a Messenger Lite session names one
			}
			f.icdcSkip = 1 // the first registration never lands in the list
			oldE2EE, newE2EE := f.e2ee, &fakeE2EE{}
			f.e2eeNext = []*fakeE2EE{oldE2EE, newE2EE}
			dir := filepath.Join(t.TempDir(), "connect")
			h := newHarnessOver(t, dir, f)
			h.signIn(t)
			h.waitDiag(t, "re-registration: registered", 1)
			h.waitDiag(t, "ICDC list", 2)
			eventually(t, "the new encrypted connection", func() bool {
				c := h.m.GateCounts().(GateCounts)
				return c.E2EEConnected
			})
			h.settle()

			n, keys, ids := f.registered()
			if n != 2 {
				t.Fatalf("Meta was asked to register %d times, want 2", n)
			}
			if keys[0] == keys[1] {
				t.Errorf("the re-registration kept the old keys")
			}
			switch {
			case same && ids[1] != inventedSessionUUID:
				t.Errorf("the re-registration's Facebook device ID is not the Messenger Lite session's")
			case !same && (ids[1] == uuid.Nil || ids[1] == ids[0]):
				t.Errorf("without a session device ID, the re-registration did not get a new one of its own")
			}
			if h.m.value(reregisteredKey) == "" {
				t.Errorf("the re-registration marker is not set")
			}
			lines := h.icdcLines(t)
			if len(lines) != 2 || !strings.Contains(lines[0], "among them false") || !strings.Contains(lines[1], "among them true") {
				t.Errorf("the ICDC lines are %q", lines)
			}
			text := h.diagText(t)
			if c := strings.Count(text, "re-registration: attempted"); c != 1 {
				t.Errorf("%d re-registrations attempted, want 1", c)
			}
			if want := "device ID changed " + map[bool]string{false: "true", true: "false"}[same]; !strings.Contains(text, want) {
				t.Errorf("the diagnostics do not say %q:\n%s", want, text)
			}
			oldE2EE.mu.Lock()
			oldDisc := oldE2EE.disconnects
			oldE2EE.mu.Unlock()
			newE2EE.mu.Lock()
			newConn := newE2EE.connects
			newE2EE.mu.Unlock()
			if oldDisc < 1 || newConn != 1 {
				t.Errorf("the old socket was disconnected %d times and the new one connected %d times", oldDisc, newConn)
			}
			if s, _ := h.state(); s != network.Live {
				t.Errorf("the state after re-registering is %s, want live", s)
			}
			h.close()

			saved := savedDevices(t, dir)
			if len(saved) != 1 {
				t.Fatalf("device.db holds %d devices, want 1", len(saved))
			}
			if saved[0].identity != keys[1] || saved[0].fbUUID != ids[1] {
				t.Errorf("device.db holds the old device, or the new one without the device ID it registered with")
			}

			// A later helper: his identity is absent again, and the marker is set.
			f.mu.Lock()
			f.icdc = [][32]byte{inventedIdentity(0x11)}
			f.mu.Unlock()
			later := newHarnessOver(t, dir, f)
			later.signIn(t)
			later.waitDiag(t, "ICDC list", 3)
			later.settle()
			later.f.clock.Advance(48 * time.Hour)
			later.settle()
			if n, _, _ := f.registered(); n != 2 {
				t.Fatalf("with the marker set, Meta was asked to register %d times, want 2", n)
			}
			l := later.icdcLines(t)
			if !strings.Contains(l[len(l)-1], "among them false") {
				t.Errorf("the later ICDC line reads %q", l[len(l)-1])
			}
			if c := strings.Count(later.diagText(t), "re-registration: attempted"); c != 1 {
				t.Errorf("%d re-registrations attempted in all, want 1", c)
			}
		})
	}
}

// A re-registration that fails is the registration-unclear block that a
// failed first registration is: no second attempt in the run, nothing on
// a restart's same session, and the same keys and device ID tried again
// only at a new sign-in, which then replaces the old device.
func TestAFailedReRegistrationDoesNotLoop(t *testing.T) {
	f := newFakeMeta()
	f.icdcSkip = 1
	f.registerErrs = []error{nil, errors.New("invented: the answer was lost")}
	dir := filepath.Join(t.TempDir(), "connect")
	h := newHarnessOver(t, dir, f)
	if err := h.m.Login(context.Background(), network.LoginParams{Method: network.LoginSession, Session: validSession()}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	eventually(t, "the failed re-registration", func() bool {
		s, k := h.state()
		return s == network.ReconnectNeeded && k == string(wire.RegistrationUnclear)
	})
	h.settle()
	h.f.clock.Advance(48 * time.Hour)
	h.settle()
	n, keys, ids := f.registered()
	if n != 2 {
		t.Fatalf("Meta was asked to register %d times, want 2 (the first, and one re-registration)", n)
	}
	text := h.diagText(t)
	if strings.Count(text, "re-registration: attempted") != 1 || strings.Count(text, "re-registration: failed") != 1 {
		t.Errorf("the diagnostics read:\n%s", text)
	}
	if h.m.value(reregisteredKey) == "" {
		t.Errorf("the re-registration marker is not set after a failed attempt")
	}

	// He signs in again: the same new keys and device ID are registered once more.
	h.login(t, newSession())
	eventually(t, "the encrypted connection", func() bool {
		c := h.m.GateCounts().(GateCounts)
		return c.Registered && c.E2EEConnected
	})
	h.settle()
	n, keys, ids = f.registered()
	if n != 3 {
		t.Fatalf("Meta was asked to register %d times, want 3", n)
	}
	if keys[2] != keys[1] || ids[2] != ids[1] {
		t.Errorf("the new sign-in registered other keys or another device ID than the failed attempt's")
	}
	if c := strings.Count(h.diagText(t), "re-registration: attempted"); c != 1 {
		t.Errorf("%d re-registrations attempted, want 1", c)
	}
	h.close()
	saved := savedDevices(t, dir)
	if len(saved) != 1 || saved[0].identity != keys[1] {
		t.Fatalf("device.db holds %d devices, want only the new one", len(saved))
	}
}

// A fresh registration (a first sign-in) uses a Messenger Lite session's
// device ID; a web cookie session names none, and the device keeps the one
// RegisterE2EE makes for it, as mautrix-meta's connector leaves it.
func TestTheFirstRegistrationsDeviceID(t *testing.T) {
	lite := newFakeMeta()
	lite.sessionUUID = inventedSessionUUID
	h := newHarnessOver(t, filepath.Join(t.TempDir(), "connect"), lite)
	h.signIn(t)
	if _, _, ids := lite.registered(); len(ids) != 1 || ids[0] != inventedSessionUUID {
		t.Fatalf("the first registration's Facebook device ID is not the Messenger Lite session's")
	}

	web := newFakeMeta()
	dir := filepath.Join(t.TempDir(), "connect")
	h2 := newHarnessOver(t, dir, web)
	h2.signIn(t)
	_, _, ids := web.registered()
	if len(ids) != 1 || ids[0] == uuid.Nil {
		t.Fatalf("a web session's registration had no Facebook device ID")
	}
	if h2.m.value("fbdevice") != "" {
		t.Errorf("a Facebook device ID was kept in the store")
	}
	h2.close()
	if saved := savedDevices(t, dir); len(saved) != 1 || saved[0].fbUUID != ids[0] {
		t.Errorf("the device was not saved with the device ID it registered with")
	}
}

// A fetch of the list that fails, or a list with no identity in it, writes
// its line and does nothing else: no registration, no marker.
func TestNoListNoReRegistration(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		setup      func(f *fakeMeta)
	}{
		{"the fetch fails", "ICDC list failed", func(f *fakeMeta) { f.icdcErr = errors.New("invented: refused") }},
		{"the list is empty", "ICDC list has 0 identities", func(f *fakeMeta) { f.icdc, f.icdcSkip = nil, 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMeta()
			tc.setup(f)
			h := newHarnessOver(t, filepath.Join(t.TempDir(), "connect"), f)
			h.signIn(t)
			h.waitDiag(t, "ICDC list", 1)
			h.settle()
			h.f.clock.Advance(48 * time.Hour)
			h.settle()
			if l := h.icdcLines(t); len(l) != 1 || !strings.Contains(l[0], tc.line) {
				t.Errorf("the ICDC lines are %q, want one with %q", l, tc.line)
			}
			if n, _, _ := f.registered(); n != 1 {
				t.Errorf("Meta was asked to register %d times, want 1", n)
			}
			if h.m.value(reregisteredKey) != "" || strings.Contains(h.diagText(t), "re-registration") {
				t.Errorf("a re-registration was marked or logged")
			}
		})
	}
}

// A run retired while the re-registration begins (after its marker is
// kept) arms nothing: no device to replace, no registration, and the
// marker stays set.
func TestAReRegistrationOfARetiredRunArmsNothing(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.settle()
	h.m.mu.Lock()
	gen := h.m.runs.Gen
	h.m.mu.Unlock()
	h.m.testReregister = func() {
		h.m.mu.Lock()
		h.m.retire()
		h.m.mu.Unlock()
	}
	began, err := h.m.reregister(context.Background(), gen)
	if began || err != nil {
		t.Fatalf("reregister of a retired run began %v, %v", began, err)
	}
	h.m.devStore.mu.Lock()
	armed := h.m.devStore.replacing != nil
	h.m.devStore.mu.Unlock()
	if armed {
		t.Errorf("a retired run armed a device to replace")
	}
	h.settle()
	if n, _, _ := h.f.registered(); n != 1 {
		t.Errorf("Meta was asked to register %d times, want 1", n)
	}
	if h.m.value(reregisteredKey) == "" {
		t.Errorf("the marker was cleared")
	}
}

// A marker that cannot be read counts as set: nothing re-registers on it.
func TestAnUnreadableMarkerCountsAsSet(t *testing.T) {
	h := newHarness(t)
	h.signIn(t)
	h.settle()
	h.m.mu.Lock()
	gen := h.m.runs.Gen
	h.m.mu.Unlock()
	h.m.values.Drop(reregisteredKey)
	h.st.Close() // every read of the store fails now
	if !h.m.reregistered() {
		t.Fatalf("an unreadable marker reads as unset")
	}
	if began, _ := h.m.reregister(context.Background(), gen); began {
		t.Errorf("a re-registration began on an unreadable marker")
	}
	if n, _, _ := h.f.registered(); n != 1 {
		t.Errorf("Meta was asked to register %d times, want 1", n)
	}
}
