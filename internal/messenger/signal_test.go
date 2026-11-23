package messenger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
	"go.mau.fi/whatsmeow/proto/waAdv"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	watypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// Why a message failed to decrypt, which whatsmeow reports only as "no valid
// sessions", reaches the diagnostics file in libsignal's own words, without
// numbers, once the encrypted side is up: here a first message whose sender
// was given a one-time key under one of ours that isn't ours fails its MAC.
func TestWhyAMessageFailedToDecryptIsWritten(t *testing.T) {
	quiet.Init()
	defer quiet.SignalTo(nil)
	h := newHarness(t)
	h.signIn(t)
	ctx := context.Background()

	db, err := store.OpenDevice(filepath.Join(t.TempDir(), "pair", "device.db"), testKey)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := c.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	us, them := c.NewDevice(), c.NewDevice()
	us.ID = &watypes.JID{User: "1000001", Device: 7, Server: watypes.MessengerServer}
	them.ID = &watypes.JID{User: "1000002", Device: 3, Server: watypes.MessengerServer}
	for _, d := range []*wastore.Device{us, them} {
		d.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
		if err := d.Save(ctx); err != nil {
			t.Fatal(err)
		}
	}
	ours, err := us.PreKeys.GenOnePreKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	notOurs := keys.NewKeyPair()
	bundle := prekey.NewBundle(us.RegistrationID, uint32(us.ID.Device),
		optional.NewOptionalUint32(ours.KeyID), us.SignedPreKey.KeyID,
		ecc.NewDjbECPublicKey(*notOurs.Pub), ecc.NewDjbECPublicKey(*us.SignedPreKey.Pub), *us.SignedPreKey.Signature,
		identity.NewKey(ecc.NewDjbECPublicKey(*us.IdentityKey.Pub)))
	toUs := session.NewBuilderFromSignal(them, us.ID.SignalAddress(), wastore.SignalProtobufSerializer)
	if err := toUs.ProcessBundle(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	sent, err := session.NewCipher(toUs, us.ID.SignalAddress()).Encrypt(ctx, []byte("invented"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := protocol.NewPreKeySignalMessageFromBytes(sent.Serialize(),
		wastore.SignalProtobufSerializer.PreKeySignalMessage, wastore.SignalProtobufSerializer.SignalMessage)
	if err != nil {
		t.Fatal(err)
	}
	fromThem := session.NewBuilderFromSignal(us, them.ID.SignalAddress(), wastore.SignalProtobufSerializer)
	if _, err := session.NewCipher(fromThem, them.ID.SignalAddress()).DecryptMessage(ctx, first); err == nil {
		t.Fatal("the message decrypted")
	}

	b, err := os.ReadFile(filepath.Join(h.dir, Name, "diagnostics.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "signal: Unable to verify ciphertext mac: mismatching MAC in signal message") {
		t.Errorf("the reason isn't in the diagnostics file: %q", b)
	}
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.HasPrefix(strings.SplitN(l, " ", 2)[1], "signal:") && strings.ContainsAny(l[20:], "0123456789") {
			t.Errorf("a number survived: %q", l)
		}
	}
}
