package messenger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/serialize"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
	"go.mau.fi/whatsmeow/proto/waAdv"
	wastore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	watypes "go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/store"
)

// A first message from a newer client decrypts even when its inner
// SignalMessage carries a field this library doesn't know: the sender's MAC
// covers the bytes as sent, so it has to be checked over those bytes, not over
// a re-encoding of the fields we parsed (which drops the unknown one and
// fails as "mismatching MAC in signal message").
func TestAFirstMessageWithAnUnknownFieldDecrypts(t *testing.T) {
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
	bundle := prekey.NewBundle(us.RegistrationID, uint32(us.ID.Device),
		optional.NewOptionalUint32(ours.KeyID), us.SignedPreKey.KeyID,
		ecc.NewDjbECPublicKey(*ours.Pub), ecc.NewDjbECPublicKey(*us.SignedPreKey.Pub), *us.SignedPreKey.Signature,
		identity.NewKey(ecc.NewDjbECPublicKey(*us.IdentityKey.Pub)))
	toUs := session.NewBuilderFromSignal(them, us.ID.SignalAddress(), wastore.SignalProtobufSerializer)
	if err := toUs.ProcessBundle(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	// The sender's MAC key for its first message, read before Encrypt moves
	// the chain on.
	rec, err := them.LoadSession(ctx, us.ID.SignalAddress())
	if err != nil {
		t.Fatal(err)
	}
	macKey := rec.SessionState().SenderChainKey().MessageKeys().MacKey()

	sent, err := session.NewCipher(toUs, us.ID.SignalAddress()).Encrypt(ctx, []byte("invented"))
	if err != nil {
		t.Fatal(err)
	}

	// Re-send it the way a newer client would: same fields, plus one this
	// library has never heard of, and the MAC computed over the bytes sent.
	raw := sent.Serialize()
	var pk serialize.PreKeySignalMessage
	if err := proto.Unmarshal(raw[1:], &pk); err != nil {
		t.Fatal(err)
	}
	inner := pk.GetMessage()
	body := append([]byte{}, inner[:len(inner)-protocol.MacLength]...) // version byte + protobuf
	body = protowire.AppendTag(body, 15, protowire.VarintType)
	body = protowire.AppendVarint(body, 1)
	mac := hmac.New(sha256.New, macKey)
	mac.Write(ecc.NewDjbECPublicKey(*them.IdentityKey.Pub).Serialize())
	mac.Write(ecc.NewDjbECPublicKey(*us.IdentityKey.Pub).Serialize())
	mac.Write(body)
	pk.Message = append(body, mac.Sum(nil)[:protocol.MacLength]...)
	outer, err := proto.Marshal(&pk)
	if err != nil {
		t.Fatal(err)
	}

	first, err := protocol.NewPreKeySignalMessageFromBytes(append([]byte{raw[0]}, outer...),
		wastore.SignalProtobufSerializer.PreKeySignalMessage, wastore.SignalProtobufSerializer.SignalMessage)
	if err != nil {
		t.Fatal(err)
	}
	fromThem := session.NewBuilderFromSignal(us, them.ID.SignalAddress(), wastore.SignalProtobufSerializer)
	got, err := session.NewCipher(fromThem, them.ID.SignalAddress()).DecryptMessage(ctx, first)
	if err != nil {
		t.Fatalf("the first message didn't decrypt: %v", err)
	}
	if string(got) != "invented" {
		t.Errorf("decrypted %q", got)
	}
}
