package server

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/sealed"
	"github.com/matwhiting/pllum-connect/internal/store"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

func storeKeyBytes(t *testing.T) [32]byte {
	t.Helper()
	b, err := hex.DecodeString(testKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	var k [32]byte
	copy(k[:], b)
	return k
}

// Media kept in the clear before files were sealed is sealed at hello, once
// the key has opened the store and before any network starts.
func TestHelloSealsLegacyMediaBeforeTheNetworksStart(t *testing.T) {
	key := storeKeyBytes(t)
	var media string
	var atMake []byte
	fake := &fakeNetwork{name: "whatsapp"}
	h := startNets(t, func(emit network.Emit) []network.Network {
		atMake, _ = os.ReadFile(filepath.Join(media, "legacy"))
		fake.emit, fake.rawEmit = emit.For("whatsapp"), emit
		return []network.Network{fake}
	}, func(dir string, _ [32]byte) (*store.Store, error) {
		media = filepath.Join(dir, "media")
		if err := os.MkdirAll(media, 0o700); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(media, "legacy"), []byte("invented legacy photo"), 0o600); err != nil {
			return nil, err
		}
		return &store.Store{}, nil
	}, nil)
	h.hello()

	if !sealed.IsSealed(atMake) {
		t.Fatal("the networks were made before the legacy file was sealed")
	}
	raw, _ := os.ReadFile(filepath.Join(media, "legacy"))
	if bytes.Contains(raw, []byte("invented")) {
		t.Fatal("the legacy file is still in the clear")
	}
	got, err := sealed.Open(sealed.Key(key), raw)
	if err != nil || string(got) != "invented legacy photo" {
		t.Fatalf("the sealed file opens to %q, %v", got, err)
	}
}

// A key that does not open the store seals nothing: a wrong key never
// touches his media.
func TestAKeyThatDoesNotOpenTheStoreSealsNothing(t *testing.T) {
	var media string
	h := startNets(t, func(emit network.Emit) []network.Network { return nil },
		func(dir string, _ [32]byte) (*store.Store, error) {
			media = filepath.Join(dir, "media")
			os.MkdirAll(media, 0o700)
			os.WriteFile(filepath.Join(media, "legacy"), []byte("invented legacy photo"), 0o600)
			return nil, &wire.Error{Kind: wire.StoreKey}
		}, nil)
	h.send(`{"id":"h","method":"hello","params":{"protocol":1,"storeKey":"` + testKeyHex + `","app":"x"}}`)
	if k := kindOf(t, h.next()); k != string(wire.StoreKey) {
		t.Fatalf("hello gave %v, want storeKey", k)
	}
	if raw, _ := os.ReadFile(filepath.Join(media, "legacy")); string(raw) != "invented legacy photo" {
		t.Fatalf("the file was changed without the store opening: %q", raw)
	}
}
