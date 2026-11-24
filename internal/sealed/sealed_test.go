package sealed

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The store key the server tests use: 00 01 … 1f.
func storeKey() [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = byte(i)
	}
	return k
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex: %v", err)
	}
	return b
}

// Made by Pllum's CryptoKit (HKDF<SHA256> and AES.GCM.seal with a fixed
// nonce, then magic + SealedBox.combined), so a file sealed here opens there
// and the other way round.
const (
	swiftFilesKey = "d1f5831371e8357f5cd5274335a63c3b2ffb481ada85f1ce1c1c499aa4e3a9b4"
	swiftPlain    = "invented media bytes, sealed by CryptoKit"
	swiftSealed   = "504c4c554d454e4301a0a1a2a3a4a5a6a7a8a9aaabaee578bfd478e32c0b8b0d7174e2decadd4ae131c03ee9def795a326503275e6f547240486e529ef3e684f00b22941cfc2ee383e850c1c93c3"
)

func TestKeyMatchesCryptoKit(t *testing.T) {
	k := Key(storeKey())
	if got := hex.EncodeToString(k[:]); got != swiftFilesKey {
		t.Fatalf("files key = %s, want CryptoKit's %s", got, swiftFilesKey)
	}
}

func TestSealMatchesCryptoKit(t *testing.T) {
	nonce := make([]byte, 12)
	for i := range nonce {
		nonce[i] = 0xA0 + byte(i)
	}
	got, err := sealWithNonce(Key(storeKey()), nonce, []byte(swiftPlain))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if want := mustHex(t, swiftSealed); !bytes.Equal(got, want) {
		t.Fatalf("sealed =\n%x\nwant CryptoKit's\n%x", got, want)
	}
}

func TestOpensCryptoKit(t *testing.T) {
	got, err := Open(Key(storeKey()), mustHex(t, swiftSealed))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if string(got) != swiftPlain {
		t.Fatalf("opened %q", got)
	}
}

func TestRoundTrip(t *testing.T) {
	k := Key(storeKey())
	plain := []byte("invented photo bytes")
	a, err := Seal(k, plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	b, _ := Seal(k, plain)
	if !IsSealed(a) || !bytes.HasPrefix(a, []byte("PLLUMENC\x01")) {
		t.Fatalf("no magic: %x", a[:9])
	}
	if len(a) != len(Magic)+12+len(plain)+16 {
		t.Fatalf("sealed length %d", len(a))
	}
	if bytes.Equal(a, b) {
		t.Fatal("two seals share a nonce")
	}
	if bytes.Contains(a, plain) {
		t.Fatal("plaintext in the sealed bytes")
	}
	got, err := Open(k, a)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("open = %q, %v", got, err)
	}
}

func TestTamperFails(t *testing.T) {
	k := Key(storeKey())
	s, _ := Seal(k, []byte("invented photo bytes"))
	for _, i := range []int{len(Magic), len(Magic) + 12, len(s) - 1} {
		bad := bytes.Clone(s)
		bad[i] ^= 1
		if _, err := Open(k, bad); err == nil {
			t.Errorf("a flipped byte at %d opened", i)
		}
	}
	if _, err := Open(k, s[:len(s)-1]); err == nil {
		t.Error("a cut file opened")
	}
	if _, err := Open(k, s[:len(Magic)+5]); err == nil {
		t.Error("a file shorter than a nonce opened")
	}
	other := Key([32]byte{1})
	if _, err := Open(other, s); err == nil {
		t.Error("opened under another key")
	}
}

func TestLegacyPlaintextReadsAsIs(t *testing.T) {
	plain := []byte("\xff\xd8\xffinvented legacy jpeg")
	got, err := Open(Key(storeKey()), plain)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("legacy = %q, %v", got, err)
	}
	if IsSealed(plain) {
		t.Fatal("plaintext reads as sealed")
	}
}

func TestWriteFileSealsAtomically(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "media")
	path := filepath.Join(dir, "abc")
	k := Key(storeKey())
	if err := WriteFile(dir, path, k, []byte("invented photo bytes")); err != nil {
		t.Fatalf("write: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("file is %o", fi.Mode().Perm())
	}
	if di, _ := os.Stat(dir); di.Mode().Perm() != 0o700 {
		t.Errorf("folder is %o", di.Mode().Perm())
	}
	raw, _ := os.ReadFile(path)
	if !IsSealed(raw) || bytes.Contains(raw, []byte("invented")) {
		t.Fatal("written in the clear")
	}
	got, err := ReadFile(path, k)
	if err != nil || string(got) != "invented photo bytes" {
		t.Fatalf("read = %q, %v", got, err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("left %d files behind", len(entries))
	}
}

func TestSealLegacyIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	k := Key(storeKey())
	plainPath := filepath.Join(dir, "legacy")
	if err := os.WriteFile(plainPath, []byte("invented legacy photo"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(plainPath, old, old); err != nil {
		t.Fatal(err)
	}
	sealedPath := filepath.Join(dir, "sealed")
	if err := WriteFile(dir, sealedPath, k, []byte("invented sealed photo")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(sealedPath)
	// a part file being written, a link and a folder are never touched
	part := filepath.Join(dir, ".part-123")
	os.WriteFile(part, []byte("invented part"), 0o600)
	target := filepath.Join(t.TempDir(), "elsewhere")
	os.WriteFile(target, []byte("invented elsewhere"), 0o600)
	os.Symlink(target, filepath.Join(dir, "link"))
	os.Mkdir(filepath.Join(dir, "sub"), 0o700)

	n, err := SealLegacy(dir, k)
	if err != nil || n != 1 {
		t.Fatalf("first pass sealed %d, %v; want 1", n, err)
	}
	raw, _ := os.ReadFile(plainPath)
	if !IsSealed(raw) {
		t.Fatal("legacy file still plain")
	}
	if got, _ := Open(k, raw); string(got) != "invented legacy photo" {
		t.Fatalf("legacy file opened to %q", got)
	}
	if fi, _ := os.Stat(plainPath); !fi.ModTime().Equal(old) || fi.Mode().Perm() != 0o600 {
		t.Errorf("legacy file's time %v (want %v) or mode %o changed", fi.ModTime(), old, fi.Mode().Perm())
	}
	if after, _ := os.ReadFile(sealedPath); !bytes.Equal(after, before) {
		t.Fatal("an already sealed file was sealed again")
	}
	if b, _ := os.ReadFile(part); string(b) != "invented part" {
		t.Fatal("a part file was touched")
	}
	if b, _ := os.ReadFile(target); string(b) != "invented elsewhere" {
		t.Fatal("a link was followed")
	}

	sealedOnce, _ := os.ReadFile(plainPath)
	n, err = SealLegacy(dir, k)
	if err != nil || n != 0 {
		t.Fatalf("second pass sealed %d, %v; want 0", n, err)
	}
	if again, _ := os.ReadFile(plainPath); !bytes.Equal(again, sealedOnce) {
		t.Fatal("second pass changed a sealed file")
	}
}

func TestSealLegacyWithoutFolder(t *testing.T) {
	n, err := SealLegacy(filepath.Join(t.TempDir(), "missing"), Key(storeKey()))
	if err != nil || n != 0 {
		t.Fatalf("missing folder: %d, %v", n, err)
	}
}

// Sealed here with a fixed nonce; Pllum's FileSealTests opens the same bytes
// with CryptoKit.
const (
	goPlain  = "invented media bytes, sealed by Go"
	goSealed = "504c4c554d454e4301101112131415161718191a1b00fd041ac1dc037dd6f40353aec7a9dda802a3e9329a315b9fa905b5560cc2e4d27d4be716e77d23c20965c95961cc9ae6c8"
)

func TestGoVectorForSwift(t *testing.T) {
	nonce := make([]byte, 12)
	for i := range nonce {
		nonce[i] = 0x10 + byte(i)
	}
	got, err := sealWithNonce(Key(storeKey()), nonce, []byte(goPlain))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if hex.EncodeToString(got) != goSealed {
		t.Fatalf("Go's vector changed: %x", got)
	}
}
