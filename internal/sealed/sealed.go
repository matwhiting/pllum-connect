// Package sealed keeps the media the helper downloads sealed at rest, in
// Pllum's own file format (Pllum's FileSeal): the magic "PLLUMENC\x01", then
// an AES-256-GCM box as CryptoKit's SealedBox.combined lays it out (a 12-byte
// nonce, the ciphertext, the 16-byte tag). The key is HKDF-SHA256 of the
// store key hello carries, with no salt and info "pllum-files-v1", so Pllum
// opens these files with the files key it already has, and the helper never
// keeps a second secret.
//
// A file without the magic is one from before: it reads as it is, and
// SealLegacy seals it once.
package sealed

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Magic is the first bytes of a sealed file.
var Magic = []byte("PLLUMENC\x01")

// Info is HKDF's info for the files key, the same as Pllum's.
const Info = "pllum-files-v1"

const nonceSize = 12

// ErrUnreadable is a sealed file that does not open: changed, cut short or
// sealed under another key. Its bytes are never returned.
var ErrUnreadable = errors.New("sealed: unreadable")

// Key is the files key: HKDF-SHA256 of the store key, no salt, info
// "pllum-files-v1", 32 bytes.
func Key(storeKey [32]byte) [32]byte {
	b, err := hkdf.Key(sha256.New, storeKey[:], nil, Info, 32)
	if err != nil {
		// only a length over 255 hashes fails
		panic(err)
	}
	var k [32]byte
	copy(k[:], b)
	return k
}

// IsSealed reports whether data starts with the magic.
func IsSealed(data []byte) bool { return bytes.HasPrefix(data, Magic) }

// Seal is plain sealed under key with a fresh random nonce.
func Seal(key [32]byte, plain []byte) ([]byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return sealWithNonce(key, nonce, plain)
}

func sealWithNonce(key [32]byte, nonce, plain []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(Magic)+nonceSize+len(plain)+gcm.Overhead())
	out = append(out, Magic...)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, plain, nil), nil
}

// Open is data opened when it is sealed, and data as it is when it is not
// (a file from before). A sealed file that does not open is ErrUnreadable.
func Open(key [32]byte, data []byte) ([]byte, error) {
	if !IsSealed(data) {
		return data, nil
	}
	box := data[len(Magic):]
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(box) < nonceSize+gcm.Overhead() {
		return nil, ErrUnreadable
	}
	plain, err := gcm.Open(nil, box[:nonceSize], box[nonceSize:], nil)
	if err != nil {
		return nil, ErrUnreadable
	}
	return plain, nil
}

func newGCM(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// WriteFile writes data sealed to path in dir, whole or not at all: dir is
// made 0700, the sealed bytes go to a hidden ".part-*" file there, made 0600,
// which is then renamed into place.
func WriteFile(dir, path string, key [32]byte, data []byte) error {
	out, err := Seal(key, data)
	if err != nil {
		return err
	}
	return writeAtomic(dir, path, out)
}

// ReadFile is a file's contents, opened when sealed and as they are when
// not. A last link is never followed.
func ReadFile(path string, key [32]byte) ([]byte, error) {
	data, err := readNoFollow(path)
	if err != nil {
		return nil, err
	}
	return Open(key, data)
}

// SealLegacy seals every plain file in dir (one from before files were
// sealed), each written whole by itself as WriteFile writes, keeping its
// modification time (the folder's trim goes by it). A sealed file, a hidden
// one (a part being written), a link and a folder are left alone, so a second
// pass changes nothing. It must run before anything else writes in dir. A
// missing dir is nothing to do. A file that fails is left as it was, and the
// rest still go; the first failure is returned with the count sealed.
func SealLegacy(dir string, key [32]byte) (int, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n := 0
	var first error
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || !e.Type().IsRegular() {
			continue
		}
		path := filepath.Join(dir, e.Name())
		done, err := sealOne(dir, path, key)
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		if done {
			n++
		}
	}
	return n, first
}

func sealOne(dir, path string, key [32]byte) (bool, error) {
	fi, err := os.Lstat(path)
	if err != nil || !fi.Mode().IsRegular() {
		return false, err
	}
	data, err := readNoFollow(path)
	if err != nil {
		return false, err
	}
	if IsSealed(data) {
		return false, nil
	}
	out, err := Seal(key, data)
	if err != nil {
		return false, err
	}
	if err := writeAtomic(dir, path, out); err != nil {
		return false, err
	}
	_ = os.Chtimes(path, fi.ModTime(), fi.ModTime())
	return true, nil
}

func readNoFollow(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, os.ErrInvalid
	}
	return io.ReadAll(f)
}

func writeAtomic(dir, path string, data []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".part-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, werr := f.Write(data)
	cerr := f.Close()
	if werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Chmod(tmp, 0o600)
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
	}
	if werr != nil {
		os.Remove(tmp)
		return werr
	}
	return nil
}
