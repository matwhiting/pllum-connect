package messenger

import (
	"errors"
	"os"

	"github.com/matwhiting/pllum-connect/internal/sealed"
)

// openMedia is a media file's bytes, opened with the files key of testKey;
// a file not sealed is an error, so a test never passes on plaintext.
func openMedia(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !sealed.IsSealed(raw) {
		return nil, errors.New("the media file is not sealed")
	}
	return sealed.Open(sealed.Key(testKey), raw)
}
