package quiet

import (
	"errors"
	"io"
	"log"
	"os"
	"slices"
	"testing"

	"github.com/rs/zerolog"
	signallog "go.mau.fi/libsignal/logger"
)

func TestQuietSwitchesEveryLoggerOff(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	nop := zerolog.Nop()
	zerolog.DefaultContextLogger = &nop
	log.SetOutput(os.Stderr)

	Init()

	if got := zerolog.GlobalLevel(); got != zerolog.Disabled {
		t.Errorf("zerolog global level = %v, want disabled", got)
	}
	if zerolog.DefaultContextLogger != nil {
		t.Errorf("zerolog.DefaultContextLogger is set, want nil")
	}
	if log.Writer() != io.Discard {
		t.Errorf("the standard log package still writes somewhere")
	}
}

// libsignal's own logger prints to stdout, the helper's pipe to Pllum, unless
// it is replaced: after Init, nothing libsignal logs reaches stdout.
func TestLibsignalWritesNothingToStdout(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	Init()
	signallog.Debug("debug line")
	signallog.Info("info line")
	signallog.Warning("warning line")
	signallog.Error("error line")
	os.Stdout = stdout
	w.Close()
	got, _ := io.ReadAll(r)
	if len(got) != 0 {
		t.Errorf("libsignal wrote to stdout: %q", got)
	}
}

// libsignal's warnings and errors (why a message failed to decrypt) go to the
// sink SignalTo sets; its debug lines (whole session records, keys and all)
// and info lines never do.
func TestLibsignalWarningsAndErrorsReachTheSink(t *testing.T) {
	Init()
	var got []string
	SignalTo(func(line string) { got = append(got, line) })
	defer SignalTo(nil)
	signallog.Debug("Decrypting ciphertext with record: ", "KEYCANARY")
	signallog.Info("info line")
	signallog.Warning(errors.New("failed to verify ciphertext MAC"))
	signallog.Error("Unable to decrypt message with state: ", "wrong message version")
	want := []string{"failed to verify ciphertext MAC", "Unable to decrypt message with state: wrong message version"}
	if !slices.Equal(got, want) {
		t.Errorf("the sink got %q, want %q", got, want)
	}
}
