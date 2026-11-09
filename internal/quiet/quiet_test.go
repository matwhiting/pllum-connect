package quiet

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/rs/zerolog"
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
