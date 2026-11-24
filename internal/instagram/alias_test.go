package instagram

import (
	"path/filepath"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/network"
)

var _ network.ParticipantAliaser = (*Instagram)(nil)

// Every ID in these tests is invented.

func TestParticipantAliasTranslatesAnFBIDThroughThePeopleSeen(t *testing.T) {
	h := openHarness(t, filepath.Join(t.TempDir(), "connect"), nil)
	const fbid, igid = "100000000000001", "17841400000000001"
	if err := h.g.keep("user."+fbid, igid); err != nil {
		t.Fatal(err)
	}

	if got, ok := h.g.ParticipantAlias(fbid); !ok || got != igid {
		t.Errorf("ParticipantAlias(mapped FBID) = %q, %v, want the IGID", got, ok)
	}
	if got, ok := h.g.ParticipantAlias("100000000000099"); ok || got != "" {
		t.Errorf("ParticipantAlias(unmapped FBID) = %q, %v, want nothing", got, ok)
	}
}

func TestParticipantAliasReadsNothingForANonDigitID(t *testing.T) {
	h := openHarness(t, filepath.Join(t.TempDir(), "connect"), nil)
	for _, id := range []string{"", "abc", "1000x0001", " 100000000000001", "-100000000000001", "１００"} {
		if got, ok := h.g.ParticipantAlias(id); ok || got != "" {
			t.Errorf("ParticipantAlias(%q) = %q, %v, want nothing", id, got, ok)
		}
		if h.g.values.Held("user." + id) {
			t.Errorf("ParticipantAlias(%q) read a network value", id)
		}
	}
}
