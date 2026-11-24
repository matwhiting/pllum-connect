package network

import (
	"path/filepath"
	"testing"

	"github.com/matwhiting/pllum-connect/internal/store"
)

func TestValuesAreReadOnceAndWrittenOnlyWhenTheyChange(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "connect"), [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.PutNetworkValue("instagram", "seq", "41"); err != nil {
		t.Fatal(err)
	}
	v := NewValues(st, "instagram")
	if v.Held("seq") || v.Get("seq") != "41" || !v.Held("seq") {
		t.Fatalf("seq was not read once and held")
	}
	// Written behind its back: the memory is what it goes by.
	_ = st.PutNetworkValue("instagram", "seq", "42")
	if v.Get("seq") != "41" {
		t.Errorf("Get read the store again")
	}
	if err := v.Keep("seq", "43"); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := st.NetworkValue("instagram", "seq"); got != "43" || v.Get("seq") != "43" {
		t.Errorf("Keep stored %q, holds %q", got, v.Get("seq"))
	}
	if other := NewValues(st, "messenger"); other.Get("seq") != "" {
		t.Errorf("another network reads instagram's value")
	}
	v.Drop("seq")
	_ = st.PutNetworkValue("instagram", "seq", "44")
	if v.Get("seq") != "44" {
		t.Errorf("a dropped value was not read again")
	}
	if err := v.Put("seq", ""); err != nil || v.Get("seq") != "" {
		t.Errorf("Put of \"\" did not remove it")
	}
}
