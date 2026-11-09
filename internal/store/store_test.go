package store

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/matwhiting/pllum-connect/internal/wire"
)

// canary is invented text that must never reach a file in the clear.
const canary = "canary-velvet-plum-4417"

// checkFolder fails unless dir is 0700, every file under it is 0600 and no
// file holds canary or SQLite's plain-text header.
func checkFolder(t *testing.T, dir string) {
	t.Helper()
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("the folder is %o, want 700", perm)
	}
	files := 0
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			if perm := info.Mode().Perm(); perm != 0o700 {
				t.Errorf("folder %s is %o, want 700", filepath.Base(path), perm)
			}
			return nil
		}
		files++
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("%s is %o, want 600", filepath.Base(path), perm)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(b, []byte(canary)) {
			t.Errorf("%s holds the canary in the clear", filepath.Base(path))
		}
		if bytes.HasPrefix(b, []byte("SQLite format 3")) {
			t.Errorf("%s is a plain SQLite file", filepath.Base(path))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if files == 0 {
		t.Fatalf("no files in the store folder")
	}
}

func TestTheStoreIsEncryptedAndOnlyHis(t *testing.T) {
	// A folder made too open beforehand is closed up.
	dir := filepath.Join(t.TempDir(), "connect")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	title := "Group " + canary
	if err := s.PutChat(net, ChatInfo{ID: groupChat, Kind: "group", Title: &title,
		Participants: []Participant{{ID: phoneChat, Name: "Name " + canary}}}); err != nil {
		t.Fatalf("PutChat: %v", err)
	}
	for i := range 50 {
		put(t, s, text(groupChat, idN("3EB0C", i), base+int64(i)*1000, "line "+canary))
	}
	importOne(t, s, ImportBatch{BeeperChatID: "bc-canary", Chat: ImportChat{Kind: "single", Title: canary},
		Messages: []ImportMessage{beeperMsg("bm-canary", base, "copied "+canary)}})
	if p, err := s.Search(net, Query{Text: canary, Limit: 50}); err != nil || len(p.Items) == 0 {
		t.Fatalf("Search: %d found, %v", len(p.Items), err)
	}

	checkFolder(t, dir) // while open, with the write-ahead log in use
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	checkFolder(t, dir)
}

func TestAWrongKeyIsStoreKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "connect")
	s, err := Open(dir, testKey)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	put(t, s, text(phoneChat, "3EB0K0001", base, "kept"))
	s.Close()

	_, err = Open(dir, otherKey)
	var we *wire.Error
	if !errors.As(err, &we) || we.Kind != wire.StoreKey {
		t.Fatalf("a wrong key gave %v, want storeKey", err)
	}

	// The store is still there, whole, for the right key.
	s, err = Open(dir, testKey)
	if err != nil {
		t.Fatalf("reopening with the right key: %v", err)
	}
	defer s.Close()
	if got := ids(all(t, s, phoneChat)); len(got) != 1 || got[0] != "3EB0K0001" {
		t.Fatalf("after a wrong key the store holds %v", got)
	}

	// A network's session database says the same.
	dev := filepath.Join(dir, "whatsapp", "device.db")
	db, err := OpenDevice(dev, testKey)
	if err != nil {
		t.Fatalf("OpenDevice: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatalf("writing the device file: %v", err)
	}
	db.Close()
	if _, err := OpenDevice(dev, otherKey); wire.KindOf(err) != wire.StoreKey {
		t.Fatalf("a wrong device key gave %v, want storeKey", err)
	}
	if _, err := os.Stat(dev); err != nil {
		t.Fatalf("the device file went: %v", err)
	}
}

func TestWhatsmeowsDeviceStoreUpgradesOnTheEncryptedFile(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "connect")
	path := filepath.Join(dir, "whatsapp", "device.db")
	jid := types.NewJID("10000000001", types.DefaultUserServer)

	db, err := OpenDevice(path, testKey)
	if err != nil {
		t.Fatalf("OpenDevice: %v", err)
	}
	c := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := c.Upgrade(ctx); err != nil {
		t.Fatalf("Upgrade: %v", err)
	}
	dev := c.NewDevice()
	dev.ID = &jid
	// Pairing gives a device its signed identity; invented bytes stand in.
	dev.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte("invented-details"),
		AccountSignatureKey: make([]byte, 32),
		AccountSignature:    make([]byte, 64),
		DeviceSignature:     make([]byte, 64),
	}
	dev.PushName = canary
	if err := dev.Save(ctx); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = OpenDevice(path, testKey)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer db.Close()
	c = sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := c.Upgrade(ctx); err != nil {
		t.Fatalf("Upgrade again: %v", err)
	}
	got, err := c.GetFirstDevice(ctx)
	if err != nil {
		t.Fatalf("GetFirstDevice: %v", err)
	}
	if got.ID == nil || *got.ID != jid {
		t.Fatalf("the device was not found after reopening")
	}
	db.Close()
	checkFolder(t, dir)
}

func TestCountsAreCountsOnly(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0N0001", base+1000, "one"))
	put(t, s, text(phoneChat, "3EB0N0002", base+2000, "two"))
	importOne(t, s, ImportBatch{BeeperChatID: "bc-1", ChatID: phoneChat, Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-1", base+1000, "one"), beeperMsg("bm-2", base-5000, "older")}})
	importOne(t, s, ImportBatch{BeeperChatID: "bc-2", Chat: ImportChat{Kind: "single"},
		Messages: []ImportMessage{beeperMsg("bm-3", base-9000, "elsewhere")}})

	c, err := s.Counts(net)
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	want := Counts{Chats: 2, Messages: 4, Imported: 3, Matched: 1, UnplacedChats: 1}
	oldest := base - 9000
	want.OldestMs = &oldest
	if c.Chats != want.Chats || c.Messages != want.Messages || c.Imported != want.Imported ||
		c.Matched != want.Matched || c.UnplacedChats != want.UnplacedChats || c.OldestMs == nil || *c.OldestMs != oldest {
		t.Fatalf("Counts = %+v, want %+v", c, want)
	}

	// An empty store has no oldest time.
	e, err := openTest(t).Counts(net)
	if err != nil || e.OldestMs != nil || e.Messages != 0 {
		t.Fatalf("empty Counts = %+v, %v", e, err)
	}
}
