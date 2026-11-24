package store

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// The benchmarks run on an invented store shaped like a real one: 600 chats
// (60 of them groups of 30), about 30,000 lines (ten busy groups of 1,000
// and 34 in every other chat), a reaction on every tenth line, and a read
// marker that leaves the newest fifth of each chat unread. Every ID and text
// is invented.

const (
	benchChats  = 600
	benchGroups = 60
	benchPeople = 30
)

func benchChat(i int) string {
	if i < benchGroups {
		return fmt.Sprintf("1200000000%08d@g.us", i)
	}
	return fmt.Sprintf("1000%08d@s.whatsapp.net", i)
}

func benchLine(chat, i int) string { return fmt.Sprintf("BENCH%04d%06d", chat, i) }

var (
	fixtureOnce sync.Once
	fixtureDir  string
	fixtureErr  error
)

// TestMain removes the benchmarks' fixture once the run is over.
func TestMain(m *testing.M) {
	code := m.Run()
	if fixtureDir != "" {
		os.RemoveAll(filepath.Dir(fixtureDir))
	}
	os.Exit(code)
}

// benchFixture builds the invented store once per run, straight in SQL (a
// line at a time through PutMessage would take most of a minute), and
// returns the folder holding it, closed.
func benchFixture(b *testing.B) string {
	b.Helper()
	fixtureOnce.Do(func() {
		dir, err := os.MkdirTemp("", "pllum-connect-bench")
		if err != nil {
			fixtureErr = err
			return
		}
		fixtureDir = filepath.Join(dir, "connect")
		s, err := Open(fixtureDir, testKey)
		if err != nil {
			fixtureErr = err
			return
		}
		defer s.Close()
		fixtureErr = s.write(func(tx *sql.Tx) error {
			for c := range benchChats {
				per := 34
				if c < 10 {
					per = 1000
				}
				chat := benchChat(c)
				kind := "single"
				if c < benchGroups {
					kind = "group"
				}
				first := base + int64(c)*1000
				readThrough := first + int64(per*4/5)*60000
				if _, err := tx.Exec(`INSERT INTO chats (network, chat_id, kind, title, read_through_ms)
						VALUES (?, ?, ?, ?, ?)`, net, chat, kind, "Chat "+chat[:8], readThrough); err != nil {
					return err
				}
				people := 1
				if kind == "group" {
					people = benchPeople
				}
				for p := range people {
					if _, err := tx.Exec(`INSERT INTO participants (network, chat_id, participant_id, name, position)
							VALUES (?, ?, ?, ?, ?)`, net, chat, fmt.Sprintf("2000%08d@s.whatsapp.net", c*100+p),
						fmt.Sprintf("Person %d", p), p); err != nil {
						return err
					}
				}
				for i := range per {
					id := benchLine(c, i)
					if _, err := tx.Exec(`INSERT INTO messages
							(network, chat_id, msg_id, sender_id, from_me, ts_ms, text, kind, source)
							VALUES (?, ?, ?, ?, ?, ?, ?, 'text', 'sync')`,
						net, chat, id, "2000@s.whatsapp.net", i%3 == 0, first+int64(i)*60000,
						fmt.Sprintf("invented line %d of chat %d", i, c)); err != nil {
						return err
					}
					if i%10 == 0 {
						if _, err := tx.Exec(`INSERT INTO reactions (network, chat_id, msg_id, sender_id, emoji)
								VALUES (?, ?, ?, ?, '👍')`, net, chat, id, "2000@s.whatsapp.net"); err != nil {
							return err
						}
					}
				}
			}
			return refreshAll(tx)
		})
	})
	if fixtureErr != nil {
		b.Fatalf("fixture: %v", fixtureErr)
	}
	return fixtureDir
}

// openFixture opens a copy of the fixture, for a benchmark that writes.
func openFixture(b *testing.B) *Store {
	b.Helper()
	src := benchFixture(b)
	dst := filepath.Join(b.TempDir(), "connect")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"messages.db", "messages.db-wal"} {
		in, err := os.Open(filepath.Join(src, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			b.Fatal(err)
		}
		out, err := os.OpenFile(filepath.Join(dst, name), os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			b.Fatal(err)
		}
		in.Close()
		out.Close()
	}
	s, err := Open(dst, testKey)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// historyImport is a 5,000-line history sync into the fixture: 40
// conversations of 125 lines each, newer than what is held and so unread,
// the first 20 into groups that already hold lines.
func historyImport() [][]Message {
	var convs [][]Message
	for c := range 40 {
		chat := benchChat(c * 3)
		var ms []Message
		for i := range 125 {
			ms = append(ms, Message{
				ID: fmt.Sprintf("HIST%04d%06d", c, i), ChatID: chat, SenderID: "2001@s.whatsapp.net",
				TimeMs: base + 100_000_000 + int64(i)*1000, Text: fmt.Sprintf("invented history %d", i),
				Kind: "text", Source: SourceSync, FromMe: i%4 == 0,
			})
		}
		convs = append(convs, ms)
	}
	return convs
}

// BenchmarkHistoryImport is one 5,000-line history sync, a line at a time
// (one transaction and one chat refresh per line).
func BenchmarkHistoryImport(b *testing.B) {
	convs := historyImport()
	for b.Loop() {
		b.StopTimer()
		s := openFixture(b)
		b.StartTimer()
		for _, ms := range convs {
			for _, m := range ms {
				if _, err := s.PutMessage(net, m); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*5000), "µs/line")
}

// BenchmarkHistoryImportBatched is the same sync a conversation at a time
// (a Batch: one transaction and one refresh of the chat for each), as
// WhatsApp writes it.
func BenchmarkHistoryImportBatched(b *testing.B) {
	convs := historyImport()
	for b.Loop() {
		b.StopTimer()
		s := openFixture(b)
		b.StartTimer()
		for _, ms := range convs {
			err := s.Batch(net, func(bt *Batch) error {
				for _, m := range ms {
					if _, err := bt.PutMessage(m); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*5000), "µs/line")
}

// BenchmarkChats is the first page of 200 chats, with their participants.
func BenchmarkChats(b *testing.B) {
	s := openFixture(b)
	for b.Loop() {
		p, err := s.Chats(net, 0, "", 200)
		if err != nil || len(p.Items) != 200 {
			b.Fatalf("Chats: %d, %v", len(p.Items), err)
		}
	}
}

// BenchmarkMessages is the newest page of a chat's lines, with reactions.
func BenchmarkMessages(b *testing.B) {
	s := openFixture(b)
	chat := benchChat(1)
	for b.Loop() {
		p, err := s.Messages(net, chat, "", 200)
		if err != nil || len(p.Items) != 200 {
			b.Fatalf("Messages: %d, %v", len(p.Items), err)
		}
	}
}

// BenchmarkChatOfLine finds the chat of a line by its ID alone, as LinkedIn
// does for a line whose event names no conversation.
func BenchmarkChatOfLine(b *testing.B) {
	s := openFixture(b)
	want := benchChat(450)
	id := benchLine(450, 20)
	for b.Loop() {
		if c, ok := s.ChatOfLine(net, id); !ok || c != want {
			b.Fatalf("ChatOfLine: %q %v", c, ok)
		}
	}
}

// BenchmarkReadBurst is a burst of eight reads at once, as Pllum sends
// after a changed event.
func BenchmarkReadBurst(b *testing.B) {
	s := openFixture(b)
	for b.Loop() {
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				if _, ok, err := s.Chat(net, benchChat(i)); err != nil || !ok {
					b.Errorf("Chat: %v %v", ok, err)
				}
			})
		}
		wg.Wait()
	}
}

// BenchmarkNextChatByPages is how LinkedIn found the next chat to page
// before it had a query of its own: every page of Chats, here with no chat
// wanted (the walk's whole cost, as when every chat's history is done).
func BenchmarkNextChatByPages(b *testing.B) {
	s := openFixture(b)
	for b.Loop() {
		cursor, seen := "", 0
		for range 50 {
			p, err := s.Chats(net, 0, cursor, 200)
			if err != nil {
				b.Fatal(err)
			}
			seen += len(p.Items)
			if p.Cursor == "" {
				break
			}
			cursor = p.Cursor
		}
		if seen != benchChats {
			b.Fatalf("walked %d chats", seen)
		}
	}
}

// BenchmarkNextChatByIDs is the same walk through ChatIDs, as LinkedIn finds
// its next chat now: chat IDs only, narrowed in the query.
func BenchmarkNextChatByIDs(b *testing.B) {
	s := openFixture(b)
	for b.Loop() {
		cursor, seen := "", 0
		for range 50 {
			ids, next, err := s.ChatIDs(net, ChatFilter{Writable: true, HistoryOpen: true}, cursor, 200)
			if err != nil {
				b.Fatal(err)
			}
			seen += len(ids)
			if next == "" {
				break
			}
			cursor = next
		}
		if seen != benchChats {
			b.Fatalf("walked %d chats", seen)
		}
	}
}

// BenchmarkLiveWrite is one line arriving live into a chat of 34: a new
// incoming line (new), 200 of them in one batch, where the commit no longer
// hides the per-line work (batch), and the same line written again whole,
// as a network sending it twice does (again); each also in a chat holding
// 300 lines of Beeper's copy older than any of its own (copies), as chats
// copied before the switch do.
func BenchmarkLiveWrite(b *testing.B) {
	for _, copies := range []bool{false, true} {
		name := "plain"
		if copies {
			name = "copies"
		}
		b.Run("new/"+name, func(b *testing.B) {
			s, next := liveWriteFixture(b, copies)
			i := 0
			for b.Loop() {
				m := next(i)
				if _, err := s.PutMessage(net, m); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
		b.Run("batch/"+name, func(b *testing.B) {
			s, next := liveWriteFixture(b, copies)
			i := 0
			for b.Loop() {
				err := s.Batch(net, func(bt *Batch) error {
					for range 200 {
						if _, err := bt.PutMessage(next(i)); err != nil {
							return err
						}
						i++
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(i), "µs/line")
		})
		b.Run("again/"+name, func(b *testing.B) {
			s, next := liveWriteFixture(b, copies)
			m := next(0)
			if _, err := s.PutMessage(net, m); err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				if _, err := s.PutMessage(net, m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// liveWriteFixture opens a copy of the fixture for BenchmarkLiveWrite, with
// Beeper's copy seeded into its chat when copies, and the i'th new line.
func liveWriteFixture(b *testing.B, copies bool) (*Store, func(i int) Message) {
	b.Helper()
	s := openFixture(b)
	c := 300
	chat := benchChat(c)
	first := base + int64(c)*1000
	if copies {
		lines := make([]copyLine, 300)
		for i := range lines {
			lines[i] = copyLine{beeperID: fmt.Sprintf("$bench%06d", i), fromMe: i%3 == 0,
				ms: first - int64(300-i)*60000, text: fmt.Sprintf("invented copy %d", i)}
		}
		seedCopies(b, s, chat, "!bench:beeper.local", lines...)
	}
	newest := first + 34*60000
	return s, func(i int) Message {
		return Message{ID: fmt.Sprintf("LIVE%08d", i), ChatID: chat, SenderID: chat, TimeMs: newest + int64(i)*1000,
			Text: fmt.Sprintf("invented live line %d", i), Kind: "text", Source: SourceLive}
	}
}
