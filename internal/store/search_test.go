package store

import (
	"slices"
	"testing"
)

func TestSearchFindsWordsAndHisOwnLinesWithNoWords(t *testing.T) {
	s := openTest(t)
	put(t, s, text(phoneChat, "3EB0S0001", base+1000, "Pancakes on Sunday?"))
	put(t, s, mine(phoneChat, "3EB0S0002", base+2000, "Yes, pancakes!"))
	put(t, s, text(otherChat, "3EB0S0003", base+3000, "No pancake for me"))
	put(t, s, text(otherChat, "3EB0S0004", base+4000, "Something else"))
	photo := mine(otherChat, "3EB0S0005", base+5000, "")
	photo.Kind, photo.Media = "photo", []Media{{Kind: "photo", HasFile: true}}
	put(t, s, photo)
	hidden := mine(otherChat, "3EB0S0006", base+6000, "pancake notice")
	hidden.Hidden = ptr(true)
	put(t, s, hidden)

	find := func(q Query) []string {
		t.Helper()
		if q.Limit == 0 {
			q.Limit = 50
		}
		p, err := s.Search(net, q)
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		return ids(p.Items)
	}

	if got := find(Query{Text: "pancake"}); !slices.Equal(got, []string{"3EB0S0003", "3EB0S0002", "3EB0S0001"}) {
		t.Errorf("pancake found %v, want the three lines newest first", got)
	}
	if got := find(Query{Text: "PANCAKES sunday"}); !slices.Equal(got, []string{"3EB0S0001"}) {
		t.Errorf("two words found %v", got)
	}
	if got := find(Query{Text: "pancake", ChatIDs: []string{phoneChat}}); !slices.Equal(got, []string{"3EB0S0002", "3EB0S0001"}) {
		t.Errorf("in one chat found %v", got)
	}

	me := true
	if got := find(Query{FromMe: &me}); !slices.Equal(got, []string{"3EB0S0005", "3EB0S0002"}) {
		t.Errorf("his lines = %v, want his two visible lines newest first", got)
	}
	notMe := false
	if got := find(Query{FromMe: &notMe, AfterMs: base + 1000}); !slices.Equal(got, []string{"3EB0S0004", "3EB0S0003"}) {
		t.Errorf("others after a time = %v", got)
	}
	if got := find(Query{SenderID: otherChat, BeforeMs: base + 4000}); !slices.Equal(got, []string{"3EB0S0003"}) {
		t.Errorf("by sender before a time = %v", got)
	}
	if got := find(Query{Text: "!!!"}); len(got) != 0 {
		t.Errorf("punctuation found %v", got)
	}

	// Paging newest first with a cursor.
	var paged []string
	cursor := ""
	for range 10 {
		p, err := s.Search(net, Query{Text: "pancake", Limit: 1, Cursor: cursor})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		paged = append(paged, ids(p.Items)...)
		if p.Cursor == "" {
			break
		}
		cursor = p.Cursor
	}
	if !slices.Equal(paged, []string{"3EB0S0003", "3EB0S0002", "3EB0S0001"}) {
		t.Errorf("paged = %v", paged)
	}
}

func TestFindChatsByNameOrNumber(t *testing.T) {
	s := openTest(t)
	alex, sam := "Alex Example", "Sam Sample"
	if err := s.PutChat(net, ChatInfo{ID: phoneChat, Title: &alex}); err != nil {
		t.Fatal(err)
	}
	phone := "+10000000002"
	if err := s.PutChat(net, ChatInfo{ID: otherChat, Title: &sam, Phone: &phone}); err != nil {
		t.Fatal(err)
	}
	group := "Weekend Plans"
	if err := s.PutChat(net, ChatInfo{ID: groupChat, Kind: "group", Title: &group,
		Participants: []Participant{{ID: phoneChat, Name: "Alex Example"}}}); err != nil {
		t.Fatal(err)
	}
	put(t, s, text(groupChat, "3EB0F0001", base+1000, "hi"))

	got, err := s.FindChats(net, "alex", 20)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, c := range got {
		found = append(found, c.ID)
	}
	if !slices.Equal(found, []string{groupChat, phoneChat}) {
		t.Errorf("alex found %v", found)
	}
	got, _ = s.FindChats(net, "0000000002", 20)
	if len(got) != 1 || got[0].ID != otherChat {
		t.Errorf("a number found %v", got)
	}
}
