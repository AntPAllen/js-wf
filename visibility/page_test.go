package visibility

import "testing"

func TestVisibilityPageCursorAndBounds(t *testing.T) {
	rows := []Row{{Type: "a", ID: "one"}, {Type: "a", ID: "two"}, {Type: "b", ID: "one"}}
	first := slicePage(rows, "", "", 1)
	if len(first.Rows) != 1 || first.Rows[0].ID != "one" || first.Next == "" {
		t.Fatalf("first page: %+v", first)
	}
	typ, id, err := validatePage("queued", first.Next, 1)
	if err != nil || typ != "a" || id != "one" {
		t.Fatalf("cursor=%s/%s err=%v", typ, id, err)
	}
	second := slicePage(rows, typ, id, 1)
	if len(second.Rows) != 1 || second.Rows[0].ID != "two" || second.Next == "" {
		t.Fatalf("second page: %+v", second)
	}
	typ, id, err = decodeCursor(second.Next)
	if err != nil {
		t.Fatal(err)
	}
	last := slicePage(rows, typ, id, 1)
	if len(last.Rows) != 1 || last.Rows[0].Type != "b" || last.Next != "" {
		t.Fatalf("last page: %+v", last)
	}
	for _, cursor := range []string{"!", "W10", "WzEsMl0"} {
		if _, _, err := decodeCursor(cursor); err == nil {
			t.Fatalf("accepted invalid cursor %q", cursor)
		}
	}
	for _, limit := range []int{0, -1, 1001} {
		if _, _, err := validatePage("", "", limit); err == nil {
			t.Fatalf("accepted limit %d", limit)
		}
	}
}
