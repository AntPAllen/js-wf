package retainedgraph

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestReadRangeOrderedBoundedTraversalAndFailures(t *testing.T) {
	ctx := context.Background()
	store, root := newStore(), Empty()
	for i := uint64(0); i < 131; i++ {
		var err error
		root, err = Append(ctx, store, root, Record{Data: number(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, bounds := range [][2]uint64{{0, 131}, {0, 128}, {63, 65}, {128, 131}, {77, 77}, {131, 131}} {
		store.gets = 0
		next := bounds[0]
		err := ReadRange(ctx, store, root, bounds[0], bounds[1], func(index uint64, record Record) error {
			if index != next || !bytes.Equal(record.Data, number(index)) {
				t.Fatal(index, next, record)
			}
			next++
			return nil
		})
		if err != nil || next != bounds[1] {
			t.Fatal(bounds, next, err)
		}
		if bounds == [2]uint64{0, 131} && store.gets != 259 {
			t.Fatal("shared branches fetched repeatedly", store.gets)
		}
		if bounds[0] == bounds[1] && store.gets != 0 {
			t.Fatal("empty range performed GETs", store.gets)
		}
	}
	store.gets = 0
	for index := uint64(0); index < root.Count; index++ {
		record, err := Read(ctx, store, root, index)
		if err != nil || !bytes.Equal(record.Data, number(index)) {
			t.Fatal("point read disagrees with range", index, record, err)
		}
	}
	if store.gets != 1029 {
		t.Fatal("unexpected point-read census", store.gets)
	}
	t.Log("131-record range: 259 node GETs; equivalent point reads: 1029 node GETs")
	for _, bounds := range [][2]uint64{{5, 4}, {0, 132}} {
		if err := ReadRange(ctx, store, root, bounds[0], bounds[1], func(uint64, Record) error { return nil }); !errors.Is(err, ErrIndex) {
			t.Fatal(bounds, err)
		}
	}
	stop := errors.New("visitor stopped")
	visits := 0
	if err := ReadRange(ctx, store, root, 0, 131, func(uint64, Record) error { visits++; return stop }); !errors.Is(err, stop) || visits != 1 {
		t.Fatal(visits, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	visits = 0
	if err := ReadRange(canceled, store, root, 0, 131, func(uint64, Record) error { visits++; cancel(); return nil }); !errors.Is(err, context.Canceled) || visits != 1 {
		t.Fatal(visits, err)
	}
	// A second call must observe physical corruption rather than stale nodes.
	store.objects[root.Frontier[0].Link.Reference.Object] = []byte(`{}`)
	visits = 0
	if err := ReadRange(ctx, store, root, 0, 131, func(uint64, Record) error { visits++; return nil }); !errors.Is(err, ErrInvalid) || visits != 0 {
		t.Fatal("corrupt branch accepted", visits, err)
	}
}
