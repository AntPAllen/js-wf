package retainedgraph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

type coordinate struct {
	first  uint64
	height uint8
}

// Independent reference census: decode stored children directly, without
// production Walk, Read, FindTree or Contains calls. Only acknowledged roots
// select a fork. Objects uploaded on competing forks remain in the store.
func membershipCensus(t *testing.T, store *memoryStore, root Root) (map[coordinate]Tree, map[uint64][]Link) {
	t.Helper()
	trees := map[coordinate]Tree{}
	blobs := map[uint64][]Link{}
	var visit func(Tree)
	visit = func(tree Tree) {
		var n node
		if err := json.Unmarshal(store.objects[tree.Link.Reference.Object], &n); err != nil {
			t.Fatal(err)
		}
		if n.First != tree.First || n.Height != tree.Height {
			t.Fatal("reference census position mismatch")
		}
		trees[coordinate{tree.First, tree.Height}] = tree
		if n.Record != nil {
			blobs[n.First] = append([]Link{}, n.Record.Blobs...)
			return
		}
		if len(n.Children) != 2 {
			t.Fatal("reference census branch mismatch")
		}
		visit(Tree{First: n.First, Height: n.Height - 1, Link: n.Children[0]})
		visit(Tree{First: n.First + (uint64(1) << (n.Height - 1)), Height: n.Height - 1, Link: n.Children[1]})
	}
	for _, tree := range root.Frontier {
		visit(tree)
	}
	return trees, blobs
}

func TestAppendGraphMembershipSeededCensusAndForks(t *testing.T) {
	ctx := context.Background()
	queries := 0
	for seed := int64(1); seed <= 32; seed++ {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			random := rand.New(rand.NewSource(seed))
			store := newStore()
			root := Empty()
			snapshots := []Root{root}
			staged := []Tree{}
			for i := 0; i < 128; i++ {
				record := Record{Data: []byte(fmt.Sprintf("%d/%d/%d", seed, i, random.Uint64()))}
				if random.Intn(3) == 0 {
					record.Blobs = []Link{external()}
				}
				if random.Intn(4) == 0 {
					fork, err := Append(ctx, store, root, Record{Data: []byte(fmt.Sprintf("fork/%d/%d", seed, i)), Blobs: []Link{external()}})
					if err != nil {
						t.Fatal(err)
					}
					forkCensus, _ := membershipCensus(t, store, fork)
					oldCensus, _ := membershipCensus(t, store, root)
					for coord, tree := range forkCensus {
						if old, ok := oldCensus[coord]; !ok || old != tree {
							staged = append(staged, tree)
						}
					}
				}
				next, err := Append(ctx, store, root, record)
				if err != nil {
					t.Fatal(err)
				}
				root = next
				if (i+1)%16 == 0 {
					snapshots = append(snapshots, root)
				}
			}
			for _, snapshot := range snapshots {
				expected, payloads := membershipCensus(t, store, snapshot)
				for coord, want := range expected {
					before := store.gets
					got, err := FindTree(ctx, store, snapshot, coord.first, coord.height)
					if err != nil || got != want {
						t.Fatalf("count=%d coordinate=%v got=%v want=%v error=%v", snapshot.Count, coord, got, want, err)
					}
					if store.gets-before > 7 {
						t.Fatal("membership scanned unrelated history")
					}
					present, err := ContainsNode(ctx, store, snapshot, want)
					if err != nil || !present {
						t.Fatal("live node not protected", err)
					}
					other := want
					other.Link.Reference.Generation++
					other.Link.Reference.Object = other.Link.Hash + "/" + strconv.FormatUint(other.Link.Reference.Generation, 10) + "/other"
					present, err = ContainsNode(ctx, store, snapshot, other)
					if err != nil || present {
						t.Fatal("same hash adopted different generation", err)
					}
					other = want
					other.Link.Reference.Object += "different"
					present, err = ContainsNode(ctx, store, snapshot, other)
					if err != nil || present {
						t.Fatal("same generation adopted different upload", err)
					}
					queries += 4
				}
				for index, links := range payloads {
					for _, link := range links {
						before := store.gets
						present, err := ContainsBlob(ctx, store, snapshot, index, link)
						if err != nil || !present {
							t.Fatal("live payload not protected", err)
						}
						if store.gets-before > 8 {
							t.Fatal("payload membership scanned history")
						}
						other := link
						other.Reference.Object += "different"
						present, err = ContainsBlob(ctx, store, snapshot, index, other)
						if err != nil || present {
							t.Fatal("payload upload alias adopted", err)
						}
						queries += 2
					}
				}
				for _, tree := range staged {
					present, err := ContainsNode(ctx, store, snapshot, tree)
					want := expected[coordinate{tree.First, tree.Height}] == tree
					if err != nil || present != want {
						t.Fatal("staged fork membership mismatch", snapshot.Count, tree.First, tree.Height, present, want, err)
					}
					queries++
				}
				present, err := ContainsBlob(ctx, store, snapshot, snapshot.Count, external())
				if err != nil || present {
					t.Fatal("snapshot adopted later payload", err)
				}
			}
		})
	}
	t.Logf("RETAINED_GRAPH_MEMBERSHIP seeds=32 records_per_seed=128 queries=%d max_node_path_reads=7 max_payload_path_reads=8", queries)
}

func TestAppendGraphMembershipUncertaintyAndBounds(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	root := Empty()
	for i := 0; i < 8; i++ {
		var err error
		root, err = Append(ctx, store, root, Record{Data: number(uint64(i)), Blobs: []Link{external()}})
		if err != nil {
			t.Fatal(err)
		}
	}
	leaf, err := FindTree(ctx, store, root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	target := root.Frontier[0]
	// Protect a direct authoritative receipt even when its target bytes are
	// unavailable. A failed ancestry read cannot be interpreted as absence.
	data := store.objects[target.Link.Reference.Object]
	delete(store.objects, target.Link.Reference.Object)
	present, err := ContainsNode(ctx, store, root, target)
	if err != nil || !present {
		t.Fatal("missing referenced root node lost protection", err)
	}
	if _, err = ContainsNode(ctx, store, root, leaf); err == nil {
		t.Fatal("missing ancestry reported certain membership")
	}
	if _, err = ContainsBlob(ctx, store, root, 0, external()); err == nil {
		t.Fatal("missing ancestry reported certain payload membership")
	}
	store.objects[target.Link.Reference.Object] = append([]byte("bad"), data...)
	if _, err = ContainsNode(ctx, store, root, leaf); err == nil {
		t.Fatal("corrupt ancestry reported certain membership")
	}
	store.objects[target.Link.Reference.Object] = data
	for _, q := range []coordinate{{1, 1}, {0, 63}, {math.MaxUint64, 1}} {
		if _, err = FindTree(ctx, store, root, q.first, q.height); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid alignment accepted", q, err)
		}
	}
	for _, q := range []coordinate{{8, 0}, {0, 4}, {uint64(1) << 63, 62}} {
		if _, err = FindTree(ctx, store, root, q.first, q.height); !errors.Is(err, ErrIndex) {
			t.Fatal("outside subtree accepted", q, err)
		}
	}
	badRoot := root
	badRoot.Schema = "unsupported"
	if _, err = ContainsNode(ctx, store, badRoot, leaf); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid root reported membership", err)
	}
	badLink := leaf
	badLink.Link.Reference.Generation = 0
	if _, err = ContainsNode(ctx, store, root, badLink); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid receipt accepted", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = ContainsNode(canceled, store, root, leaf); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	if _, err = ContainsBlob(canceled, store, root, 0, external()); !errors.Is(err, context.Canceled) {
		t.Fatal("payload cancellation ignored", err)
	}
}

type membershipErrorStore struct {
	Store
	err error
}

func (s membershipErrorStore) Get(context.Context, Link, int) ([]byte, error) { return nil, s.err }

func TestAppendGraphMembershipStorageErrorsNeverBecomeAbsence(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	root := Empty()
	for i := 0; i < 4; i++ {
		var err error
		root, err = Append(ctx, store, root, Record{Blobs: []Link{external()}})
		if err != nil {
			t.Fatal(err)
		}
	}
	leaf, err := FindTree(ctx, store, root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, cause := range []error{ErrIndex, fmt.Errorf("store bounds: %w", ErrIndex), context.DeadlineExceeded, context.Canceled, errors.New("lost storage reply")} {
		broken := membershipErrorStore{Store: store, err: cause}
		present, err := ContainsNode(ctx, broken, root, leaf)
		if present || !errors.Is(err, cause) {
			t.Fatalf("node lost storage cause: present=%v error=%v cause=%v", present, err, cause)
		}
		present, err = ContainsBlob(ctx, broken, root, 0, external())
		if present || !errors.Is(err, cause) {
			t.Fatalf("payload lost storage cause: present=%v error=%v cause=%v", present, err, cause)
		}
	}
}
