package retainedgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestAppendGraphExtensionSeededDeltaAndBounds(t *testing.T) {
	ctx := context.Background()
	checked := 0
	maxReads := 0
	for seed := int64(1); seed <= 32; seed++ {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			random := rand.New(rand.NewSource(seed))
			store := newStore()
			base := Empty()
			for i := 0; i < 128; i++ {
				record := Record{Data: []byte(fmt.Sprintf("%d/%d/%d", seed, i, random.Uint64()))}
				if random.Intn(3) == 0 {
					record.Blobs = []Link{external()}
				}
				next, err := Append(ctx, store, base, record)
				if err != nil {
					t.Fatal(err)
				}
				gets, puts := store.gets, store.puts
				delta, err := ValidateAppend(ctx, store, base, next)
				if err != nil {
					t.Fatal(err)
				}
				reads := store.gets - gets
				maxReads = max(maxReads, reads)
				wantNodes := bits.TrailingZeros64(^base.Count) + 1
				if len(delta.Nodes) != wantNodes || reads != wantNodes || store.puts != puts {
					t.Fatal("delta rewrote or scanned history", len(delta.Nodes), reads, wantNodes)
				}
				wantRecord, err := canonicalRecord(record)
				if err != nil || !reflect.DeepEqual(delta.Record, wantRecord) {
					t.Fatal("delta changed leaf record", err)
				}
				if (i+1)&i == 0 || (i+2)&(i+1) == 0 {
					oldTrees, _ := membershipCensus(t, store, base)
					newTrees, _ := membershipCensus(t, store, next)
					oldLinks := map[Link]bool{}
					for _, tree := range oldTrees {
						oldLinks[tree.Link] = true
					}
					fresh := map[Tree]bool{}
					for _, tree := range newTrees {
						if !oldLinks[tree.Link] {
							fresh[tree] = true
						}
					}
					if len(fresh) != len(delta.Nodes) {
						t.Fatal("delta differs from independent census")
					}
					for _, tree := range delta.Nodes {
						if !fresh[tree] {
							t.Fatal("delta contains inherited or unrelated node")
						}
						delete(fresh, tree)
					}
					if len(fresh) != 0 {
						t.Fatal("delta omitted new node")
					}
				}
				// Copies returned to an intent registrar must not alias stored node bytes.
				delta.Record.Data[0] ^= 0xff
				if len(delta.Record.Blobs) > 0 {
					delta.Record.Blobs[0].Reference.Generation++
				}
				again, err := ValidateAppend(ctx, store, base, next)
				if err != nil || !reflect.DeepEqual(again.Record, wantRecord) {
					t.Fatal("delta aliases stored leaf", err)
				}
				base = next
				checked++
			}
		})
	}
	t.Logf("RETAINED_GRAPH_EXTENSION seeds=32 records_per_seed=128 appends=%d max_delta_nodes=%d max_validation_reads=%d", checked, maxReads, maxReads)
}

func rewriteExtensionNode(t *testing.T, store *memoryStore, tree Tree, change func(*node)) Tree {
	t.Helper()
	var n node
	if err := json.Unmarshal(store.objects[tree.Link.Reference.Object], &n); err != nil {
		t.Fatal(err)
	}
	change(&n)
	data, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	hash := digest(data)
	ref, err := store.Put(context.Background(), hash, data)
	if err != nil {
		t.Fatal(err)
	}
	return Tree{First: tree.First, Height: tree.Height, Link: Link{Hash: hash, Reference: ref}}
}

func TestAppendGraphExtensionRejectsChangedInheritance(t *testing.T) {
	for _, mode := range []string{"frontier_upload", "frontier_generation", "merged_upload", "merged_generation", "deep_merge"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := newStore()
			base := Empty()
			count := 1
			if mode == "frontier_upload" || mode == "frontier_generation" {
				count = 2
			}
			if mode == "deep_merge" {
				count = 3
			}
			for i := 0; i < count; i++ {
				var err error
				base, err = Append(ctx, store, base, Record{Data: number(uint64(i))})
				if err != nil {
					t.Fatal(err)
				}
			}
			next, err := Append(ctx, store, base, Record{Data: []byte("next")})
			if err != nil {
				t.Fatal(err)
			}
			next.Frontier = append([]Tree{}, next.Frontier...)
			different := func(link Link) Link {
				if mode == "frontier_generation" || mode == "merged_generation" {
					link.Reference.Generation++
					link.Reference.Object = link.Hash + "/" + strconv.FormatUint(link.Reference.Generation, 10) + "/other"
				} else {
					link.Reference.Object += "other"
				}
				return link
			}
			if count == 2 {
				next.Frontier[0].Link = different(next.Frontier[0].Link)
			} else if count == 1 {
				next.Frontier[0] = rewriteExtensionNode(t, store, next.Frontier[0], func(n *node) { n.Children[0] = different(n.Children[0]) })
			} else {
				top := next.Frontier[0]
				var n node
				if err = json.Unmarshal(store.objects[top.Link.Reference.Object], &n); err != nil {
					t.Fatal(err)
				}
				right := Tree{First: 2, Height: 1, Link: n.Children[1]}
				right = rewriteExtensionNode(t, store, right, func(n *node) { n.Children[0] = different(n.Children[0]) })
				next.Frontier[0] = rewriteExtensionNode(t, store, top, func(n *node) { n.Children[1] = right.Link })
			}
			if err = next.Validate(); err != nil {
				t.Fatal("malicious fixture must retain valid frontier", err)
			}
			if _, err = ValidateAppend(ctx, store, base, next); !errors.Is(err, ErrInvalid) {
				t.Fatal("changed inherited receipt adopted", err)
			}
		})
	}
}

func TestAppendGraphExtensionUncertaintyAndBoundaryCases(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	base := Empty()
	for i := 0; i < 7; i++ {
		var err error
		base, err = Append(ctx, store, base, Record{Data: number(uint64(i))})
		if err != nil {
			t.Fatal(err)
		}
	}
	next, err := Append(ctx, store, base, Record{Data: []byte("new"), Blobs: []Link{external()}})
	if err != nil {
		t.Fatal(err)
	}
	// Inherited bytes are not reaudited: immutable authority receipts, not a
	// fresh full-history walk, carry inheritance through publication.
	old := base.Frontier[0]
	oldData := store.objects[old.Link.Reference.Object]
	delete(store.objects, old.Link.Reference.Object)
	delta, err := ValidateAppend(ctx, store, base, next)
	if err != nil || len(delta.Nodes) != 4 || !bytes.Equal(delta.Record.Data, []byte("new")) {
		t.Fatal("validation reaudited inherited subtree", err)
	}
	store.objects[old.Link.Reference.Object] = oldData
	top := next.Frontier[0]
	data := store.objects[top.Link.Reference.Object]
	delete(store.objects, top.Link.Reference.Object)
	if _, err = ValidateAppend(ctx, store, base, next); err == nil {
		t.Fatal("missing new spine accepted")
	}
	store.objects[top.Link.Reference.Object] = append([]byte("bad"), data...)
	if _, err = ValidateAppend(ctx, store, base, next); !errors.Is(err, ErrInvalid) {
		t.Fatal("corrupt new spine accepted", err)
	}
	store.objects[top.Link.Reference.Object] = data
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = ValidateAppend(canceled, store, base, next); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	for _, cause := range []error{ErrIndex, context.DeadlineExceeded, context.Canceled, errors.New("lost read reply")} {
		if _, err = ValidateAppend(ctx, membershipErrorStore{Store: store, err: cause}, base, next); !errors.Is(err, cause) {
			t.Fatal("storage error lost", err)
		}
	}
	if _, err = ValidateAppend(ctx, store, base, base); !errors.Is(err, ErrInvalid) {
		t.Fatal("no-op accepted", err)
	}
	later, err := Append(ctx, store, next, Record{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateAppend(ctx, store, base, later); !errors.Is(err, ErrInvalid) {
		t.Fatal("multi-record extension accepted", err)
	}
	if _, err = ValidateAppend(ctx, nil, base, next); !errors.Is(err, ErrInvalid) {
		t.Fatal("nil store accepted", err)
	}
	exhausted := Root{Schema: Schema, Count: math.MaxInt64, Frontier: []Tree{}}
	var first uint64
	for height := 62; height >= 0; height-- {
		exhausted.Frontier = append(exhausted.Frontier, Tree{First: first, Height: uint8(height), Link: external()})
		first += uint64(1) << uint(height)
	}
	if err = exhausted.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateAppend(ctx, store, exhausted, Empty()); !errors.Is(err, ErrInvalid) {
		t.Fatal("population exhaustion accepted", err)
	}
}
