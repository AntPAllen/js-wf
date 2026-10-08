package retainedgraph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"js-wf/internal/blobpublication"
)

type memoryStore struct {
	mu                  sync.Mutex
	objects             map[string][]byte
	puts, gets, encoded int
	failPut             error
	badReceipt          bool
	afterPut            func()
}

func newStore() *memoryStore { return &memoryStore{objects: map[string][]byte{}} }
func (s *memoryStore) Put(ctx context.Context, hash string, data []byte) (blobpublication.Reference, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return blobpublication.Reference{}, err
	}
	s.puts++
	s.encoded += len(data)
	ref := blobpublication.Reference{Generation: 1, Object: fmt.Sprintf("%s/1/upload%d", hash, s.puts)}
	s.objects[ref.Object] = bytes.Clone(data)
	if s.afterPut != nil {
		s.afterPut()
	}
	if s.badReceipt {
		ref.Generation = 2
	}
	return ref, s.failPut
}
func (s *memoryStore) Get(ctx context.Context, link Link, limit int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.gets++
	data, ok := s.objects[link.Reference.Object]
	if !ok {
		return nil, errors.New("missing node")
	}
	if len(data) > limit {
		return nil, errors.New("node exceeds requested limit")
	}
	return bytes.Clone(data), nil
}
func external() Link {
	h := digest([]byte("large payload staged separately"))
	return Link{Hash: h, Reference: blobpublication.Reference{Generation: 3, Object: h + "/3/external"}}
}
func number(n uint64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, n); return b }

func TestAppendGraph100000BoundedWritesAndCompleteTraversal(t *testing.T) {
	const count = 100000
	ctx := context.Background()
	store := newStore()
	root := Empty()
	saved := map[int]Root{}
	maxPuts, maxGets, maxRoot := 0, 0, 0
	for i := 0; i < count; i++ {
		beforePut, beforeGet := store.puts, store.gets
		record := Record{Data: number(uint64(i))}
		if i%127 == 0 {
			record.Blobs = []Link{external()}
		}
		next, err := Append(ctx, store, root, record)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if root.Count != uint64(i) {
			t.Fatal("append mutated its original root")
		}
		puts, gets := store.puts-beforePut, store.gets-beforeGet
		if puts > 18 || gets > 17 {
			t.Fatalf("append rewrote history: index=%d writes=%d reads=%d", i, puts, gets)
		}
		maxPuts = max(maxPuts, puts)
		maxGets = max(maxGets, gets)
		encoded, err := next.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > 8192 {
			t.Fatalf("root grew with population: %d", len(encoded))
		}
		maxRoot = max(maxRoot, len(encoded))
		decoded, err := DecodeRoot(encoded)
		if err != nil || !reflect.DeepEqual(decoded, next) {
			t.Fatal("root roundtrip", err)
		}
		root = next
		if i == 31 || i == 1023 || i == 65535 {
			saved[i+1] = root
		}
	}
	if root.Count != count || store.puts >= 2*count || store.encoded > 2048*count {
		t.Fatal("cumulative node storage is not linear")
	}
	for _, i := range []uint64{0, 1, 31, 32, 1023, 1024, 65535, 65536, 99999} {
		before := store.gets
		r, err := Read(ctx, store, root, i)
		if err != nil || !bytes.Equal(r.Data, number(i)) {
			t.Fatalf("read %d: %v", i, err)
		}
		if store.gets-before > 18 {
			t.Fatal("point read scanned the population")
		}
	}
	for n, old := range saved {
		r, err := Read(ctx, store, old, uint64(n-1))
		if err != nil || !bytes.Equal(r.Data, number(uint64(n-1))) {
			t.Fatal("old root changed", n, err)
		}
		if _, err = Read(ctx, store, old, uint64(n)); !errors.Is(err, ErrIndex) {
			t.Fatal("old snapshot adopted a later record")
		}
	}
	nodes, payloads := 0, 0
	err := Walk(ctx, store, root, func(l Link, node bool) error {
		if node {
			nodes++
		} else {
			payloads++
			if l != external() {
				t.Fatal("payload edge changed")
			}
		}
		return nil
	})
	if err != nil || nodes != 2*count-len(root.Frontier) || payloads != (count+126)/127 {
		t.Fatal("incomplete graph census", nodes, payloads, err)
	}
	t.Logf("RETAINED_GRAPH_SCALE records=%d staged_nodes=%d max_append_writes=%d max_append_reads=%d max_root_bytes=%d encoded_node_bytes=%d walked_nodes=%d payload_edges=%d", count, store.puts, maxPuts, maxGets, maxRoot, store.encoded, nodes, payloads)
}

func TestAppendGraphFailedUploadNeverAdoptsOrMutatesRoot(t *testing.T) {
	for _, mode := range []string{"lost_ack", "bad_receipt", "cancel_after_upload"} {
		t.Run(mode, func(t *testing.T) {
			store := newStore()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "lost_ack":
				store.failPut = errors.New("committed upload reply lost")
			case "bad_receipt":
				store.badReceipt = true
			case "cancel_after_upload":
				store.afterPut = cancel
			}
			original := Empty()
			root, err := Append(ctx, store, original, Record{Data: []byte("pending")})
			if err == nil || root.Schema != "" || original.Count != 0 || len(original.Frontier) != 0 || len(store.objects) != 1 {
				t.Fatal("ambiguous upload adopted", root, err)
			}
		})
	}
}

func TestAppendGraphCorruptOrMissingNodesFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"missing", "corrupt_bytes", "wrong_position", "unknown_field", "duplicate_field", "trailing_json", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			store := newStore()
			root, err := Append(ctx, store, Empty(), Record{Data: []byte("owned")})
			if err != nil {
				t.Fatal(err)
			}
			link := root.Frontier[0].Link
			switch mode {
			case "missing":
				delete(store.objects, link.Reference.Object)
			case "corrupt_bytes":
				store.objects[link.Reference.Object][0] ^= 1
			default:
				data := store.objects[link.Reference.Object]
				var n node
				if err := json.Unmarshal(data, &n); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "wrong_position":
					n.First = 1
					data, _ = json.Marshal(n)
				case "unknown_field":
					data = append(data[:len(data)-1], []byte(",\"extra\":1}")...)
				case "duplicate_field":
					data = append([]byte("{\"schema\":\"ignored\","), data[1:]...)
				case "trailing_json":
					data = append(data, []byte("{}")...)
				case "oversized":
					data = bytes.Repeat([]byte("x"), MaxNodeBytes+1)
				}
				h := digest(data)
				link = Link{Hash: h, Reference: blobpublication.Reference{Generation: 1, Object: h + "/1/substituted"}}
				store.objects[link.Reference.Object] = data
				root.Frontier[0].Link = link
			}
			if _, err := Read(ctx, store, root, 0); err == nil {
				t.Fatal("corruption was read")
			}
			if err := Walk(ctx, store, root, func(Link, bool) error { return nil }); err == nil {
				t.Fatal("incomplete census accepted")
			}
			before := root
			if _, err := Append(ctx, store, root, Record{}); err == nil {
				t.Fatal("corrupt merge consumed")
			}
			if !reflect.DeepEqual(root, before) {
				t.Fatal("failure changed input")
			}
		})
	}
}

func TestAppendGraphFrontierAndRecordBounds(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	base, err := Append(ctx, store, Empty(), Record{})
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []Root{{Schema: Schema, Count: 1}, {Schema: Schema, Frontier: base.Frontier}, {Schema: "other"}, {Schema: Schema, Count: math.MaxUint64, Frontier: base.Frontier}, {Schema: Schema, Count: 2, Frontier: append([]Tree{}, base.Frontier[0], base.Frontier[0])}} {
		puts := store.puts
		if _, err := Append(ctx, store, root, Record{}); err == nil {
			t.Fatal("invalid frontier accepted")
		}
		if store.puts != puts {
			t.Fatal("invalid root wrote a node")
		}
	}
	for _, record := range []Record{{Data: make([]byte, MaxDataBytes+1)}, {Blobs: []Link{external(), external()}}, {Blobs: []Link{{Hash: external().Hash, Reference: blobpublication.Reference{Generation: 4, Object: external().Reference.Object}}}}, {Blobs: make([]Link, MaxBlobReferences+1)}} {
		puts := store.puts
		if _, err := Append(ctx, store, Empty(), record); err == nil {
			t.Fatal("invalid payload accepted")
		}
		if store.puts != puts {
			t.Fatal("invalid payload uploaded")
		}
	}
	encoded, _ := base.Encode()
	for _, data := range [][]byte{append(bytes.Clone(encoded), byte('\n')), append(bytes.Clone(encoded), []byte("{}")...), []byte(`{"schema":"unknown","count":0,"frontier":[]}`), bytes.Repeat([]byte("x"), MaxRootBytes+1)} {
		if _, err := DecodeRoot(data); err == nil {
			t.Fatal("invalid header decoded")
		}
	}
}

func TestAppendGraphCancellationAndVisitorStop(t *testing.T) {
	store := newStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Append(ctx, store, Empty(), Record{}); !errors.Is(err, context.Canceled) || store.puts != 0 {
		t.Fatal(err)
	}
	if _, err := Read(ctx, store, Empty(), 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := Walk(ctx, store, Empty(), func(Link, bool) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx = context.Background()
	root, err := Append(ctx, store, Empty(), Record{Blobs: []Link{external()}})
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("stop census")
	visits := 0
	err = Walk(ctx, store, root, func(Link, bool) error { visits++; return sentinel })
	if err != sentinel || visits != 1 {
		t.Fatal("visitor error swallowed", err, visits)
	}
	active, stop := context.WithCancel(context.Background())
	visits = 0
	err = Walk(active, store, root, func(Link, bool) error { visits++; stop(); return nil })
	if !errors.Is(err, context.Canceled) || visits != 1 {
		t.Fatal("walk continued after cancel", err, visits)
	}
}

func TestAppendGraphIndependentForksAndCopies(t *testing.T) {
	ctx := context.Background()
	store := newStore()
	record := Record{Data: []byte("initial"), Blobs: []Link{external()}}
	root, err := Append(ctx, store, Empty(), record)
	if err != nil {
		t.Fatal(err)
	}
	record.Data[0] = 'X'
	record.Blobs[0] = Link{}
	var roots [2]Root
	var errs [2]error
	var wg sync.WaitGroup
	for i := range roots {
		wg.Go(func() { roots[i], errs[i] = Append(ctx, store, root, Record{Data: number(uint64(i))}) })
	}
	wg.Wait()
	for i, next := range roots {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		r, err := Read(ctx, store, next, 1)
		if err != nil || !bytes.Equal(r.Data, number(uint64(i))) {
			t.Fatal("fork adopted other writer", err)
		}
		r.Data[0] = 99
		again, err := Read(ctx, store, next, 1)
		if err != nil || !bytes.Equal(again.Data, number(uint64(i))) {
			t.Fatal("read alias")
		}
	}
	old, err := Read(ctx, store, root, 0)
	if err != nil || string(old.Data) != "initial" || old.Blobs[0] != external() {
		t.Fatal("caller alias changed immutable leaf")
	}
	if _, err := Read(ctx, store, root, 1); !errors.Is(err, ErrIndex) {
		t.Fatal("staging a fork published it")
	}
}
