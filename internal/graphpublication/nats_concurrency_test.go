package graphpublication

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/retainedgraph"
)

// Seeded release permutations vary a six-actor native race; execution timing is
// deliberately not deterministic. Tier1 graph traces are a separate workload.
func TestNativeGraphConcurrentPublishersAndCollectors(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for seed := int64(1); seed <= 2; seed++ {
			t.Run(fmt.Sprintf("R%d/seed%d", replicas, seed), func(t *testing.T) {
				port, c := nativeGraphObjectFixture(t, replicas)
				p := Protocol{Port: port}
				rng := rand.New(rand.NewSource(seed))
				bootstrap, err := p.PrepareAppend(c, "history", 0, []byte("base"), [][]byte{[]byte("shared")}, time.Now().UTC().Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				root, err := p.Commit(c, bootstrap)
				if err != nil {
					t.Fatal(err)
				}
				first, err := retainedgraph.Read(c, nativeGraphReadStore{port}, root.Graph, 0)
				if err != nil || len(first.Blobs) != 1 {
					t.Fatal(first, err)
				}
				owned := first.Blobs[0]
				expected := map[string]bool{"base": true}
				committed, aborted := 0, 0
				for round := 0; round < 4; round++ {
					base, err := port.ReadRoot(c, "history")
					if err != nil {
						t.Fatal(err)
					}
					var wg sync.WaitGroup
					prepared := make([]Prepared, 4)
					prepareErrors := make([]error, 4)
					for worker := 0; worker < 4; worker++ {
						wg.Add(1)
						go func(worker int) {
							defer wg.Done()
							prepared[worker], prepareErrors[worker] = p.PrepareAppendWithOwned(c, "history", base.Head, []byte(fmt.Sprintf("seed%d-round%d-worker%d", seed, round, worker)), nil, []OwnedPayload{{Index: 0, Link: owned}}, time.Now().UTC().Add(time.Hour))
						}(worker)
					}
					wg.Wait()
					for worker, err := range prepareErrors {
						if err != nil {
							t.Fatalf("prepare round%d worker%d: %v", round, worker, err)
						}
					}
					gates := make([]chan struct{}, 6)
					for i := range gates {
						gates[i] = make(chan struct{})
					}
					errorsByActor := make([]error, 6)
					results := make([]Root, 4)
					for actor := 0; actor < 6; actor++ {
						wg.Add(1)
						go func(actor int) {
							defer wg.Done()
							<-gates[actor]
							if actor < 4 {
								results[actor], errorsByActor[actor] = p.Commit(c, prepared[actor])
							} else {
								_, errorsByActor[actor] = p.Sweep(c, time.Now().Add(2*time.Hour))
							}
						}(actor)
					}
					for _, actor := range rng.Perm(6) {
						close(gates[actor])
					}
					wg.Wait()
					winners := 0
					for actor, err := range errorsByActor {
						if actor >= 4 {
							if err != nil && !errors.Is(err, ErrConflict) {
								t.Fatalf("collector round%d actor%d: %v", round, actor, err)
							}
							continue
						}
						if err == nil {
							winners++
							committed++
							expected[fmt.Sprintf("seed%d-round%d-worker%d", seed, round, actor)] = true
							if results[actor].Graph.Count != base.Graph.Count+1 {
								t.Fatal("wrong acknowledged count")
							}
						} else {
							aborted++
							if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrRevoked) && !errors.Is(err, jetstream.ErrObjectNotFound) && !errors.Is(err, jetstream.ErrMsgNotFound) {
								t.Fatalf("commit round%d actor%d: %v", round, actor, err)
							}
						}
					}
					if winners > 1 {
						t.Fatal("multiple original-head winners", winners)
					}
					current, err := port.ReadRoot(c, "history")
					if err != nil {
						t.Fatal(err)
					}
					if current.Head < base.Head || int(current.Graph.Count) != len(expected) {
						t.Fatal("head/population", current.Head, current.Graph.Count, len(expected))
					}
					// Raw child JSON census independently checks every committed actor value and
					// exact shared payload edge; it does not call membership/Walk/Read helpers.
					found := map[string]bool{}
					seen := map[retainedgraph.Link]bool{}
					var visit func(retainedgraph.Tree)
					visit = func(tree retainedgraph.Tree) {
						if seen[tree.Link] {
							t.Fatal("duplicate node")
						}
						seen[tree.Link] = true
						raw, err := port.Get(c, tree.Link, retainedgraph.MaxNodeBytes)
						if err != nil {
							t.Fatal("committed node missing", err)
						}
						var node struct {
							First    uint64                `json:"first"`
							Height   uint8                 `json:"height"`
							Children []retainedgraph.Link  `json:"children"`
							Record   *retainedgraph.Record `json:"record"`
						}
						if err = json.Unmarshal(raw, &node); err != nil || node.First != tree.First || node.Height != tree.Height {
							t.Fatal("position", err)
						}
						if tree.Height == 0 {
							if node.Record == nil || len(node.Record.Blobs) != 1 || node.Record.Blobs[0] != owned || found[string(node.Record.Data)] {
								t.Fatal("leaf receipts/values")
							}
							found[string(node.Record.Data)] = true
							return
						}
						if len(node.Children) != 2 {
							t.Fatal("children")
						}
						for i, link := range node.Children {
							visit(retainedgraph.Tree{First: tree.First + uint64(i)*(uint64(1)<<(tree.Height-1)), Height: tree.Height - 1, Link: link})
						}
					}
					for _, tree := range current.Graph.Frontier {
						visit(tree)
					}
					if len(found) != len(expected) {
						t.Fatal("lost or fabricated value")
					}
					for value := range expected {
						if !found[value] {
							t.Fatal("missing acknowledged value", value)
						}
					}
					if _, err = p.Sweep(c, time.Now().Add(3*time.Hour)); err != nil {
						t.Fatal("joined cleanup", err)
					}
				}
				final, err := port.ReadRoot(c, "history")
				if err != nil {
					t.Fatal(err)
				}
				if err = p.Retire(c, "history", final.Head); err != nil {
					t.Fatal(err)
				}
				if _, err = p.Sweep(c, time.Now().Add(4*time.Hour)); err != nil {
					t.Fatal(err)
				}
				nativeGraphNoObjects(t, port, c)
				t.Logf("R%d seed%d attempts16 committed%d aborted%d; all canonical values/receipts preserved and physical chunks retired", replicas, seed, committed, aborted)
			})
		}
	}
}
