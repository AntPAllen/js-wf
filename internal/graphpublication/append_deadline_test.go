package graphpublication

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// A transport request deadline need not expire its caller's context. These
// cuts distinguish publication state without interpreting an error as proof
// that the original-head CAS did or did not commit.
func TestGraphAppendRequestDeadlineRecovery(t *testing.T) {
	for _, mode := range []string{"grant_read", "cas_not_committed", "cas_committed_readback", "cas_committed_unknown", "cas_committed_cancelled", "cas_committed_replaced"} {
		t.Run(mode, func(t *testing.T) {
			m, p := newModel(mode)
			prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("entry"), [][]byte{[]byte("result")}, epoch.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			objects := make(map[string][]byte, len(m.objects))
			for key, value := range m.objects {
				objects[key] = append([]byte(nil), value...)
			}
			attempt, cancel := context.WithCancel(ctx)
			defer cancel()
			casCalls, readbacks := 0, 0
			m.rootBefore = func(string, uint64, Root) error {
				casCalls++
				if mode == "cas_not_committed" {
					return context.DeadlineExceeded
				}
				return nil
			}
			if mode == "grant_read" {
				m.readBlobHook = func(string) error { return context.DeadlineExceeded }
			} else if mode != "cas_not_committed" {
				m.rootAfter = func() error {
					if mode == "cas_committed_cancelled" {
						cancel()
					}
					if mode == "cas_committed_replaced" {
						// A successor retires the publication before readback.
						m.roots["owner"] = Root{Schema: Schema, Head: 2, Graph: EmptyRoot().Graph}
					}
					m.readRootHook = func(string) error {
						readbacks++
						if mode == "cas_committed_unknown" {
							return context.DeadlineExceeded
						}
						return nil
					}
					return context.DeadlineExceeded
				}
			}
			root, err := p.Commit(attempt, prepared)
			if mode == "cas_committed_readback" {
				if err != nil || root.Head != 1 || !samePublication(root, prepared.publication) {
					t.Fatal("exact witness did not resolve lost acknowledgment", root, err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) || root.Head != 0 {
				t.Fatal("uncertain request incorrectly accepted", root, err)
			}
			if !reflect.DeepEqual(objects, m.objects) {
				t.Fatal("commit recovery rewrote immutable objects")
			}
			if mode == "grant_read" && casCalls != 0 || mode != "grant_read" && casCalls != 1 {
				t.Fatal("unexpected publication attempts", casCalls)
			}
			if mode == "cas_committed_cancelled" && readbacks != 0 {
				t.Fatal("cancelled attempt performed readback", readbacks)
			}
			if mode != "cas_committed_cancelled" && attempt.Err() != nil {
				t.Fatal("request deadline expired the enclosing context")
			}
			m.readBlobHook, m.readRootHook, m.rootBefore, m.rootAfter = nil, nil, nil, nil
			actual, err := m.ReadRoot(ctx, "owner")
			if err != nil {
				t.Fatal(err)
			}
			wantCount := uint64(1)
			if mode == "grant_read" || mode == "cas_not_committed" || mode == "cas_committed_replaced" {
				wantCount = 0
			}
			if actual.Graph.Count != wantCount {
				t.Fatal("wrong authoritative population after cut", actual.Graph.Count, wantCount)
			}
			// Reusing this exact private plan can resolve its publication;
			// matching content from a replacement is never sufficient.
			recovered, err := p.Commit(ctx, prepared)
			if mode == "cas_committed_replaced" {
				if !errors.Is(err, ErrConflict) || recovered.Head != 0 {
					t.Fatal("replaced publication resurrected", recovered, err)
				}
			} else if err != nil || recovered.Head != 1 || !samePublication(recovered, prepared.publication) {
				t.Fatal("exact prepared publication failed recovery", recovered, err)
			}
			if !reflect.DeepEqual(objects, m.objects) {
				t.Fatal("recovery mutated object attempts")
			}
		})
	}
}
