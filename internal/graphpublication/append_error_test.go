package graphpublication

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

type commitGraphFault struct {
	Port
	failGet bool
}

func (p commitGraphFault) Get(c context.Context, l retainedgraph.Link, n int) ([]byte, error) {
	if p.failGet {
		return nil, context.DeadlineExceeded
	}
	return p.Port.Get(c, l, n)
}

func TestGraphAppendCommitErrorPhases(t *testing.T) {
	for _, mode := range []string{"context", "plan", "observe", "conflict", "preservation", "graph", "node_grant", "payload_grant", "owned_payload", "root_rejected", "root_lost", "root_unknown", "root_cancelled", "conflict_readback_unknown"} {
		t.Run(mode, func(t *testing.T) {
			m, p := newModel(mode)
			prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("entry"), [][]byte{[]byte("body")}, epoch.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "owned_payload" {
				if _, err = p.Commit(ctx, prepared); err != nil {
					t.Fatal(err)
				}
				owned := OwnedPayload{Index: 0, Link: prepared.publication.Graph.Frontier[0].Link}
				// The payload edge, rather than its containing leaf, is reusable ownership.
				delta, err := retainedgraph.ValidateAppend(ctx, stageStore{protocol: p}, prepared.base.Graph, prepared.publication.Graph)
				if err != nil {
					t.Fatal(err)
				}
				owned.Link = delta.Record.Blobs[0]
				prepared, err = p.PrepareAppendWithOwned(ctx, "owner", 1, []byte("reuse"), nil, []OwnedPayload{owned}, epoch.Add(time.Minute))
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := normalizeRoot(m.roots["owner"])
			if err != nil {
				t.Fatal(err)
			}
			attempt, cancel := context.WithCancel(ctx)
			defer cancel()
			phase := ""
			cause := error(context.DeadlineExceeded)
			readback := false
			switch mode {
			case "context":
				phase = "check append commit context"
				cause = context.Canceled
				cancel()
			case "plan":
				phase = "validate append plan"
				prepared.destination = ""
				cause = nil
			case "observe":
				phase = "observe append commit head"
				m.readRootHook = func(string) error { return context.DeadlineExceeded }
			case "conflict":
				phase = "check append commit head"
				cause = ErrConflict
				r := EmptyRoot()
				r.Head++
				m.roots["owner"] = r
			case "preservation":
				phase = "validate append preservation"
				cause = nil
				prepared.publication.Readers = []ReaderPin{{}}
			case "graph":
				phase = "verify append graph"
				p.Port = commitGraphFault{Port: m, failGet: true}
			case "node_grant":
				phase = "verify append node grant"
				m.readBlobHook = func(string) error { return context.DeadlineExceeded }
			case "payload_grant":
				phase = "verify append payload grant"
				m.readBlobHook = func(k string) error {
					if k == authorityKey(prepared.publication.Graph.Frontier[0].Link.Hash, prepared.publication.Token) {
						return nil
					}
					return context.DeadlineExceeded
				}
			case "owned_payload":
				phase = "verify append owned payload"
				m.readBlobHook = func(k string) error {
					if _, ok := m.blobs[k].Fence.Intents[prepared.publication.Token]; ok {
						return nil
					}
					return context.DeadlineExceeded
				}
			case "root_rejected":
				phase = "publish append root"
				readback = true
				m.rootBefore = func(string, uint64, Root) error { return context.DeadlineExceeded }
			case "root_lost":
				m.rootAfter = func() error { return context.DeadlineExceeded }
			case "root_unknown":
				phase = "publish append root"
				readback = true
				m.rootAfter = func() error {
					m.readRootHook = func(string) error { return context.DeadlineExceeded }
					return context.DeadlineExceeded
				}
			case "root_cancelled":
				phase = "publish append root"
				m.rootAfter = func() error { cancel(); return context.DeadlineExceeded }
			case "conflict_readback_unknown":
				phase = "publish append root"
				cause = ErrConflict
				readback = true
				m.rootBefore = func(string, uint64, Root) error {
					m.readRootHook = func(string) error { return context.DeadlineExceeded }
					return ErrConflict
				}
			}
			got, err := p.Commit(attempt, prepared)
			if mode == "root_lost" {
				if err != nil || !samePublication(got, prepared.publication) {
					t.Fatal("exact readback", got, err)
				}
				return
			}
			var detail *AppendCommitError
			if err == nil || !errors.As(err, &detail) || detail.Phase != phase || detail.ReadbackAttempted != readback {
				t.Fatal("phase/readback", detail, err)
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Fatal("primary cause lost", err)
			}
			if !strings.Contains(err.Error(), phase) {
				t.Fatal("phase text lost", err)
			}
			unknown := mode == "root_unknown" || mode == "conflict_readback_unknown"
			if errors.Is(detail.ReadbackErr, context.DeadlineExceeded) != unknown {
				t.Fatal("readback detail", detail)
			}
			if mode == "conflict_readback_unknown" && errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("secondary readback reclassified primary conflict", err)
			}
			final, err := normalizeRoot(m.roots["owner"])
			if err != nil {
				t.Fatal(err)
			}
			committed := mode == "root_unknown" || mode == "root_cancelled"
			if committed {
				if !samePublication(final, prepared.publication) {
					t.Fatal("committed root missing")
				}
			} else if !samePublication(before, final) {
				t.Fatal("failed commit changed publication")
			}
		})
	}
}
