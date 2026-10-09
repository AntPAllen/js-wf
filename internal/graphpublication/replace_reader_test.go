package graphpublication

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGraphReplaceReaderCurrentForestsAndOldHandleRevocation(t *testing.T) {
	for _, mode := range []string{"unchanged", "named-streams", "full-slots", "lost-ack"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root, old := readerFixture(t)
			checkpoint, err := old.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if mode == "named-streams" {
				root = appendStream(t, p, root, "signals", "new-signal", nil)
			}
			if mode == "full-slots" {
				for len(root.Readers) < MaxReaders {
					_, root, err = p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour))
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "lost-ack" {
				m.rootAfter = func() error { return lostReply }
			}
			next, ack, err := p.ReplaceReader(ctx, old, root.Head, func() time.Time { return epoch }, epoch.Add(3*time.Hour))
			if err != nil || ack.Head != root.Head+1 || len(ack.Readers) != len(root.Readers) || next.id == old.id || !reflect.DeepEqual(next.graph, root.Graph) || !reflect.DeepEqual(next.streams, root.Streams) {
				t.Fatal("invalid replacement", next, ack, err)
			}
			if _, err = p.ReadRetained(ctx, old, 0, epoch); !errors.Is(err, ErrRevoked) {
				t.Fatal("old reader survived", err)
			}
			if _, _, err = p.ResumeReader(ctx, checkpoint, epoch); !errors.Is(err, ErrRevoked) {
				t.Fatal("old checkpoint survived", err)
			}
			if _, err = p.ReadRetained(ctx, next, 0, epoch); err != nil {
				t.Fatal(err)
			}
			if mode == "named-streams" {
				if record, e := p.ReadRetainedStream(ctx, next, "signals", 0, epoch); e != nil || string(record.Data) != "new-signal" {
					t.Fatal(record, e)
				}
			}
			// The returned graph cannot mutate durable ownership.
			next.graph.Frontier[0].Link.Hash = "caller mutation"
			if m.roots["history"].Readers[0].Graph.Frontier[0].Link.Hash == "caller mutation" {
				t.Fatal("aliased pin")
			}
		})
	}
}

func TestGraphReplaceReaderFailClosed(t *testing.T) {
	for _, mode := range []string{"expired", "expiry-during-read", "shorter-expiry", "reused-id", "released", "unknown-read", "lost-ack-unknown-witness", "lost-ack-advanced-head", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root, old := readerFixture(t)
			now, expires := epoch, epoch.Add(3*time.Hour)
			want := ErrRevoked
			switch mode {
			case "expired":
				now = epoch.Add(2 * time.Hour)
			case "expiry-during-read":
				m.readRootHook = func(string) error { now = epoch.Add(2 * time.Hour); return nil }
			case "shorter-expiry":
				expires = epoch.Add(time.Hour)
				want = nil
			case "reused-id":
				p.NewID = func() (string, error) { return old.id, nil }
				want = nil
			case "released":
				var err error
				root, err = p.ReleaseReader(ctx, old, root.Head)
				if err != nil {
					t.Fatal(err)
				}
			case "unknown-read":
				m.readRootHook = func(string) error { return lostReply }
				want = lostReply
			case "lost-ack-unknown-witness":
				m.rootAfter = func() error { m.readRootHook = func(string) error { return lostReply }; return lostReply }
				want = lostReply
			case "lost-ack-advanced-head":
				m.rootAfter = func() error { r := m.roots["history"]; r.Head++; m.roots["history"] = r; return lostReply }
				want = lostReply
			case "conflict":
				m.rootBefore = func(_ string, expected uint64, _ Root) error {
					_, err := p.RenewReader(ctx, old, expected, epoch.Add(4*time.Hour))
					return err
				}
				want = ErrConflict
			}
			before, _ := json.Marshal(m.roots["history"])
			next, _, err := p.ReplaceReader(ctx, old, root.Head, func() time.Time { return now }, expires)
			if err == nil || next.id != "" || want != nil && !errors.Is(err, want) {
				t.Fatal("replacement accepted uncertainty", next, err)
			}
			if mode != "conflict" && mode != "lost-ack-unknown-witness" && mode != "lost-ack-advanced-head" {
				after, _ := json.Marshal(m.roots["history"])
				if string(before) != string(after) {
					t.Fatal("rejected replacement mutated root")
				}
			}
		})
	}
}
