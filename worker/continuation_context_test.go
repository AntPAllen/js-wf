package worker

import (
	"context"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
)

type contextOnlyGraphPort struct{ graphpublication.Port }
type contextOnlyCheckpointPort struct {
	journal.CompactionCheckpointPort
}

func TestContinuationPublicationContextOwnershipAndBounds(t *testing.T) {
	for _, mode := range []string{"legacy", "graph-ephemeral", "storage-without-archive", "durable"} {
		t.Run(mode, func(t *testing.T) {
			w := &Worker{}
			if mode != "legacy" {
				cfg := journal.GraphConfig{Protocol: graphpublication.Protocol{Port: contextOnlyGraphPort{}}, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: mode != "storage-without-archive"}
				if mode == "durable" || mode == "storage-without-archive" {
					cfg.CompactionCheckpoints = contextOnlyCheckpointPort{}
				}
				var e error
				w.graphJournal, e = journal.NewGraphStore(cfg)
				if e != nil {
					t.Fatal(e)
				}
			}
			for _, parentDeadline := range []bool{false, true} {
				parent, stopParent := context.WithCancel(context.Background())
				defer stopParent()
				if parentDeadline {
					var stop context.CancelFunc
					parent, stop = context.WithTimeout(parent, time.Hour)
					defer stop()
				}
				child, stop := w.continuationPublicationContext(parent)
				deadline, has := child.Deadline()
				if mode == "durable" {
					if parentDeadline {
						original, _ := parent.Deadline()
						if !has || !deadline.Equal(original) {
							t.Fatal("caller deadline changed", deadline, original)
						}
					} else if has {
						t.Fatal("durable whole deadline remains", deadline)
					}
				} else if !has || time.Until(deadline) > 15*time.Second || time.Until(deadline) < 14*time.Second {
					t.Fatal("legacy deadline changed", deadline, has)
				}
				stopParent()
				if child.Err() != context.Canceled {
					t.Fatal("parent cancellation detached", child.Err())
				}
				stop()
			}
			parent := context.Background()
			child, stop := w.continuationPublicationContext(parent)
			stop()
			if child.Err() != context.Canceled || parent.Err() != nil {
				t.Fatal("cleanup cancellation escaped", child.Err(), parent.Err())
			}
			expired, stopExpired := context.WithDeadline(parent, time.Now().Add(-time.Second))
			defer stopExpired()
			child, stop = w.continuationPublicationContext(expired)
			defer stop()
			if child.Err() != context.DeadlineExceeded {
				t.Fatal("expired caller revived", child.Err())
			}
		})
	}
}
