package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
)

func TestGraphReconcileLoopRetriesUncertaintyWithoutSkippingBoundary(t *testing.T) {
	for _, mode := range []string{"unknown", "conflict", "expired", "stale", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			port := &prefixLoopPort{stop: stop}
			cause := journal.ErrUnknown
			switch mode {
			case "conflict":
				cause = graphpublication.ErrConflict
			case "expired":
				cause = graphpublication.ErrRevoked
			case "stale":
				cause = journal.ErrStale
			case "corrupt":
				cause = journal.ErrGap
			}
			err := RunLoopWithPort(ctx, port, "graph-prefix", "start", time.Second, 500, func(attempt context.Context, _ uint64, _ int, _ bool) (ScanResult, error) {
				port.scanContext = attempt
				return ScanResult{RetrySequence: 3}, cause
			})
			fatal := mode == "stale" || mode == "corrupt"
			want := 1
			if fatal {
				want = 0
				if !errors.Is(err, cause) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if port.saves != want {
				t.Fatalf("cursor saves=%d want%d", port.saves, want)
			}
		})
	}
}
