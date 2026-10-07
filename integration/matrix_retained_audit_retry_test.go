//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
)

func TestMatrixRetainedAuditTransientCadence(t *testing.T) {
	start := time.Unix(0, 0)
	now := start
	var attempts []time.Duration
	var waits []time.Duration
	expected := integrity.Report{Invocations: 1680, Journals: 1680, Terminal: 1680, Entries: 18480}
	unavailable := errors.New("nats: JetStream system temporarily unavailable")
	report, err := matrixRetainedAuditWithClock(context.Background(), func(ctx context.Context) (integrity.Report, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 20*time.Second {
			t.Fatal("unbounded audit request")
		}
		attempts = append(attempts, now.Sub(start))
		if now.Sub(start) < 19*time.Second {
			now = now.Add(800 * time.Millisecond)
			return integrity.Report{}, fmt.Errorf("open retained store: %w", unavailable)
		}
		return expected, nil
	}, func() time.Time { return now }, func(ctx context.Context, d time.Duration) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 60*time.Second {
			t.Fatal("retry exceeded original sixty-second maximum")
		}
		waits = append(waits, d)
		now = now.Add(d)
		return nil
	})
	if err != nil || report != expected {
		t.Fatalf("audit before recovery report=%+v err=%v attempts=%v", report, err, attempts)
	}
	if !reflect.DeepEqual(attempts, []time.Duration{0, 10 * time.Second, 20 * time.Second}) || !reflect.DeepEqual(waits, []time.Duration{9200 * time.Millisecond, 9200 * time.Millisecond}) {
		t.Fatalf("attempts=%v waits=%v", attempts, waits)
	}
}
func TestMatrixRetainedAuditStopsOnInvariantOrExhaustion(t *testing.T) {
	invariant := errors.New("retained journal invariant violation")
	unavailable := &jetstream.APIError{Code: 503, ErrorCode: 10008}
	for _, failure := range []error{invariant, unavailable} {
		calls, waits := 0, 0
		now := time.Unix(0, 0)
		_, err := matrixRetainedAuditWithClock(context.Background(), func(context.Context) (integrity.Report, error) { calls++; return integrity.Report{}, failure }, func() time.Time { return now }, func(_ context.Context, d time.Duration) error { waits++; now = now.Add(d); return nil })
		wantCalls, wantWaits := 3, 2
		if failure == invariant {
			wantCalls, wantWaits = 1, 0
		}
		if !errors.Is(err, failure) || calls != wantCalls || waits != wantWaits {
			t.Fatalf("failure=%v err=%v calls=%d waits=%d", failure, err, calls, waits)
		}
	}
}
func TestMatrixRetainedAuditHonorsCancellationDuringWait(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	calls := 0
	_, err := matrixRetainedAuditWithClock(ctx, func(context.Context) (integrity.Report, error) {
		calls++
		return integrity.Report{}, &jetstream.APIError{Code: 503, ErrorCode: 10008}
	}, time.Now, func(ctx context.Context, _ time.Duration) error { stop(); <-ctx.Done(); return ctx.Err() })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestMatrixRetainedAuditRejectsConflictingReaders(t *testing.T) {
	names := []string{"WF_TIER3_BATCHED_RETAINED_AUDIT", "WF_TIER3_STREAMING_STATE_RETAINED_AUDIT", "WF_TIER3_CONCURRENT_STATE_RETAINED_AUDIT", "WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT", "WF_TIER3_PARALLEL_STATE_RETAINED_AUDIT"}
	for _, pair := range [][2]int{{0, 1}, {0, 2}, {1, 2}, {0, 3}, {1, 3}, {2, 3}, {0, 4}, {1, 4}, {2, 4}, {3, 4}} {
		t.Run(fmt.Sprintf("%d-%d", pair[0], pair[1]), func(t *testing.T) {
			for _, name := range names {
				t.Setenv(name, "0")
			}
			t.Setenv(names[pair[0]], "1")
			t.Setenv(names[pair[1]], "1")
			cutoff := uint64(1)
			for _, cut := range []*uint64{nil, &cutoff} {
				report, err := matrixRetainedCheck(context.Background(), nil, cut)
				if err == nil || err.Error() != "conflicting retained audit modes" || report != (integrity.Report{}) {
					t.Fatalf("report=%+v err=%v", report, err)
				}
			}
		})
	}
}
