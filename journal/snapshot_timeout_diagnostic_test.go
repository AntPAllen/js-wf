package journal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestSnapshotObjectDeadlinePreservesObservedCause(t *testing.T) {
	for _, prior := range []bool{false, true} {
		t.Run(map[bool]string{false: "first_lookup", true: "prior_missing_object"}[prior], func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			calls := 0
			_, err := verifiedSnapshotObject(ctx, func(attempt context.Context, _ string) ([]byte, error) {
				calls++
				if prior && calls == 1 {
					return nil, jetstream.ErrObjectNotFound
				}
				<-attempt.Done()
				return nil, attempt.Err()
			}, "snapshot-probe", "unused")
			var gap *snapshotObjectGap
			if !errors.As(err, &gap) || !errors.Is(err, ErrGap) || ctx.Err() != nil {
				t.Fatalf("failure classification changed: calls=%d err=%v outer=%v", calls, err, ctx.Err())
			}
			want, wantCalls := context.DeadlineExceeded.Error(), 1
			if prior {
				want, wantCalls = jetstream.ErrObjectNotFound.Error(), 2
			}
			if calls != wantCalls || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "<nil>") {
				t.Fatalf("observed cause lost: calls=%d err=%v want=%s", calls, err, want)
			}
		})
	}
}
