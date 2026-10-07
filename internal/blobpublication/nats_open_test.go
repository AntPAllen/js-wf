package blobpublication

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"testing"
	"time"
)

type scriptedAuthorityJS struct {
	jetstream.JetStream
	calls     int
	failures  int
	missing   bool
	cancel    context.CancelFunc
	deadlines []time.Time
}

func (j *scriptedAuthorityJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	j.calls++
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("unbounded admission")
	}
	j.deadlines = append(j.deadlines, deadline)
	if j.cancel != nil {
		j.cancel()
		return nil, context.DeadlineExceeded
	}
	if j.missing {
		return nil, jetstream.ErrStreamNotFound
	}
	if j.calls <= j.failures {
		return nil, context.DeadlineExceeded
	}
	return &scriptedAuthorityStream{}, nil
}

type scriptedAuthorityStream struct{ jetstream.Stream }

func (*scriptedAuthorityStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{Config: AuthorityStreamConfig("AUTH", "wf.auth", 1)}, nil
}
func TestNativeAuthorityOpeningBoundedReadRetries(t *testing.T) {
	for _, failures := range []int{0, 1, 2, 3} {
		t.Run(string(rune('0'+failures)), func(t *testing.T) {
			parent, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			js := &scriptedAuthorityJS{failures: failures}
			started := time.Now()
			_, err := OpenNativeAuthority(parent, js, "AUTH", "wf.auth")
			if failures < 3 && err != nil || failures == 3 && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			expected := failures + 1
			if expected > 3 {
				expected = 3
			}
			if js.calls != expected {
				t.Fatal("wrong admission bound", js.calls)
			}
			for _, deadline := range js.deadlines {
				if deadline.After(started.Add(2*time.Second + time.Second/10)) {
					t.Fatal("lookup inherited full parent budget")
				}
			}
			if parent.Err() != nil {
				t.Fatal("local failure canceled parent")
			}
		})
	}
}
func TestNativeAuthorityOpeningHonorsParentAndMissingStream(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	js := &scriptedAuthorityJS{cancel: cancel}
	if _, err := OpenNativeAuthority(parent, js, "AUTH", "wf.auth"); !errors.Is(err, context.Canceled) || js.calls != 1 {
		t.Fatal("parent cancellation retried", err, js.calls)
	}
	missing := &scriptedAuthorityJS{missing: true}
	if _, err := OpenNativeAuthority(context.Background(), missing, "AUTH", "wf.auth"); !errors.Is(err, jetstream.ErrStreamNotFound) || missing.calls != 1 {
		t.Fatal("missing stream retried", err, missing.calls)
	}
}
