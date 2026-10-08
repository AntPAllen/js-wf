package graphpublication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type graphAdmissionJS struct {
	jetstream.JetStream
	stage           string
	failures, calls int
	deadlines       []time.Time
	cancel          context.CancelFunc
}

func (s *graphAdmissionJS) step(c context.Context, stage string) error {
	deadline, ok := c.Deadline()
	if !ok {
		return errors.New("unbounded admission")
	}
	s.deadlines = append(s.deadlines, deadline)
	if s.stage != stage {
		return nil
	}
	s.calls++
	if s.cancel != nil {
		s.cancel()
		return context.DeadlineExceeded
	}
	if s.failures < 0 {
		return jetstream.ErrStreamNotFound
	}
	if s.calls <= s.failures {
		return context.DeadlineExceeded
	}
	return nil
}
func (s *graphAdmissionJS) Stream(c context.Context, name string) (jetstream.Stream, error) {
	if err := s.step(c, "stream"); err != nil {
		return nil, err
	}
	return &graphAdmissionStream{owner: s}, nil
}
func (s *graphAdmissionJS) ObjectStore(c context.Context, bucket string) (jetstream.ObjectStore, error) {
	if err := s.step(c, "reader"); err != nil {
		return nil, err
	}
	return nil, nil
}

type graphAdmissionStream struct {
	jetstream.Stream
	owner *graphAdmissionJS
}

func (s *graphAdmissionStream) Info(c context.Context, o ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	if err := s.owner.step(c, "validate"); err != nil {
		return nil, err
	}
	return &jetstream.StreamInfo{Config: NativeObjectStreamConfig("GRAPH", 1)}, nil
}
func TestNativeGraphObjectOpeningBoundsReadOnlyAdmission(t *testing.T) {
	for _, stage := range []string{"stream", "validate", "reader"} {
		for failures := 0; failures <= 3; failures++ {
			t.Run(stage+string(rune('0'+failures)), func(t *testing.T) {
				parent, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				start := time.Now()
				js := &graphAdmissionJS{stage: stage, failures: failures}
				a := &NativeAuthority{js: js}
				_, err := OpenNativePort(parent, a, "GRAPH")
				if failures < 3 && err != nil || failures == 3 && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				expected := failures + 1
				if expected > 3 {
					expected = 3
				}
				if js.calls != expected {
					t.Fatal("wrong attempt bound", js.calls)
				}
				for _, deadline := range js.deadlines {
					if deadline.After(start.Add(2100 * time.Millisecond)) {
						t.Fatal("lookup inherited full parent budget")
					}
				}
				if parent.Err() != nil {
					t.Fatal("lookup canceled parent")
				}
			})
		}
	}
	for _, stage := range []string{"stream", "validate", "reader"} {
		js := &graphAdmissionJS{stage: stage, failures: -1}
		if _, err := OpenNativePort(context.Background(), &NativeAuthority{js: js}, "GRAPH"); !errors.Is(err, jetstream.ErrStreamNotFound) || js.calls != 1 {
			t.Fatal("missing storage retried", stage, err, js.calls)
		}
		parent, cancel := context.WithCancel(context.Background())
		js = &graphAdmissionJS{stage: stage, cancel: cancel}
		if _, err := OpenNativePort(parent, &NativeAuthority{js: js}, "GRAPH"); !errors.Is(err, context.Canceled) || js.calls != 1 {
			t.Fatal("canceled parent retried", stage, err, js.calls)
		}
	}
}
