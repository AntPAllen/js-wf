package integrity

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type staleCohortStream struct {
	jetstream.Stream
	first, last uint64
}

func (s staleCohortStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: s.first, LastSeq: s.last}}, nil
}
func (s staleCohortStream) GetMsg(_ context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return &jetstream.RawStreamMsg{Sequence: seq}, nil
}
func TestCapturedCohortOverridesStalePositiveAndEmptyMetadata(t *testing.T) {
	cutoff := uint64(840)
	for _, state := range []jetstream.StreamState{{FirstSeq: 1, LastSeq: 812}, {FirstSeq: 0, LastSeq: 0}, {FirstSeq: 1, LastSeq: 868}} {
		var visited []uint64
		err := scanThrough(context.Background(), staleCohortStream{first: state.FirstSeq, last: state.LastSeq}, &cutoff, func(raw *jetstream.RawStreamMsg) error { visited = append(visited, raw.Sequence); return nil })
		if err != nil || len(visited) != 840 || visited[0] != 1 || visited[839] != 840 {
			t.Fatal("captured cohort shrunk", state, len(visited), err)
		}
	}
}
func TestCapturedCohortValidatesAcknowledgedWitness(t *testing.T) {
	for _, raw := range []*jetstream.RawStreamMsg{nil, {Subject: "wf.inv.audit.id", Sequence: 812}, {Subject: "wf.jrn.audit.id", Sequence: 840}} {
		_, err := CaptureInvocationCutoff(context.Background(), 822, func(context.Context) (*jetstream.RawStreamMsg, error) { return raw, nil })
		if err == nil {
			t.Fatal("invalid capture accepted", raw)
		}
	}
	cutoff, err := CaptureInvocationCutoff(context.Background(), 822, func(context.Context) (*jetstream.RawStreamMsg, error) {
		return &jetstream.RawStreamMsg{Subject: "wf.inv.audit.id", Sequence: 840}, nil
	})
	if err != nil || cutoff != 840 {
		t.Fatal(cutoff, err)
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if _, err = CaptureInvocationCutoff(ctx, 822, func(context.Context) (*jetstream.RawStreamMsg, error) {
		t.Fatal("lookup after cancellation")
		return nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestRetainedWindowUsesSameCapturedBoundary(t *testing.T) {
	cutoff := uint64(6)
	s := &candidateControlStream{last: 3}
	s.consumer = candidateControlConsumer{sequences: []uint64{1, 2, 3, 4, 5, 6}, stream: "CONTROL"}
	var visited []uint64
	err := scanBatchThrough(context.Background(), s, &cutoff, func(raw *jetstream.RawStreamMsg) error { visited = append(visited, raw.Sequence); return nil })
	if err != nil || !reflect.DeepEqual(visited, []uint64{1, 2, 3, 4, 5, 6}) {
		t.Fatal(visited, err)
	}
}
