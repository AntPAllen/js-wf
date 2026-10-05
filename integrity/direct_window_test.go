package integrity

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDirectWindowSharedScannerMatchesBatchControls(t *testing.T) {
	visitorFailure := errors.New("visitor rejected retained record")
	for _, tc := range []struct {
		name       string
		seq        []uint64
		stream     string
		batchErr   error
		visitorErr error
		cutoff     *uint64
	}{
		{name: "complete", seq: []uint64{1, 2, 3, 4, 5, 6}, stream: "CONTROL"},
		{name: "cutoff", seq: []uint64{1, 2, 3, 4, 5, 6}, stream: "CONTROL", cutoff: func() *uint64 { x := uint64(4); return &x }()},
		{name: "gaps", seq: []uint64{1, 3, 6}, stream: "CONTROL"},
		{name: "wrong-stream", seq: []uint64{1, 2}, stream: "OTHER"},
		{name: "backward", seq: []uint64{1, 1}, stream: "CONTROL"},
		{name: "visitor-error", seq: []uint64{1, 2, 3}, stream: "CONTROL", visitorErr: visitorFailure},
		{name: "bounded-recovery", seq: []uint64{1, 2, 3}, stream: "CONTROL", batchErr: nats.ErrTimeout},
		{name: "semantic-delivery-error", seq: []uint64{1, 2}, stream: "CONTROL", batchErr: errors.New("semantic delivery failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reports [2][]uint64
			var failures [2]error
			var streams [2]*candidateControlStream
			for mode := range 2 {
				s := &candidateControlStream{consumer: candidateControlConsumer{sequences: tc.seq, stream: tc.stream, err: tc.batchErr}}
				streams[mode] = s
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				visit := func(m *jetstream.RawStreamMsg) error {
					reports[mode] = append(reports[mode], m.Sequence)
					return tc.visitorErr
				}
				if mode == 0 {
					failures[mode] = scanBatchThrough(ctx, s, tc.cutoff, visit)
				} else {
					failures[mode] = scanRetainedThroughWithWindow(ctx, s, tc.cutoff, visit, 512, func(_ context.Context, c jetstream.Consumer, _ int) (retainedMessageWindow, error) {
						control := c.(candidateControlConsumer)
						return func(accept func(jetstream.Msg)) error {
							for _, seq := range control.sequences {
								accept(candidateControlMsg{stream: control.stream, seq: seq})
							}
							return control.err
						}, nil
					}, nil, sdkMessageCoordinates)
				}
				cancel()
			}
			if !reflect.DeepEqual(reports[0], reports[1]) || errorText(failures[0]) != errorText(failures[1]) || !reflect.DeepEqual(streams[0].requests, streams[1].requests) || !reflect.DeepEqual(streams[0].createStarts, streams[1].createStarts) {
				t.Fatalf("batch/direct mismatch reports=%v failures=%v gaps=%v/%v resumes=%v/%v", reports, failures, streams[0].requests, streams[1].requests, streams[0].createStarts, streams[1].createStarts)
			}
			if tc.visitorErr != nil && (!errors.Is(failures[1], tc.visitorErr) || len(streams[1].createStarts) != 1 || len(streams[1].requests) != 0) {
				t.Fatal("visitor error weakened or retried")
			}
		})
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestDirectCallbackWindowsKeepCursorAndJoin(t *testing.T) {
	c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, count: 6}
	d, err := newCallbackDelivery(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var seen []uint64
	for range 2 {
		if err := walkCallbackWindow(ctx, 3, d, func(msg jetstream.Msg) {
			meta, e := msg.Metadata()
			if e != nil {
				t.Fatal(e)
			}
			seen = append(seen, meta.Sequence.Stream)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2, 3, 4, 5, 6}) {
		t.Fatal(seen)
	}
	if err := d.stopAndJoin(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDirectCallbackWindowCancellationAndTransportErrors(t *testing.T) {
	for _, kind := range []string{"cancel", "heartbeat", "semantic"} {
		t.Run(kind, func(t *testing.T) {
			c := &callbackTestConsumer{ctx: &callbackTestContext{requested: make(chan struct{}), closed: make(chan struct{})}, count: 6}
			d, err := newCallbackDelivery(c)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			failure := errors.New("callback transport semantic error")
			if kind == "heartbeat" {
				failure = jetstream.ErrNoHeartbeat
				d.errors <- failure
			}
			if kind == "semantic" {
				d.errors <- failure
			}
			count := 0
			err = walkCallbackWindow(ctx, 6, d, func(jetstream.Msg) {
				count++
				if kind == "cancel" {
					cancel()
				}
			})
			if kind == "cancel" {
				if !errors.Is(err, context.Canceled) || count != 1 {
					t.Fatalf("count=%d err=%v", count, err)
				}
			} else if !errors.Is(err, failure) || count != 0 {
				t.Fatalf("count=%d err=%v", count, err)
			}
			if kind == "heartbeat" && !errors.Is(err, nats.ErrTimeout) {
				t.Fatal("heartbeat recovery classification lost")
			}
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := d.stopAndJoin(cleanup); err != nil {
				t.Fatal(err)
			}
		})
	}
}
