package integrity

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type retryScanStream struct {
	jetstream.Stream
	requests []uint64
	mu       sync.Mutex
	attempts map[uint64]int
}

func (s *retryScanStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 4}}, nil
}

func (s *retryScanStream) GetMsg(ctx context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Second {
		panic("unbounded audit request")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = make(map[uint64]int)
	}
	s.requests = append(s.requests, seq)
	s.attempts[seq]++
	if seq == 2 && s.attempts[seq] == 1 {
		return nil, nats.ErrTimeout
	}
	if seq == 3 {
		return nil, jetstream.ErrMsgNotFound
	}
	return &jetstream.RawStreamMsg{Sequence: seq}, nil
}

func TestScanRetriesCurrentSequence(t *testing.T) {
	stream := &retryScanStream{}
	var visited []uint64
	err := scan(context.Background(), stream, func(msg *jetstream.RawStreamMsg) error { visited = append(visited, msg.Sequence); return nil })
	if err != nil || !reflect.DeepEqual(visited, []uint64{1, 2, 4}) || !reflect.DeepEqual(stream.attempts, map[uint64]int{1: 1, 2: 2, 3: 1, 4: 1}) {
		t.Fatalf("visited=%v requests=%v err=%v", visited, stream.requests, err)
	}
}

func TestAuditReadBoundsRetriesAndPreservesErrors(t *testing.T) {
	for _, test := range []struct {
		err   error
		calls int
	}{
		{nats.ErrTimeout, 3}, {jetstream.ErrNoStreamResponse, 3},
		{&jetstream.APIError{ErrorCode: 10008}, 3},
		{jetstream.ErrMsgNotFound, 1}, {errors.New("invalid retained payload"), 1},
	} {
		calls := 0
		_, err := auditRead(context.Background(), func(context.Context) (int, error) { calls++; return 0, test.err })
		if !errors.Is(err, test.err) || calls != test.calls {
			t.Fatalf("err=%v calls=%d want=%d", err, calls, test.calls)
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	_, err := auditRead(ctx, func(context.Context) (int, error) { t.Fatal("read after cancellation"); return 0, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
