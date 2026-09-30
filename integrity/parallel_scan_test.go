package integrity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type delayedAuditStream struct {
	jetstream.Stream
	mu           sync.Mutex
	active, peak int
	requests     map[uint64]int
	corrupt      uint64
	persistent   uint64
}

func (s *delayedAuditStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: 1, LastSeq: 512}}, nil
}
func (s *delayedAuditStream) GetMsg(ctx context.Context, sequence uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.peak {
		s.peak = s.active
	}
	if s.requests == nil {
		s.requests = make(map[uint64]int)
	}
	s.requests[sequence]++
	attempt := s.requests[sequence]
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	if sequence == s.persistent {
		return nil, fmt.Errorf("corrupt record")
	}
	if sequence == 55 && attempt == 1 {
		return nil, jetstream.ErrNoStreamResponse
	}
	if sequence%11 == 0 {
		return nil, jetstream.ErrMsgNotFound
	}
	if sequence == s.corrupt {
		return &jetstream.RawStreamMsg{Sequence: sequence + 1}, nil
	}
	return &jetstream.RawStreamMsg{Sequence: sequence}, nil
}

func TestAuditScanCompletesWithinDeadlineWithOrderedHolesAndRetry(t *testing.T) {
	stream := &delayedAuditStream{}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	var visited []uint64
	err := scan(ctx, stream, func(msg *jetstream.RawStreamMsg) error { visited = append(visited, msg.Sequence); return nil })
	var want []uint64
	for sequence := uint64(1); sequence <= 512; sequence++ {
		if sequence%11 != 0 {
			want = append(want, sequence)
		}
	}
	if err != nil || !reflect.DeepEqual(visited, want) {
		t.Fatalf("scan visited=%d want=%d err=%v", len(visited), len(want), err)
	}
	if stream.peak <= 1 || stream.peak > 32 || stream.active != 0 || stream.requests[55] != 2 {
		t.Fatalf("read window peak=%d active=%d retries=%d", stream.peak, stream.active, stream.requests[55])
	}
}

func TestAuditScanCutoffAndCorruption(t *testing.T) {
	stream := &delayedAuditStream{}
	cutoff := uint64(137)
	if err := scanThrough(context.Background(), stream, &cutoff, func(*jetstream.RawStreamMsg) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for sequence := range stream.requests {
		if sequence > cutoff {
			t.Fatalf("read past cutoff: %d", sequence)
		}
	}
	for _, test := range []struct {
		stream *delayedAuditStream
		name   string
	}{
		{&delayedAuditStream{corrupt: 47}, "wrong sequence"},
		{&delayedAuditStream{persistent: 47}, "semantic read error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var last uint64
			if err := scan(context.Background(), test.stream, func(msg *jetstream.RawStreamMsg) error { last = msg.Sequence; return nil }); err == nil || last >= 47 {
				t.Fatalf("corruption ignored or visited beyond it: last=%d err=%v", last, err)
			}
			if test.stream.requests[47] != 1 || test.stream.active != 0 {
				t.Fatalf("semantic error retried or reader leaked: %+v", test.stream.requests)
			}
		})
	}
	sentinel := errors.New("invariant failed")
	if err := scan(context.Background(), &delayedAuditStream{}, func(*jetstream.RawStreamMsg) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("lost invariant error: %v", err)
	}
}
