//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type matrixTerminalCohortStream struct {
	jetstream.Stream
	first, last, missing, wrong uint64
	malformed                   bool
}

func (s matrixTerminalCohortStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: s.first, LastSeq: s.last}}, nil
}
func (s matrixTerminalCohortStream) GetMsg(_ context.Context, sequence uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if sequence == s.missing {
		return nil, jetstream.ErrMsgNotFound
	}
	subject := fmt.Sprintf("wf.inv.audit.id%d", sequence)
	if s.malformed {
		subject = "wf.jrn.audit.id"
	}
	if sequence == s.wrong {
		sequence--
	}
	return &jetstream.RawStreamMsg{Sequence: sequence, Subject: subject}, nil
}
func TestMatrixTerminalCohortUsesCapturedPopulation(t *testing.T) {
	for _, state := range [][2]uint64{{1, 812}, {0, 0}, {1, 868}} {
		visited := 0
		err := matrixVisitCompletedInvocations(context.Background(), matrixTerminalCohortStream{first: state[0], last: state[1]}, 840, 840, func(raw *jetstream.RawStreamMsg) error {
			visited++
			if raw.Sequence != uint64(visited) {
				return errors.New("out of order")
			}
			return nil
		})
		if err != nil || visited != 840 {
			t.Fatal(state, visited, err)
		}
	}
}
func TestMatrixTerminalCohortRejectsMissingAndInvalidInvocations(t *testing.T) {
	for _, stream := range []matrixTerminalCohortStream{{first: 1, last: 812, missing: 840}, {first: 1, last: 812, wrong: 823}, {first: 1, last: 812, malformed: true}, {first: 2, last: 840}} {
		if err := matrixVisitCompletedInvocations(context.Background(), stream, 840, 840, func(*jetstream.RawStreamMsg) error { return nil }); err == nil {
			t.Fatal("incomplete terminal population accepted", stream)
		}
	}
	sentinel := errors.New("latency audit failed")
	calls := 0
	err := matrixVisitCompletedInvocations(context.Background(), matrixTerminalCohortStream{first: 1, last: 840}, 840, 840, func(*jetstream.RawStreamMsg) error { calls++; return sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatal("visitor error lost", calls, err)
	}
}
