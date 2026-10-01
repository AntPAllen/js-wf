//go:build linux

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestMatrixClockRoleWaitSurvivesTransientElectionReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	calls := 0
	info, err := waitMatrixClockRole(ctx, func(context.Context) (*jetstream.StreamInfo, error) {
		calls++
		if calls <= 4 {
			return nil, context.DeadlineExceeded
		}
		leader := "skewed"
		if calls == 6 {
			leader = "replacement"
		}
		return &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{Leader: leader}}, nil
	}, func(leader string) bool { return leader == "replacement" })
	if err != nil || calls != 6 || info.Cluster.Leader != "replacement" {
		t.Fatalf("calls=%d info=%+v err=%v", calls, info, err)
	}
}

func TestMatrixClockRoleWaitPreservesDeadlineAndPermanentErrors(t *testing.T) {
	permanent := errors.New("invalid stream configuration")
	calls := 0
	_, err := waitMatrixClockRole(context.Background(), func(context.Context) (*jetstream.StreamInfo, error) {
		calls++
		return nil, permanent
	}, func(string) bool { return true })
	if !errors.Is(err, permanent) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = waitMatrixClockRole(ctx, func(attempt context.Context) (*jetstream.StreamInfo, error) {
		<-attempt.Done()
		return nil, attempt.Err()
	}, func(string) bool { return true })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline err=%v", err)
	}
}
