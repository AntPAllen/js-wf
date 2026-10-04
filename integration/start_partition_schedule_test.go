package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPartitionStartFaultScheduleTracksWorkload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	ticks := make(chan time.Time)
	applied := make(chan bool)
	result := make(chan error, 1)
	go func() {
		result <- runPartitionStartFaults(ctx, done, ticks, func(ctx context.Context, partitioned bool) error {
			select {
			case applied <- partitioned:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	// More than twelve ticks must execute while producers still have work.
	for i := 0; i < 16; i++ {
		select {
		case ticks <- time.Unix(int64(i), 0):
		case err := <-result:
			t.Fatalf("fault schedule stopped before workload completion at tick %d: %v", i, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case partitioned := <-applied:
			if partitioned != (i%2 == 1) {
				t.Fatalf("tick %d partitioned=%v", i, partitioned)
			}
		case err := <-result:
			t.Fatalf("fault schedule stopped before workload completion at tick %d: %v", i, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(done)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestPartitionStartFaultSchedulePreservesFailure(t *testing.T) {
	expected := errors.New("route cut failed")
	ticks := make(chan time.Time, 1)
	ticks <- time.Time{}
	err := runPartitionStartFaults(context.Background(), make(chan struct{}), ticks, func(context.Context, bool) error { return expected })
	if !errors.Is(err, expected) {
		t.Fatalf("route cause lost: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = runPartitionStartFaults(ctx, make(chan struct{}), nil, func(context.Context, bool) error { t.Fatal("called after cancellation"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	closed := make(chan time.Time)
	close(closed)
	if err := runPartitionStartFaults(context.Background(), make(chan struct{}), closed, func(context.Context, bool) error { return nil }); err == nil {
		t.Fatal("closed ticks silently shortened fault exposure")
	}
}
