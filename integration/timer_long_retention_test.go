package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// This checks retention of a real 30-day workflow schedule across complete
// cluster restarts. Firing it still requires a server clock that can advance.
func TestThirtyDayTimerRetainedAcrossTwoFullRestarts(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "long-timer", "thirty-days"
	const duration = 30 * 24 * time.Hour
	part := identity.Partition(typ, id, provision.Partitions)
	w, err := worker.New(ctx, all[1], "long-timer-before-restarts", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "long", duration); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, part) }()
	workerStopped := false
	defer func() {
		if !workerStopped {
			stopWorker()
			if err := <-workerDone; err != nil {
				t.Errorf("timer worker: %v", err)
			}
		}
	}()
	if _, err := client.New(all[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	var fireAt time.Time
	for ctx.Err() == nil {
		records, _, err := journal.New(all[2]).Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) == 3 && records[0].Kind == journal.Started && records[1].Kind == journal.StepRequested && records[2].Kind == journal.Suspended {
			var request struct {
				Kind   string    `json:"kind"`
				FireAt time.Time `json:"fire_at"`
			}
			if err := json.Unmarshal(records[1].Payload, &request); err != nil || request.Kind != "timer" {
				t.Fatalf("timer journal request=%s err=%v", records[1].Payload, err)
			}
			fireAt = request.FireAt
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	remaining := time.Until(fireAt)
	if fireAt.IsZero() || remaining < duration-5*time.Second || remaining > duration+5*time.Second {
		t.Fatalf("30-day timer fire_at=%s, remaining=%s", fireAt, time.Until(fireAt))
	}
	stopWorker()
	workerErr := <-workerDone
	workerStopped = true
	if workerErr != nil {
		t.Fatal(workerErr)
	}
	source := fmt.Sprintf("wf.schedule.%s.%s.%d", typ, id, 0)
	target := identity.RunSubject(typ, id, provision.Partitions)
	for index, js := range all {
		if err := checkLongSchedule(ctx, js, source, target, fireAt); err != nil {
			t.Fatalf("node %d before restart: %v", index, err)
		}
	}
	for restart := 0; restart < 2; restart++ {
		for index := range cluster.Servers {
			cluster.KillNode(index)
		}
		for index := range cluster.Servers {
			if err := cluster.RestartNode(index); err != nil {
				t.Fatalf("restart %d node %d: %v", restart+1, index, err)
			}
		}
		all = make([]jetstream.JetStream, len(cluster.Clients))
		for index, nc := range cluster.Clients {
			all[index], err = jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
		}
		for index, js := range all {
			var lastErr error
			for ctx.Err() == nil {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				// A fresh context is needed because cluster leadership settles
				// after the TCP clients reconnect.
				lastErr = checkLongSchedule(attempt, js, source, target, fireAt)
				stop()
				if lastErr == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if lastErr != nil {
				t.Fatalf("node %d after restart %d: %v (ctx=%v)", index, restart+1, lastErr, ctx.Err())
			}
		}
	}
	t.Logf("30-day schedule readable through all three nodes after two full cluster restarts; fire_at=%s", fireAt.Format(time.RFC3339Nano))
}

func checkLongSchedule(ctx context.Context, js jetstream.JetStream, source, target string, fireAt time.Time) error {
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		return err
	}
	info, err := run.Info(ctx)
	if err != nil {
		return err
	}
	if info.Config.MaxAge != 0 || !info.Config.AllowMsgSchedules || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2 {
		return fmt.Errorf("long schedule stream config or replicas: %+v", info)
	}
	stored, err := run.GetLastMsgForSubject(ctx, source)
	if err != nil {
		return fmt.Errorf("schedule source: %w", err)
	}
	if got := stored.Header.Get(jetstream.ScheduleHeader); got != "@at "+fireAt.UTC().Format(time.RFC3339Nano) {
		return fmt.Errorf("schedule header=%q", got)
	}
	if got := stored.Header.Get(jetstream.ScheduleTargetHeader); got != target {
		return fmt.Errorf("schedule target=%q", got)
	}
	if _, err := run.GetLastMsgForSubject(ctx, target); !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("30-day timer fired early or target read failed: %v", err)
	}
	return nil
}
