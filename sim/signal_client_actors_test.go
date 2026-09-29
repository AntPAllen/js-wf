package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func runConcurrentClientSignalRepair(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("client_signal_scan_race"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewSignalTransport(schedule)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const typ, id = "test", "client-race"
	if _, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")}); err != nil {
		return trace, err
	}
	mode, err := schedule.Choose([]string{"normal", "lost_publish_ack", "lost_enqueue_ack", "drop_enqueue"})
	if err != nil {
		return trace, err
	}
	switch mode {
	case "lost_publish_ack":
		if err := model.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
			return trace, err
		}
	case "lost_enqueue_ack", "drop_enqueue":
		kind := "lose_ack_after_commit"
		if mode == "drop_enqueue" {
			kind = "drop_before_commit"
		}
		if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
			return trace, err
		}
	}
	var cursor uint64
	actors := []ClientSignalActor{
		{Name: "client", Run: func(ctx context.Context, start client.StartPort, signal client.SignalPort, _ reconcile.SignalScanPort) error {
			_, err := client.NewWithSignalPorts(start, signal).Signal(ctx, typ, id, "go", []byte("payload"), "same")
			return err
		}},
		{Name: "scanner", Run: func(ctx context.Context, _ client.StartPort, _ client.SignalPort, port reconcile.SignalScanPort) error {
			result, err := reconcile.NewSignalScanWithPort(port).Scan(ctx, 1, 1, false)
			cursor = result.NextSequence
			return err
		}},
	}
	results, err := RunClientSignalActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	clientErr, scannerErr := results["client"], results["scanner"]
	switch mode {
	case "normal":
		if clientErr != nil || scannerErr != nil {
			return trace, fmt.Errorf("normal actors=%v", results)
		}
	case "lost_publish_ack":
		if !errors.Is(clientErr, client.ErrSignalUnknown) || scannerErr != nil {
			return trace, fmt.Errorf("lost publish actors=%v", results)
		}
	default:
		if clientErr == nil && scannerErr == nil || clientErr != nil && !errors.Is(clientErr, client.ErrEnqueueUnknown) || scannerErr != nil && !errors.Is(scannerErr, ErrTransportLost) {
			return trace, fmt.Errorf("enqueue fault actors=%v", results)
		}
	}
	if cursor == 0 {
		return trace, fmt.Errorf("scanner returned zero cursor: %v", results)
	}
	scan := reconcile.NewSignalScanWithPort(model)
	for attempt := 0; attempt < 3; attempt++ {
		result, err := scan.Scan(ctx, cursor, 1, false)
		if err != nil {
			return trace, err
		}
		cursor = result.NextSequence
	}
	if signals, runs := model.SignalFor(typ, id, "go"), model.Runs(); len(signals) != 1 || len(runs) != 1 || string(runs[0].Data) != "test.client-race" {
		return trace, fmt.Errorf("mode=%s actors=%v signals=%v runs=%v", mode, results, signals, runs)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeClientSignalRepairReplay(t *testing.T) {
	if os.Getenv("SIM_CLIENT_SIGNAL_RACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runConcurrentClientSignalRepair(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CLIENT_SIGNAL_RACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]int{}
	shapes := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runConcurrentClientSignalRepair(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-client-signal-race-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-client-signal-race.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen]++
		shapes[signalRaceShape(generated)]++
		if seed <= 10 {
			replayed, err := runConcurrentClientSignalRepair(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d client signal race replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"normal", "lost_publish_ack", "lost_enqueue_ack", "drop_enqueue"} {
		if modes[mode] == 0 {
			t.Fatalf("seeded schedules missed mode %s: %+v", mode, modes)
		}
	}
	for _, shape := range []string{"publish_before_get", "publish_between_get_and_info", "publish_after_scan"} {
		if shapes[shape] == 0 {
			t.Fatalf("seeded schedules missed %s: %+v", shape, shapes)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("client-signal-race-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeClientSignalRepairReplay$")
		cmd.Env = append(os.Environ(), "SIM_CLIENT_SIGNAL_RACE_HELPER=1", "SIM_CLIENT_SIGNAL_RACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("client signal race child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("cooperative client signal trace changed across processes")
	}
}
