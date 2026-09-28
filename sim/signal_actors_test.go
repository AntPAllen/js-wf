package sim

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func runConcurrentSignalRepair(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("signal_publish_scan_race"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewSignalTransport(schedule)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	const typ, id = "test", "race"
	generation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")})
	if err != nil {
		return trace, err
	}
	message := &nats.Msg{Subject: "wf.sig.test.race.go", Data: []byte("payload"), Header: nats.Header{}}
	message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
	var published, cursor uint64
	actors := []SignalActor{
		{Name: "publisher", Run: func(ctx context.Context, yield YieldFunc, _ reconcile.SignalScanPort) error {
			return yield(ctx, "publish_signal", func() { published = model.PublishSignal(message) })
		}},
		{Name: "scanner", Run: func(ctx context.Context, _ YieldFunc, port reconcile.SignalScanPort) error {
			result, err := reconcile.NewSignalScanWithPort(port).Scan(ctx, 1, 1, false)
			cursor = result.NextSequence
			return err
		}},
	}
	results, err := RunSignalActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	if results["publisher"] != nil || results["scanner"] != nil || published != 1 || cursor == 0 {
		return trace, fmt.Errorf("actors=%v published=%d cursor=%d", results, published, cursor)
	}
	scan := reconcile.NewSignalScanWithPort(model)
	for attempt := 0; attempt < 3; attempt++ {
		result, err := scan.Scan(ctx, cursor, 1, false)
		if err != nil {
			return trace, err
		}
		cursor = result.NextSequence
	}
	if runs := model.Runs(); len(runs) != 1 || string(runs[0].Data) != "test.race" {
		return trace, fmt.Errorf("signal repair after interleaving retained %+v", runs)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func signalRaceShape(trace Trace) string {
	var published, get, info int
	for i, event := range trace.Transport {
		switch event.Operation {
		case "publish_signal":
			if published == 0 {
				published = i + 1
			}
		case "get_signal":
			if get == 0 {
				get = i + 1
			}
		case "signal_stream_info":
			if info == 0 {
				info = i + 1
			}
		}
	}
	if published < get {
		return "publish_before_get"
	}
	if info != 0 && published < info {
		return "publish_between_get_and_info"
	}
	return "publish_after_scan"
}

func TestCooperativeSignalPublishScanReplay(t *testing.T) {
	if os.Getenv("SIM_SIGNAL_RACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runConcurrentSignalRepair(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SIGNAL_RACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	shapes := map[string]int{}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runConcurrentSignalRepair(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-signal-race-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-signal-race.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		shapes[signalRaceShape(generated)]++
		if seed <= 10 {
			replayed, err := runConcurrentSignalRepair(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d signal race replay: %v", seed, err)
			}
		}
	}
	for _, shape := range []string{"publish_before_get", "publish_between_get_and_info", "publish_after_scan"} {
		if shapes[shape] == 0 {
			t.Fatalf("seeded schedules missed %s: %+v", shape, shapes)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("signal-race-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeSignalPublishScanReplay$")
		cmd.Env = append(os.Environ(), "SIM_SIGNAL_RACE_HELPER=1", "SIM_SIGNAL_RACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("signal race child %d: %v: %s", i, err, output)
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
		t.Fatal("cooperative signal trace changed across processes")
	}
}
